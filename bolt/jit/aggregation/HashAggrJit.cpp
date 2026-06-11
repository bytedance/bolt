#ifdef ENABLE_BOLT_JIT

#include "bolt/jit/aggregation/HashAggrJit.h"

#include <glog/logging.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Support/raw_ostream.h>
#include <cstddef>

#include <cmath>
#include <sstream>

#include <fmt/format.h>

#include "bolt/common/base/Exceptions.h"
#include "bolt/jit/ThrustJITv2.h"

extern "C" {

using bytedance::bolt::jit::HashAggrJitDecodedInput;
using bytedance::bolt::jit::HashAggrJitOutput;

namespace {

void logHashAggrJitFunctionIR(
    const llvm::Module& module,
    const std::string& moduleKey,
    llvm::StringRef functionName,
    llvm::StringRef stage,
    bool hasError) {
  if (!VLOG_IS_ON(1)) {
    return;
  }
  const auto* function = module.getFunction(functionName);
  if (function == nullptr) {
    VLOG(1) << "HashAggrJit generated LLVM IR for chunk " << moduleKey
            << " stage=" << stage.str() << " function=" << functionName.str()
            << " error=" << hasError << ": <missing function>";
    return;
  }
  std::string ir;
  llvm::raw_string_ostream out(ir);
  function->print(out);
  out.flush();
  VLOG(1) << "HashAggrJit generated LLVM IR for chunk " << moduleKey
          << " stage=" << stage.str() << " function=" << functionName.str()
          << " error=" << hasError << ":\n"
          << ir;
}

constexpr uint64_t kDecodedInputIndicesOffset =
    offsetof(HashAggrJitDecodedInput, indices);
constexpr uint64_t kDecodedInputNullsOffset =
    offsetof(HashAggrJitDecodedInput, nulls);
constexpr uint64_t kDecodedInputDecodedVectorOffset =
    offsetof(HashAggrJitDecodedInput, decodedVector);
constexpr uint64_t kDecodedInputFirstRowFieldOffset =
    offsetof(HashAggrJitDecodedInput, rowField0Values);
constexpr uint64_t kDecodedInputRowFieldNullsOffsetDelta =
    offsetof(HashAggrJitDecodedInput, rowField0Nulls) -
    offsetof(HashAggrJitDecodedInput, rowField0Values);
constexpr uint64_t kDecodedInputRowFieldStride =
    offsetof(HashAggrJitDecodedInput, rowField1Values) -
    offsetof(HashAggrJitDecodedInput, rowField0Values);

constexpr uint64_t kOutputNullsOffset = offsetof(HashAggrJitOutput, nulls);
constexpr uint64_t kOutputVectorOffset = offsetof(HashAggrJitOutput, vector);
constexpr uint64_t kOutputFirstRowFieldOffset =
    offsetof(HashAggrJitOutput, rowField0Values);
constexpr uint64_t kOutputRowFieldNullsOffsetDelta =
    offsetof(HashAggrJitOutput, rowField0Nulls) -
    offsetof(HashAggrJitOutput, rowField0Values);
constexpr uint64_t kOutputRowFieldStride =
    offsetof(HashAggrJitOutput, rowField1Values) -
    offsetof(HashAggrJitOutput, rowField0Values);

} // namespace

// Link anchor: the JIT extract/output runtime helpers live in separate
// translation units (HashAggrRuntime.cpp / HashAggrDecimalRuntime.cpp) and are
// only ever looked up by name through the ORC JIT global symbol table, never
// referenced at C++ link time. Without an explicit reference the linker would
// drop those objects from the final executable and the JIT would fail to
// resolve the symbols. Referencing one symbol per object forces the whole
// object (and thus every helper it defines) to be retained. This TU is always
// pulled in by any JIT user (HashAggrJitChunk), so the anchor propagates.
void jit_HashAggrResizeVector(char* vector, int32_t size);
void jit_HashAggrExtractFinalDecimalSum(
    char* vector,
    int32_t row,
    char* group,
    int32_t offset,
    int32_t precision,
    int32_t scale,
    int8_t longDecimal);

[[maybe_unused]] __attribute__((used)) const void* const
    kHashAggrRuntimeLinkAnchors[] = {
        reinterpret_cast<const void*>(&jit_HashAggrResizeVector),
        reinterpret_cast<const void*>(&jit_HashAggrExtractFinalDecimalSum)};

} // extern "C"

namespace bytedance::bolt::jit {
namespace {

llvm::FunctionCallee declareFunction(
    llvm::Module& module,
    llvm::StringRef name,
    llvm::Type* returnType,
    llvm::ArrayRef<llvm::Type*> argTypes) {
  return module.getOrInsertFunction(
      name, llvm::FunctionType::get(returnType, argTypes, false));
}

void ensureBuiltinDeclarations(llvm::Module& module) {
  auto& context = module.getContext();
  auto* i8Ty = llvm::Type::getInt8Ty(context);
  auto* i16Ty = llvm::Type::getInt16Ty(context);
  auto* i32Ty = llvm::Type::getInt32Ty(context);
  auto* i64Ty = llvm::Type::getInt64Ty(context);
  auto* i128Ty = llvm::Type::getInt128Ty(context);
  auto* floatTy = llvm::Type::getFloatTy(context);
  auto* doubleTy = llvm::Type::getDoubleTy(context);
  auto* voidTy = llvm::Type::getVoidTy(context);
  auto* i8PtrTy = llvm::PointerType::get(context, 0);

  declareFunction(module, "jit_GetDecodedValueBool", i8Ty, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_GetDecodedValueI8", i8Ty, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_GetDecodedValueI16", i16Ty, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_GetDecodedValueI32", i32Ty, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_GetDecodedValueI64", i64Ty, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_GetDecodedValueI128", i128Ty, {i8PtrTy, i32Ty});
  declareFunction(
      module, "jit_GetDecodedValueFloat", floatTy, {i8PtrTy, i32Ty});
  declareFunction(
      module, "jit_GetDecodedValueDouble", doubleTy, {i8PtrTy, i32Ty});
  declareFunction(
      module, "jit_GetDecodedRowFieldDouble", doubleTy, {i8PtrTy, i32Ty, i32Ty});
  declareFunction(
      module, "jit_GetDecodedRowFieldI8", i8Ty, {i8PtrTy, i32Ty, i32Ty});
  declareFunction(
      module, "jit_GetDecodedRowFieldI64", i64Ty, {i8PtrTy, i32Ty, i32Ty});
  declareFunction(
      module, "jit_GetDecodedRowFieldI128", i128Ty, {i8PtrTy, i32Ty, i32Ty});
  declareFunction(
      module, "jit_GetDecodedRowFieldIsNull", i8Ty, {i8PtrTy, i32Ty, i32Ty});
  declareFunction(module, "jit_GetDecodedIsNull", i8Ty, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_HashAggrResizeVector", voidTy, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_HashAggrSetFlatI8", voidTy, {i8PtrTy, i32Ty, i8Ty, i8Ty});
  declareFunction(module, "jit_HashAggrSetFlatI16", voidTy, {i8PtrTy, i32Ty, i16Ty, i8Ty});
  declareFunction(module, "jit_HashAggrSetFlatI32", voidTy, {i8PtrTy, i32Ty, i32Ty, i8Ty});
  declareFunction(module, "jit_HashAggrSetFlatI64", voidTy, {i8PtrTy, i32Ty, i64Ty, i8Ty});
  declareFunction(module, "jit_HashAggrSetFlatFloat", voidTy, {i8PtrTy, i32Ty, floatTy, i8Ty});
  declareFunction(module, "jit_HashAggrSetFlatDouble", voidTy, {i8PtrTy, i32Ty, doubleTy, i8Ty});
  // Decimal extract helpers: (vector, row, group, offset, precision, scale,
  // longDecimal).
  declareFunction(
      module,
      "jit_HashAggrExtractFinalDecimalSum",
      voidTy,
      {i8PtrTy, i32Ty, i8PtrTy, i32Ty, i32Ty, i32Ty, i8Ty});
  declareFunction(
      module,
      "jit_HashAggrExtractPartialDecimalSum",
      voidTy,
      {i8PtrTy, i32Ty, i8PtrTy, i32Ty, i32Ty, i32Ty, i8Ty});
  declareFunction(
      module,
      "jit_HashAggrExtractFinalDecimalAvg",
      voidTy,
      {i8PtrTy, i32Ty, i8PtrTy, i32Ty, i32Ty, i32Ty, i8Ty});
  declareFunction(
      module,
      "jit_HashAggrExtractPartialDecimalAvg",
      voidTy,
      {i8PtrTy, i32Ty, i8PtrTy, i32Ty, i32Ty, i32Ty, i8Ty});
}

llvm::Type* llvmType(llvm::IRBuilder<>& builder, HashAggrJitValueKind kind) {
  switch (kind) {
    case HashAggrJitValueKind::Bool:
    case HashAggrJitValueKind::Int8:
      return builder.getInt8Ty();
    case HashAggrJitValueKind::Int16:
      return builder.getInt16Ty();
    case HashAggrJitValueKind::Int32:
      return builder.getInt32Ty();
    case HashAggrJitValueKind::Int64:
      return builder.getInt64Ty();
    case HashAggrJitValueKind::Int128:
      return builder.getInt128Ty();
    case HashAggrJitValueKind::Float:
      return builder.getFloatTy();
    case HashAggrJitValueKind::Double:
      return builder.getDoubleTy();
  }
  return builder.getInt64Ty();
}

std::string decodedValueFunction(HashAggrJitValueKind kind) {
  switch (kind) {
    case HashAggrJitValueKind::Bool:
      return "jit_GetDecodedValueBool";
    case HashAggrJitValueKind::Int8:
      return "jit_GetDecodedValueI8";
    case HashAggrJitValueKind::Int16:
      return "jit_GetDecodedValueI16";
    case HashAggrJitValueKind::Int32:
      return "jit_GetDecodedValueI32";
    case HashAggrJitValueKind::Int64:
      return "jit_GetDecodedValueI64";
    case HashAggrJitValueKind::Int128:
      return "jit_GetDecodedValueI128";
    case HashAggrJitValueKind::Float:
      return "jit_GetDecodedValueFloat";
    case HashAggrJitValueKind::Double:
      return "jit_GetDecodedValueDouble";
  }
  return "jit_GetDecodedValueI64";
}

std::string decodedRowFieldFunction(HashAggrJitValueKind kind) {
  switch (kind) {
    case HashAggrJitValueKind::Bool:
    case HashAggrJitValueKind::Int8:
      return "jit_GetDecodedRowFieldI8";
    case HashAggrJitValueKind::Int64:
      return "jit_GetDecodedRowFieldI64";
    case HashAggrJitValueKind::Int128:
      return "jit_GetDecodedRowFieldI128";
    case HashAggrJitValueKind::Double:
      return "jit_GetDecodedRowFieldDouble";
    case HashAggrJitValueKind::Int16:
    case HashAggrJitValueKind::Int32:
    case HashAggrJitValueKind::Float:
      break;
  }
  return "";
}

std::string setFlatValueFunction(HashAggrJitValueKind kind);

bool isFloatKind(HashAggrJitValueKind kind) {
  return kind == HashAggrJitValueKind::Float ||
      kind == HashAggrJitValueKind::Double;
}

bool supportsRawFlatOutput(HashAggrJitValueKind kind) {
  switch (kind) {
    case HashAggrJitValueKind::Int8:
    case HashAggrJitValueKind::Int16:
    case HashAggrJitValueKind::Int32:
    case HashAggrJitValueKind::Int64:
    case HashAggrJitValueKind::Float:
    case HashAggrJitValueKind::Double:
      return true;
    case HashAggrJitValueKind::Bool:
    case HashAggrJitValueKind::Int128:
      return false;
  }
  return false;
}

llvm::Value* loadOutputValues(llvm::IRBuilder<>& builder, llvm::Value* output) {
  auto* i8PtrTy = llvm::PointerType::get(builder.getContext(), 0);
  auto* valuesPtrPtr = builder.CreatePointerCast(output, i8PtrTy->getPointerTo());
  return builder.CreateLoad(i8PtrTy, valuesPtrPtr, "output_values");
}

llvm::Value* loadOutputNulls(llvm::IRBuilder<>& builder, llvm::Value* output) {
  auto* i64Ty = builder.getInt64Ty();
  auto* nullsAddr = builder.CreateConstInBoundsGEP1_64(
      builder.getInt8Ty(), output, kOutputNullsOffset);
  auto* nullsPtrPtr =
      builder.CreatePointerCast(nullsAddr, i64Ty->getPointerTo()->getPointerTo());
  return builder.CreateLoad(i64Ty->getPointerTo(), nullsPtrPtr, "output_nulls");
}

llvm::Value* loadOutputVector(llvm::IRBuilder<>& builder, llvm::Value* output) {
  auto* i8PtrTy = llvm::PointerType::get(builder.getContext(), 0);
  auto* vectorAddr = builder.CreateConstInBoundsGEP1_64(
      builder.getInt8Ty(), output, kOutputVectorOffset);
  auto* vectorPtrPtr = builder.CreatePointerCast(vectorAddr, i8PtrTy->getPointerTo());
  return builder.CreateLoad(i8PtrTy, vectorPtrPtr, "output_vector");
}

llvm::Value* loadPointerField(
    llvm::IRBuilder<>& builder,
    llvm::Value* descriptor,
    uint64_t offset,
    llvm::Type* pointerType,
    llvm::StringRef name) {
  auto* fieldAddr = builder.CreateConstInBoundsGEP1_64(
      builder.getInt8Ty(), descriptor, offset);
  auto* fieldPtrPtr = builder.CreatePointerCast(fieldAddr, pointerType->getPointerTo());
  return builder.CreateLoad(pointerType, fieldPtrPtr, name);
}

llvm::Value* loadDecodedIndex(
    llvm::IRBuilder<>& builder,
    llvm::Value* decoded,
    llvm::Value* row) {
  auto* i32Ty = builder.getInt32Ty();
  auto* indices = loadPointerField(
      builder,
      decoded,
      kDecodedInputIndicesOffset,
      i32Ty->getPointerTo(),
      "decoded_indices");
  return builder.CreateLoad(i32Ty, builder.CreateInBoundsGEP(i32Ty, indices, row));
}

llvm::Value* loadDecodedRowFieldPointer(
    llvm::IRBuilder<>& builder,
    llvm::Value* decoded,
    int32_t field,
    bool nulls) {
  auto* i8PtrTy = llvm::PointerType::get(builder.getContext(), 0);
  auto* pointerType = nulls ? builder.getInt64Ty()->getPointerTo() : i8PtrTy;
  auto offset = kDecodedInputFirstRowFieldOffset +
      static_cast<uint64_t>(field) * kDecodedInputRowFieldStride +
      (nulls ? kDecodedInputRowFieldNullsOffsetDelta : 0);
  return loadPointerField(
      builder,
      decoded,
      offset,
      pointerType,
      nulls ? "decoded_row_field_nulls" : "decoded_row_field_values");
}

llvm::Value* loadOutputRowFieldPointer(
    llvm::IRBuilder<>& builder,
    llvm::Value* output,
    int32_t field,
    bool nulls) {
  auto* i8PtrTy = llvm::PointerType::get(builder.getContext(), 0);
  auto* pointerType = nulls ? builder.getInt64Ty()->getPointerTo() : i8PtrTy;
  auto offset = kOutputFirstRowFieldOffset +
      static_cast<uint64_t>(field) * kOutputRowFieldStride +
      (nulls ? kOutputRowFieldNullsOffsetDelta : 0);
  return loadPointerField(
      builder,
      output,
      offset,
      pointerType,
      nulls ? "output_row_field_nulls" : "output_row_field_values");
}

void emitOutputNullBit(
    llvm::IRBuilder<>& builder,
    llvm::Value* nulls,
    llvm::Value* row,
    llvm::Value* isNull) {
  auto* i64Ty = builder.getInt64Ty();
  auto* wordIndex = builder.CreateLShr(row, builder.getInt32(6));
  auto* bitIndex = builder.CreateAnd(row, builder.getInt32(63));
  auto* wordAddr = builder.CreateInBoundsGEP(
      i64Ty, nulls, builder.CreateZExt(wordIndex, builder.getInt64Ty()));
  auto* word = builder.CreateLoad(i64Ty, wordAddr);
  auto* mask = builder.CreateShl(
      builder.getInt64(1), builder.CreateZExt(bitIndex, builder.getInt64Ty()));
  auto* notNullWord = builder.CreateOr(word, mask);
  auto* nullWord = builder.CreateAnd(word, builder.CreateNot(mask));
  auto* isNullBool = builder.CreateICmpNE(isNull, builder.getInt8(0));
  builder.CreateStore(
      builder.CreateSelect(isNullBool, nullWord, notNullWord), wordAddr);
}

llvm::LoadInst* loadValue(
    llvm::IRBuilder<>& builder,
    llvm::Value* row,
    llvm::Type* type,
    int32_t offset) {
  auto* addr = builder.CreateConstInBoundsGEP1_64(
      builder.getInt8Ty(), row, static_cast<uint64_t>(offset));
  auto* castAddr = builder.CreatePointerCast(addr, type->getPointerTo());
  auto* load = builder.CreateLoad(type, castAddr);
  load->setAlignment(llvm::Align(1));
  return load;
}

void storeValue(
    llvm::IRBuilder<>& builder,
    llvm::Value* row,
    llvm::Type* type,
    int32_t offset,
    llvm::Value* value) {
  auto* addr = builder.CreateConstInBoundsGEP1_64(
      builder.getInt8Ty(), row, static_cast<uint64_t>(offset));
  auto* castAddr = builder.CreatePointerCast(addr, type->getPointerTo());
  auto* store = builder.CreateStore(value, castAddr);
  store->setAlignment(llvm::Align(1));
}

llvm::Value* castValue(
    llvm::IRBuilder<>& builder,
    llvm::Value* value,
    HashAggrJitValueKind from,
    HashAggrJitValueKind to) {
  if (from == to) {
    return value;
  }
  auto* toType = llvmType(builder, to);
  if (isFloatKind(from) && isFloatKind(to)) {
    return builder.CreateFPCast(value, toType);
  }
  if (!isFloatKind(from) && isFloatKind(to)) {
    return builder.CreateSIToFP(value, toType);
  }
  if (isFloatKind(from) && !isFloatKind(to)) {
    return builder.CreateFPToSI(value, toType);
  }
  return builder.CreateSExtOrTrunc(value, toType);
}

llvm::Value* isAccumulatorNull(
    llvm::IRBuilder<>& builder,
    llvm::Value* group,
    const HashAggrJitSlot& slot) {
  auto* byte = loadValue(builder, group, builder.getInt8Ty(), slot.nullByte);
  auto* mask = llvm::ConstantInt::get(builder.getInt8Ty(), slot.nullMask);
  return builder.CreateICmpNE(
      builder.CreateAnd(byte, mask), builder.getInt8(0));
}

void clearAccumulatorNull(
    llvm::IRBuilder<>& builder,
    llvm::Value* group,
    const HashAggrJitSlot& slot) {
  auto* byte = loadValue(builder, group, builder.getInt8Ty(), slot.nullByte);
  auto* mask = llvm::ConstantInt::get(
      builder.getInt8Ty(), static_cast<uint8_t>(~slot.nullMask));
  storeValue(
      builder,
      group,
      builder.getInt8Ty(),
      slot.nullByte,
      builder.CreateAnd(byte, mask));
}

void setAccumulatorNull(
    llvm::IRBuilder<>& builder,
    llvm::Value* group,
    const HashAggrJitSlot& slot) {
  auto* byte = loadValue(builder, group, builder.getInt8Ty(), slot.nullByte);
  auto* mask = llvm::ConstantInt::get(builder.getInt8Ty(), slot.nullMask);
  storeValue(
      builder,
      group,
      builder.getInt8Ty(),
      slot.nullByte,
      builder.CreateOr(byte, mask));
}

llvm::Value* loadDecodedValue(
    llvm::IRBuilder<>& builder,
    llvm::Value* decoded,
    llvm::Value* row,
    const HashAggrJitSlot& slot) {
  auto* i8PtrTy = llvm::PointerType::get(builder.getContext(), 0);
  auto* i32Ty = builder.getInt32Ty();

  auto* valuesPtrPtr = builder.CreatePointerCast(decoded, i8PtrTy->getPointerTo());
  auto* values = builder.CreateLoad(i8PtrTy, valuesPtrPtr, "decoded_values");

  auto* indicesAddr = builder.CreateConstInBoundsGEP1_64(
      builder.getInt8Ty(), decoded, kDecodedInputIndicesOffset);
  auto* indicesPtrPtr =
      builder.CreatePointerCast(indicesAddr, i32Ty->getPointerTo()->getPointerTo());
  auto* indices = builder.CreateLoad(i32Ty->getPointerTo(), indicesPtrPtr, "decoded_indices");
  auto* index = builder.CreateLoad(
      i32Ty, builder.CreateInBoundsGEP(i32Ty, indices, row));

  if (slot.desc.inputKind == HashAggrJitValueKind::Bool) {
    auto* wordTy = builder.getInt64Ty();
    auto* wordIndex = builder.CreateLShr(index, builder.getInt32(6));
    auto* bitIndex = builder.CreateAnd(index, builder.getInt32(63));
    auto* words = builder.CreatePointerCast(values, wordTy->getPointerTo());
    auto* word = builder.CreateLoad(
        wordTy,
        builder.CreateInBoundsGEP(
            wordTy, words, builder.CreateZExt(wordIndex, builder.getInt64Ty())));
    auto* shifted = builder.CreateLShr(word, builder.CreateZExt(bitIndex, wordTy));
    return builder.CreateZExt(
        builder.CreateICmpNE(
            builder.CreateAnd(shifted, builder.getInt64(1)), builder.getInt64(0)),
        builder.getInt8Ty());
  }

  auto* type = llvmType(builder, slot.desc.inputKind);
  auto* typedValues = builder.CreatePointerCast(values, type->getPointerTo());
  auto* valueAddr = builder.CreateInBoundsGEP(
      type, typedValues, builder.CreateZExt(index, builder.getInt64Ty()));
  auto* load = builder.CreateLoad(type, valueAddr);
  load->setAlignment(llvm::Align(1));
  return load;
}

llvm::Value* loadDecodedNulls(llvm::IRBuilder<>& builder, llvm::Value* decoded) {
  auto* i8PtrTy = llvm::PointerType::get(builder.getContext(), 0);
  auto* nullsAddr = builder.CreateConstInBoundsGEP1_64(
      builder.getInt8Ty(), decoded, kDecodedInputNullsOffset);
  auto* nullsPtrPtr = builder.CreatePointerCast(nullsAddr, i8PtrTy->getPointerTo());
  return builder.CreateLoad(i8PtrTy, nullsPtrPtr, "decoded_nulls");
}

llvm::Value* isDecodedNull(
    llvm::IRBuilder<>& builder,
    llvm::Value* nulls,
    llvm::Value* row) {
  auto* i64Ty = builder.getInt64Ty();
  auto* nullWords = builder.CreatePointerCast(nulls, i64Ty->getPointerTo());
  auto* wordIndex = builder.CreateLShr(row, builder.getInt32(6));
  auto* bitIndex = builder.CreateAnd(row, builder.getInt32(63));
  auto* word = builder.CreateLoad(
      i64Ty,
      builder.CreateInBoundsGEP(
          i64Ty, nullWords, builder.CreateZExt(wordIndex, builder.getInt64Ty())));
  auto* shifted = builder.CreateLShr(word, builder.CreateZExt(bitIndex, i64Ty));
  return builder.CreateICmpNE(
      builder.CreateAnd(shifted, builder.getInt64(1)), builder.getInt64(0));
}

llvm::Value* loadDecodedVector(llvm::IRBuilder<>& builder, llvm::Value* decoded) {
  auto* i8PtrTy = llvm::PointerType::get(builder.getContext(), 0);
  auto* decodedVectorAddr = builder.CreateConstInBoundsGEP1_64(
      builder.getInt8Ty(), decoded, kDecodedInputDecodedVectorOffset);
  auto* decodedVectorPtrPtr =
      builder.CreatePointerCast(decodedVectorAddr, i8PtrTy->getPointerTo());
  return builder.CreateLoad(i8PtrTy, decodedVectorPtrPtr, "decoded_vector");
}

} // namespace

HashAggrJitCodegen::HashAggrJitCodegen(llvm::Module& module) : module_(module) {
  ensureBuiltinDeclarations(module_);
}

llvm::Type* HashAggrJitCodegen::llvmType(HashAggrJitValueKind kind) const {
  return ::bytedance::bolt::jit::llvmType(builder(), kind);
}

llvm::Value* HashAggrJitCodegen::loadDecodedValue(
    llvm::Value* decoded,
    llvm::Value* row,
    const HashAggrJitSlot& slot) const {
  return ::bytedance::bolt::jit::loadDecodedValue(builder(), decoded, row, slot);
}

llvm::Value* HashAggrJitCodegen::loadDecodedNulls(llvm::Value* decoded) const {
  return ::bytedance::bolt::jit::loadDecodedNulls(builder(), decoded);
}

llvm::Value* HashAggrJitCodegen::isDecodedNull(
    llvm::Value* nulls,
    llvm::Value* row) const {
  return ::bytedance::bolt::jit::isDecodedNull(builder(), nulls, row);
}

llvm::Value* HashAggrJitCodegen::isAccumulatorNull(
    llvm::Value* group,
    const HashAggrJitSlot& slot) const {
  return ::bytedance::bolt::jit::isAccumulatorNull(builder(), group, slot);
}

void HashAggrJitCodegen::clearAccumulatorNull(
    llvm::Value* group,
    const HashAggrJitSlot& slot) const {
  ::bytedance::bolt::jit::clearAccumulatorNull(builder(), group, slot);
}

void HashAggrJitCodegen::setAccumulatorNull(
    llvm::Value* group,
    const HashAggrJitSlot& slot) const {
  ::bytedance::bolt::jit::setAccumulatorNull(builder(), group, slot);
}

llvm::LoadInst* HashAggrJitCodegen::loadValue(
    llvm::Value* row,
    llvm::Type* type,
    int32_t offset) const {
  return ::bytedance::bolt::jit::loadValue(builder(), row, type, offset);
}

void HashAggrJitCodegen::storeValue(
    llvm::Value* row,
    llvm::Type* type,
    int32_t offset,
    llvm::Value* value) const {
  ::bytedance::bolt::jit::storeValue(builder(), row, type, offset, value);
}

llvm::Value* HashAggrJitCodegen::castValue(
    llvm::Value* value,
    HashAggrJitValueKind from,
    HashAggrJitValueKind to) const {
  return ::bytedance::bolt::jit::castValue(builder(), value, from, to);
}

bool HashAggrJitCodegen::isFloatKind(HashAggrJitValueKind kind) const {
  return ::bytedance::bolt::jit::isFloatKind(kind);
}

llvm::Value* HashAggrJitCodegen::loadDecodedRowField(
    llvm::Value* decoded,
    llvm::Value* row,
    int32_t field,
    HashAggrJitValueKind kind) const {
  if (field == 0 || field == 1) {
    auto* rawValues = ::bytedance::bolt::jit::loadDecodedRowFieldPointer(
        builder(), decoded, field, false);
    auto* index = ::bytedance::bolt::jit::loadDecodedIndex(builder(), decoded, row);
    auto* type = llvmType(kind);
    auto* typedValues = builder().CreatePointerCast(rawValues, type->getPointerTo());
    auto* valueAddr = builder().CreateInBoundsGEP(
        type, typedValues, builder().CreateZExt(index, builder().getInt64Ty()));
    auto* value = builder().CreateLoad(type, valueAddr);
    value->setAlignment(llvm::Align(1));
    return value;
  }
  const auto name = decodedRowFieldFunction(kind);
  BOLT_CHECK(
      !name.empty(), "Unsupported decoded row field kind for HashAggrJit");
  auto* decodedVector = ::bytedance::bolt::jit::loadDecodedVector(builder(), decoded);
  return builder().CreateCall(
      module_.getFunction(name),
      {decodedVector, row, builder().getInt32(field)});
}

llvm::Value* HashAggrJitCodegen::isDecodedRowFieldNull(
    llvm::Value* decoded,
    llvm::Value* row,
    int32_t field) const {
  if (field == 0 || field == 1) {
    auto* rawNulls = ::bytedance::bolt::jit::loadDecodedRowFieldPointer(
        builder(), decoded, field, true);
    auto* hasRawNulls = builder().CreateICmpNE(
        rawNulls, llvm::ConstantPointerNull::get(builder().getInt64Ty()->getPointerTo()));
    auto* nullCheckBlock = llvm::BasicBlock::Create(
        module_.getContext(), "row_field_null_check", builder().GetInsertBlock()->getParent());
    auto* rawDoneBlock = llvm::BasicBlock::Create(
        module_.getContext(), "row_field_null_done", builder().GetInsertBlock()->getParent());
    builder().CreateCondBr(hasRawNulls, nullCheckBlock, rawDoneBlock);
    auto* noNullsEnd = builder().GetInsertBlock();

    builder().SetInsertPoint(nullCheckBlock);
    auto* index = ::bytedance::bolt::jit::loadDecodedIndex(builder(), decoded, row);
    auto* isNull = ::bytedance::bolt::jit::isDecodedNull(builder(), rawNulls, index);
    builder().CreateBr(rawDoneBlock);
    auto* nullCheckEnd = builder().GetInsertBlock();

    builder().SetInsertPoint(rawDoneBlock);
    auto* fastNull = builder().CreatePHI(builder().getInt1Ty(), 2, "row_field_raw_is_null");
    fastNull->addIncoming(builder().getFalse(), noNullsEnd);
    fastNull->addIncoming(isNull, nullCheckEnd);
    return fastNull;
  }
  auto* decodedVector = ::bytedance::bolt::jit::loadDecodedVector(builder(), decoded);
  return builder().CreateICmpNE(
      builder().CreateCall(
          module_.getFunction("jit_GetDecodedRowFieldIsNull"),
          {decodedVector, row, builder().getInt32(field)}),
      builder().getInt8(0));
}

void HashAggrJitCodegen::emitFlatValue(
    llvm::Value* output,
    llvm::Value* row,
    HashAggrJitValueKind kind,
    llvm::Value* value,
    llvm::Value* isNull) const {
  if (supportsRawFlatOutput(kind)) {
    auto* type = llvmType(kind);
    auto* values = ::bytedance::bolt::jit::loadOutputValues(builder(), output);
    auto* typedValues = builder().CreatePointerCast(values, type->getPointerTo());
    auto* valueAddr = builder().CreateInBoundsGEP(
        type, typedValues, builder().CreateZExt(row, builder().getInt64Ty()));
    auto* store = builder().CreateStore(value, valueAddr);
    store->setAlignment(llvm::Align(1));
    auto* nulls = ::bytedance::bolt::jit::loadOutputNulls(builder(), output);
    ::bytedance::bolt::jit::emitOutputNullBit(builder(), nulls, row, isNull);
    return;
  }

  const auto setter = setFlatValueFunction(kind);
  if (setter.empty()) {
    return;
  }
  auto* vector = ::bytedance::bolt::jit::loadOutputVector(builder(), output);
  builder().CreateCall(
      module_.getFunction(setter),
      {vector, row, value, isNull});
}

void HashAggrJitCodegen::resizeResultVector(
    llvm::Value* output,
    llvm::Value* size) const {
  auto* vector = ::bytedance::bolt::jit::loadOutputVector(builder(), output);
  builder().CreateCall(
      module_.getFunction("jit_HashAggrResizeVector"),
      {vector, size});
}

void HashAggrJitCodegen::emitPartialAvgResult(
    llvm::Value* output,
    llvm::Value* row,
    llvm::Value* sum,
    llvm::Value* count,
    llvm::Value* isNull) const {
  // The extract admission path (runHashAggrJitExtractChunks) guarantees the
  // partial avg ROW output has flat sum/count children before the chunk runs,
  // so rowField0/1 values are always populated and we can write them directly
  // without a runtime fast/helper branch.
  auto* sumValues = ::bytedance::bolt::jit::loadOutputRowFieldPointer(
      builder(), output, 0, false);
  auto* sumTypedValues =
      builder().CreatePointerCast(sumValues, builder().getDoubleTy()->getPointerTo());
  auto* countValues = ::bytedance::bolt::jit::loadOutputRowFieldPointer(
      builder(), output, 1, false);
  auto* countTypedValues =
      builder().CreatePointerCast(countValues, builder().getInt64Ty()->getPointerTo());
  auto* row64 = builder().CreateZExt(row, builder().getInt64Ty());
  auto* sumAddr = builder().CreateInBoundsGEP(builder().getDoubleTy(), sumTypedValues, row64);
  auto* sumStore = builder().CreateStore(sum, sumAddr);
  sumStore->setAlignment(llvm::Align(1));
  auto* countAddr = builder().CreateInBoundsGEP(builder().getInt64Ty(), countTypedValues, row64);
  auto* countStore = builder().CreateStore(count, countAddr);
  countStore->setAlignment(llvm::Align(1));
  auto* nulls = ::bytedance::bolt::jit::loadOutputNulls(builder(), output);
  ::bytedance::bolt::jit::emitOutputNullBit(builder(), nulls, row, isNull);
}

void HashAggrJitCodegen::emitDecimalSumExtract(
    llvm::Value* output,
    llvm::Value* row,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    bool partialOutput) const {
  const char* fn = partialOutput ? "jit_HashAggrExtractPartialDecimalSum"
                                 : "jit_HashAggrExtractFinalDecimalSum";
  auto* longDecimal = builder().getInt8(
      slot.desc.inputKind == HashAggrJitValueKind::Int128 ? 1 : 0);
  auto* vector = ::bytedance::bolt::jit::loadOutputVector(builder(), output);
  builder().CreateCall(
      module_.getFunction(fn),
      {vector,
       row,
       group,
       builder().getInt32(slot.offset),
       builder().getInt32(slot.desc.precision),
       builder().getInt32(slot.desc.scale),
       longDecimal});
}

void HashAggrJitCodegen::emitDecimalAvgExtract(
    llvm::Value* output,
    llvm::Value* row,
    llvm::Value* group,
    const HashAggrJitSlot& slot,
    bool partialOutput) const {
  const char* fn = partialOutput ? "jit_HashAggrExtractPartialDecimalAvg"
                                 : "jit_HashAggrExtractFinalDecimalAvg";
  auto* longDecimal = builder().getInt8(
      slot.desc.inputKind == HashAggrJitValueKind::Int128 ? 1 : 0);
  auto* vector = ::bytedance::bolt::jit::loadOutputVector(builder(), output);
  builder().CreateCall(
      module_.getFunction(fn),
      {vector,
       row,
       group,
       builder().getInt32(slot.offset),
       builder().getInt32(slot.desc.precision),
       builder().getInt32(slot.desc.scale),
       longDecimal});
}

void HashAggrJitCodegen::emitDecimalAddWithOverflow(
    llvm::Value* group,
    int32_t sumOffset,
    int32_t overflowOffset,
    llvm::Value* addend) const {
  auto& b = builder();
  auto* i128Ty = b.getInt128Ty();
  auto* i64Ty = b.getInt64Ty();
  auto* zero128 = llvm::ConstantInt::get(i128Ty, 0);

  auto* oldSum = loadValue(group, i128Ty, sumOffset);
  auto* newSum = b.CreateAdd(oldSum, addend);
  storeValue(group, i128Ty, sumOffset, newSum);

  // Mirror jitHashAggrAddWithOverflow:
  //   +1 if a>0 && b>0 && result<0   (positive overflow)
  //   -1 if a<0 && b<0 && result>=0  (negative overflow)
  auto* aPos = b.CreateICmpSGT(oldSum, zero128);
  auto* bPos = b.CreateICmpSGT(addend, zero128);
  auto* rNeg = b.CreateICmpSLT(newSum, zero128);
  auto* posOverflow = b.CreateAnd(b.CreateAnd(aPos, bPos), rNeg);

  auto* aNeg = b.CreateICmpSLT(oldSum, zero128);
  auto* bNeg = b.CreateICmpSLT(addend, zero128);
  auto* rNonNeg = b.CreateICmpSGE(newSum, zero128);
  auto* negOverflow = b.CreateAnd(b.CreateAnd(aNeg, bNeg), rNonNeg);

  auto* carry = b.CreateSub(
      b.CreateZExt(posOverflow, i64Ty), b.CreateZExt(negOverflow, i64Ty));
  auto* oldOverflow = loadValue(group, i64Ty, overflowOffset);
  storeValue(group, i64Ty, overflowOffset, b.CreateAdd(oldOverflow, carry));
}

namespace {

bool genAddDenseIR(
    llvm::Module& module,
    const std::string& fn,
    const std::vector<HashAggrJitSlot>& slots,
    bool checkInputNulls);

bool genInitIR(
    llvm::Module& module,
    const std::string& fn,
    const std::vector<HashAggrJitSlot>& slots) {
  auto& context = module.getContext();
  llvm::IRBuilder<> builder(context);
  HashAggrJitCodegen codegen(module);
  codegen.setBuilder(&builder);
  auto* voidTy = builder.getVoidTy();
  auto* i8PtrTy = llvm::PointerType::get(context, 0);
  auto* i8PtrPtrTy = i8PtrTy->getPointerTo();
  auto* i32Ty = builder.getInt32Ty();
  auto* funcTy = llvm::FunctionType::get(voidTy, {i8PtrPtrTy, i32Ty}, false);
  auto* func = llvm::Function::Create(
      funcTy, llvm::Function::ExternalLinkage, fn, module);
  auto argIt = func->arg_begin();
  llvm::Value* newGroups = &*argIt++;
  newGroups->setName("new_groups");
  llvm::Value* numNewGroups = &*argIt++;
  numNewGroups->setName("num_new_groups");

  auto* entry = llvm::BasicBlock::Create(context, "entry", func);
  auto* loop = llvm::BasicBlock::Create(context, "loop", func);
  auto* end = llvm::BasicBlock::Create(context, "end", func);
  builder.SetInsertPoint(entry);
  builder.CreateCondBr(
      builder.CreateICmpSLE(numNewGroups, builder.getInt32(0)), end, loop);

  builder.SetInsertPoint(loop);
  auto* index = builder.CreatePHI(i32Ty, 2, "idx");
  index->addIncoming(builder.getInt32(0), entry);
  auto* groupAddr = builder.CreateInBoundsGEP(i8PtrTy, newGroups, index);
  auto* group = builder.CreateLoad(i8PtrTy, groupAddr);

  for (const auto& slot : slots) {
    if (slot.desc.ops == nullptr || slot.desc.ops->initGroup == nullptr) {
      return false;
    }
    slot.desc.ops->initGroup(codegen, group, slot);
  }

  auto* next = builder.CreateAdd(index, builder.getInt32(1));
  index->addIncoming(next, builder.GetInsertBlock());
  builder.CreateCondBr(builder.CreateICmpSLT(next, numNewGroups), loop, end);

  builder.SetInsertPoint(end);
  builder.CreateRetVoid();

  return !llvm::verifyFunction(*func, &llvm::errs());
}

bool genAddDenseIR(
    llvm::Module& module,
    const std::string& fn,
    const std::vector<HashAggrJitSlot>& slots,
    bool checkInputNulls) {
  auto& context = module.getContext();
  llvm::IRBuilder<> builder(context);
  HashAggrJitCodegen codegen(module);
  codegen.setBuilder(&builder);
  auto* voidTy = builder.getVoidTy();
  auto* i8PtrTy = llvm::PointerType::get(context, 0);
  auto* i8PtrPtrTy = i8PtrTy->getPointerTo();
  auto* i32Ty = builder.getInt32Ty();
  auto* funcTy = llvm::FunctionType::get(voidTy, {i8PtrPtrTy, i32Ty, i8PtrPtrTy}, false);
  auto* func = llvm::Function::Create(funcTy, llvm::Function::ExternalLinkage, fn, module);
  auto argIt = func->arg_begin();
  llvm::Value* groups = &*argIt++;
  groups->setName("groups");
  llvm::Value* numRows = &*argIt++;
  numRows->setName("num_rows");
  llvm::Value* decodedInputs = &*argIt++;
  decodedInputs->setName("decoded_inputs");

  auto* entry = llvm::BasicBlock::Create(context, "entry", func);
  auto* loop = llvm::BasicBlock::Create(context, "loop", func);
  auto* end = llvm::BasicBlock::Create(context, "end", func);
  builder.SetInsertPoint(entry);
  builder.CreateCondBr(builder.CreateICmpSLE(numRows, builder.getInt32(0)), end, loop);

  builder.SetInsertPoint(loop);
  auto* row = builder.CreatePHI(i32Ty, 2, "row");
  row->addIncoming(builder.getInt32(0), entry);
  auto* groupAddr = builder.CreateInBoundsGEP(i8PtrTy, groups, row);
  auto* group = builder.CreateLoad(i8PtrTy, groupAddr);

  for (auto i = 0; i < slots.size(); ++i) {
    const auto& slot = slots[i];
    auto* updateBlock = llvm::BasicBlock::Create(context, "slot_update", func, end);
    auto* nextBlock = llvm::BasicBlock::Create(context, "slot_next", func, end);
    auto* decodedAddr = builder.CreateConstInBoundsGEP1_64(i8PtrTy, decodedInputs, i);
    auto* decoded = builder.CreateLoad(i8PtrTy, decodedAddr);
    if (checkInputNulls && !slot.desc.countStar) {
      auto* nulls = codegen.loadDecodedNulls(decoded);
      auto* nullCheckBlock =
          llvm::BasicBlock::Create(context, "slot_null_check", func, end);
      auto* hasNulls = builder.CreateICmpNE(
          nulls, llvm::ConstantPointerNull::get(i8PtrTy));
      builder.CreateCondBr(hasNulls, nullCheckBlock, updateBlock);

      builder.SetInsertPoint(nullCheckBlock);
      auto* isNull = codegen.isDecodedNull(nulls, row);
      builder.CreateCondBr(isNull, nextBlock, updateBlock);
    } else {
      builder.CreateBr(updateBlock);
    }

    builder.SetInsertPoint(updateBlock);
    if (slot.desc.ops == nullptr) {
      return false;
    }
    auto* addFn =
        slot.desc.mergeInput ? slot.desc.ops->addIntermediateResults : slot.desc.ops->addRawInput;
    if (addFn == nullptr) {
      return false;
    }
    addFn(codegen, group, decoded, row, slot, checkInputNulls, nextBlock);
    builder.CreateBr(nextBlock);
    builder.SetInsertPoint(nextBlock);
  }

  auto* next = builder.CreateAdd(row, builder.getInt32(1));
  row->addIncoming(next, builder.GetInsertBlock());
  builder.CreateCondBr(builder.CreateICmpSLT(next, numRows), loop, end);

  builder.SetInsertPoint(end);
  builder.CreateRetVoid();

  return !llvm::verifyFunction(*func, &llvm::errs());
}

std::string setFlatValueFunction(HashAggrJitValueKind kind) {
  switch (kind) {
    case HashAggrJitValueKind::Int8:
      return "jit_HashAggrSetFlatI8";
    case HashAggrJitValueKind::Int16:
      return "jit_HashAggrSetFlatI16";
    case HashAggrJitValueKind::Int32:
      return "jit_HashAggrSetFlatI32";
    case HashAggrJitValueKind::Int64:
      return "jit_HashAggrSetFlatI64";
    case HashAggrJitValueKind::Float:
      return "jit_HashAggrSetFlatFloat";
    case HashAggrJitValueKind::Double:
      return "jit_HashAggrSetFlatDouble";
    // Bool output vectors are FlatVector<bool>, which cannot reuse the int8
    // setter. JIT extract is not yet supported for Bool.
    case HashAggrJitValueKind::Bool:
    case HashAggrJitValueKind::Int128:
      return "";
  }
  return "";
}

bool genExtractIR(
    llvm::Module& module,
    const std::string& fn,
    const std::vector<HashAggrJitSlot>& slots,
    bool partialOutput) {
  auto& context = module.getContext();
  llvm::IRBuilder<> builder(context);
  HashAggrJitCodegen codegen(module);
  codegen.setBuilder(&builder);
  auto* voidTy = builder.getVoidTy();
  auto* i8PtrTy = llvm::PointerType::get(context, 0);
  auto* i8PtrPtrTy = i8PtrTy->getPointerTo();
  auto* i32Ty = builder.getInt32Ty();
  auto* funcTy = llvm::FunctionType::get(voidTy, {i8PtrPtrTy, i32Ty, i8PtrPtrTy}, false);
  auto* func = llvm::Function::Create(funcTy, llvm::Function::ExternalLinkage, fn, module);
  auto argIt = func->arg_begin();
  llvm::Value* groups = &*argIt++;
  groups->setName("groups");
  llvm::Value* numGroups = &*argIt++;
  numGroups->setName("num_groups");
  llvm::Value* resultVectors = &*argIt++;
  resultVectors->setName("result_vectors");

  auto* entry = llvm::BasicBlock::Create(context, "entry", func);
  auto* loop = llvm::BasicBlock::Create(context, "loop", func);
  auto* end = llvm::BasicBlock::Create(context, "end", func);
  builder.SetInsertPoint(entry);
  for (auto i = 0; i < slots.size(); ++i) {
    if (slots[i].desc.ops == nullptr || slots[i].desc.ops->canExtract == nullptr ||
        !slots[i].desc.ops->canExtract(slots[i], partialOutput)) {
      continue;
    }
    auto* vectorAddr = builder.CreateConstInBoundsGEP1_64(i8PtrTy, resultVectors, i);
    auto* vector = builder.CreateLoad(i8PtrTy, vectorAddr);
    codegen.resizeResultVector(vector, numGroups);
  }
  builder.CreateCondBr(builder.CreateICmpSLE(numGroups, builder.getInt32(0)), end, loop);

  builder.SetInsertPoint(loop);
  auto* row = builder.CreatePHI(i32Ty, 2, "row");
  row->addIncoming(builder.getInt32(0), entry);
  auto* groupAddr = builder.CreateInBoundsGEP(i8PtrTy, groups, row);
  auto* group = builder.CreateLoad(i8PtrTy, groupAddr);

  for (auto i = 0; i < slots.size(); ++i) {
    const auto& slot = slots[i];
    if (slot.desc.ops == nullptr || slot.desc.ops->canExtract == nullptr ||
        !slot.desc.ops->canExtract(slot, partialOutput)) {
      continue;
    }
    auto* vectorAddr = builder.CreateConstInBoundsGEP1_64(i8PtrTy, resultVectors, i);
    auto* vector = builder.CreateLoad(i8PtrTy, vectorAddr);
    if (slot.desc.ops->extract == nullptr) {
      return false;
    }
    slot.desc.ops->extract(
        codegen, group, slot, HashAggrJitExtractTarget{vector, row, partialOutput});
  }

  auto* next = builder.CreateAdd(row, builder.getInt32(1));
  row->addIncoming(next, builder.GetInsertBlock());
  builder.CreateCondBr(builder.CreateICmpSLT(next, numGroups), loop, end);

  builder.SetInsertPoint(end);
  builder.CreateRetVoid();

  return !llvm::verifyFunction(*func, &llvm::errs());
}

} // namespace

HashAggrJitChunk::HashAggrJitChunk(
    std::vector<HashAggrJitSlot> slots,
    bool partialOutput)
    : slots_(std::move(slots)), partialOutput_(partialOutput) {
  std::ostringstream out;
  out << "jit_hashaggr_v2_" << (partialOutput_ ? "partial" : "final") << "_n"
      << slots_.size();
  for (const auto& slot : slots_) {
    out << "_" << (slot.desc.ops != nullptr ? slot.desc.ops->id : "unknown") << "_"
        << static_cast<int>(slot.desc.kind) << hashAggrJitValueKindName(slot.desc.inputKind)
        << hashAggrJitValueKindName(slot.desc.accumulatorKind) << "o" << slot.offset
        << "n" << slot.nullByte << "m" << static_cast<int>(slot.nullMask)
        << (slot.desc.countStar ? "s" : "x") << (slot.desc.mergeInput ? "g" : "r")
        << (slot.desc.decimal ? "d" : "n");
  }
  functionName_ = out.str();
  initFunctionName_ = functionName_ + "_init";
  addDenseFunctionName_ = functionName_ + "_add_dense";
  addDenseNoNullFunctionName_ = functionName_ + "_add_dense_no_null";
  extractFunctionName_ = functionName_ + "_extract";
}

std::string hashAggrJitValueKindName(HashAggrJitValueKind kind) {
  switch (kind) {
    case HashAggrJitValueKind::Bool:
      return "bool";
    case HashAggrJitValueKind::Int8:
      return "i8";
    case HashAggrJitValueKind::Int16:
      return "i16";
    case HashAggrJitValueKind::Int32:
      return "i32";
    case HashAggrJitValueKind::Int64:
      return "i64";
    case HashAggrJitValueKind::Int128:
      return "i128";
    case HashAggrJitValueKind::Float:
      return "f32";
    case HashAggrJitValueKind::Double:
      return "f64";
  }
  return "unknown";
}

std::optional<HashAggrJitValueKind> hashAggrJitValueKind(TypeKind kind) {
  switch (kind) {
    case TypeKind::BOOLEAN:
      return HashAggrJitValueKind::Bool;
    case TypeKind::TINYINT:
      return HashAggrJitValueKind::Int8;
    case TypeKind::SMALLINT:
      return HashAggrJitValueKind::Int16;
    case TypeKind::INTEGER:
      return HashAggrJitValueKind::Int32;
    case TypeKind::BIGINT:
      return HashAggrJitValueKind::Int64;
    case TypeKind::HUGEINT:
      return HashAggrJitValueKind::Int128;
    case TypeKind::REAL:
      return HashAggrJitValueKind::Float;
    case TypeKind::DOUBLE:
      return HashAggrJitValueKind::Double;
    default:
      return std::nullopt;
  }
}

bool isHashAggrJitSupportedType(TypeKind kind) {
  switch (kind) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      return true;
    default:
      return false;
  }
}

std::string HashAggrJitDescriptor::signature() const {
  return fmt::format(
      "{}_{}_{}_{}_{}_{}",
      ops != nullptr ? ops->id : "unknown",
      static_cast<int>(kind),
      hashAggrJitValueKindName(inputKind),
      hashAggrJitValueKindName(accumulatorKind),
      mergeInput,
      decimal);
}

bool HashAggrJitChunk::canExtract() const {
  if (extract_ == nullptr) {
    return false;
  }
  for (const auto& slot : slots_) {
    if (slot.desc.ops == nullptr || slot.desc.ops->canExtract == nullptr ||
        !slot.desc.ops->canExtract(slot, partialOutput_)) {
      return false;
    }
  }
  return true;
}

bool HashAggrJitChunk::codegen() {
  if (addDense_) {
    return true;
  }
  auto* jit = ThrustJITv2::getInstance();
  if (jit == nullptr) {
    return false;
  }
  const auto& moduleKey = functionName_;
  const auto& initFn = initFunctionName_;
  const auto& addFn = addDenseFunctionName_;
  const auto& addNoNullFn = addDenseNoNullFunctionName_;
  const auto& extractFn = extractFunctionName_;
  module_ = jit->CompileModule(
      [&](llvm::Module& module) {
        const bool ok = genInitIR(module, initFn, slots_) &&
            genAddDenseIR(module, addFn, slots_, true) &&
            genAddDenseIR(module, addNoNullFn, slots_, false) &&
            genExtractIR(module, extractFn, slots_, partialOutput_);
        const bool hasError = !ok;
        logHashAggrJitFunctionIR(module, moduleKey, initFn, "init", hasError);
        logHashAggrJitFunctionIR(module, moduleKey, addFn, "add_dense", hasError);
        logHashAggrJitFunctionIR(
            module,
            moduleKey,
            addNoNullFn,
            "add_dense_no_null",
            hasError);
        logHashAggrJitFunctionIR(
            module, moduleKey, extractFn, "extract", hasError);
        return hasError;
      },
      moduleKey);
  if (!module_) {
    return false;
  }
  init_ = reinterpret_cast<HashAggrJitInitFunc>(module_->getFuncPtr(initFn));
  addDense_ = reinterpret_cast<HashAggrJitAddDenseFunc>(module_->getFuncPtr(addFn));
  addDenseNoNull_ = reinterpret_cast<HashAggrJitAddDenseFunc>(
      module_->getFuncPtr(addNoNullFn));
  extract_ = reinterpret_cast<HashAggrJitExtractFunc>(module_->getFuncPtr(extractFn));
  if (init_ == nullptr || addDense_ == nullptr || addDenseNoNull_ == nullptr ||
      extract_ == nullptr) {
    return false;
  }
  return true;
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
