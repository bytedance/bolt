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

using bytedance::bolt::jit::HashAggrJitInputRuntime;
using bytedance::bolt::jit::HashAggrJitOutputRuntime;
using bytedance::bolt::jit::HashAggrJitRowInputRuntime;
using bytedance::bolt::jit::HashAggrJitRowOutputRuntime;
using bytedance::bolt::jit::HashAggrJitScalarInputRuntime;
using bytedance::bolt::jit::HashAggrJitScalarOutputRuntime;

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

constexpr uint64_t kScalarInputValuesOffset =
    offsetof(HashAggrJitScalarInputRuntime, values);
constexpr uint64_t kScalarInputIndicesOffset =
    offsetof(HashAggrJitScalarInputRuntime, indices);
constexpr uint64_t kScalarInputNullsOffset =
    offsetof(HashAggrJitScalarInputRuntime, nulls);
constexpr uint64_t kRowInputNullsOffset =
    offsetof(HashAggrJitRowInputRuntime, nulls);
constexpr uint64_t kRowInputChildrenOffset =
    offsetof(HashAggrJitRowInputRuntime, children);

constexpr uint64_t kScalarOutputValuesOffset =
    offsetof(HashAggrJitScalarOutputRuntime, values);
constexpr uint64_t kScalarOutputNullsOffset =
    offsetof(HashAggrJitScalarOutputRuntime, nulls);
constexpr uint64_t kScalarOutputVectorOffset =
    offsetof(HashAggrJitScalarOutputRuntime, vector);
constexpr uint64_t kRowOutputNullsOffset =
    offsetof(HashAggrJitRowOutputRuntime, nulls);
constexpr uint64_t kRowOutputChildrenOffset =
    offsetof(HashAggrJitRowOutputRuntime, children);
constexpr uint64_t kRowOutputVectorOffset =
    offsetof(HashAggrJitRowOutputRuntime, vector);

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
  auto* i32Ty = llvm::Type::getInt32Ty(context);
  auto* voidTy = llvm::Type::getVoidTy(context);
  auto* i8PtrTy = llvm::PointerType::get(context, 0);

  declareFunction(module, "jit_HashAggrResizeVector", voidTy, {i8PtrTy, i32Ty});
  // Decimal extract helpers.
  // Sum: (vector, row, group, offset, precision, scale, longDecimal).
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
      {i8PtrTy, i32Ty, i8PtrTy, i32Ty, i32Ty, i32Ty, i32Ty, i32Ty, i8Ty});
  declareFunction(
      module,
      "jit_HashAggrExtractPartialDecimalAvg",
      voidTy,
      {i8PtrTy, i32Ty, i8PtrTy, i32Ty, i32Ty, i32Ty, i32Ty, i32Ty, i8Ty});
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

llvm::Value* loadPointerField(
    llvm::IRBuilder<>& builder,
    llvm::Value* descriptor,
    uint64_t offset,
    llvm::Type* pointerType,
    llvm::StringRef name);

llvm::Value* loadScalarOutputValues(
    llvm::IRBuilder<>& builder,
    llvm::Value* output) {
  auto* i8PtrTy = llvm::PointerType::get(builder.getContext(), 0);
  return loadPointerField(
      builder, output, kScalarOutputValuesOffset, i8PtrTy, "output_values");
}

llvm::Value* loadScalarOutputNulls(
    llvm::IRBuilder<>& builder,
    llvm::Value* output) {
  return loadPointerField(
      builder,
      output,
      kScalarOutputNullsOffset,
      builder.getInt64Ty()->getPointerTo(),
      "output_nulls");
}

llvm::Value* loadScalarOutputVector(
    llvm::IRBuilder<>& builder,
    llvm::Value* output) {
  auto* i8PtrTy = llvm::PointerType::get(builder.getContext(), 0);
  return loadPointerField(
      builder, output, kScalarOutputVectorOffset, i8PtrTy, "output_vector");
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

llvm::Value* loadScalarInputIndex(
    llvm::IRBuilder<>& builder,
    llvm::Value* input,
    llvm::Value* row) {
  auto* i32Ty = builder.getInt32Ty();
  auto* indices = loadPointerField(
      builder,
      input,
      kScalarInputIndicesOffset,
      i32Ty->getPointerTo(),
      "input_indices");
  return builder.CreateLoad(i32Ty, builder.CreateInBoundsGEP(i32Ty, indices, row));
}

llvm::Value* loadScalarInputValues(
    llvm::IRBuilder<>& builder,
    llvm::Value* input) {
  auto* i8PtrTy = llvm::PointerType::get(builder.getContext(), 0);
  return loadPointerField(
      builder, input, kScalarInputValuesOffset, i8PtrTy, "input_values");
}

llvm::Value* loadScalarInputNulls(
    llvm::IRBuilder<>& builder,
    llvm::Value* input) {
  return loadPointerField(
      builder,
      input,
      kScalarInputNullsOffset,
      builder.getInt64Ty()->getPointerTo(),
      "input_nulls");
}

llvm::Value* loadRowInputNulls(llvm::IRBuilder<>& builder, llvm::Value* input) {
  return loadPointerField(
      builder,
      input,
      kRowInputNullsOffset,
      builder.getInt64Ty()->getPointerTo(),
      "row_input_nulls");
}

llvm::Value* loadRowInputChild(
    llvm::IRBuilder<>& builder,
    llvm::Value* input,
    int32_t field) {
  auto* i8PtrTy = llvm::PointerType::get(builder.getContext(), 0);
  auto* children = loadPointerField(
      builder,
      input,
      kRowInputChildrenOffset,
      i8PtrTy->getPointerTo(),
      "row_input_children");
  auto* childAddr =
      builder.CreateConstInBoundsGEP1_64(i8PtrTy, children, field);
  return builder.CreateLoad(i8PtrTy, childAddr, "row_input_child");
}

llvm::Value* loadScalarInputValue(
    llvm::IRBuilder<>& builder,
    llvm::Value* input,
    llvm::Value* row,
    HashAggrJitValueKind kind) {
  auto* values = loadScalarInputValues(builder, input);
  auto* index = loadScalarInputIndex(builder, input, row);

  if (kind == HashAggrJitValueKind::Bool) {
    auto* wordTy = builder.getInt64Ty();
    auto* wordIndex = builder.CreateLShr(index, builder.getInt32(6));
    auto* bitIndex = builder.CreateAnd(index, builder.getInt32(63));
    auto* words = builder.CreatePointerCast(values, wordTy->getPointerTo());
    auto* word = builder.CreateLoad(
        wordTy,
        builder.CreateInBoundsGEP(
            wordTy,
            words,
            builder.CreateZExt(wordIndex, builder.getInt64Ty())));
    auto* shifted =
        builder.CreateLShr(word, builder.CreateZExt(bitIndex, wordTy));
    return builder.CreateZExt(
        builder.CreateICmpNE(
            builder.CreateAnd(shifted, builder.getInt64(1)),
            builder.getInt64(0)),
        builder.getInt8Ty());
  }

  auto* type = llvmType(builder, kind);
  auto* typedValues = builder.CreatePointerCast(values, type->getPointerTo());
  auto* valueAddr = builder.CreateInBoundsGEP(
      type, typedValues, builder.CreateZExt(index, builder.getInt64Ty()));
  auto* load = builder.CreateLoad(type, valueAddr);
  load->setAlignment(llvm::Align(1));
  return load;
}

llvm::Value* loadRowOutputNulls(llvm::IRBuilder<>& builder, llvm::Value* output) {
  return loadPointerField(
      builder,
      output,
      kRowOutputNullsOffset,
      builder.getInt64Ty()->getPointerTo(),
      "row_output_nulls");
}

llvm::Value* loadRowOutputVector(
    llvm::IRBuilder<>& builder,
    llvm::Value* output) {
  auto* i8PtrTy = llvm::PointerType::get(builder.getContext(), 0);
  return loadPointerField(
      builder, output, kRowOutputVectorOffset, i8PtrTy, "row_output_vector");
}

llvm::Value* loadRowOutputChild(
    llvm::IRBuilder<>& builder,
    llvm::Value* output,
    int32_t field) {
  auto* i8PtrTy = llvm::PointerType::get(builder.getContext(), 0);
  auto* children = loadPointerField(
      builder,
      output,
      kRowOutputChildrenOffset,
      i8PtrTy->getPointerTo(),
      "row_output_children");
  auto* childAddr =
      builder.CreateConstInBoundsGEP1_64(i8PtrTy, children, field);
  return builder.CreateLoad(i8PtrTy, childAddr, "row_output_child");
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
  auto* isNullBool = isNull->getType()->isIntegerTy(1)
      ? isNull
      : builder.CreateICmpNE(isNull, builder.getInt8(0));
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

llvm::Value*
isInputNull(llvm::IRBuilder<>& builder, llvm::Value* nulls, llvm::Value* row) {
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

} // namespace

HashAggrJitCodegen::HashAggrJitCodegen(llvm::Module& module) : module_(module) {
  ensureBuiltinDeclarations(module_);
}

llvm::Type* HashAggrJitCodegen::llvmType(HashAggrJitValueKind kind) const {
  return ::bytedance::bolt::jit::llvmType(builder(), kind);
}

llvm::Value* HashAggrJitCodegen::isInputNull(
    llvm::Value* nulls,
    llvm::Value* row) const {
  return ::bytedance::bolt::jit::isInputNull(builder(), nulls, row);
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

ScalarInputAdapterCodegen::ScalarInputAdapterCodegen(
    HashAggrJitCodegen& codegen,
    llvm::Value* input)
    : codegen_(codegen), input_(input) {}

llvm::StructType* ScalarInputAdapterCodegen::irRowType(
    HashAggrJitValueKind kind) const {
  return IRRow::getType(codegen_.builder(), codegen_.llvmType(kind));
}

llvm::Value* ScalarInputAdapterCodegen::read(
    llvm::Value* row,
    HashAggrJitValueKind kind) const {
  auto* value = ::bytedance::bolt::jit::loadScalarInputValue(
      codegen_.builder(), input_, row, kind);
  // add_dense emits the top-level null guard before invoking aggregate ops.
  // Therefore rows reaching ops are non-null; keep the IRRow contract explicit
  // without duplicating the null bitmap check in every aggregate.
  return IRRow::pack(codegen_.builder(), value, codegen_.builder().getFalse());
}

llvm::Value* ScalarInputAdapterCodegen::loadNulls() const {
  return ::bytedance::bolt::jit::loadScalarInputNulls(
      codegen_.builder(), input_);
}

llvm::Value* ScalarInputAdapterCodegen::isNull(llvm::Value* row) const {
  return codegen_.isInputNull(loadNulls(), row);
}

llvm::Value* ScalarInputAdapterCodegen::readRowField(
    llvm::Value*,
    int32_t,
    HashAggrJitValueKind) const {
  BOLT_UNSUPPORTED("ScalarInputAdapterCodegen does not support ROW field load");
}

RowInputAdapterCodegen::RowInputAdapterCodegen(
    HashAggrJitCodegen& codegen,
    llvm::Value* input)
    : codegen_(codegen), input_(input) {}

llvm::Value* RowInputAdapterCodegen::loadChild(int32_t field) const {
  return ::bytedance::bolt::jit::loadRowInputChild(
      codegen_.builder(), input_, field);
}

llvm::StructType* RowInputAdapterCodegen::irRowType(
    HashAggrJitValueKind kind) const {
  return IRRow::getType(codegen_.builder(), codegen_.llvmType(kind));
}

llvm::Value* RowInputAdapterCodegen::read(llvm::Value*, HashAggrJitValueKind)
    const {
  BOLT_UNSUPPORTED("RowInputAdapterCodegen does not support scalar loadValue");
}

llvm::Value* RowInputAdapterCodegen::loadNulls() const {
  return ::bytedance::bolt::jit::loadRowInputNulls(codegen_.builder(), input_);
}

llvm::Value* RowInputAdapterCodegen::isNull(llvm::Value* row) const {
  return codegen_.isInputNull(loadNulls(), row);
}

llvm::Value* RowInputAdapterCodegen::readRowField(
    llvm::Value* row,
    int32_t field,
    HashAggrJitValueKind kind) const {
  auto* child = loadChild(field);
  auto* value = ::bytedance::bolt::jit::loadScalarInputValue(
      codegen_.builder(), child, row, kind);
  return IRRow::pack(codegen_.builder(), value, isRowFieldNull(row, field));
}

llvm::Value* RowInputAdapterCodegen::isRowFieldNull(
    llvm::Value* row,
    int32_t field) const {
  auto* child = loadChild(field);
  auto* nulls =
      ::bytedance::bolt::jit::loadScalarInputNulls(codegen_.builder(), child);
  auto* hasNulls = codegen_.builder().CreateICmpNE(
      nulls,
      llvm::ConstantPointerNull::get(
          codegen_.builder().getInt64Ty()->getPointerTo()));
  auto* function = codegen_.builder().GetInsertBlock()->getParent();
  auto* nullCheckBlock = llvm::BasicBlock::Create(
      codegen_.module().getContext(), "row_field_null_check", function);
  auto* doneBlock = llvm::BasicBlock::Create(
      codegen_.module().getContext(), "row_field_null_done", function);
  codegen_.builder().CreateCondBr(hasNulls, nullCheckBlock, doneBlock);
  auto* noNullsEnd = codegen_.builder().GetInsertBlock();

  codegen_.builder().SetInsertPoint(nullCheckBlock);
  auto* index = ::bytedance::bolt::jit::loadScalarInputIndex(
      codegen_.builder(), child, row);
  auto* isNull = codegen_.isInputNull(nulls, index);
  codegen_.builder().CreateBr(doneBlock);
  auto* nullCheckEnd = codegen_.builder().GetInsertBlock();

  codegen_.builder().SetInsertPoint(doneBlock);
  auto* result = codegen_.builder().CreatePHI(
      codegen_.builder().getInt1Ty(), 2, "row_field_is_null");
  result->addIncoming(codegen_.builder().getFalse(), noNullsEnd);
  result->addIncoming(isNull, nullCheckEnd);
  return result;
}

ScalarOutputAdapterCodegen::ScalarOutputAdapterCodegen(
    HashAggrJitCodegen& codegen,
    llvm::Value* output)
    : codegen_(codegen), output_(output) {}

llvm::Value* ScalarOutputAdapterCodegen::vector() const {
  return ::bytedance::bolt::jit::loadScalarOutputVector(
      codegen_.builder(), output_);
}

void ScalarOutputAdapterCodegen::resize(llvm::Value* size) const {
  codegen_.builder().CreateCall(
      codegen_.module().getFunction("jit_HashAggrResizeVector"),
      {vector(), size});
}

void ScalarOutputAdapterCodegen::write(
    llvm::Value* row,
    HashAggrJitValueKind kind,
    llvm::Value* irRow) const {
  auto* value = IRRow::getValue(codegen_.builder(), irRow);
  auto* isNull = IRRow::getIsNull(codegen_.builder(), irRow);
  if (supportsRawFlatOutput(kind)) {
    auto* type = codegen_.llvmType(kind);
    auto* values = ::bytedance::bolt::jit::loadScalarOutputValues(
        codegen_.builder(), output_);
    auto* typedValues = codegen_.builder().CreatePointerCast(
        values, type->getPointerTo());
    auto* valueAddr = codegen_.builder().CreateInBoundsGEP(
        type,
        typedValues,
        codegen_.builder().CreateZExt(row, codegen_.builder().getInt64Ty()));
    auto* store = codegen_.builder().CreateStore(value, valueAddr);
    store->setAlignment(llvm::Align(1));
    auto* nulls = ::bytedance::bolt::jit::loadScalarOutputNulls(
        codegen_.builder(), output_);
    ::bytedance::bolt::jit::emitOutputNullBit(
        codegen_.builder(), nulls, row, isNull);
    return;
  }
}

void ScalarOutputAdapterCodegen::writeField(
    llvm::Value*,
    int32_t,
    HashAggrJitValueKind,
    llvm::Value*) const {
  BOLT_UNSUPPORTED("ScalarOutputAdapterCodegen does not support ROW field write");
}

void ScalarOutputAdapterCodegen::writeNull(
    llvm::Value* row,
    llvm::Value* isNull) const {
  auto* nulls = ::bytedance::bolt::jit::loadScalarOutputNulls(
      codegen_.builder(), output_);
  ::bytedance::bolt::jit::emitOutputNullBit(
      codegen_.builder(), nulls, row, isNull);
}

RowOutputAdapterCodegen::RowOutputAdapterCodegen(
    HashAggrJitCodegen& codegen,
    llvm::Value* output)
    : codegen_(codegen), output_(output) {}

llvm::Value* RowOutputAdapterCodegen::loadChild(int32_t field) const {
  return ::bytedance::bolt::jit::loadRowOutputChild(
      codegen_.builder(), output_, field);
}

llvm::Value* RowOutputAdapterCodegen::vector() const {
  return ::bytedance::bolt::jit::loadRowOutputVector(codegen_.builder(), output_);
}

void RowOutputAdapterCodegen::resize(llvm::Value* size) const {
  codegen_.builder().CreateCall(
      codegen_.module().getFunction("jit_HashAggrResizeVector"),
      {vector(), size});
}

void RowOutputAdapterCodegen::write(
    llvm::Value*,
    HashAggrJitValueKind,
    llvm::Value*) const {
  BOLT_UNSUPPORTED("RowOutputAdapterCodegen does not support scalar write");
}

void RowOutputAdapterCodegen::writeField(
    llvm::Value* row,
    int32_t field,
    HashAggrJitValueKind kind,
    llvm::Value* irRow) const {
  BOLT_CHECK(
      supportsRawFlatOutput(kind),
      "Unsupported raw ROW output field kind for HashAggrJit");
  auto* child = loadChild(field);
  auto* value = IRRow::getValue(codegen_.builder(), irRow);
  auto* isNull = IRRow::getIsNull(codegen_.builder(), irRow);
  auto* type = codegen_.llvmType(kind);
  auto* values = ::bytedance::bolt::jit::loadScalarOutputValues(
      codegen_.builder(), child);
  auto* typedValues =
      codegen_.builder().CreatePointerCast(values, type->getPointerTo());
  auto* row64 = codegen_.builder().CreateZExt(row, codegen_.builder().getInt64Ty());
  auto* valueAddr = codegen_.builder().CreateInBoundsGEP(type, typedValues, row64);
  auto* store = codegen_.builder().CreateStore(value, valueAddr);
  store->setAlignment(llvm::Align(1));
  auto* nulls = ::bytedance::bolt::jit::loadScalarOutputNulls(
      codegen_.builder(), child);
  ::bytedance::bolt::jit::emitOutputNullBit(
      codegen_.builder(), nulls, row, isNull);
}

void RowOutputAdapterCodegen::writeNull(
    llvm::Value* row,
    llvm::Value* isNull) const {
  auto* nulls = ::bytedance::bolt::jit::loadRowOutputNulls(
      codegen_.builder(), output_);
  ::bytedance::bolt::jit::emitOutputNullBit(
      codegen_.builder(), nulls, row, isNull);
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
  builder().CreateCall(
      module_.getFunction(fn),
      {output,
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
      slot.desc.auxPrecision > bytedance::bolt::ShortDecimalType::kMaxPrecision
          ? 1
          : 0);
  builder().CreateCall(
      module_.getFunction(fn),
      {output,
       row,
       group,
       builder().getInt32(slot.offset),
       builder().getInt32(slot.desc.precision),
       builder().getInt32(slot.desc.scale),
       builder().getInt32(slot.desc.auxPrecision),
       builder().getInt32(slot.desc.auxScale),
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

bool usesRowInputRuntime(const HashAggrJitSlot& slot) {
  return slot.desc.mergeInput &&
      (slot.desc.kind == HashAggrJitKind::Avg ||
       (slot.desc.kind == HashAggrJitKind::Sum && slot.desc.decimal));
}

bool usesRowOutputRuntime(const HashAggrJitSlot& slot, bool partialOutput) {
  return partialOutput && slot.desc.kind == HashAggrJitKind::Avg;
}

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
  llvm::Value* inputRuntimes = &*argIt++;
  inputRuntimes->setName("input_runtimes");

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
    auto* inputAddr =
        builder.CreateConstInBoundsGEP1_64(i8PtrTy, inputRuntimes, i);
    auto* inputRuntime = builder.CreateLoad(i8PtrTy, inputAddr);
    std::unique_ptr<InputAdapterCodegen> input;
    if (usesRowInputRuntime(slot)) {
      input = std::make_unique<RowInputAdapterCodegen>(codegen, inputRuntime);
    } else {
      input =
          std::make_unique<ScalarInputAdapterCodegen>(codegen, inputRuntime);
    }
    if (checkInputNulls && !slot.desc.countStar) {
      auto* nulls = input->loadNulls();
      auto* nullCheckBlock =
          llvm::BasicBlock::Create(context, "slot_null_check", func, end);
      auto* hasNulls = builder.CreateICmpNE(
          nulls,
          llvm::ConstantPointerNull::get(builder.getInt64Ty()->getPointerTo()));
      builder.CreateCondBr(hasNulls, nullCheckBlock, updateBlock);

      builder.SetInsertPoint(nullCheckBlock);
      auto* isNull = codegen.isInputNull(nulls, row);
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
    addFn(codegen, group, *input, row, slot, checkInputNulls, nextBlock);
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
    auto* outputAddr = builder.CreateConstInBoundsGEP1_64(i8PtrTy, resultVectors, i);
    auto* outputRuntime = builder.CreateLoad(i8PtrTy, outputAddr);
    std::unique_ptr<OutputAdapterCodegen> output;
    if (usesRowOutputRuntime(slots[i], partialOutput)) {
      output = std::make_unique<RowOutputAdapterCodegen>(codegen, outputRuntime);
    } else {
      output =
          std::make_unique<ScalarOutputAdapterCodegen>(codegen, outputRuntime);
    }
    output->resize(numGroups);
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
    auto* outputAddr = builder.CreateConstInBoundsGEP1_64(i8PtrTy, resultVectors, i);
    auto* outputRuntime = builder.CreateLoad(i8PtrTy, outputAddr);
    std::unique_ptr<OutputAdapterCodegen> output;
    if (usesRowOutputRuntime(slot, partialOutput)) {
      output = std::make_unique<RowOutputAdapterCodegen>(codegen, outputRuntime);
    } else {
      output =
          std::make_unique<ScalarOutputAdapterCodegen>(codegen, outputRuntime);
    }
    if (slot.desc.ops->extract == nullptr) {
      return false;
    }
    slot.desc.ops->extract(
        codegen, group, slot, HashAggrJitExtractTarget{*output, row, partialOutput});
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
    out << "_" << (slot.desc.ops != nullptr ? slot.desc.ops->id : "unknown")
        << "_" << static_cast<int>(slot.desc.kind)
        << hashAggrJitValueKindName(slot.desc.inputKind)
        << hashAggrJitValueKindName(slot.desc.accumulatorKind) << "o"
        << slot.offset << "n" << slot.nullByte << "m"
        << static_cast<int>(slot.nullMask) << (slot.desc.countStar ? "s" : "x")
        << (slot.desc.mergeInput ? "g" : "r")
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
