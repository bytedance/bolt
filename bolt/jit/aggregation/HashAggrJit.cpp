#ifdef ENABLE_BOLT_JIT

#include "bolt/jit/aggregation/HashAggrJit.h"

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Verifier.h>

#include <sstream>

#include "bolt/jit/ThrustJITv2.h"

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
  auto* floatTy = llvm::Type::getFloatTy(context);
  auto* doubleTy = llvm::Type::getDoubleTy(context);
  auto* i8PtrTy = llvm::PointerType::get(context, 0);

  declareFunction(module, "jit_GetDecodedValueI8", i8Ty, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_GetDecodedValueI16", i16Ty, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_GetDecodedValueI32", i32Ty, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_GetDecodedValueI64", i64Ty, {i8PtrTy, i32Ty});
  declareFunction(
      module, "jit_GetDecodedValueFloat", floatTy, {i8PtrTy, i32Ty});
  declareFunction(
      module, "jit_GetDecodedValueDouble", doubleTy, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_GetDecodedIsNull", i8Ty, {i8PtrTy, i32Ty});
}

llvm::Type* llvmType(llvm::IRBuilder<>& builder, HashAggrJitValueKind kind) {
  switch (kind) {
    case HashAggrJitValueKind::Int8:
      return builder.getInt8Ty();
    case HashAggrJitValueKind::Int16:
      return builder.getInt16Ty();
    case HashAggrJitValueKind::Int32:
      return builder.getInt32Ty();
    case HashAggrJitValueKind::Int64:
      return builder.getInt64Ty();
    case HashAggrJitValueKind::Float:
      return builder.getFloatTy();
    case HashAggrJitValueKind::Double:
      return builder.getDoubleTy();
  }
  return builder.getInt64Ty();
}

std::string decodedValueFunction(HashAggrJitValueKind kind) {
  switch (kind) {
    case HashAggrJitValueKind::Int8:
      return "jit_GetDecodedValueI8";
    case HashAggrJitValueKind::Int16:
      return "jit_GetDecodedValueI16";
    case HashAggrJitValueKind::Int32:
      return "jit_GetDecodedValueI32";
    case HashAggrJitValueKind::Int64:
      return "jit_GetDecodedValueI64";
    case HashAggrJitValueKind::Float:
      return "jit_GetDecodedValueFloat";
    case HashAggrJitValueKind::Double:
      return "jit_GetDecodedValueDouble";
  }
  return "jit_GetDecodedValueI64";
}

bool isFloatKind(HashAggrJitValueKind kind) {
  return kind == HashAggrJitValueKind::Float ||
      kind == HashAggrJitValueKind::Double;
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
    llvm::Module& module,
    llvm::Value* decoded,
    llvm::Value* row,
    const HashAggrJitSlot& slot) {
  auto* callee = module.getFunction(decodedValueFunction(slot.inputKind));
  return builder.CreateCall(callee, {decoded, row});
}

void genCountUpdate(
    llvm::IRBuilder<>& builder,
    llvm::Value* group,
    const HashAggrJitSlot& slot) {
  auto* state = loadValue(builder, group, builder.getInt64Ty(), slot.offset);
  storeValue(
      builder,
      group,
      builder.getInt64Ty(),
      slot.offset,
      builder.CreateAdd(state, builder.getInt64(1)));
}

void genNonNullUpdate(
    llvm::IRBuilder<>& builder,
    llvm::Value* group,
    llvm::Value* rawValue,
    const HashAggrJitSlot& slot) {
  auto* accType = llvmType(builder, slot.accumulatorKind);
  auto* value = castValue(builder, rawValue, slot.inputKind, slot.accumulatorKind);
  switch (slot.kind) {
    case HashAggrJitKind::Sum: {
      clearAccumulatorNull(builder, group, slot);
      auto* oldValue = loadValue(builder, group, accType, slot.offset);
      auto* newValue = isFloatKind(slot.accumulatorKind)
          ? builder.CreateFAdd(oldValue, value)
          : builder.CreateAdd(oldValue, value);
      storeValue(builder, group, accType, slot.offset, newValue);
      break;
    }
    case HashAggrJitKind::Avg: {
      clearAccumulatorNull(builder, group, slot);
      auto* oldSum = loadValue(builder, group, accType, slot.offset);
      auto* newSum = builder.CreateFAdd(oldSum, value);
      storeValue(builder, group, accType, slot.offset, newSum);
      auto* oldCount = loadValue(builder, group, builder.getInt64Ty(), slot.offset + 8);
      storeValue(
          builder,
          group,
          builder.getInt64Ty(),
          slot.offset + 8,
          builder.CreateAdd(oldCount, builder.getInt64(1)));
      break;
    }
    case HashAggrJitKind::Min:
    case HashAggrJitKind::Max: {
      auto* oldValue = loadValue(builder, group, accType, slot.offset);
      auto* nullState = isAccumulatorNull(builder, group, slot);
      auto* better = slot.kind == HashAggrJitKind::Min
          ? builder.CreateICmpSLT(value, oldValue)
          : builder.CreateICmpSGT(value, oldValue);
      auto* shouldStore = builder.CreateOr(nullState, better);
      auto* selected = builder.CreateSelect(shouldStore, value, oldValue);
      storeValue(builder, group, accType, slot.offset, selected);
      clearAccumulatorNull(builder, group, slot);
      break;
    }
    case HashAggrJitKind::Count:
      genCountUpdate(builder, group, slot);
      break;
  }
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
    if (slot.kind != HashAggrJitKind::Count) {
      setAccumulatorNull(builder, group, slot);
    }
    auto* accType = llvmType(builder, slot.accumulatorKind);
    if (isFloatKind(slot.accumulatorKind)) {
      storeValue(
          builder,
          group,
          accType,
          slot.offset,
          llvm::ConstantFP::get(accType, 0.0));
    } else {
      storeValue(
          builder,
          group,
          accType,
          slot.offset,
          llvm::ConstantInt::get(accType, 0));
    }
    if (slot.kind == HashAggrJitKind::Avg) {
      storeValue(
          builder,
          group,
          builder.getInt64Ty(),
          slot.offset + 8,
          builder.getInt64(0));
    }
  }

  auto* next = builder.CreateAdd(index, builder.getInt32(1));
  index->addIncoming(next, builder.GetInsertBlock());
  builder.CreateCondBr(builder.CreateICmpSLT(next, numNewGroups), loop, end);

  builder.SetInsertPoint(end);
  builder.CreateRetVoid();

  return llvm::verifyFunction(*func, &llvm::errs());
}

bool genAddDenseIR(
    llvm::Module& module,
    const std::string& fn,
    const std::vector<HashAggrJitSlot>& slots,
    bool checkInputNulls) {
  ensureBuiltinDeclarations(module);
  auto& context = module.getContext();
  llvm::IRBuilder<> builder(context);
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
    if (slot.kind == HashAggrJitKind::Count && slot.countStar) {
      genCountUpdate(builder, group, slot);
      continue;
    }

    auto* updateBlock = llvm::BasicBlock::Create(context, "slot_update", func, end);
    auto* nextBlock = llvm::BasicBlock::Create(context, "slot_next", func, end);
    auto* decodedAddr = builder.CreateConstInBoundsGEP1_64(i8PtrTy, decodedInputs, i);
    auto* decoded = builder.CreateLoad(i8PtrTy, decodedAddr);
    if (checkInputNulls) {
      auto* isNull = builder.CreateICmpNE(
          builder.CreateCall(module.getFunction("jit_GetDecodedIsNull"), {decoded, row}),
          builder.getInt8(0));
      builder.CreateCondBr(isNull, nextBlock, updateBlock);
    } else {
      builder.CreateBr(updateBlock);
    }

    builder.SetInsertPoint(updateBlock);
    if (slot.kind == HashAggrJitKind::Count) {
      genCountUpdate(builder, group, slot);
    } else {
      auto* value = loadDecodedValue(builder, module, decoded, row, slot);
      genNonNullUpdate(builder, group, value, slot);
    }
    builder.CreateBr(nextBlock);
    builder.SetInsertPoint(nextBlock);
  }

  auto* next = builder.CreateAdd(row, builder.getInt32(1));
  row->addIncoming(next, builder.GetInsertBlock());
  builder.CreateCondBr(builder.CreateICmpSLT(next, numRows), loop, end);

  builder.SetInsertPoint(end);
  builder.CreateRetVoid();

  return llvm::verifyFunction(*func, &llvm::errs());
}

} // namespace

HashAggrJitChunk::HashAggrJitChunk(std::vector<HashAggrJitSlot> slots)
    : slots_(std::move(slots)) {}

std::string hashAggrJitValueKindName(HashAggrJitValueKind kind) {
  switch (kind) {
    case HashAggrJitValueKind::Int8:
      return "i8";
    case HashAggrJitValueKind::Int16:
      return "i16";
    case HashAggrJitValueKind::Int32:
      return "i32";
    case HashAggrJitValueKind::Int64:
      return "i64";
    case HashAggrJitValueKind::Float:
      return "f32";
    case HashAggrJitValueKind::Double:
      return "f64";
  }
  return "unknown";
}

bool isHashAggrJitSupportedType(TypeKind kind) {
  switch (kind) {
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

std::string HashAggrJitChunk::functionName() const {
  std::ostringstream out;
  out << "jit_hashaggr_v2_n" << slots_.size();
  for (const auto& slot : slots_) {
    out << "_" << static_cast<int>(slot.kind) << hashAggrJitValueKindName(slot.inputKind)
        << hashAggrJitValueKindName(slot.accumulatorKind) << "o" << slot.offset
        << "n" << slot.nullByte << "m" << static_cast<int>(slot.nullMask)
        << (slot.countStar ? "s" : "x");
  }
  return out.str();
}

std::string HashAggrJitChunk::initFunctionName() const {
  return functionName() + "_init";
}

std::string HashAggrJitChunk::addDenseNoNullFunctionName() const {
  return functionName() + "_add_dense_no_null";
}

bool HashAggrJitChunk::codegen() {
  if (addDense_) {
    return true;
  }
  auto* jit = ThrustJITv2::getInstance();
  if (jit == nullptr) {
    return false;
  }
  const auto moduleKey = functionName();
  const auto initFn = initFunctionName();
  const auto addFn = moduleKey + "_add_dense";
  const auto addNoNullFn = addDenseNoNullFunctionName();
  module_ = jit->CompileModule(
      [&](llvm::Module& module) {
        return genInitIR(module, initFn, slots_) ||
            genAddDenseIR(module, addFn, slots_, true) ||
            genAddDenseIR(module, addNoNullFn, slots_, false);
      },
      moduleKey);
  if (!module_) {
    disabled_ = true;
    return false;
  }
  init_ = reinterpret_cast<HashAggrJitInitFunc>(module_->getFuncPtr(initFn));
  addDense_ = reinterpret_cast<HashAggrJitAddDenseFunc>(module_->getFuncPtr(addFn));
  addDenseNoNull_ = reinterpret_cast<HashAggrJitAddDenseFunc>(
      module_->getFuncPtr(addNoNullFn));
  if (init_ == nullptr || addDense_ == nullptr || addDenseNoNull_ == nullptr) {
    disabled_ = true;
    return false;
  }
  return true;
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
