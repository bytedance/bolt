#ifdef ENABLE_BOLT_JIT

#include "bolt/jit/aggregation/HashAggrJit.h"

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Verifier.h>

#include <cmath>
#include <sstream>

#include <fmt/format.h>

#include "bolt/jit/ThrustJITv2.h"

extern "C" {

namespace {

struct JitDecimalSumState {
  bytedance::bolt::int128_t sum{0};
  int64_t overflow{0};
  bool isEmpty{true};
};

struct JitDecimalAvgState {
  bytedance::bolt::int128_t sum{0};
  int64_t count{0};
  int64_t overflow{0};
};

int64_t jitHashAggrAddWithOverflow(
    bytedance::bolt::int128_t left,
    bytedance::bolt::int128_t right,
    bytedance::bolt::int128_t& result) {
  result = left + right;
  if (left > 0 && right > 0 && result < 0) {
    return 1;
  }
  if (left < 0 && right < 0 && result >= 0) {
    return -1;
  }
  return 0;
}

} // namespace

__attribute__((__visibility__("default"))) void jit_HashAggrInitDecimalSum(
    char* group,
    int32_t offset) {
  new (group + offset) JitDecimalSumState();
}

__attribute__((__visibility__("default"))) void jit_HashAggrInitDecimalAvg(
    char* group,
    int32_t offset) {
  new (group + offset) JitDecimalAvgState();
}

__attribute__((__visibility__("default"))) void jit_HashAggrUpdateDecimalSumI64(
    char* group,
    int32_t offset,
    int64_t value) {
  auto* state = reinterpret_cast<JitDecimalSumState*>(group + offset);
  state->overflow += jitHashAggrAddWithOverflow(
      state->sum, static_cast<bytedance::bolt::int128_t>(value), state->sum);
  state->isEmpty = false;
}

__attribute__((__visibility__("default"))) void jit_HashAggrUpdateDecimalSumI128(
    char* group,
    int32_t offset,
    bytedance::bolt::int128_t value) {
  auto* state = reinterpret_cast<JitDecimalSumState*>(group + offset);
  state->overflow += jitHashAggrAddWithOverflow(state->sum, value, state->sum);
  state->isEmpty = false;
}

__attribute__((__visibility__("default"))) void jit_HashAggrUpdateDecimalAvgI64(
    char* group,
    int32_t offset,
    int64_t value) {
  auto* state = reinterpret_cast<JitDecimalAvgState*>(group + offset);
  state->overflow += jitHashAggrAddWithOverflow(
      state->sum, static_cast<bytedance::bolt::int128_t>(value), state->sum);
  ++state->count;
}

__attribute__((__visibility__("default"))) void jit_HashAggrUpdateDecimalAvgI128(
    char* group,
    int32_t offset,
    bytedance::bolt::int128_t value) {
  auto* state = reinterpret_cast<JitDecimalAvgState*>(group + offset);
  state->overflow += jitHashAggrAddWithOverflow(state->sum, value, state->sum);
  ++state->count;
}

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
      module, "jit_GetDecodedRowFieldI64", i64Ty, {i8PtrTy, i32Ty, i32Ty});
  declareFunction(module, "jit_GetDecodedIsNull", i8Ty, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_HashAggrInitDecimalSum", voidTy, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_HashAggrInitDecimalAvg", voidTy, {i8PtrTy, i32Ty});
  declareFunction(
      module, "jit_HashAggrUpdateDecimalSumI64", voidTy, {i8PtrTy, i32Ty, i64Ty});
  declareFunction(
      module, "jit_HashAggrUpdateDecimalSumI128", voidTy, {i8PtrTy, i32Ty, i128Ty});
  declareFunction(
      module, "jit_HashAggrUpdateDecimalAvgI64", voidTy, {i8PtrTy, i32Ty, i64Ty});
  declareFunction(
      module, "jit_HashAggrUpdateDecimalAvgI128", voidTy, {i8PtrTy, i32Ty, i128Ty});
  declareFunction(module, "jit_HashAggrResizeVector", voidTy, {i8PtrTy, i32Ty});
  declareFunction(module, "jit_HashAggrSetFlatI8", voidTy, {i8PtrTy, i32Ty, i8Ty, i8Ty});
  declareFunction(module, "jit_HashAggrSetFlatI16", voidTy, {i8PtrTy, i32Ty, i16Ty, i8Ty});
  declareFunction(module, "jit_HashAggrSetFlatI32", voidTy, {i8PtrTy, i32Ty, i32Ty, i8Ty});
  declareFunction(module, "jit_HashAggrSetFlatI64", voidTy, {i8PtrTy, i32Ty, i64Ty, i8Ty});
  declareFunction(module, "jit_HashAggrSetFlatFloat", voidTy, {i8PtrTy, i32Ty, floatTy, i8Ty});
  declareFunction(module, "jit_HashAggrSetFlatDouble", voidTy, {i8PtrTy, i32Ty, doubleTy, i8Ty});
  declareFunction(
      module,
      "jit_HashAggrSetPartialAvgDouble",
      voidTy,
      {i8PtrTy, i32Ty, doubleTy, i64Ty, i8Ty});
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
    llvm::Module& module,
    llvm::Value* group,
    llvm::Value* rawValue,
    const HashAggrJitSlot& slot) {
  auto* accType = llvmType(builder, slot.accumulatorKind);
  auto* value = castValue(builder, rawValue, slot.inputKind, slot.accumulatorKind);
  if (slot.decimal) {
    clearAccumulatorNull(builder, group, slot);
    const auto helper = slot.kind == HashAggrJitKind::Sum
        ? (slot.inputKind == HashAggrJitValueKind::Int128
               ? "jit_HashAggrUpdateDecimalSumI128"
               : "jit_HashAggrUpdateDecimalSumI64")
        : (slot.inputKind == HashAggrJitValueKind::Int128
               ? "jit_HashAggrUpdateDecimalAvgI128"
               : "jit_HashAggrUpdateDecimalAvgI64");
    builder.CreateCall(
        module.getFunction(helper),
        {group,
         builder.getInt32(slot.offset),
         slot.inputKind == HashAggrJitValueKind::Int128 ? value : rawValue});
    return;
  }
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
      llvm::Value* better;
      if (isFloatKind(slot.accumulatorKind)) {
        auto* oldIsNan = builder.CreateFCmpUNO(oldValue, oldValue);
        auto* valueIsNan = builder.CreateFCmpUNO(value, value);
        if (slot.kind == HashAggrJitKind::Min) {
          better = builder.CreateOr(
              builder.CreateAnd(oldIsNan, builder.CreateNot(valueIsNan)),
              builder.CreateAnd(
                  builder.CreateNot(valueIsNan),
                  builder.CreateFCmpOGT(oldValue, value)));
        } else {
          better = builder.CreateAnd(
              builder.CreateNot(oldIsNan),
              builder.CreateOr(valueIsNan, builder.CreateFCmpOLT(oldValue, value)));
        }
      } else {
        better = slot.kind == HashAggrJitKind::Min
            ? builder.CreateICmpSLT(value, oldValue)
            : builder.CreateICmpSGT(value, oldValue);
      }
      auto* shouldStore = builder.CreateOr(nullState, better);
      auto* selected = builder.CreateSelect(shouldStore, value, oldValue);
      storeValue(builder, group, accType, slot.offset, selected);
      clearAccumulatorNull(builder, group, slot);
      break;
    }
    case HashAggrJitKind::Count:
      if (slot.mergeInput) {
        auto* state = loadValue(builder, group, builder.getInt64Ty(), slot.offset);
        storeValue(
            builder,
            group,
            builder.getInt64Ty(),
            slot.offset,
            builder.CreateAdd(state, castValue(builder, rawValue, slot.inputKind, HashAggrJitValueKind::Int64)));
      } else {
        genCountUpdate(builder, group, slot);
      }
      break;
  }
}

void genAvgMergeUpdate(
    llvm::IRBuilder<>& builder,
    llvm::Module& module,
    llvm::Value* group,
    llvm::Value* decoded,
    llvm::Value* row,
    const HashAggrJitSlot& slot) {
  clearAccumulatorNull(builder, group, slot);
  auto* sum = builder.CreateCall(
      module.getFunction("jit_GetDecodedRowFieldDouble"),
      {decoded, row, builder.getInt32(0)});
  auto* count = builder.CreateCall(
      module.getFunction("jit_GetDecodedRowFieldI64"),
      {decoded, row, builder.getInt32(1)});

  auto* oldSum = loadValue(builder, group, builder.getDoubleTy(), slot.offset);
  storeValue(
      builder,
      group,
      builder.getDoubleTy(),
      slot.offset,
      builder.CreateFAdd(oldSum, sum));

  auto* oldCount = loadValue(builder, group, builder.getInt64Ty(), slot.offset + 8);
  storeValue(
      builder,
      group,
      builder.getInt64Ty(),
      slot.offset + 8,
      builder.CreateAdd(oldCount, count));
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
  ensureBuiltinDeclarations(module);
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
    if (slot.decimal) {
      builder.CreateCall(
          module.getFunction(
              slot.kind == HashAggrJitKind::Sum ? "jit_HashAggrInitDecimalSum"
                                                : "jit_HashAggrInitDecimalAvg"),
          {group, builder.getInt32(slot.offset)});
      continue;
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
    if (slot.kind == HashAggrJitKind::Count && !slot.mergeInput) {
      genCountUpdate(builder, group, slot);
    } else if (slot.kind == HashAggrJitKind::Avg && slot.mergeInput) {
      genAvgMergeUpdate(builder, module, group, decoded, row, slot);
    } else {
      auto* value = loadDecodedValue(builder, module, decoded, row, slot);
      genNonNullUpdate(builder, module, group, value, slot);
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
  llvm::Value* numGroups = &*argIt++;
  numGroups->setName("num_groups");
  llvm::Value* resultVectors = &*argIt++;
  resultVectors->setName("result_vectors");

  auto* entry = llvm::BasicBlock::Create(context, "entry", func);
  auto* loop = llvm::BasicBlock::Create(context, "loop", func);
  auto* end = llvm::BasicBlock::Create(context, "end", func);
  builder.SetInsertPoint(entry);
  for (auto i = 0; i < slots.size(); ++i) {
    if (slots[i].decimal || slots[i].accumulatorKind == HashAggrJitValueKind::Int128) {
      continue;
    }
    auto* vectorAddr = builder.CreateConstInBoundsGEP1_64(i8PtrTy, resultVectors, i);
    auto* vector = builder.CreateLoad(i8PtrTy, vectorAddr);
    builder.CreateCall(module.getFunction("jit_HashAggrResizeVector"), {vector, numGroups});
  }
  builder.CreateCondBr(builder.CreateICmpSLE(numGroups, builder.getInt32(0)), end, loop);

  builder.SetInsertPoint(loop);
  auto* row = builder.CreatePHI(i32Ty, 2, "row");
  row->addIncoming(builder.getInt32(0), entry);
  auto* groupAddr = builder.CreateInBoundsGEP(i8PtrTy, groups, row);
  auto* group = builder.CreateLoad(i8PtrTy, groupAddr);

  for (auto i = 0; i < slots.size(); ++i) {
    const auto& slot = slots[i];
    if (slot.decimal || slot.accumulatorKind == HashAggrJitValueKind::Int128) {
      continue;
    }
    auto* vectorAddr = builder.CreateConstInBoundsGEP1_64(i8PtrTy, resultVectors, i);
    auto* vector = builder.CreateLoad(i8PtrTy, vectorAddr);
    HashAggrJitValueKind resultKind = slot.accumulatorKind;
    llvm::Value* value = nullptr;
    llvm::Value* isNull = nullptr;
    if (partialOutput && slot.kind == HashAggrJitKind::Avg) {
      auto* sum = loadValue(
          builder, group, llvmType(builder, slot.accumulatorKind), slot.offset);
      auto* count =
          loadValue(builder, group, builder.getInt64Ty(), slot.offset + 8);
      auto* isNullValue = builder.CreateZExt(
          isAccumulatorNull(builder, group, slot), builder.getInt8Ty());
      builder.CreateCall(
          module.getFunction("jit_HashAggrSetPartialAvgDouble"),
          {vector, row, sum, count, isNullValue});
      continue;
    }
    if (slot.kind == HashAggrJitKind::Avg) {
      auto* sum = loadValue(builder, group, llvmType(builder, slot.accumulatorKind), slot.offset);
      auto* count = loadValue(builder, group, builder.getInt64Ty(), slot.offset + 8);
      auto* countIsZero = builder.CreateICmpEQ(count, builder.getInt64(0));
      auto* divisor = builder.CreateSIToFP(count, llvmType(builder, slot.accumulatorKind));
      value = builder.CreateFDiv(sum, divisor);
      isNull = builder.CreateZExt(countIsZero, builder.getInt8Ty());
    } else {
      value = loadValue(builder, group, llvmType(builder, resultKind), slot.offset);
      isNull = slot.kind == HashAggrJitKind::Count
          ? builder.getInt8(0)
          : builder.CreateZExt(isAccumulatorNull(builder, group, slot), builder.getInt8Ty());
    }
    const auto setter = setFlatValueFunction(resultKind);
    if (setter.empty()) {
      continue;
    }
    builder.CreateCall(module.getFunction(setter), {vector, row, value, isNull});
  }

  auto* next = builder.CreateAdd(row, builder.getInt32(1));
  row->addIncoming(next, builder.GetInsertBlock());
  builder.CreateCondBr(builder.CreateICmpSLT(next, numGroups), loop, end);

  builder.SetInsertPoint(end);
  builder.CreateRetVoid();

  return llvm::verifyFunction(*func, &llvm::errs());
}

} // namespace

HashAggrJitChunk::HashAggrJitChunk(
    std::vector<HashAggrJitSlot> slots,
    bool partialOutput)
    : slots_(std::move(slots)), partialOutput_(partialOutput) {}

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
      "{}_{}_{}_{}_{}",
      static_cast<int>(kind),
      hashAggrJitValueKindName(inputKind),
      hashAggrJitValueKindName(accumulatorKind),
      mergeInput,
      decimal);
}

std::string HashAggrJitChunk::functionName() const {
  std::ostringstream out;
  out << "jit_hashaggr_v2_" << (partialOutput_ ? "partial" : "final") << "_n"
      << slots_.size();
  for (const auto& slot : slots_) {
    out << "_" << static_cast<int>(slot.kind) << hashAggrJitValueKindName(slot.inputKind)
        << hashAggrJitValueKindName(slot.accumulatorKind) << "o" << slot.offset
        << "n" << slot.nullByte << "m" << static_cast<int>(slot.nullMask)
        << (slot.countStar ? "s" : "x") << (slot.mergeInput ? "g" : "r")
        << (slot.decimal ? "d" : "n");
  }
  return out.str();
}

bool HashAggrJitChunk::canExtract() const {
  if (extract_ == nullptr || disabled_) {
    return false;
  }
  for (const auto& slot : slots_) {
    if (slot.decimal || slot.accumulatorKind == HashAggrJitValueKind::Int128) {
      return false;
    }
  }
  return true;
}

std::string HashAggrJitChunk::initFunctionName() const {
  return functionName() + "_init";
}

std::string HashAggrJitChunk::addDenseNoNullFunctionName() const {
  return functionName() + "_add_dense_no_null";
}

std::string HashAggrJitChunk::extractFunctionName() const {
  return functionName() + "_extract";
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
  const auto extractFn = extractFunctionName();
  module_ = jit->CompileModule(
      [&](llvm::Module& module) {
        return genInitIR(module, initFn, slots_) ||
            genAddDenseIR(module, addFn, slots_, true) ||
            genAddDenseIR(module, addNoNullFn, slots_, false) ||
            genExtractIR(module, extractFn, slots_, partialOutput_);
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
  extract_ = reinterpret_cast<HashAggrJitExtractFunc>(module_->getFuncPtr(extractFn));
  if (init_ == nullptr || addDense_ == nullptr || addDenseNoNull_ == nullptr ||
      extract_ == nullptr) {
    disabled_ = true;
    return false;
  }
  return true;
}

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
