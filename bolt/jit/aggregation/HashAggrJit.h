#pragma once

#ifdef ENABLE_BOLT_JIT

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Module.h>

#include "bolt/jit/CompiledModule.h"
#include "bolt/jit/aggregation/HashAggrJitTypes.h"
#include "bolt/type/Type.h"

namespace bytedance::bolt::jit {

class HashAggrJitCodegen;
struct HashAggrJitExtractTarget;

struct HashAggrJitOps {
  using CreateFn =
      void (*)(HashAggrJitCodegen&, llvm::Value* group, const HashAggrJitSlot&);
  using AddFn = void (*)(
      HashAggrJitCodegen&,
      llvm::Value* group,
      llvm::Value* decoded,
      llvm::Value* row,
      const HashAggrJitSlot&,
      bool checkInputNulls,
      llvm::BasicBlock* nextBlock);
  using CanExtractFn = bool (*)(const HashAggrJitSlot&, bool partialOutput);
  using ExtractFn = void (*)(
      HashAggrJitCodegen&,
      llvm::Value* group,
      const HashAggrJitSlot&,
      const HashAggrJitExtractTarget&);

  const char* id;
  CreateFn initGroup;
  AddFn addRawInput;
  AddFn addIntermediateResults;
  CanExtractFn canExtract;
  ExtractFn extract;
};

struct HashAggrJitExtractTarget {
  llvm::Value* resultVector;
  llvm::Value* row;
  bool partialOutput;
};

class HashAggrJitCodegen {
 public:
  explicit HashAggrJitCodegen(llvm::Module& module);

  llvm::Module& module() const {
    return module_;
  }

  llvm::IRBuilder<>& builder() const {
    return *builder_;
  }

  void setBuilder(llvm::IRBuilder<>* builder) {
    builder_ = builder;
  }

  llvm::Type* llvmType(HashAggrJitValueKind kind) const;
  llvm::Value* loadDecodedValue(
      llvm::Value* decoded,
      llvm::Value* row,
      const HashAggrJitSlot& slot) const;
  llvm::Value* loadDecodedNulls(llvm::Value* decoded) const;
  llvm::Value* isDecodedNull(llvm::Value* nulls, llvm::Value* row) const;
  llvm::Value* isAccumulatorNull(
      llvm::Value* group,
      const HashAggrJitSlot& slot) const;
  void clearAccumulatorNull(llvm::Value* group, const HashAggrJitSlot& slot)
      const;
  void setAccumulatorNull(llvm::Value* group, const HashAggrJitSlot& slot)
      const;
  llvm::LoadInst* loadValue(llvm::Value* row, llvm::Type* type, int32_t offset)
      const;
  void storeValue(
      llvm::Value* row,
      llvm::Type* type,
      int32_t offset,
      llvm::Value* value) const;
  llvm::Value* castValue(
      llvm::Value* value,
      HashAggrJitValueKind from,
      HashAggrJitValueKind to) const;
  bool isFloatKind(HashAggrJitValueKind kind) const;
  llvm::Value* loadDecodedRowField(
      llvm::Value* decoded,
      llvm::Value* row,
      int32_t field,
      HashAggrJitValueKind kind) const;
  llvm::Value* isDecodedRowFieldNull(
      llvm::Value* decoded,
      llvm::Value* row,
      int32_t field) const;
  void emitFlatValue(
      llvm::Value* vector,
      llvm::Value* row,
      HashAggrJitValueKind kind,
      llvm::Value* value,
      llvm::Value* isNull) const;
  void resizeResultVector(llvm::Value* vector, llvm::Value* size) const;
  void emitPartialAvgResult(
      llvm::Value* vector,
      llvm::Value* row,
      llvm::Value* sum,
      llvm::Value* count,
      llvm::Value* isNull) const;
  // Decimal extract: calls a runtime helper that reads the JIT decimal
  // accumulator from 'group + slot.offset', applies overflow/precision checks
  // and writes the result (final flat decimal / partial row) into 'vector'.
  void emitDecimalSumExtract(
      llvm::Value* vector,
      llvm::Value* row,
      llvm::Value* group,
      const HashAggrJitSlot& slot,
      bool partialOutput) const;
  void emitDecimalAvgExtract(
      llvm::Value* vector,
      llvm::Value* row,
      llvm::Value* group,
      const HashAggrJitSlot& slot,
      bool partialOutput) const;

 private:
  llvm::Module& module_;
  llvm::IRBuilder<>* builder_{nullptr};
};

using HashAggrJitAddDenseFunc = void (*)(char** groups, int32_t numRows, char** decodedInputs);
using HashAggrJitInitFunc = void (*)(char** newGroups, int32_t numNewGroups);
using HashAggrJitExtractFunc = void (*)(char** groups, int32_t numGroups, char** resultVectors);

class HashAggrJitChunk {
 public:
  explicit HashAggrJitChunk(
      std::vector<HashAggrJitSlot> slots,
      bool partialOutput = false);

  bool codegen();

  bool enabled() const {
    return addDense_ != nullptr && !disabled_;
  }

  bool canExtract() const;

  void disable() {
    disabled_ = true;
  }

  void init(char** newGroups, int32_t numNewGroups) const {
    init_(newGroups, numNewGroups);
  }

  void addDense(
      char** groups,
      int32_t numRows,
      char** decodedInputs,
      bool inputsMayHaveNulls) const {
    if (!inputsMayHaveNulls && addDenseNoNull_ != nullptr) {
      addDenseNoNull_(groups, numRows, decodedInputs);
      return;
    }
    addDense_(groups, numRows, decodedInputs);
  }

  void extract(char** groups, int32_t numGroups, char** resultVectors) const {
    extract_(groups, numGroups, resultVectors);
  }

  const std::vector<HashAggrJitSlot>& slots() const {
    return slots_;
  }

  std::string functionName() const;
  std::string initFunctionName() const;
  std::string addDenseNoNullFunctionName() const;
  std::string extractFunctionName() const;

 private:
  std::vector<HashAggrJitSlot> slots_;
  bool partialOutput_{false};
  CompiledModuleSP module_;
  HashAggrJitInitFunc init_{nullptr};
  HashAggrJitAddDenseFunc addDense_{nullptr};
  HashAggrJitAddDenseFunc addDenseNoNull_{nullptr};
  HashAggrJitExtractFunc extract_{nullptr};
  bool disabled_{false};
};

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
