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
#include "bolt/type/Type.h"

namespace bytedance::bolt::jit {

class HashAggrJitCodegen;
struct HashAggrJitSlot;
struct HashAggrJitExtractTarget;

struct HashAggrJitPlanContext {
  bool isRawInput{false};
  bool isPartialOutput{false};
  int32_t inputCount{0};
  TypePtr inputType;

  bool isCountStar() const {
    return isRawInput && inputCount == 0;
  }
};

enum class HashAggrJitKind : uint8_t {
  Count,
  Sum,
  Min,
  Max,
  Avg,
};

enum class HashAggrJitValueKind : uint8_t {
  Bool,
  Int8,
  Int16,
  Int32,
  Int64,
  Int128,
  Float,
  Double,
};

struct HashAggrJitDescriptor {
  HashAggrJitKind kind;
  HashAggrJitValueKind inputKind;
  HashAggrJitValueKind accumulatorKind;
  bool countStar{false};
  bool mergeInput{false};
  bool decimal{false};
  const struct HashAggrJitOps* ops{nullptr};

  std::string signature() const;
};

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
  CreateFn create;
  AddFn add;
  CanExtractFn canExtract;
  ExtractFn extract;
};

struct HashAggrJitSlot {
  int32_t aggregateIndex;
  HashAggrJitKind kind;
  HashAggrJitValueKind inputKind;
  HashAggrJitValueKind accumulatorKind;
  int32_t offset;
  int32_t nullByte;
  uint8_t nullMask;
  bool countStar{false};
  bool mergeInput{false};
  bool decimal{false};
  const HashAggrJitOps* ops{nullptr};
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

bool isHashAggrJitSupportedType(TypeKind kind);
std::optional<HashAggrJitValueKind> hashAggrJitValueKind(TypeKind kind);
std::string hashAggrJitValueKindName(HashAggrJitValueKind kind);

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
