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

// JIT-internal accumulator layouts for decimal sum/avg. Shared between the JIT
// codegen runtime helpers and the extract runtime helpers (which live in a
// different translation unit and need DecimalUtil).
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

struct HashAggrJitSlot;
struct HashAggrJitExtractTarget;

// Runtime input descriptor consumed by JIT add_dense functions.
// GroupingSet prepares one descriptor per aggregate input for each batch by
// decoding the original vector into a flat/constant base plus a single indices
// mapping. This keeps generated IR independent of the batch's original vector
// encoding (flat/dictionary/constant) while allowing the hot loop to load
// values directly instead of calling jit_GetDecodedValue* helpers per row.
struct HashAggrJitDecodedInput {
  const void* values{nullptr};
  // Always points to a top-level-row -> base-row mapping. For flat inputs this
  // is a consecutive mapping; for constant inputs it maps every row to the
  // constant value index.
  const int32_t* indices{nullptr};
  // Top-level nulls. If non-null, bit 'row' indicates whether the input row is
  // null. This is intentionally row-based rather than base-index-based to keep
  // generated IR independent of dictionary/null wrapping details.
  const uint64_t* nulls{nullptr};
  // Original DecodedVector pointer. Kept as fallback for row-field helpers.
  const void* decodedVector{nullptr};
  // Raw ROW child fields for intermediate avg merge inputs. The top-level
  // ROW may still be dictionary/constant wrapped; 'indices' maps rows to the
  // flat child row. Only the first two fields are needed by avg: sum, count.
  const void* rowField0Values{nullptr};
  const uint64_t* rowField0Nulls{nullptr};
  const void* rowField1Values{nullptr};
  const uint64_t* rowField1Nulls{nullptr};
};

// Runtime output descriptor consumed by JIT extract functions. GroupingSet
// prepares one descriptor per aggregate output after resizing the result vector.
// Primitive flat outputs write values/null bits directly from generated IR;
// complex outputs keep using vector helper fallbacks via 'vector'.
struct HashAggrJitOutput {
  void* values{nullptr};
  uint64_t* nulls{nullptr};
  void* vector{nullptr};
  // Raw ROW child fields for partial avg output: field 0 = sum(double),
  // field 1 = count(int64). Other outputs leave these null and use 'values'
  // or helper fallback via 'vector'.
  void* rowField0Values{nullptr};
  uint64_t* rowField0Nulls{nullptr};
  void* rowField1Values{nullptr};
  uint64_t* rowField1Nulls{nullptr};
};

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
  // Whether initGroup marks the accumulator as null for each new group. When
  // true, GroupingSet must keep Aggregate::numNulls_ in sync (non-JIT extract
  // relies on it), mirroring the non-JIT initializeNewGroups path.
  bool initSetsNull{false};
  // Result decimal precision/scale, used by decimal extract overflow checks.
  // Only meaningful when decimal == true.
  int32_t precision{0};
  int32_t scale{0};
  // Secondary decimal precision/scale. For decimal avg extract, precision/scale
  // carry the intermediate sum type and aux* carry the result type.
  int32_t auxPrecision{0};
  int32_t auxScale{0};
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
  CreateFn initGroup;
  AddFn addRawInput;
  AddFn addIntermediateResults;
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
  bool initSetsNull{false};
  int32_t precision{0};
  int32_t scale{0};
  int32_t auxPrecision{0};
  int32_t auxScale{0};
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

bool isHashAggrJitSupportedType(TypeKind kind);
std::optional<HashAggrJitValueKind> hashAggrJitValueKind(TypeKind kind);
std::string hashAggrJitValueKindName(HashAggrJitValueKind kind);

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
