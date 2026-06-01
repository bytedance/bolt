#pragma once

#ifdef ENABLE_BOLT_JIT

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "bolt/jit/CompiledModule.h"
#include "bolt/type/Type.h"

namespace bytedance::bolt::jit {

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

  std::string signature() const;
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
