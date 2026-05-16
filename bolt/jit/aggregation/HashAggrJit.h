#pragma once

#ifdef ENABLE_BOLT_JIT

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bolt/jit/CompiledModule.h"
#include "bolt/type/Type.h"

namespace bytedance::bolt::jit {

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
  Float,
  Double,
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
};

using HashAggrJitAddDenseFunc = void (*)(char** groups, int32_t numRows, char** decodedInputs);

class HashAggrJitChunk {
 public:
  explicit HashAggrJitChunk(std::vector<HashAggrJitSlot> slots);

  bool codegen();

  bool enabled() const {
    return addDense_ != nullptr && !disabled_;
  }

  void disable() {
    disabled_ = true;
  }

  void addDense(char** groups, int32_t numRows, char** decodedInputs) const {
    addDense_(groups, numRows, decodedInputs);
  }

  const std::vector<HashAggrJitSlot>& slots() const {
    return slots_;
  }

  std::string functionName() const;

 private:
  std::vector<HashAggrJitSlot> slots_;
  CompiledModuleSP module_;
  HashAggrJitAddDenseFunc addDense_{nullptr};
  bool disabled_{false};
};

bool isHashAggrJitSupportedType(TypeKind kind);
std::string hashAggrJitValueKindName(HashAggrJitValueKind kind);

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
