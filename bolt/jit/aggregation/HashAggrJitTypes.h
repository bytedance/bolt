#pragma once

#ifdef ENABLE_BOLT_JIT

#include <cstdint>
#include <optional>
#include <string>

#include "bolt/type/Type.h"

// Lightweight, LLVM-free metadata shared between aggregate functions (which
// only produce a HashAggrJitDescriptor) and the JIT codegen layer. Keeping this
// header free of <llvm/IR/...> lets Aggregate.h and other non-JIT translation
// units depend on the JIT planning interface without pulling in the heavy LLVM
// IR headers. The codegen-only declarations (HashAggrJitOps function-pointer
// table, HashAggrJitCodegen, HashAggrJitChunk) live in HashAggrJit.h.

namespace bytedance::bolt::jit {

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

// Forward declaration: the codegen function-pointer table is defined in
// HashAggrJit.h (it references llvm:: types). Descriptors only hold a pointer
// to it, so a forward declaration is enough here and keeps this header
// LLVM-free.
struct HashAggrJitOps;

struct HashAggrJitDescriptor {
  HashAggrJitKind kind;
  HashAggrJitValueKind inputKind;
  HashAggrJitValueKind accumulatorKind;
  bool countStar{false};
  bool mergeInput{false};
  bool decimal{false};
  // Result decimal precision/scale, used by decimal extract overflow checks.
  // Only meaningful when decimal == true.
  int32_t precision{0};
  int32_t scale{0};
  // Secondary decimal precision/scale. For decimal avg extract, precision/scale
  // carry the intermediate sum type and aux* carry the result type.
  int32_t auxPrecision{0};
  int32_t auxScale{0};
  const HashAggrJitOps* ops{nullptr};

  std::string signature() const;
};

struct HashAggrJitSlot {
  int32_t aggregateIndex;
  int32_t offset;
  int32_t nullByte;
  uint8_t nullMask;
  // All aggregate-level traits live in the descriptor; IR-side code reads them
  // through 'desc'. Only the row-layout fields above are slot-specific.
  HashAggrJitDescriptor desc;
};

bool isHashAggrJitSupportedType(TypeKind kind);
std::optional<HashAggrJitValueKind> hashAggrJitValueKind(TypeKind kind);
std::string hashAggrJitValueKindName(HashAggrJitValueKind kind);

// Per-aggregate codegen function tables. The definitions (which reference
// llvm:: types) live in bolt/jit/aggregation/ops/*Ops.cpp. Aggregate functions
// only need the returned pointer to populate HashAggrJitDescriptor::ops, so
// these declarations stay LLVM-free here.
const HashAggrJitOps* getCountOps();
const HashAggrJitOps* getMinMaxOps();
const HashAggrJitOps* getSumOps();
const HashAggrJitOps* getAvgOps();
const HashAggrJitOps* getDecimalSumOps();
const HashAggrJitOps* getDecimalAvgOps();

} // namespace bytedance::bolt::jit

#endif // ENABLE_BOLT_JIT
