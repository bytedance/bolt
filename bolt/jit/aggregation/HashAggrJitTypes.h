#pragma once

#ifdef ENABLE_BOLT_JIT

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "bolt/type/Type.h"

// Lightweight, LLVM-free metadata shared between aggregate functions (which
// only produce a HashAggrJitDescriptor) and the JIT codegen layer. Keeping this
// header free of <llvm/IR/...> lets Aggregate.h and other non-JIT translation
// units depend on the JIT planning interface without pulling in the heavy LLVM
// IR headers. The codegen-only declarations (HashAggrJitOps function-pointer
// table, HashAggrJitCodegen, HashAggrJitChunk) live in HashAggrJit.h.

namespace bytedance::bolt::jit {

// Runtime scalar input consumed by JIT add_dense functions. 'indices' maps the
// add_dense row to the scalar value row. The owner decides the indexing
// contract for 'nulls': top-level scalar inputs pass row-indexed nulls, while
// ROW child scalar inputs pass child/base-indexed nulls and the row adapter
// applies 'indices' before checking the bit.
struct HashAggrJitScalarInputRuntime {
  const void* values{nullptr};
  const int32_t* indices{nullptr};
  const uint64_t* nulls{nullptr};
};

// Runtime ROW input. ROW itself has no value/indices wrapping in the generated
// IR; children are scalar runtimes. Current JIT merge inputs only require
// row-of-scalars, so recursive ROW children are intentionally not represented.
struct HashAggrJitRowInputRuntime {
  const uint64_t* nulls{nullptr};
  const HashAggrJitScalarInputRuntime* const* children{nullptr};
  int32_t numChildren{0};
};

// Shape-less runtime input. The generated code knows at compile time whether a
// slot reads a scalar or row input and selects the corresponding union member
// through InputAdapterCodegen.
union HashAggrJitInputRuntime {
  HashAggrJitInputRuntime() : scalar{} {}

  HashAggrJitScalarInputRuntime scalar;
  HashAggrJitRowInputRuntime row;
};

// Runtime scalar output consumed by JIT extract functions. Primitive flat
// outputs write values/null bits directly from generated IR; outputs that need
// vector semantics keep using helper fallbacks via 'vector'.
struct HashAggrJitScalarOutputRuntime {
  void* values{nullptr};
  uint64_t* nulls{nullptr};
  void* vector{nullptr};
};

// Runtime ROW output. Current JIT partial outputs only require row-of-scalars,
// so recursive ROW children are intentionally not represented. 'vector' keeps
// the top-level vector available for helper based complex writes (e.g. decimal
// partial extract), while child scalar runtimes expose raw field buffers for
// direct generated-IR stores.
struct HashAggrJitRowOutputRuntime {
  uint64_t* nulls{nullptr};
  HashAggrJitScalarOutputRuntime* const* children{nullptr};
  int32_t numChildren{0};
  void* vector{nullptr};
};

// Shape-less runtime output. The generated extract code knows at compile time
// whether a slot writes a scalar or row output and selects the corresponding
// union member through OutputAdapterCodegen.
union HashAggrJitOutputRuntime {
  HashAggrJitOutputRuntime() : scalar{} {}

  HashAggrJitScalarOutputRuntime scalar;
  HashAggrJitRowOutputRuntime row;
};

struct HashAggrJitPlanContext {
  bool isRawInput{false};
  bool isPartialOutput{false};
  std::vector<TypePtr> inputTypes;
  TypePtr outputType;

  bool isCountStar() const {
    return isRawInput && inputTypes.empty();
  }
};

enum class HashAggrJitKind : uint8_t {
  Count,
  Sum,
  DecimalSum,
  Min,
  Max,
  Avg,
  DecimalAvg,
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

enum class HashAggrJitRuntimeShape : uint8_t {
  Scalar,
  Row,
};

// Forward declaration: the codegen function-pointer table is defined in
// HashAggrJit.h (it references llvm:: types). Descriptors only hold a pointer
// to it, so a forward declaration is enough here and keeps this header
// LLVM-free.
struct HashAggrJitOps;

struct HashAggrJitDescriptor {
  HashAggrJitKind kind;
  HashAggrJitValueKind rawInputKind;
  HashAggrJitValueKind accumulatorKind;
  HashAggrJitPlanContext context;
  const HashAggrJitOps* ops{nullptr};

  bool isCountStar() const {
    return context.isCountStar();
  }

  bool isRawInput() const {
    return context.isRawInput;
  }

  bool isDecimal() const {
    return kind == HashAggrJitKind::DecimalSum ||
        kind == HashAggrJitKind::DecimalAvg;
  }

  HashAggrJitRuntimeShape inputShape() const {
    return context.inputTypes.size() == 1 && context.inputTypes[0] &&
            context.inputTypes[0]->isRow()
        ? HashAggrJitRuntimeShape::Row
        : HashAggrJitRuntimeShape::Scalar;
  }

  HashAggrJitRuntimeShape outputShape() const {
    return context.outputType && context.outputType->isRow()
        ? HashAggrJitRuntimeShape::Row
        : HashAggrJitRuntimeShape::Scalar;
  }

  // std::string signature() const;
};

struct HashAggrJitSlot {
  int32_t aggregateIndex;
  int32_t offset;
  int32_t nullByte;
  uint8_t nullMask;

  HashAggrJitDescriptor desc;

  std::string getDescription() const;
};

bool isHashAggrJitSupportedType(TypeKind kind);
std::optional<HashAggrJitValueKind> hashAggrJitValueKind(TypeKind kind);
std::string hashAggrJitKindName(HashAggrJitKind kind);
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
