/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <array>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <string_view>
#include <vector>

#include "bolt/row/CompactRow.h"
#include "bolt/row/UnsafeRowDeserializers.h"
#include "bolt/row/UnsafeRowFast.h"
#include "bolt/row/dense/DenseRow.h"
#include "bolt/row/dense/DenseRowScalar.h"
#include "bolt/row/dense/IntVarint.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

namespace bytedance::bolt::row {
namespace {

// Serialize a RowVector to one contiguous buffer + (N + 1) cumulative row
// offsets via DenseRow (the self-allocating shape the benchmark relies on).
// DenseRow is marker-less, so strip any top-level nulls first.
struct DenseSerialized {
  BufferPtr buffer;
  BufferPtr rowOffsets;
};

DenseSerialized denseSerialize(
    const RowVectorPtr& data,
    memory::MemoryPool* pool) {
  RowVectorPtr input = data;
  if (data->mayHaveNulls()) {
    input = std::make_shared<RowVector>(
        pool, data->type(), /*nulls=*/nullptr, data->size(), data->children());
  }
  DenseRow rows(input);
  const auto n = rows.numRows();
  auto offsetsBuf = AlignedBuffer::allocate<size_t>(n + 1, pool);
  auto* offs = offsetsBuf->asMutable<size_t>();
  size_t cum = 0;
  for (vector_size_t r = 0; r < n; ++r) {
    offs[r] = cum;
    cum += rows.rowSizes()[r];
  }
  offs[n] = cum;
  auto buf = AlignedBuffer::allocate<char>(std::max<size_t>(cum, 1u), pool);
  rows.serialize(
      reinterpret_cast<uint8_t*>(buf->asMutable<char>()),
      folly::Range<const size_t*>(offs, n));
  return {std::move(buf), std::move(offsetsBuf)};
}

class DenseSerializeBenchmark {
 public:
  void serialize(const RowTypePtr& rowType) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(rowType);
    suspender.dismiss();

    auto serialized = denseSerialize(data, pool());
    BOLT_CHECK_GE(serialized.buffer->size(), 0);
  }

  void deserialize(const RowTypePtr& rowType) {
    folly::BenchmarkSuspender suspender;
    auto data = makeData(rowType);
    auto serialized = denseSerialize(data, pool());
    const auto* bytes = serialized.buffer->as<char>();
    const auto* offsets = serialized.rowOffsets->as<size_t>();
    const auto rowCount = data->size();
    std::vector<std::string_view> rows;
    rows.reserve(rowCount);
    for (vector_size_t i = 0; i < rowCount; ++i) {
      rows.emplace_back(bytes + offsets[i], offsets[i + 1] - offsets[i]);
    }
    suspender.dismiss();

    auto copy = DenseRow::deserialize(rows, rowType, pool());
    BOLT_CHECK_EQ(copy->size(), data->size());
  }

 private:
  RowVectorPtr makeData(const RowTypePtr& rowType) {
    VectorFuzzer::Options options;
    options.vectorSize = 1'000;

    const auto seed = 1;
    VectorFuzzer fuzzer(options, pool_.get(), seed);
    // fuzzFlat (not fuzzInputRow) so all inputs are flat — fuzzInputRow may
    // wrap children in dictionaries, which skews the codec measurement (extra
    // decode/null-merge cost unrelated to the row serializer).
    return std::dynamic_pointer_cast<RowVector>(fuzzer.fuzzFlat(rowType));
  }

  memory::MemoryPool* pool() {
    return pool_.get();
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};

enum class SerdeDataKind {
  kDefault,
  kBigintScalar,
  kBigintArray,
  kBigintNestedArray,
  kBigintMap,
  kDoubleRandom,
  kStringLen8,
  kStringLen100,
  kMultiScalar5Small,
  kMultiScalar10Small,
};

struct BigintRange {
  int64_t minInclusive;
  int64_t maxInclusive;
};

struct SerdeOnlyBenchmarkCase {
  RowTypePtr rowType;
  SerdeDataKind dataKind{SerdeDataKind::kDefault};
  // For kBigint* data kinds: nullopt means full int64 range (i.e., the
  // previous "random" case); otherwise the BIGINT values are drawn
  // uniformly from [min, max].
  std::optional<BigintRange> bigintRange{std::nullopt};
  // Fraction of null child values, applied on the kDefault fuzz path. Lets a
  // case exercise the null-handling (non-fast) encode/decode path.
  double nullRatio{0.0};
};

RowVectorPtr makeRangeBigintData(
    VectorFuzzer& fuzzer,
    int64_t minValueInclusive,
    int64_t maxValueInclusive) {
  using namespace generator_spec_maker;

  BOLT_CHECK_LE(minValueInclusive, maxValueInclusive);

  auto rowSpec = RANDOM_ROW({RANDOM_BIGINT(
      [minValueInclusive, maxValueInclusive](FuzzerGenerator& rng) -> int64_t {
        return std::uniform_int_distribution<int64_t>(
            minValueInclusive, maxValueInclusive)(rng);
      })});
  auto vector = fuzzer.fuzz(*rowSpec);
  auto rowVector = std::dynamic_pointer_cast<RowVector>(vector);
  BOLT_CHECK_NOT_NULL(rowVector);
  return rowVector;
}

// Returns a generator that produces BIGINT values uniformly distributed in
// [min, max]. Captured into RANDOM_BIGINT specs below.
auto makeBigintGen(int64_t minValueInclusive, int64_t maxValueInclusive) {
  return
      [minValueInclusive, maxValueInclusive](FuzzerGenerator& rng) -> int64_t {
        return std::uniform_int_distribution<int64_t>(
            minValueInclusive, maxValueInclusive)(rng);
      };
}

// Generator that produces array/map sizes uniformly in [0, 10]. Matches
// VectorFuzzer's default container-length distribution.
auto containerSizeGen() {
  return [](FuzzerGenerator& rng) -> vector_size_t {
    return std::uniform_int_distribution<vector_size_t>(0, 10)(rng);
  };
}

RowVectorPtr makeRangeBigintArrayData(
    VectorFuzzer& fuzzer,
    int64_t minValueInclusive,
    int64_t maxValueInclusive) {
  using namespace generator_spec_maker;
  BOLT_CHECK_LE(minValueInclusive, maxValueInclusive);

  // ROW({BIGINT, ARRAY(BIGINT in [min,max])}).
  auto rowSpec = RANDOM_ROW({
      RANDOM_BIGINT(makeBigintGen(minValueInclusive, maxValueInclusive)),
      RANDOM_ARRAY(
          RANDOM_BIGINT(makeBigintGen(minValueInclusive, maxValueInclusive)),
          containerSizeGen()),
  });
  auto vector = fuzzer.fuzz(*rowSpec);
  auto rowVector = std::dynamic_pointer_cast<RowVector>(vector);
  BOLT_CHECK_NOT_NULL(rowVector);
  return rowVector;
}

RowVectorPtr makeRangeBigintNestedArrayData(
    VectorFuzzer& fuzzer,
    int64_t minValueInclusive,
    int64_t maxValueInclusive) {
  using namespace generator_spec_maker;
  BOLT_CHECK_LE(minValueInclusive, maxValueInclusive);

  // ROW({BIGINT, ARRAY(ARRAY(BIGINT in [min,max]))}).
  auto rowSpec = RANDOM_ROW({
      RANDOM_BIGINT(makeBigintGen(minValueInclusive, maxValueInclusive)),
      RANDOM_ARRAY(
          RANDOM_ARRAY(
              RANDOM_BIGINT(
                  makeBigintGen(minValueInclusive, maxValueInclusive)),
              containerSizeGen()),
          containerSizeGen()),
  });
  auto vector = fuzzer.fuzz(*rowSpec);
  auto rowVector = std::dynamic_pointer_cast<RowVector>(vector);
  BOLT_CHECK_NOT_NULL(rowVector);
  return rowVector;
}

// Flat 5-column ROW with the BIGINT constrained to [min, max] and the other
// four narrow-/fixed-width columns (DOUBLE, BOOLEAN, TINYINT, REAL) drawn
// from their default distributions. Used to measure the multi-scalar path
// when the bigint values fit in 1-byte varints.
RowVectorPtr makeMultiScalar5SmallData(
    VectorFuzzer& fuzzer,
    int64_t minValueInclusive,
    int64_t maxValueInclusive) {
  using namespace generator_spec_maker;
  auto rowSpec = RANDOM_ROW({
      RANDOM_BIGINT(makeBigintGen(minValueInclusive, maxValueInclusive)),
      RANDOM_DOUBLE([](FuzzerGenerator& rng) -> double {
        return std::uniform_real_distribution<double>(-1.0, 1.0)(rng);
      }),
      RANDOM_BOOLEAN([](FuzzerGenerator& rng) -> bool {
        return std::uniform_int_distribution<int>(0, 1)(rng) != 0;
      }),
      RANDOM_TINYINT([](FuzzerGenerator& rng) -> int8_t {
        return std::uniform_int_distribution<int>(-127, 127)(rng);
      }),
      RANDOM_REAL([](FuzzerGenerator& rng) -> float {
        return std::uniform_real_distribution<float>(-1.0f, 1.0f)(rng);
      }),
  });
  auto vector = fuzzer.fuzz(*rowSpec);
  auto rowVector = std::dynamic_pointer_cast<RowVector>(vector);
  BOLT_CHECK_NOT_NULL(rowVector);
  return rowVector;
}

// Flat 10-column ROW where every integer-width column (BIGINT, INTEGER,
// SMALLINT, TINYINT) is constrained to [min, max] so its varint encoding
// is short. DOUBLE / REAL stay fixed-width.
RowVectorPtr makeMultiScalar10SmallData(
    VectorFuzzer& fuzzer,
    int64_t minValueInclusive,
    int64_t maxValueInclusive) {
  using namespace generator_spec_maker;
  auto narrowReal = [](FuzzerGenerator& rng) -> float {
    return std::uniform_real_distribution<float>(-1.0f, 1.0f)(rng);
  };
  auto narrowDouble = [](FuzzerGenerator& rng) -> double {
    return std::uniform_real_distribution<double>(-1.0, 1.0)(rng);
  };
  auto narrowBool = [](FuzzerGenerator& rng) -> bool {
    return std::uniform_int_distribution<int>(0, 1)(rng) != 0;
  };
  const auto clamp32 = [&](int64_t v) -> int32_t {
    return static_cast<int32_t>(std::clamp(
        v,
        static_cast<int64_t>(std::numeric_limits<int32_t>::min()),
        static_cast<int64_t>(std::numeric_limits<int32_t>::max())));
  };
  const auto clamp16 = [&](int64_t v) -> int16_t {
    return static_cast<int16_t>(std::clamp(
        v,
        static_cast<int64_t>(std::numeric_limits<int16_t>::min()),
        static_cast<int64_t>(std::numeric_limits<int16_t>::max())));
  };
  const auto clamp8 = [&](int64_t v) -> int8_t {
    return static_cast<int8_t>(std::clamp(
        v,
        static_cast<int64_t>(std::numeric_limits<int8_t>::min()),
        static_cast<int64_t>(std::numeric_limits<int8_t>::max())));
  };
  const int32_t intMin = clamp32(minValueInclusive);
  const int32_t intMax = clamp32(maxValueInclusive);
  const int16_t smallMin = clamp16(minValueInclusive);
  const int16_t smallMax = clamp16(maxValueInclusive);
  const int8_t tinyMin = clamp8(minValueInclusive);
  const int8_t tinyMax = clamp8(maxValueInclusive);

  auto rowSpec = RANDOM_ROW({
      RANDOM_BIGINT(makeBigintGen(minValueInclusive, maxValueInclusive)),
      RANDOM_INTEGER([intMin, intMax](FuzzerGenerator& rng) -> int32_t {
        return std::uniform_int_distribution<int32_t>(intMin, intMax)(rng);
      }),
      RANDOM_SMALLINT([smallMin, smallMax](FuzzerGenerator& rng) -> int16_t {
        return static_cast<int16_t>(
            std::uniform_int_distribution<int>(smallMin, smallMax)(rng));
      }),
      RANDOM_TINYINT([tinyMin, tinyMax](FuzzerGenerator& rng) -> int8_t {
        return static_cast<int8_t>(
            std::uniform_int_distribution<int>(tinyMin, tinyMax)(rng));
      }),
      RANDOM_REAL(narrowReal),
      RANDOM_DOUBLE(narrowDouble),
      RANDOM_BOOLEAN(narrowBool),
      RANDOM_BIGINT(makeBigintGen(minValueInclusive, maxValueInclusive)),
      RANDOM_INTEGER([intMin, intMax](FuzzerGenerator& rng) -> int32_t {
        return std::uniform_int_distribution<int32_t>(intMin, intMax)(rng);
      }),
      RANDOM_DOUBLE(narrowDouble),
  });
  auto vector = fuzzer.fuzz(*rowSpec);
  auto rowVector = std::dynamic_pointer_cast<RowVector>(vector);
  BOLT_CHECK_NOT_NULL(rowVector);
  return rowVector;
}

RowVectorPtr makeRangeBigintMapData(
    VectorFuzzer& fuzzer,
    int64_t minValueInclusive,
    int64_t maxValueInclusive) {
  using namespace generator_spec_maker;
  BOLT_CHECK_LE(minValueInclusive, maxValueInclusive);

  // ROW({BIGINT, MAP(BIGINT in [min,max], REAL)}).
  auto rowSpec = RANDOM_ROW({
      RANDOM_BIGINT(makeBigintGen(minValueInclusive, maxValueInclusive)),
      RANDOM_MAP(
          RANDOM_BIGINT(makeBigintGen(minValueInclusive, maxValueInclusive)),
          RANDOM_REAL([](FuzzerGenerator& rng) -> float {
            return std::uniform_real_distribution<float>(-1.0f, 1.0f)(rng);
          }),
          containerSizeGen()),
  });
  auto vector = fuzzer.fuzz(*rowSpec);
  auto rowVector = std::dynamic_pointer_cast<RowVector>(vector);
  BOLT_CHECK_NOT_NULL(rowVector);
  return rowVector;
}

// For kBigintScalar / kBigintArray with a range, falls back to the
// fuzzer's full-range generator if `range` is nullopt.
constexpr BigintRange kFullBigintRange{
    std::numeric_limits<int64_t>::min(),
    std::numeric_limits<int64_t>::max()};

RowVectorPtr makeSerdeOnlyData(
    const SerdeOnlyBenchmarkCase& benchmarkCase,
    memory::MemoryPool* pool) {
  VectorFuzzer::Options options;
  options.vectorSize = 1'000;
  options.nullRatio = benchmarkCase.nullRatio;

  if (benchmarkCase.dataKind == SerdeDataKind::kStringLen8) {
    options.stringLength = 8;
    options.stringVariableLength = false;
  }

  if (benchmarkCase.dataKind == SerdeDataKind::kStringLen100) {
    options.stringLength = 100;
    options.stringVariableLength = false;
  }

  const auto seed = 1;
  VectorFuzzer fuzzer(options, pool, seed);

  switch (benchmarkCase.dataKind) {
    case SerdeDataKind::kBigintScalar: {
      const auto range = benchmarkCase.bigintRange.value_or(kFullBigintRange);
      return makeRangeBigintData(
          fuzzer, range.minInclusive, range.maxInclusive);
    }
    case SerdeDataKind::kBigintArray: {
      const auto range = benchmarkCase.bigintRange.value_or(kFullBigintRange);
      return makeRangeBigintArrayData(
          fuzzer, range.minInclusive, range.maxInclusive);
    }
    case SerdeDataKind::kBigintNestedArray: {
      const auto range = benchmarkCase.bigintRange.value_or(kFullBigintRange);
      return makeRangeBigintNestedArrayData(
          fuzzer, range.minInclusive, range.maxInclusive);
    }
    case SerdeDataKind::kBigintMap: {
      const auto range = benchmarkCase.bigintRange.value_or(kFullBigintRange);
      return makeRangeBigintMapData(
          fuzzer, range.minInclusive, range.maxInclusive);
    }
    case SerdeDataKind::kMultiScalar5Small: {
      const auto range = benchmarkCase.bigintRange.value_or(kFullBigintRange);
      return makeMultiScalar5SmallData(
          fuzzer, range.minInclusive, range.maxInclusive);
    }
    case SerdeDataKind::kMultiScalar10Small: {
      const auto range = benchmarkCase.bigintRange.value_or(kFullBigintRange);
      return makeMultiScalar10SmallData(
          fuzzer, range.minInclusive, range.maxInclusive);
    }
    case SerdeDataKind::kDoubleRandom:
    case SerdeDataKind::kStringLen8:
    case SerdeDataKind::kStringLen100:
    case SerdeDataKind::kDefault:
      // fuzzFlat (not fuzzInputRow): guarantee flat children. fuzzInputRow may
      // wrap a column in a dictionary, which adds decode/null-merge cost
      // (DecodedVector::setFlatNulls) unrelated to the row codec.
      return std::dynamic_pointer_cast<RowVector>(
          fuzzer.fuzzFlat(benchmarkCase.rowType));
  }

  BOLT_UNREACHABLE();
}

size_t computeUnsafeTotalSize(
    UnsafeRowFast& unsafeRow,
    const RowTypePtr& rowType,
    vector_size_t numRows) {
  size_t totalSize = 0;
  if (auto fixedRowSize = UnsafeRowFast::fixedRowSize(rowType)) {
    totalSize += fixedRowSize.value() * numRows;
  } else {
    for (auto i = 0; i < numRows; ++i) {
      totalSize += unsafeRow.rowSize(i);
    }
  }
  return totalSize;
}

size_t computeCompactTotalSize(
    CompactRow& compactRow,
    const RowTypePtr& rowType,
    vector_size_t numRows) {
  size_t totalSize = 0;
  if (auto fixedRowSize = CompactRow::fixedRowSize(rowType)) {
    totalSize += fixedRowSize.value() * numRows;
  } else {
    for (auto i = 0; i < numRows; ++i) {
      totalSize += compactRow.rowSize(i);
    }
  }
  return totalSize;
}

size_t serializeUnsafeToBuffer(
    UnsafeRowFast& unsafeRow,
    vector_size_t numRows,
    char* rawBuffer) {
  size_t offset = 0;
  for (auto i = 0; i < numRows; ++i) {
    offset += unsafeRow.serialize(i, rawBuffer + offset);
  }
  return offset;
}

size_t serializeCompactToBuffer(
    CompactRow& compactRow,
    vector_size_t numRows,
    char* rawBuffer) {
  size_t offset = 0;
  for (auto i = 0; i < numRows; ++i) {
    offset += compactRow.serialize(i, rawBuffer + offset);
  }
  return offset;
}

std::vector<std::optional<std::string_view>> serializeUnsafeRows(
    UnsafeRowFast& unsafeRow,
    vector_size_t numRows,
    BufferPtr& buffer) {
  std::vector<std::optional<std::string_view>> serialized;
  serialized.reserve(numRows);
  auto* rawBuffer = buffer->asMutable<char>();

  size_t offset = 0;
  for (auto i = 0; i < numRows; ++i) {
    auto rowSize = unsafeRow.serialize(i, rawBuffer + offset);
    serialized.push_back(std::string_view(rawBuffer + offset, rowSize));
    offset += rowSize;
  }

  BOLT_CHECK_EQ(buffer->size(), offset);
  return serialized;
}

std::vector<std::string_view> serializeCompactRows(
    CompactRow& compactRow,
    vector_size_t numRows,
    BufferPtr& buffer) {
  std::vector<std::string_view> serialized;
  serialized.reserve(numRows);
  auto* rawBuffer = buffer->asMutable<char>();

  size_t offset = 0;
  for (auto i = 0; i < numRows; ++i) {
    auto rowSize = compactRow.serialize(i, rawBuffer + offset);
    serialized.push_back(std::string_view(rawBuffer + offset, rowSize));
    offset += rowSize;
  }

  BOLT_CHECK_EQ(buffer->size(), offset);
  return serialized;
}

int serializeUnsafeSerdeOnly(
    int nIters,
    const SerdeOnlyBenchmarkCase& benchmarkCase) {
  auto pool = memory::memoryManager()->addLeafPool();

  folly::BenchmarkSuspender suspender;
  auto data = makeSerdeOnlyData(benchmarkCase, pool.get());
  UnsafeRowFast unsafeRow(data);
  const auto totalSize =
      computeUnsafeTotalSize(unsafeRow, benchmarkCase.rowType, data->size());
  auto buffer = AlignedBuffer::allocate<char>(totalSize, pool.get());
  auto* rawBuffer = buffer->asMutable<char>();
  BOLT_CHECK_EQ(
      serializeUnsafeToBuffer(unsafeRow, data->size(), rawBuffer), totalSize);
  suspender.dismiss();

  for (int i = 0; i < nIters; ++i) {
    folly::doNotOptimizeAway(
        serializeUnsafeToBuffer(unsafeRow, data->size(), rawBuffer));
  }
  return nIters * data->size();
}

int serializeCompactSerdeOnly(
    int nIters,
    const SerdeOnlyBenchmarkCase& benchmarkCase) {
  auto pool = memory::memoryManager()->addLeafPool();

  folly::BenchmarkSuspender suspender;
  auto data = makeSerdeOnlyData(benchmarkCase, pool.get());
  CompactRow compactRow(data);
  const auto numRows = data->size();
  // Columnar/batch interface (matching DenseRow): precompute absolute per-row
  // offsets, then time a single batch serialize() — which uses CompactRow's
  // range path + fixed-width bulk-copy fast path.
  std::vector<size_t> offsets(numRows);
  size_t cum = 0;
  const auto fixed = CompactRow::fixedRowSize(benchmarkCase.rowType);
  for (vector_size_t r = 0; r < numRows; ++r) {
    offsets[r] = cum;
    cum += fixed ? *fixed : static_cast<size_t>(compactRow.rowSize(r));
  }
  // CompactRow's batch serialize needs the buffer pre-zeroed (null-bit
  // handling); re-serializing the same data into it is idempotent.
  auto buffer =
      AlignedBuffer::allocate<char>(std::max<size_t>(cum, 1u), pool.get(), 0);
  auto* rawBuffer = buffer->asMutable<char>();
  suspender.dismiss();

  for (int i = 0; i < nIters; ++i) {
    compactRow.serialize(0, numRows, offsets.data(), rawBuffer);
    folly::doNotOptimizeAway(rawBuffer);
  }
  return nIters * numRows;
}

int deserializeUnsafeSerdeOnly(
    int nIters,
    const SerdeOnlyBenchmarkCase& benchmarkCase) {
  auto pool = memory::memoryManager()->addLeafPool();

  folly::BenchmarkSuspender suspender;
  auto data = makeSerdeOnlyData(benchmarkCase, pool.get());
  UnsafeRowFast unsafeRow(data);
  const auto totalSize =
      computeUnsafeTotalSize(unsafeRow, benchmarkCase.rowType, data->size());
  auto buffer = AlignedBuffer::allocate<char>(totalSize, pool.get());
  auto serialized = serializeUnsafeRows(unsafeRow, data->size(), buffer);
  suspender.dismiss();

  for (int i = 0; i < nIters; ++i) {
    folly::doNotOptimizeAway(UnsafeRowDeserializer::deserialize(
        serialized, benchmarkCase.rowType, pool.get()));
  }
  return nIters * data->size();
}

int deserializeCompactSerdeOnly(
    int nIters,
    const SerdeOnlyBenchmarkCase& benchmarkCase) {
  auto pool = memory::memoryManager()->addLeafPool();

  folly::BenchmarkSuspender suspender;
  auto data = makeSerdeOnlyData(benchmarkCase, pool.get());
  CompactRow compactRow(data);
  const auto totalSize =
      computeCompactTotalSize(compactRow, benchmarkCase.rowType, data->size());
  auto buffer = AlignedBuffer::allocate<char>(totalSize, pool.get());
  auto serialized = serializeCompactRows(compactRow, data->size(), buffer);
  suspender.dismiss();

  for (int i = 0; i < nIters; ++i) {
    folly::doNotOptimizeAway(
        CompactRow::deserialize(serialized, benchmarkCase.rowType, pool.get()));
  }
  return nIters * data->size();
}

#define SERDE_ONLY_BENCHMARKS(name, benchmarkCase) \
  BENCHMARK_NAMED_PARAM_MULTI(                     \
      serializeUnsafeSerdeOnly,                    \
      unsafe_serde_only_serialize_##name,          \
      benchmarkCase);                              \
                                                   \
  BENCHMARK_NAMED_PARAM_MULTI(                     \
      serializeCompactSerdeOnly,                   \
      compact_serde_only_serialize_##name,         \
      benchmarkCase);                              \
                                                   \
  BENCHMARK_NAMED_PARAM_MULTI(                     \
      deserializeUnsafeSerdeOnly,                  \
      unsafe_serde_only_deserialize_##name,        \
      benchmarkCase);                              \
                                                   \
  BENCHMARK_NAMED_PARAM_MULTI(                     \
      deserializeCompactSerdeOnly,                 \
      compact_serde_only_deserialize_##name,       \
      benchmarkCase);

int serializeDenseSerdeOnly(
    int nIters,
    const SerdeOnlyBenchmarkCase& benchmarkCase) {
  auto pool = memory::memoryManager()->addLeafPool();

  folly::BenchmarkSuspender suspender;
  auto data = makeSerdeOnlyData(benchmarkCase, pool.get());
  // Build the serializer + offsets + output buffer ONCE outside the timed loop
  // and time only serialize() into the reused buffer — matching
  // serializeCompactSerdeOnly / serializeUnsafeSerdeOnly so the comparison is
  // write-vs-write (not build+alloc-vs-write). DenseRow is marker-less, so
  // strip any top-level nulls first.
  RowVectorPtr input = data;
  if (data->mayHaveNulls()) {
    input = std::make_shared<RowVector>(
        pool.get(), data->type(), nullptr, data->size(), data->children());
  }
  DenseRow rows(input);
  const auto n = rows.numRows();
  auto offsetsBuf = AlignedBuffer::allocate<size_t>(n + 1, pool.get());
  auto* offs = offsetsBuf->asMutable<size_t>();
  size_t cum = 0;
  for (vector_size_t r = 0; r < n; ++r) {
    offs[r] = cum;
    cum += rows.rowSizes()[r];
  }
  offs[n] = cum;
  auto buffer =
      AlignedBuffer::allocate<char>(std::max<size_t>(cum, 1u), pool.get());
  auto* base = reinterpret_cast<uint8_t*>(buffer->asMutable<char>());
  suspender.dismiss();

  for (int i = 0; i < nIters; ++i) {
    rows.serialize(base, folly::Range<const size_t*>(offs, n));
    folly::doNotOptimizeAway(base);
  }
  return nIters * data->size();
}

int deserializeDenseSerdeOnly(
    int nIters,
    const SerdeOnlyBenchmarkCase& benchmarkCase) {
  auto pool = memory::memoryManager()->addLeafPool();

  folly::BenchmarkSuspender suspender;
  auto data = makeSerdeOnlyData(benchmarkCase, pool.get());
  auto serialized = denseSerialize(data, pool.get());
  const auto* bytes = serialized.buffer->as<char>();
  const auto* offsets = serialized.rowOffsets->as<size_t>();
  const auto rowCount = data->size();
  std::vector<std::string_view> rows;
  rows.reserve(rowCount);
  for (vector_size_t i = 0; i < rowCount; ++i) {
    rows.emplace_back(bytes + offsets[i], offsets[i + 1] - offsets[i]);
  }
  suspender.dismiss();

  for (int i = 0; i < nIters; ++i) {
    folly::doNotOptimizeAway(
        DenseRow::deserialize(rows, benchmarkCase.rowType, pool.get()));
  }
  return nIters * data->size();
}

#define DENSE_SERDE_ONLY_BENCHMARKS(name, benchmarkCase) \
  BENCHMARK_NAMED_PARAM_MULTI(                           \
      serializeDenseSerdeOnly,                           \
      dense_serde_only_serialize_##name,                 \
      benchmarkCase);                                    \
                                                         \
  BENCHMARK_NAMED_PARAM_MULTI(                           \
      deserializeDenseSerdeOnly,                         \
      dense_serde_only_deserialize_##name,               \
      benchmarkCase);

#define DENSE_BENCHMARKS(name, rowType) \
  BENCHMARK(dense_serialize_##name) {   \
    DenseSerializeBenchmark benchmark;  \
    benchmark.serialize(rowType);       \
  }                                     \
                                        \
  BENCHMARK(dense_deserialize_##name) { \
    DenseSerializeBenchmark benchmark;  \
    benchmark.deserialize(rowType);     \
  }

DENSE_BENCHMARKS(
    fixedWidth5,
    ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()}));

DENSE_BENCHMARKS(
    fixedWidth10,
    ROW({
        BIGINT(),
        BIGINT(),
        BIGINT(),
        BIGINT(),
        BIGINT(),
        BIGINT(),
        DOUBLE(),
        BIGINT(),
        BIGINT(),
        BIGINT(),
    }));

DENSE_BENCHMARKS(
    fixedWidth20,
    ROW({
        BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(),
        BIGINT(), BIGINT(), BIGINT(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(),
        DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), BIGINT(), BIGINT(),
    }));

DENSE_BENCHMARKS(strings1, ROW({BIGINT(), VARCHAR()}));

DENSE_BENCHMARKS(
    strings5,
    ROW({
        BIGINT(),
        VARCHAR(),
        VARCHAR(),
        VARCHAR(),
        VARCHAR(),
        VARCHAR(),
    }));

DENSE_BENCHMARKS(arrays, ROW({BIGINT(), ARRAY(BIGINT())}));

DENSE_BENCHMARKS(nestedArrays, ROW({BIGINT(), ARRAY(ARRAY(BIGINT()))}));

DENSE_BENCHMARKS(maps, ROW({BIGINT(), MAP(BIGINT(), REAL())}));

DENSE_BENCHMARKS(
    structs,
    ROW({BIGINT(), ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()})}));

constexpr BigintRange kRangeLt2Pow8{-((1LL << 8) - 1), (1LL << 8) - 1};
constexpr BigintRange kRangeLt2Pow32{-((1LL << 32) - 1), (1LL << 32) - 1};

const SerdeOnlyBenchmarkCase kBigintLt2Pow8{
    ROW({BIGINT()}),
    SerdeDataKind::kBigintScalar,
    kRangeLt2Pow8};
const SerdeOnlyBenchmarkCase kBigintLt2Pow32{
    ROW({BIGINT()}),
    SerdeDataKind::kBigintScalar,
    kRangeLt2Pow32};
const SerdeOnlyBenchmarkCase kBigintRandom{
    ROW({BIGINT()}),
    SerdeDataKind::kBigintScalar};
// Full-range BIGINT with ~40% null children: exercises the null-handling
// (non-fast) encode/decode path that the SIMD/contiguous fast paths skip.
const SerdeOnlyBenchmarkCase kBigintRandomNullable{
    ROW({BIGINT()}),
    SerdeDataKind::kDefault,
    std::nullopt,
    0.4};
const SerdeOnlyBenchmarkCase kDoubleRandom{
    ROW({DOUBLE()}),
    SerdeDataKind::kDoubleRandom};
const SerdeOnlyBenchmarkCase kStringLen8{
    ROW({VARCHAR()}),
    SerdeDataKind::kStringLen8};
const SerdeOnlyBenchmarkCase kStringLen100{
    ROW({VARCHAR()}),
    SerdeDataKind::kStringLen100};
const SerdeOnlyBenchmarkCase kArrays{
    ROW({BIGINT(), ARRAY(BIGINT())}),
    SerdeDataKind::kDefault};
const SerdeOnlyBenchmarkCase kNestedArrays{
    ROW({BIGINT(), ARRAY(ARRAY(BIGINT()))}),
    SerdeDataKind::kDefault};
const SerdeOnlyBenchmarkCase kMaps{
    ROW({BIGINT(), MAP(BIGINT(), REAL())}),
    SerdeDataKind::kDefault};
const SerdeOnlyBenchmarkCase kArraysBigintLt2Pow8{
    ROW({BIGINT(), ARRAY(BIGINT())}),
    SerdeDataKind::kBigintArray,
    kRangeLt2Pow8};
const SerdeOnlyBenchmarkCase kArraysBigintLt2Pow32{
    ROW({BIGINT(), ARRAY(BIGINT())}),
    SerdeDataKind::kBigintArray,
    kRangeLt2Pow32};
const SerdeOnlyBenchmarkCase kNestedArraysBigintLt2Pow8{
    ROW({BIGINT(), ARRAY(ARRAY(BIGINT()))}),
    SerdeDataKind::kBigintNestedArray,
    kRangeLt2Pow8};
const SerdeOnlyBenchmarkCase kNestedArraysBigintLt2Pow32{
    ROW({BIGINT(), ARRAY(ARRAY(BIGINT()))}),
    SerdeDataKind::kBigintNestedArray,
    kRangeLt2Pow32};
const SerdeOnlyBenchmarkCase kMapsBigintLt2Pow8{
    ROW({BIGINT(), MAP(BIGINT(), REAL())}),
    SerdeDataKind::kBigintMap,
    kRangeLt2Pow8};
const SerdeOnlyBenchmarkCase kMapsBigintLt2Pow32{
    ROW({BIGINT(), MAP(BIGINT(), REAL())}),
    SerdeDataKind::kBigintMap,
    kRangeLt2Pow32};

// Flat row of multiple simple-type columns. Exercises the top-level ROW
// driver against scalar leaf encoders (no nested ARRAY/MAP), which is the
// path most directly comparable to CompactRow's strength.
const SerdeOnlyBenchmarkCase kMultiScalar5{
    ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()}),
    SerdeDataKind::kDefault};
const SerdeOnlyBenchmarkCase kMultiScalar10{
    ROW(
        {BIGINT(),
         INTEGER(),
         SMALLINT(),
         TINYINT(),
         REAL(),
         DOUBLE(),
         BOOLEAN(),
         BIGINT(),
         INTEGER(),
         DOUBLE()}),
    SerdeDataKind::kDefault};

// Small-value variants: BIGINT (and INTEGER/SMALLINT/TINYINT for the 10-col
// case) restricted to [-(2^8-1), 2^8-1] so every integer encodes in a
// single varint byte. Highlights dense's strength on narrow scalar data
// where its on-wire size drops well below CompactRow's fixed widths.
const SerdeOnlyBenchmarkCase kMultiScalar5SmallLt2Pow8{
    ROW({BIGINT(), DOUBLE(), BOOLEAN(), TINYINT(), REAL()}),
    SerdeDataKind::kMultiScalar5Small,
    kRangeLt2Pow8};
const SerdeOnlyBenchmarkCase kMultiScalar10SmallLt2Pow8{
    ROW(
        {BIGINT(),
         INTEGER(),
         SMALLINT(),
         TINYINT(),
         REAL(),
         DOUBLE(),
         BOOLEAN(),
         BIGINT(),
         INTEGER(),
         DOUBLE()}),
    SerdeDataKind::kMultiScalar10Small,
    kRangeLt2Pow8};

struct NamedSerdeOnlyBenchmarkCase {
  const char* name;
  const SerdeOnlyBenchmarkCase* benchmarkCase;
};

const std::array<NamedSerdeOnlyBenchmarkCase, 20> kSerdeOnlyBenchmarkCases{{
    {"bigint_lt_2pow8", &kBigintLt2Pow8},
    {"bigint_lt_2pow32", &kBigintLt2Pow32},
    {"bigint_random", &kBigintRandom},
    {"bigint_random_nullable", &kBigintRandomNullable},
    {"double_random", &kDoubleRandom},
    {"string_len8", &kStringLen8},
    {"string_len100", &kStringLen100},
    {"arrays", &kArrays},
    {"nestedArrays", &kNestedArrays},
    {"maps", &kMaps},
    {"arrays_bigint_lt_2pow8", &kArraysBigintLt2Pow8},
    {"arrays_bigint_lt_2pow32", &kArraysBigintLt2Pow32},
    {"nestedArrays_bigint_lt_2pow8", &kNestedArraysBigintLt2Pow8},
    {"nestedArrays_bigint_lt_2pow32", &kNestedArraysBigintLt2Pow32},
    {"maps_bigint_lt_2pow8", &kMapsBigintLt2Pow8},
    {"maps_bigint_lt_2pow32", &kMapsBigintLt2Pow32},
    {"multiScalar5", &kMultiScalar5},
    {"multiScalar10", &kMultiScalar10},
    {"multiScalar5_small_lt_2pow8", &kMultiScalar5SmallLt2Pow8},
    {"multiScalar10_small_lt_2pow8", &kMultiScalar10SmallLt2Pow8},
}};

struct SerdeOnlySerializedSizes {
  vector_size_t rowCount;
  size_t unsafeBytes;
  size_t compactBytes;
  size_t denseBytes;
};

SerdeOnlySerializedSizes computeSerdeOnlySerializedSizes(
    const SerdeOnlyBenchmarkCase& benchmarkCase) {
  auto pool = memory::memoryManager()->addLeafPool();
  auto data = makeSerdeOnlyData(benchmarkCase, pool.get());

  UnsafeRowFast unsafeRow(data);
  const auto unsafeBytes =
      computeUnsafeTotalSize(unsafeRow, benchmarkCase.rowType, data->size());

  CompactRow compactRow(data);
  const auto compactBytes =
      computeCompactTotalSize(compactRow, benchmarkCase.rowType, data->size());

  const auto denseBytes = denseSerialize(data, pool.get()).buffer->size();

  return {
      data->size(),
      unsafeBytes,
      compactBytes,
      denseBytes,
  };
}

SERDE_ONLY_BENCHMARKS(bigint_lt_2pow8, kBigintLt2Pow8);

SERDE_ONLY_BENCHMARKS(bigint_lt_2pow32, kBigintLt2Pow32);

SERDE_ONLY_BENCHMARKS(bigint_random, kBigintRandom);

SERDE_ONLY_BENCHMARKS(bigint_random_nullable, kBigintRandomNullable);

SERDE_ONLY_BENCHMARKS(double_random, kDoubleRandom);

SERDE_ONLY_BENCHMARKS(string_len8, kStringLen8);

SERDE_ONLY_BENCHMARKS(string_len100, kStringLen100);

SERDE_ONLY_BENCHMARKS(arrays, kArrays);

SERDE_ONLY_BENCHMARKS(nestedArrays, kNestedArrays);

SERDE_ONLY_BENCHMARKS(maps, kMaps);

SERDE_ONLY_BENCHMARKS(arrays_bigint_lt_2pow8, kArraysBigintLt2Pow8);
SERDE_ONLY_BENCHMARKS(arrays_bigint_lt_2pow32, kArraysBigintLt2Pow32);
SERDE_ONLY_BENCHMARKS(nestedArrays_bigint_lt_2pow8, kNestedArraysBigintLt2Pow8);
SERDE_ONLY_BENCHMARKS(
    nestedArrays_bigint_lt_2pow32,
    kNestedArraysBigintLt2Pow32);
SERDE_ONLY_BENCHMARKS(maps_bigint_lt_2pow8, kMapsBigintLt2Pow8);
SERDE_ONLY_BENCHMARKS(maps_bigint_lt_2pow32, kMapsBigintLt2Pow32);

DENSE_SERDE_ONLY_BENCHMARKS(arrays_bigint_lt_2pow8, kArraysBigintLt2Pow8);
DENSE_SERDE_ONLY_BENCHMARKS(arrays_bigint_lt_2pow32, kArraysBigintLt2Pow32);
DENSE_SERDE_ONLY_BENCHMARKS(
    nestedArrays_bigint_lt_2pow8,
    kNestedArraysBigintLt2Pow8);
DENSE_SERDE_ONLY_BENCHMARKS(
    nestedArrays_bigint_lt_2pow32,
    kNestedArraysBigintLt2Pow32);
DENSE_SERDE_ONLY_BENCHMARKS(maps_bigint_lt_2pow8, kMapsBigintLt2Pow8);
DENSE_SERDE_ONLY_BENCHMARKS(maps_bigint_lt_2pow32, kMapsBigintLt2Pow32);

DENSE_SERDE_ONLY_BENCHMARKS(bigint_lt_2pow8, kBigintLt2Pow8);

DENSE_SERDE_ONLY_BENCHMARKS(bigint_lt_2pow32, kBigintLt2Pow32);

DENSE_SERDE_ONLY_BENCHMARKS(bigint_random, kBigintRandom);

DENSE_SERDE_ONLY_BENCHMARKS(bigint_random_nullable, kBigintRandomNullable);

DENSE_SERDE_ONLY_BENCHMARKS(double_random, kDoubleRandom);

DENSE_SERDE_ONLY_BENCHMARKS(string_len8, kStringLen8);

DENSE_SERDE_ONLY_BENCHMARKS(string_len100, kStringLen100);

DENSE_SERDE_ONLY_BENCHMARKS(arrays, kArrays);

DENSE_SERDE_ONLY_BENCHMARKS(nestedArrays, kNestedArrays);

DENSE_SERDE_ONLY_BENCHMARKS(maps, kMaps);

SERDE_ONLY_BENCHMARKS(multiScalar5, kMultiScalar5);
SERDE_ONLY_BENCHMARKS(multiScalar10, kMultiScalar10);
DENSE_SERDE_ONLY_BENCHMARKS(multiScalar5, kMultiScalar5);
DENSE_SERDE_ONLY_BENCHMARKS(multiScalar10, kMultiScalar10);

SERDE_ONLY_BENCHMARKS(multiScalar5_small_lt_2pow8, kMultiScalar5SmallLt2Pow8);
SERDE_ONLY_BENCHMARKS(multiScalar10_small_lt_2pow8, kMultiScalar10SmallLt2Pow8);
DENSE_SERDE_ONLY_BENCHMARKS(
    multiScalar5_small_lt_2pow8,
    kMultiScalar5SmallLt2Pow8);
DENSE_SERDE_ONLY_BENCHMARKS(
    multiScalar10_small_lt_2pow8,
    kMultiScalar10SmallLt2Pow8);

} // namespace

void printSerdeOnlySerializedSizes() {
  std::cout << "SerdeOnly serialized size stats (bytes + bytes/row)\n";
  std::cout
      << "case,row_count,unsafe_bytes,compact_bytes,dense_bytes,unsafe_bpr,compact_bpr,dense_bpr\n";

  for (const auto& namedCase : kSerdeOnlyBenchmarkCases) {
    const auto stats =
        computeSerdeOnlySerializedSizes(*namedCase.benchmarkCase);
    const auto toBytesPerRow = [rowCount =
                                    stats.rowCount](size_t bytes) -> double {
      return static_cast<double>(bytes) / static_cast<double>(rowCount);
    };

    std::cout << namedCase.name << ',' << stats.rowCount << ','
              << stats.unsafeBytes << ',' << stats.compactBytes << ','
              << stats.denseBytes << ',' << std::fixed << std::setprecision(2)
              << toBytesPerRow(stats.unsafeBytes) << ','
              << toBytesPerRow(stats.compactBytes) << ','
              << toBytesPerRow(stats.denseBytes) << '\n';
  }
  std::cout.flush();
}

// ===========================================================================
// Size-pass microbenchmark: times the REAL scalar::addColumnSizes on a flat
// nullable BIGINT column across value magnitudes. Calling the compiled function
// (not a local copy) keeps it from auto-vectorizing in this TU, so this matches
// the production embedded behavior. A/B the internal kernel by toggling the
// SIMD wiring in scalar::addColumnSizes and rebuilding.
// ===========================================================================
namespace size_bench {
constexpr vector_size_t kN = 4096;

struct SizeInput {
  VectorPtr vec; // flat nullable BIGINT
  DecodedVector decoded;
  std::vector<size_t> rowSizes;
};

// magnitude: 0 = small [-100,100], 1 = full int32, 2 = full int64; ~10% nulls.
std::unique_ptr<SizeInput> makeInput(memory::MemoryPool* pool, int mag) {
  auto in = std::make_unique<SizeInput>();
  in->vec = BaseVector::create(BIGINT(), kN, pool);
  auto* flat = in->vec->asUnchecked<FlatVector<int64_t>>();
  auto* raw = flat->mutableRawValues();
  std::mt19937_64 rng(0x9E3779B97F4A7C15ull ^ static_cast<uint64_t>(mag));
  for (vector_size_t i = 0; i < kN; ++i) {
    if (mag == 0) {
      raw[i] = static_cast<int64_t>(rng() % 201) - 100;
    } else if (mag == 1) {
      raw[i] = static_cast<int64_t>(static_cast<int32_t>(rng()));
    } else {
      raw[i] = static_cast<int64_t>(rng());
    }
    if (rng() % 10 == 0) {
      flat->setNull(i, true);
    }
  }
  in->decoded.decode(*in->vec);
  in->rowSizes.assign(kN, 0);
  return in;
}

// Dictionary-wrapped (reversed indices) → non-identity, so addColumnSizes takes
// the SCALAR nullableInt64SerializedSize loop instead of the SIMD kernel.
std::unique_ptr<SizeInput> makeDictInput(memory::MemoryPool* pool, int mag) {
  auto in = makeInput(pool, mag); // reuse value/null generation
  auto flat = in->vec; // the flat nullable BIGINT
  auto indices = allocateIndices(kN, pool);
  auto* idx = indices->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < kN; ++i) {
    idx[i] = kN - 1 - i; // reversed -> non-identity
  }
  in->vec = BaseVector::wrapInDictionary(nullptr, indices, kN, flat);
  in->decoded.decode(*in->vec);
  return in;
}
} // namespace size_bench

#define SIZE_BENCH(tag, mag)                                          \
  BENCHMARK(addColumnSizes_##tag) {                                   \
    static auto pool = memory::memoryManager()->addLeafPool();        \
    static auto in = size_bench::makeInput(pool.get(), mag);          \
    scalar::addColumnSizes(                                           \
        *BIGINT(), in->decoded, size_bench::kN, in->rowSizes.data()); \
    folly::doNotOptimizeAway(in->rowSizes[0]);                        \
  }                                                                   \
  BENCHMARK(addColumnSizes_dict_##tag) {                              \
    static auto pool = memory::memoryManager()->addLeafPool();        \
    static auto in = size_bench::makeDictInput(pool.get(), mag);      \
    scalar::addColumnSizes(                                           \
        *BIGINT(), in->decoded, size_bench::kN, in->rowSizes.data()); \
    folly::doNotOptimizeAway(in->rowSizes[0]);                        \
  }

SIZE_BENCH(small, 0)
SIZE_BENCH(medium_i32, 1)
SIZE_BENCH(large_i64, 2)

} // namespace bytedance::bolt::row

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  bytedance::bolt::memory::MemoryManager::initialize({});
  bytedance::bolt::row::printSerdeOnlySerializedSizes();
  folly::runBenchmarks();
  return 0;
}
