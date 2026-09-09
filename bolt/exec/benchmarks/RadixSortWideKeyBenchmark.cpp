/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include <folly/ScopeGuard.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <time.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <limits>
#include <numeric>

#include "bolt/common/base/SpillConfig.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/core/PlanFragment.h"
#include "bolt/core/QueryConfig.h"
#include "bolt/core/QueryCtx.h"
#include "bolt/exec/Driver.h"
#include "bolt/exec/Operator.h"
#include "bolt/exec/SortBuffer.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/radixsort/RadixSortBuffer.h"
#include "bolt/exec/radixsort/RadixSortKey.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"
#include "bolt/functions/prestosql/types/HyperLogLogType.h"
#include "bolt/functions/prestosql/types/JsonType.h"
#include "bolt/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "bolt/serializers/PrestoSerializer.h"
#include "bolt/vector/FlatVector.h"

DEFINE_string(bolt_benchmark_wide_type, "mixed", "Key family.");
DEFINE_uint32(bolt_benchmark_wide_columns, 64, "Number of key columns.");
DEFINE_uint32(bolt_benchmark_wide_rows, 8192, "Total rows.");
DEFINE_uint32(bolt_benchmark_wide_batch, 512, "Input batch rows.");
DEFINE_uint32(bolt_benchmark_wide_output, 1024, "Output batch rows.");
DEFINE_uint32(bolt_benchmark_wide_elements, 8, "Elements per complex value.");
DEFINE_uint32(
    bolt_benchmark_wide_physical_rows,
    0,
    "If non-zero, repeat this many physical top-level rows through a dictionary.");
DEFINE_uint32(bolt_benchmark_wide_string_size, 64, "String bytes.");
DEFINE_uint32(bolt_benchmark_wide_null_pct, 0, "Percent nulls.");
DEFINE_uint32(
    bolt_benchmark_wide_child_layers,
    0,
    "Dictionary layers on children.");
DEFINE_uint32(
    bolt_benchmark_wide_layers,
    0,
    "Dictionary layers on top-level keys.");
DEFINE_bool(
    bolt_benchmark_wide_valid_nulls,
    false,
    "Keep all-valid null bitmaps.");
DEFINE_bool(
    bolt_benchmark_wide_constant,
    false,
    "Constant-encode key columns.");
DEFINE_bool(
    bolt_benchmark_wide_escapes,
    false,
    "Embed zero and one bytes in strings.");
DEFINE_bool(
    bolt_benchmark_wide_verify,
    false,
    "Compare every output with independent comparator order.");
DEFINE_uint32(
    bolt_benchmark_wide_prefix,
    0,
    "Number of constant-value leading keys.");
DEFINE_string(
    bolt_benchmark_wide_order,
    "random",
    "random, sorted, reverse, low_card.");
DEFINE_string(bolt_benchmark_wide_direction, "mixed", "asc, desc, mixed.");
DEFINE_string(bolt_benchmark_wide_impl, "both", "legacy, radix, both.");
DEFINE_uint32(
    bolt_benchmark_wide_runs,
    4,
    "In-process repetitions including a warmup.");
DEFINE_bool(bolt_benchmark_wide_jit, true, "Enable legacy comparator JIT.");
DEFINE_bool(
    bolt_benchmark_wide_known_nulls,
    true,
    "Pass observed top-level nullability to the decoder.");
DEFINE_bool(
    bolt_benchmark_wide_mask_alternate,
    false,
    "Skip alternate columns where codec supports selective decode.");
DEFINE_string(bolt_benchmark_wide_mode, "codec", "codec or buffer.");
DEFINE_uint32(
    bolt_benchmark_wide_spill_every,
    0,
    "In buffer mode, spill after every N input batches.");
DEFINE_string(
    bolt_benchmark_wide_spill_codec,
    "none",
    "Spill compression: none, lz4, zstd.");
DEFINE_bool(
    bolt_benchmark_wide_special_values,
    false,
    "Include numeric limits, infinities, NaNs and signed zeros.");
DEFINE_bool(
    bolt_benchmark_wide_permute,
    false,
    "Use non-identity dictionary mappings.");
DEFINE_bool(
    bolt_benchmark_wide_map_keys_sorted,
    true,
    "Generate MAP keys in ascending rather than descending order.");
DEFINE_bool(
    bolt_benchmark_wide_list_types,
    false,
    "Print supported case names and exit.");

namespace bytedance::bolt::exec::radixsort::benchmark {
namespace {

uint64_t mix(uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

TypePtr keyType(const std::string& kind, uint32_t column) {
  if (kind == "bool")
    return BOOLEAN();
  if (kind == "i8")
    return TINYINT();
  if (kind == "i16")
    return SMALLINT();
  if (kind == "i32")
    return INTEGER();
  if (kind == "i64")
    return BIGINT();
  if (kind == "i128")
    return HUGEINT();
  if (kind == "float")
    return REAL();
  if (kind == "double")
    return DOUBLE();
  if (kind == "decimal64")
    return DECIMAL(18, 3);
  if (kind == "decimal128")
    return DECIMAL(38, 6);
  if (kind == "timestamp")
    return TIMESTAMP();
  if (kind == "date")
    return DATE();
  if (kind == "interval_day_time")
    return INTERVAL_DAY_TIME();
  if (kind == "interval_year_month")
    return INTERVAL_YEAR_MONTH();
  if (kind == "unknown")
    return UNKNOWN();
  if (kind == "json")
    return JSON();
  if (kind == "hyperloglog")
    return HYPERLOGLOG();
  if (kind == "timestamp_tz")
    return TIMESTAMP_WITH_TIME_ZONE();
  if (kind == "array")
    return ARRAY(BIGINT());
  if (kind == "map")
    return MAP(VARCHAR(), VARCHAR());
  if (kind == "row")
    return ROW({BIGINT(), VARCHAR()});
  if (kind == "string")
    return VARCHAR();
  if (kind == "binary")
    return VARBINARY();
  if (kind == "array_i64")
    return ARRAY(BIGINT());
  if (kind == "array_string")
    return ARRAY(VARCHAR());
  if (kind == "array_decimal")
    return ARRAY(DECIMAL(38, 6));
  if (kind == "map_string")
    return MAP(VARCHAR(), VARCHAR());
  if (kind == "map_i64")
    return MAP(BIGINT(), BIGINT());
  if (kind == "row_fixed")
    return ROW({BIGINT(), DOUBLE(), TIMESTAMP()});
  if (kind == "row_string")
    return ROW({BIGINT(), VARCHAR(), VARCHAR()});
  if (kind == "nested")
    return ROW({ARRAY(MAP(VARCHAR(), VARCHAR())), ARRAY(BIGINT())});
  if (kind == "array_array")
    return ARRAY(ARRAY(BIGINT()));
  if (kind.starts_with("array:"))
    return ARRAY(keyType(kind.substr(6), column));
  if (kind.starts_with("map:"))
    return MAP(INTEGER(), keyType(kind.substr(4), column));
  if (kind.starts_with("row:"))
    return ROW({keyType(kind.substr(4), column)});
  if (kind == "mixed_complex") {
    static const std::vector<std::string> kinds{
        "array_i64", "map_string", "row_string", "array_decimal"};
    return keyType(kinds[column % kinds.size()], column);
  }
  BOLT_CHECK_EQ(kind, "mixed");
  static const std::vector<TypePtr> mixed{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      HUGEINT(),
      REAL(),
      DOUBLE(),
      DECIMAL(18, 3),
      DECIMAL(38, 6),
      TIMESTAMP(),
      VARCHAR(),
      VARBINARY(),
      DATE()};
  return mixed[column % mixed.size()];
}

uint64_t rowValue(uint64_t logical, uint64_t salt, bool constant) {
  if (constant)
    return salt;
  if (FLAGS_bolt_benchmark_wide_order == "sorted")
    return logical;
  if (FLAGS_bolt_benchmark_wide_order == "reverse")
    return FLAGS_bolt_benchmark_wide_rows - logical;
  const auto value = mix(logical + salt * 123457);
  return FLAGS_bolt_benchmark_wide_order == "low_card" ? value % 8 : value;
}

template <typename T>
T scalarValue(T regular, vector_size_t row, const Type& type, bool mapKey) {
  if constexpr ((std::is_integral_v<T> ||
                 std::is_same_v<T, int128_t>)&&!std::is_same_v<T, bool>) {
    if (FLAGS_bolt_benchmark_wide_special_values && !mapKey) {
      T minimum = std::numeric_limits<T>::min();
      T maximum = std::numeric_limits<T>::max();
      if (type.isShortDecimal()) {
        maximum = static_cast<T>(999999999999999999LL);
        minimum = -maximum;
      } else if (type.isLongDecimal()) {
        maximum = static_cast<T>(
            HugeInt::fromString("99999999999999999999999999999999999999"));
        minimum = -maximum;
      }
      switch (row % 6) {
        case 0:
          return minimum;
        case 1:
          return maximum;
        case 2:
          return T{0};
        case 3:
          return T{-1};
        case 4:
          return T{1};
        default:
          return regular;
      }
    }
  }
  return regular;
}

VectorPtr wrap(VectorPtr vector, uint32_t layers) {
  for (uint32_t layer = 0; layer < layers; ++layer) {
    auto indices =
        AlignedBuffer::allocate<vector_size_t>(vector->size(), vector->pool());
    std::iota(
        indices->asMutable<vector_size_t>(),
        indices->asMutable<vector_size_t>() + vector->size(),
        0);
    if (FLAGS_bolt_benchmark_wide_permute) {
      std::reverse(
          indices->asMutable<vector_size_t>(),
          indices->asMutable<vector_size_t>() + vector->size());
    }
    vector =
        BaseVector::wrapInDictionary(nullptr, indices, vector->size(), vector);
  }
  return vector;
}

VectorPtr makeVector(
    const TypePtr& type,
    vector_size_t size,
    uint64_t offset,
    uint64_t salt,
    bool constant,
    memory::MemoryPool* pool,
    bool mapKey = false) {
  VectorPtr vector;
  const auto count = FLAGS_bolt_benchmark_wide_elements;
  BOLT_CHECK_LE(
      static_cast<uint64_t>(size) * count,
      std::numeric_limits<vector_size_t>::max());
  auto value = [&](vector_size_t row) {
    if (mapKey) {
      const auto entry = static_cast<uint64_t>(row % count);
      return FLAGS_bolt_benchmark_wide_map_keys_sorted ? entry
                                                       : count - entry - 1;
    }
    return rowValue(offset + row, salt, constant);
  };
  const auto floatingValue = [&](vector_size_t row) {
    if (FLAGS_bolt_benchmark_wide_special_values) {
      constexpr std::array<double, 8> kValues{
          0.0,
          -0.0,
          1.0,
          -1.0,
          std::numeric_limits<double>::infinity(),
          -std::numeric_limits<double>::infinity(),
          std::numeric_limits<double>::quiet_NaN(),
          std::numeric_limits<double>::max()};
      return kValues[row % kValues.size()];
    }
    return static_cast<double>(static_cast<int32_t>(value(row))) / 8;
  };
  switch (type->kind()) {
    case TypeKind::UNKNOWN:
      vector = BaseVector::createNullConstant(type, size, pool);
      break;
    case TypeKind::ARRAY:
    case TypeKind::MAP: {
      auto offsets = AlignedBuffer::allocate<vector_size_t>(size, pool);
      auto sizes = AlignedBuffer::allocate<vector_size_t>(size, pool);
      for (vector_size_t row = 0; row < size; ++row) {
        offsets->asMutable<vector_size_t>()[row] = row * count;
        sizes->asMutable<vector_size_t>()[row] = count;
      }
      auto child = wrap(
          makeVector(
              type->childAt(0),
              size * count,
              offset * count,
              salt + 1,
              constant,
              pool,
              type->kind() == TypeKind::MAP),
          FLAGS_bolt_benchmark_wide_child_layers);
      if (type->kind() == TypeKind::ARRAY) {
        vector = std::make_shared<ArrayVector>(
            pool, type, nullptr, size, offsets, sizes, child);
      } else {
        auto values = wrap(
            makeVector(
                type->childAt(1),
                size * count,
                offset * count,
                salt + 2,
                constant,
                pool),
            FLAGS_bolt_benchmark_wide_child_layers);
        vector = std::make_shared<MapVector>(
            pool, type, nullptr, size, offsets, sizes, child, values);
      }
      break;
    }
    case TypeKind::ROW: {
      std::vector<VectorPtr> children;
      for (uint32_t child = 0; child < type->size(); ++child) {
        children.push_back(wrap(
            makeVector(
                type->childAt(child),
                size,
                offset,
                salt + child + 1,
                constant,
                pool),
            FLAGS_bolt_benchmark_wide_child_layers));
      }
      vector = std::make_shared<RowVector>(
          pool, type, nullptr, size, std::move(children));
      break;
    }
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      auto flat = BaseVector::create<FlatVector<StringView>>(type, size, pool);
      for (vector_size_t row = 0; row < size; ++row) {
        auto number = value(row);
        std::string string(FLAGS_bolt_benchmark_wide_string_size, 'x');
        for (uint32_t index = 0; index < std::min<size_t>(16, string.size());
             ++index) {
          string[string.size() - index - 1] =
              "0123456789abcdef"[(number >> (index * 4)) & 15];
        }
        if (mapKey)
          string += std::to_string(number);
        if (FLAGS_bolt_benchmark_wide_escapes)
          string += std::string("\0\1", 2);
        flat->set(row, StringView(string));
      }
      vector = flat;
      break;
    }
#define MAKE_SCALAR(KIND, CPP, EXPR)                                   \
  case TypeKind::KIND: {                                               \
    auto flat = BaseVector::create<FlatVector<CPP>>(type, size, pool); \
    for (vector_size_t row = 0; row < size; ++row) {                   \
      const auto number = value(row);                                  \
      flat->set(row, scalarValue<CPP>((EXPR), row, *type, mapKey));    \
    }                                                                  \
    vector = flat;                                                     \
    break;                                                             \
  }
      MAKE_SCALAR(BOOLEAN, bool, number & 1)
      MAKE_SCALAR(TINYINT, int8_t, static_cast<int8_t>(number))
      MAKE_SCALAR(SMALLINT, int16_t, static_cast<int16_t>(number))
      MAKE_SCALAR(INTEGER, int32_t, static_cast<int32_t>(number))
      MAKE_SCALAR(
          BIGINT, int64_t, static_cast<int64_t>(number % 999999999999999999ULL))
      MAKE_SCALAR(
          HUGEINT,
          int128_t,
          static_cast<int128_t>(static_cast<int64_t>(number)) * 123456789)
      MAKE_SCALAR(REAL, float, static_cast<float>(floatingValue(row)))
      MAKE_SCALAR(DOUBLE, double, floatingValue(row))
      MAKE_SCALAR(
          TIMESTAMP,
          Timestamp,
          Timestamp(number % 1000000000, number % 1000000000))
#undef MAKE_SCALAR
    default:
      BOLT_FAIL("Unsupported benchmark type {}", type->toString());
  }
  if (!mapKey && size > 0 && type->kind() != TypeKind::UNKNOWN) {
    for (vector_size_t row = 0; row < size; ++row) {
      if (mix(offset + row + salt) % 100 < FLAGS_bolt_benchmark_wide_null_pct) {
        vector->setNull(row, true);
      }
    }
    if (FLAGS_bolt_benchmark_wide_valid_nulls &&
        FLAGS_bolt_benchmark_wide_null_pct == 0) {
      vector->setNull(0, true);
      vector->setNull(0, false);
    }
  }
  return vector;
}

uint64_t cpuUs() {
  timespec time{};
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &time);
  return static_cast<uint64_t>(time.tv_sec) * 1000000 + time.tv_nsec / 1000;
}

struct StringStorage {
  uint64_t used{0};
  uint64_t capacity{0};
};

void addStringStorage(const VectorPtr& vector, StringStorage& storage) {
  if (vector == nullptr) {
    return;
  }
  if (vector->typeKind() == TypeKind::VARCHAR ||
      vector->typeKind() == TypeKind::VARBINARY) {
    auto* strings = vector->as<FlatVector<StringView>>();
    if (strings != nullptr) {
      for (const auto& buffer : strings->stringBuffers()) {
        storage.used += buffer->size();
        storage.capacity += buffer->capacity();
      }
    }
  } else if (auto* row = vector->as<RowVector>()) {
    for (const auto& child : row->children())
      addStringStorage(child, storage);
  } else if (auto* array = vector->as<ArrayVector>()) {
    addStringStorage(array->elements(), storage);
  } else if (auto* map = vector->as<MapVector>()) {
    addStringStorage(map->mapKeys(), storage);
    addStringStorage(map->mapValues(), storage);
  }
}

void runCodec(
    uint32_t trial,
    const RowTypePtr& type,
    const std::vector<RowVectorPtr>& inputs,
    const std::vector<CompareFlags>& flags) {
  auto root = memory::memoryManager()->addRootPool();
  auto pool = root->addLeafChild("codec");
  std::unique_ptr<RadixSortKeyCodec> codec;
  RadixSortKeyCodec::bind(type->children(), flags, codec);
  EncodedKeyBatch keys;
  BufferPtr cursorScratch;
  RowVectorPtr output;
  uint64_t encodeUs = 0;
  uint64_t decodeUs = 0;
  uint64_t encodeAllocs = 0;
  uint64_t decodeAllocs = 0;
  uint64_t encodedBytes = 0;
  StringStorage strings;
  for (const auto& input : inputs) {
    auto before = pool->stats().numAllocs;
    auto start = cpuUs();
    codec->encode(*input, pool.get(), keys);
    encodeUs += cpuUs() - start;
    encodeAllocs += pool->stats().numAllocs - before;

    std::vector<std::array<char, sizeof(uint64_t)>> fixed;
    std::vector<EncodedKeyView> views(input->size());
    if (keys.format() == EncodedKeyFormat::kFixed64) {
      fixed.resize(input->size());
      for (vector_size_t row = 0; row < input->size(); ++row) {
        storeUnaligned<uint64_t>(
            fixed[row].data(), toBigEndian(keys.fixedKeyAt(row)));
        views[row] = {std::string_view(fixed[row].data(), sizeof(uint64_t))};
      }
      encodedBytes += input->size() * sizeof(uint64_t);
    } else {
      for (vector_size_t row = 0; row < input->size(); ++row) {
        views[row] = {keys.variableKeyAt(row)};
        encodedBytes += views[row].bytes.size();
      }
    }
    before = pool->stats().numAllocs;
    std::vector<uint8_t> decodedColumns;
    if (FLAGS_bolt_benchmark_wide_mask_alternate) {
      decodedColumns.resize(type->size());
      for (uint32_t column = 0; column < type->size(); ++column)
        decodedColumns[column] = column % 2 == 0;
    }
    std::vector<uint8_t> mayHaveNulls;
    if (FLAGS_bolt_benchmark_wide_known_nulls) {
      for (const auto& child : input->children())
        mayHaveNulls.push_back(child->mayHaveNulls());
    }
    start = cpuUs();
    codec->decode(
        views, decodedColumns, mayHaveNulls, pool.get(), cursorScratch, output);
    decodeUs += cpuUs() - start;
    decodeAllocs += pool->stats().numAllocs - before;
    addStringStorage(output, strings);
    if (FLAGS_bolt_benchmark_wide_verify) {
      for (column_index_t column = 0; column < type->size(); ++column) {
        if (output->childAt(column) == nullptr)
          continue;
        for (vector_size_t row = 0; row < input->size(); ++row) {
          BOLT_CHECK(
              output->childAt(column)->equalValueAt(
                  input->childAt(column).get(), row, row),
              "Codec mismatch column {} row {}",
              column,
              row);
        }
      }
      auto compareRows = [&](vector_size_t left, vector_size_t right) {
        for (column_index_t column = 0; column < type->size(); ++column) {
          const auto result =
              input->childAt(column)
                  ->compare(
                      input->childAt(column).get(), left, right, flags[column])
                  .value();
          if (result != 0)
            return result;
        }
        return 0;
      };
      for (vector_size_t row = 1; row < input->size(); ++row) {
        const auto expected = compareRows(row - 1, row);
        const auto actual = keys.format() == EncodedKeyFormat::kFixed64
            ? (keys.fixedKeyAt(row - 1) > keys.fixedKeyAt(row)) -
                (keys.fixedKeyAt(row - 1) < keys.fixedKeyAt(row))
            : views[row - 1].bytes.compare(views[row].bytes);
        BOLT_CHECK_EQ(
            (actual > 0) - (actual < 0), (expected > 0) - (expected < 0));
      }
    }
  }
  std::printf(
      "RESULT impl=codec trial=%u type=%s columns=%u rows=%u encode=%.3f decode=%.3f "
      "total=%.3f encodeAllocs=%llu decodeAllocs=%llu peak=%lld encodedBytes=%llu "
      "stringUsed=%llu stringCapacity=%llu verified=%d\n",
      trial,
      FLAGS_bolt_benchmark_wide_type.c_str(),
      FLAGS_bolt_benchmark_wide_columns,
      FLAGS_bolt_benchmark_wide_rows,
      encodeUs / 1000.0,
      decodeUs / 1000.0,
      (encodeUs + decodeUs) / 1000.0,
      (unsigned long long)encodeAllocs,
      (unsigned long long)decodeAllocs,
      (long long)pool->peakBytes(),
      (unsigned long long)encodedBytes,
      (unsigned long long)strings.used,
      (unsigned long long)strings.capacity,
      FLAGS_bolt_benchmark_wide_verify);
  std::fflush(stdout);
}

template <typename Buffer>
void run(
    const char* name,
    uint32_t trial,
    const RowTypePtr& type,
    const std::vector<RowVectorPtr>& inputs,
    const std::vector<CompareFlags>& flags,
    const std::vector<column_index_t>& channels,
    const std::vector<uint64_t>& expected,
    OperatorCtx* context) {
  auto root = memory::memoryManager()->addRootPool();
  auto pool = root->addLeafChild("sort");
  tsan_atomic<bool> nonReclaimable{false};
  std::string spillDirectory;
  auto cleanup = folly::makeGuard([&] {
    if (!spillDirectory.empty())
      std::filesystem::remove_all(spillDirectory);
  });
  std::unique_ptr<common::SpillConfig> spillConfig;
  if (FLAGS_bolt_benchmark_wide_spill_every != 0) {
    auto pattern =
        (std::filesystem::temp_directory_path() / "radix-wide-XXXXXX").string();
    BOLT_CHECK_NOT_NULL(mkdtemp(pattern.data()));
    spillDirectory = pattern;
    spillConfig = std::make_unique<common::SpillConfig>(
        [&]() -> const std::string& { return spillDirectory; },
        [](uint64_t) {},
        "radix-wide",
        0,
        false,
        1 << 20,
        nullptr,
        5,
        10,
        0,
        0,
        0,
        0,
        0,
        0,
        FLAGS_bolt_benchmark_wide_spill_codec);
    spillConfig->setJITenableForSpill(FLAGS_bolt_benchmark_wide_jit);
  }
  std::unique_ptr<Buffer> buffer;
  if constexpr (std::is_same_v<Buffer, SortBuffer>) {
    buffer = std::make_unique<Buffer>(
        type,
        channels,
        flags,
        pool.get(),
        &nonReclaimable,
        spillConfig.get(),
        0,
        spillConfig == nullptr ? context : nullptr);
  } else {
    buffer = std::make_unique<Buffer>(
        type, channels, flags, pool.get(), spillConfig.get());
  }
  auto before = pool->stats();
  auto start = cpuUs();
  uint32_t inputBatches = 0;
  uint64_t spillCpu = 0;
  for (const auto& input : inputs) {
    buffer->addInput(input);
    if (spillConfig != nullptr &&
        ++inputBatches % FLAGS_bolt_benchmark_wide_spill_every == 0) {
      const auto spillStart = cpuUs();
      buffer->spill();
      spillCpu += cpuUs() - spillStart;
    }
  }
  const auto inputCpu = cpuUs() - start;
  auto afterInput = pool->stats();
  start = cpuUs();
  buffer->noMoreInput();
  const auto sortCpu = cpuUs() - start;
  uint64_t outputCpu = 0;
  uint64_t rows = 0;
  while (true) {
    start = cpuUs();
    auto output = buffer->getOutput(FLAGS_bolt_benchmark_wide_output);
    outputCpu += cpuUs() - start;
    if (!output)
      break;
    if (FLAGS_bolt_benchmark_wide_verify) {
      for (vector_size_t row = 0; row < output->size(); ++row) {
        auto index = expected[rows + row];
        const auto& input = inputs[index / FLAGS_bolt_benchmark_wide_batch];
        for (column_index_t column = 0; column < type->size(); ++column) {
          BOLT_CHECK(
              output->childAt(column)->equalValueAt(
                  input->childAt(column).get(),
                  row,
                  index % FLAGS_bolt_benchmark_wide_batch),
              "Mismatch row {} column {}",
              rows + row,
              column);
        }
      }
    }
    rows += output->size();
    start = cpuUs();
    output.reset();
    outputCpu += cpuUs() - start;
  }
  BOLT_CHECK_EQ(rows, FLAGS_bolt_benchmark_wide_rows);
  auto afterOutput = pool->stats();
  const auto spilled = buffer->spilledStats();
  std::printf(
      "RESULT impl=%s trial=%u type=%s columns=%u rows=%llu input=%.3f sort=%.3f output=%.3f total=%.3f inputAllocs=%llu outputAllocs=%llu peak=%lld verified=%d spillCpu=%.3f spillRuns=%llu spillInputBytes=%llu spilledBytes=%llu\n",
      name,
      trial,
      FLAGS_bolt_benchmark_wide_type.c_str(),
      FLAGS_bolt_benchmark_wide_columns,
      (unsigned long long)rows,
      inputCpu / 1000.0,
      sortCpu / 1000.0,
      outputCpu / 1000.0,
      (inputCpu + sortCpu + outputCpu) / 1000.0,
      (unsigned long long)(afterInput.numAllocs - before.numAllocs),
      (unsigned long long)(afterOutput.numAllocs - afterInput.numAllocs),
      (long long)afterOutput.peakBytes,
      FLAGS_bolt_benchmark_wide_verify,
      spillCpu / 1000.0,
      (unsigned long long)(spilled ? spilled->spillRuns : 0),
      (unsigned long long)(spilled ? spilled->spilledInputBytes : 0),
      (unsigned long long)(spilled ? spilled->spilledBytes : 0));
  std::fflush(stdout);
}

void mainBenchmark() {
  BOLT_CHECK_GT(FLAGS_bolt_benchmark_wide_columns, 0);
  BOLT_CHECK_GT(FLAGS_bolt_benchmark_wide_rows, 0);
  BOLT_CHECK_GT(FLAGS_bolt_benchmark_wide_batch, 0);
  BOLT_CHECK_GT(FLAGS_bolt_benchmark_wide_output, 0);
  BOLT_CHECK_LE(
      FLAGS_bolt_benchmark_wide_rows,
      std::numeric_limits<vector_size_t>::max());
  BOLT_CHECK_LE(
      FLAGS_bolt_benchmark_wide_batch,
      std::numeric_limits<vector_size_t>::max());
  BOLT_CHECK_LE(
      FLAGS_bolt_benchmark_wide_output,
      std::numeric_limits<vector_size_t>::max());
  BOLT_CHECK_LE(FLAGS_bolt_benchmark_wide_null_pct, 100);
  BOLT_CHECK_LE(
      FLAGS_bolt_benchmark_wide_prefix, FLAGS_bolt_benchmark_wide_columns);
  auto source = memory::memoryManager()->addLeafPool("source");
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<CompareFlags> flags;
  std::vector<column_index_t> channels;
  for (uint32_t column = 0; column < FLAGS_bolt_benchmark_wide_columns;
       ++column) {
    names.push_back("k" + std::to_string(column));
    types.push_back(keyType(FLAGS_bolt_benchmark_wide_type, column));
    const bool ascending = FLAGS_bolt_benchmark_wide_direction == "asc" ||
        (FLAGS_bolt_benchmark_wide_direction == "mixed" && column % 2 == 0);
    flags.push_back(
        {column % 3 == 0,
         ascending,
         false,
         CompareFlags::NullHandlingMode::kNullAsValue});
    channels.push_back(column);
  }
  const auto type = ROW(std::move(names), std::move(types));
  std::vector<RowVectorPtr> inputs;
  for (uint32_t offset = 0; offset < FLAGS_bolt_benchmark_wide_rows;
       offset += FLAGS_bolt_benchmark_wide_batch) {
    const auto count = std::min(
        FLAGS_bolt_benchmark_wide_batch,
        FLAGS_bolt_benchmark_wide_rows - offset);
    std::vector<VectorPtr> children;
    for (uint32_t column = 0; column < type->size(); ++column) {
      const auto physicalRows = FLAGS_bolt_benchmark_wide_physical_rows == 0
          ? count
          : std::min<vector_size_t>(
                count, FLAGS_bolt_benchmark_wide_physical_rows);
      auto child = makeVector(
          type->childAt(column),
          physicalRows,
          offset,
          column + 1,
          column < FLAGS_bolt_benchmark_wide_prefix,
          source.get());
      if (physicalRows < count) {
        auto indices =
            AlignedBuffer::allocate<vector_size_t>(count, source.get());
        for (vector_size_t row = 0; row < count; ++row) {
          indices->asMutable<vector_size_t>()[row] = row % physicalRows;
        }
        child = BaseVector::wrapInDictionary(
            nullptr, std::move(indices), count, std::move(child));
      }
      if (FLAGS_bolt_benchmark_wide_constant) {
        child = BaseVector::wrapInConstant(count, 0, child);
      } else {
        child = wrap(child, FLAGS_bolt_benchmark_wide_layers);
      }
      children.push_back(child);
    }
    inputs.push_back(std::make_shared<RowVector>(
        source.get(), type, nullptr, count, std::move(children)));
  }
  if (FLAGS_bolt_benchmark_wide_mode == "codec") {
    for (uint32_t trial = 0; trial < FLAGS_bolt_benchmark_wide_runs; ++trial) {
      runCodec(trial, type, inputs, flags);
    }
    return;
  }
  BOLT_CHECK_EQ(FLAGS_bolt_benchmark_wide_mode, "buffer");
  std::vector<uint64_t> expected;
  if (FLAGS_bolt_benchmark_wide_verify) {
    expected.resize(FLAGS_bolt_benchmark_wide_rows);
    std::iota(expected.begin(), expected.end(), 0);
    std::sort(expected.begin(), expected.end(), [&](uint64_t l, uint64_t r) {
      const auto& left = inputs[l / FLAGS_bolt_benchmark_wide_batch];
      const auto& right = inputs[r / FLAGS_bolt_benchmark_wide_batch];
      for (column_index_t c = 0; c < type->size(); ++c) {
        const auto cmp = left->childAt(c)
                             ->compare(
                                 right->childAt(c).get(),
                                 l % FLAGS_bolt_benchmark_wide_batch,
                                 r % FLAGS_bolt_benchmark_wide_batch,
                                 flags[c])
                             .value();
        if (cmp != 0)
          return cmp < 0;
      }
      return l < r;
    });
  }
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(1);
  auto query = core::QueryCtx::create(executor.get());
  query->testingOverrideConfigUnsafe(
      {{core::QueryConfig::kJitLevel,
        FLAGS_bolt_benchmark_wide_jit ? "-1" : "0"}});
  core::PlanFragment plan;
  plan.planNode = std::make_shared<core::ValuesNode>("values", inputs);
  auto task = Task::create(
      "wide-sort", std::move(plan), 0, query, Task::ExecutionMode::kParallel);
  DriverCtx driver(task, 0, 0, 0, 0);
  OperatorCtx context(&driver, "wide", 0, "OrderBy");
  for (uint32_t trial = 0; trial < FLAGS_bolt_benchmark_wide_runs; ++trial) {
    if (FLAGS_bolt_benchmark_wide_impl != "radix")
      run<SortBuffer>(
          "legacy", trial, type, inputs, flags, channels, expected, &context);
    if (FLAGS_bolt_benchmark_wide_impl != "legacy")
      run<RadixSortBuffer>(
          "radix", trial, type, inputs, flags, channels, expected, nullptr);
  }
  task->testingFinish();
}
} // namespace
} // namespace bytedance::bolt::exec::radixsort::benchmark

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  if (FLAGS_bolt_benchmark_wide_list_types) {
    std::puts(
        "bool i8 i16 i32 i64 i128 float double decimal64 decimal128 timestamp "
        "date interval_day_time interval_year_month string binary unknown json "
        "hyperloglog timestamp_tz array_i64 array_string array_decimal map_string "
        "map_i64 row_fixed row_string nested array_array mixed mixed_complex");
    std::puts(
        "Aliases: array, map, row. Recursive cases: array:<type>, map:<value-type>, row:<type>. "
        "OPAQUE, FUNCTION, VARIANT and INVALID are not supported sort-key types.");
    return 0;
  }
  bytedance::bolt::memory::MemoryManager::initialize({});
  if (FLAGS_bolt_benchmark_wide_spill_every != 0) {
    bytedance::bolt::filesystems::registerLocalFileSystem();
    bytedance::bolt::serializer::presto::PrestoVectorSerde::
        registerVectorSerde();
  }
  bytedance::bolt::exec::radixsort::benchmark::mainBenchmark();
}
