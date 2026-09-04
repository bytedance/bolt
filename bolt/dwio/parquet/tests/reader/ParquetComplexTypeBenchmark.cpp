/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2026-08-12.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/common/compression/Compression.h"
#include "bolt/dwio/common/BufferedInput.h"
#include "bolt/dwio/common/FileSink.h"
#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/common/Statistics.h"
#include "bolt/dwio/parquet/reader/ParquetReader.h"
#include "bolt/dwio/parquet/writer/Writer.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <sys/resource.h>

#include <algorithm>
#include <atomic>
#include <fstream>
#include <map>
#include <random>
#include <sstream>
#include <string_view>

using namespace bytedance::bolt;
using namespace bytedance::bolt::dwio;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::parquet;
using namespace bytedance::bolt::test;

DEFINE_int32(
    parquet_complex_data_page_version,
    1,
    "Parquet data page version to write. Supported values: 1 and 2.");
DEFINE_int32(
    parquet_complex_repdef_streaming_window_size,
    2 * 1024,
    "Number of rep/def levels decoded per streaming window; 0 disables "
    "streaming.");
DEFINE_bool(
    parquet_complex_print_runtime_stats,
    false,
    "Print Parquet reader runtime statistics for each scan.");
DEFINE_bool(
    parquet_complex_print_memory_stats,
    false,
    "Print scan-only RSS and MemoryPool statistics for each scan.");

namespace {

constexpr uint32_t kSeed = 20260812;
constexpr vector_size_t kRowsPerBatch = 2'048;
constexpr int32_t kNumBatches = 8;
constexpr uint64_t kRowsPerRowGroup = 4'096;
constexpr int64_t kBytesPerRowGroup = 128 * 1'024 * 1'024;
constexpr int64_t kDataPageSize = 64 * 1'024;

int64_t processRssKb() {
  std::ifstream input("/proc/self/status");
  std::string line;
  while (std::getline(input, line)) {
    if (line.rfind("VmRSS:", 0) != 0) {
      continue;
    }
    std::istringstream stream(line.substr(6));
    int64_t value = 0;
    stream >> value;
    return value;
  }
  return -1;
}

enum class DataSetKind {
  kControl,
  kList,
  kMap,
  kStringList,
  kStringMap,
  kNullable,
  kLongList,
  kNested,
  kMapStruct,
  kWideFeature,
};

enum class ProjectionKind {
  kAll,
  kRepresentative,
  kLongOnly,
  kNullHeavy,
};

enum class LengthShape {
  kFixedOne,
  kFixedSmall,
  kFixedMedium,
  kFixedLarge,
  kSparse,
  kBounded,
  kLong2K,
  kLongTail,
};

struct ColumnSpec {
  std::string name;
  TypePtr type;
};

struct DataSetSpec {
  std::string name;
  std::vector<ColumnSpec> columns;
  bool disableDictionary{true};
};

std::string toString(DataSetKind kind) {
  switch (kind) {
    case DataSetKind::kControl:
      return "control";
    case DataSetKind::kList:
      return "list";
    case DataSetKind::kMap:
      return "map";
    case DataSetKind::kStringList:
      return "stringList";
    case DataSetKind::kStringMap:
      return "stringMap";
    case DataSetKind::kNullable:
      return "nullable";
    case DataSetKind::kLongList:
      return "longList";
    case DataSetKind::kNested:
      return "nested";
    case DataSetKind::kMapStruct:
      return "mapStruct";
    case DataSetKind::kWideFeature:
      return "wideFeature";
  }
  return "unknown";
}

std::string toString(ProjectionKind kind) {
  switch (kind) {
    case ProjectionKind::kAll:
      return "all";
    case ProjectionKind::kRepresentative:
      return "representative";
    case ProjectionKind::kLongOnly:
      return "longOnly";
    case ProjectionKind::kNullHeavy:
      return "nullHeavy";
  }
  return "unknown";
}

int32_t stableHash(uint64_t value) {
  value ^= value >> 33;
  value *= 0xff51afd7ed558ccdULL;
  value ^= value >> 33;
  value *= 0xc4ceb9fe1a85ec53ULL;
  value ^= value >> 33;
  return static_cast<int32_t>(value & 0x7fffffff);
}

bool periodicNull(vector_size_t row, int32_t percent, uint64_t salt = 0) {
  if (percent <= 0) {
    return false;
  }
  if (percent >= 100) {
    return true;
  }
  return stableHash(static_cast<uint64_t>(row) * 1315423911ULL + salt) % 100 <
      percent;
}

vector_size_t listSizeAt(vector_size_t row, LengthShape shape, uint64_t salt) {
  const auto h = stableHash(static_cast<uint64_t>(row) * 2654435761ULL + salt);
  switch (shape) {
    case LengthShape::kFixedOne:
      return 1;
    case LengthShape::kFixedSmall:
      return 4;
    case LengthShape::kFixedMedium:
      return 50;
    case LengthShape::kFixedLarge:
      return 256;
    case LengthShape::kSparse:
      if (h % 100 < 78) {
        return 0;
      }
      return 1 + (h % 8);
    case LengthShape::kBounded:
      if (h % 100 < 8) {
        return 0;
      }
      return 1 + (h % 20);
    case LengthShape::kLong2K:
      return 2'000;
    case LengthShape::kLongTail:
      if (h % 100 < 70) {
        return h % 33;
      }
      if (h % 100 < 95) {
        return 512 + (h % 1'536);
      }
      return 2'048 + (h % 4'096);
  }
  return 0;
}

template <typename T>
T numericValue(vector_size_t row, vector_size_t index, uint64_t salt) {
  const auto value = static_cast<int64_t>(
      stableHash(static_cast<uint64_t>(row) * 1'000'003ULL + index + salt));
  if constexpr (std::is_same_v<T, float>) {
    return static_cast<float>(value % 10'000) / 100.0f;
  } else if constexpr (std::is_same_v<T, double>) {
    return static_cast<double>(value % 1'000'000) / 1'000.0;
  } else {
    return static_cast<T>(value);
  }
}

BufferPtr makeNullsBuffer(
    memory::MemoryPool* pool,
    vector_size_t size,
    std::function<bool(vector_size_t)> isNullAt) {
  BufferPtr nulls;
  for (vector_size_t row = 0; row < size; ++row) {
    if (isNullAt(row)) {
      if (!nulls) {
        nulls = AlignedBuffer::allocate<bool>(size, pool, bits::kNotNull);
      }
      bits::setNull(nulls->asMutable<uint64_t>(), row, true);
    }
  }
  return nulls;
}

template <typename T>
ArrayVectorPtr makeNumericList(
    VectorMaker& maker,
    vector_size_t size,
    LengthShape shape,
    int32_t listNullPct,
    int32_t elementNullPct,
    uint64_t salt,
    const TypePtr& arrayType = ARRAY(CppToType<T>::create())) {
  return maker.arrayVector<T>(
      size,
      [shape, salt](vector_size_t row) { return listSizeAt(row, shape, salt); },
      [salt](vector_size_t index) {
        return numericValue<T>(index / 4'096, index, salt);
      },
      [listNullPct, salt](vector_size_t row) {
        return periodicNull(row, listNullPct, salt + 17);
      },
      [elementNullPct, salt](vector_size_t index) {
        return periodicNull(index, elementNullPct, salt + 29);
      },
      arrayType);
}

ArrayVectorPtr makeStringList(
    VectorMaker& maker,
    vector_size_t size,
    LengthShape shape,
    int32_t listNullPct,
    uint64_t salt) {
  return maker.arrayVector<std::string>(
      size,
      [shape, salt](vector_size_t row) { return listSizeAt(row, shape, salt); },
      [salt](vector_size_t row, vector_size_t index) {
        return fmt::format("s{}_{}", row % 997, (index + salt) % 131);
      },
      [listNullPct, salt](vector_size_t row) {
        return periodicNull(row, listNullPct, salt + 37);
      },
      ARRAY(VARCHAR()));
}

VectorPtr makeStructValue(
    VectorMaker& maker,
    const std::string& fieldName,
    const VectorPtr& value,
    int32_t structNullPct,
    uint64_t salt) {
  const auto size = value->size();
  auto type = ROW({fieldName}, {value->type()});
  auto nulls = makeNullsBuffer(
      value->pool(), size, [structNullPct, salt](vector_size_t row) {
        return periodicNull(row, structNullPct, salt);
      });
  if (nulls) {
    auto clearCollectionSize =
        [&](auto& self, const VectorPtr& vector, vector_size_t row) -> void {
      if (auto array = vector->as<ArrayVector>()) {
        array->setOffsetAndSize(row, array->offsetAt(row), 0);
        return;
      }
      if (auto map = vector->as<MapVector>()) {
        map->setOffsetAndSize(row, map->offsetAt(row), 0);
        return;
      }
      if (auto rowVector = vector->as<RowVector>()) {
        for (const auto& child : rowVector->children()) {
          self(self, child, row);
        }
      }
    };
    for (vector_size_t row = 0; row < size; ++row) {
      if (bits::isBitNull(nulls->as<uint64_t>(), row)) {
        clearCollectionSize(clearCollectionSize, value, row);
      }
    }
  }
  return std::make_shared<RowVector>(
      value->pool(), type, nulls, size, std::vector<VectorPtr>{value});
}

ArrayVectorPtr makeNestedList(
    VectorMaker& maker,
    vector_size_t size,
    int32_t outerNullPct,
    uint64_t salt) {
  std::vector<vector_size_t> outerOffsets;
  std::vector<vector_size_t> outerNulls;
  std::vector<vector_size_t> innerOffsets;
  std::vector<int64_t> values;
  outerOffsets.reserve(size);
  for (vector_size_t row = 0; row < size; ++row) {
    outerOffsets.push_back(innerOffsets.size());
    if (periodicNull(row, outerNullPct, salt)) {
      outerNulls.push_back(row);
      continue;
    }
    const auto outerSize =
        listSizeAt(row, LengthShape::kSparse, salt + 3) == 0 ? 0 : 24;
    for (vector_size_t outerIndex = 0; outerIndex < outerSize; ++outerIndex) {
      innerOffsets.push_back(values.size());
      for (vector_size_t innerIndex = 0; innerIndex < 11; ++innerIndex) {
        values.push_back(
            numericValue<int64_t>(row, outerIndex * 11 + innerIndex, salt));
      }
    }
  }
  auto elements = maker.flatVector<int64_t>(values);
  auto innerArrays = maker.arrayVector(innerOffsets, elements);
  return maker.arrayVector(outerOffsets, innerArrays, outerNulls);
}

ArrayVectorPtr
makeListOfStruct(VectorMaker& maker, vector_size_t size, uint64_t salt) {
  auto rowType = ROW({"id", "score"}, {BIGINT(), DOUBLE()});
  std::vector<std::vector<std::optional<std::tuple<int64_t, double>>>> data;
  data.reserve(size);
  for (vector_size_t row = 0; row < size; ++row) {
    const auto length = listSizeAt(row, LengthShape::kBounded, salt);
    std::vector<std::optional<std::tuple<int64_t, double>>> values;
    values.reserve(length);
    for (vector_size_t i = 0; i < length; ++i) {
      if (periodicNull(row * 1'024 + i, 7, salt + 41)) {
        values.push_back(std::nullopt);
      } else {
        values.push_back(std::make_tuple(
            numericValue<int64_t>(row, i, salt),
            numericValue<double>(row, i, salt + 1)));
      }
    }
    data.push_back(std::move(values));
  }
  return maker.arrayOfRowVector(data, rowType);
}

MapVectorPtr makeStringBigintMap(
    VectorMaker& maker,
    vector_size_t size,
    uint64_t salt,
    LengthShape shape,
    int32_t mapNullPct,
    int32_t valueNullPct) {
  std::vector<std::optional<
      std::vector<std::pair<std::string, std::optional<int64_t>>>>>
      maps;
  maps.reserve(size);
  for (vector_size_t row = 0; row < size; ++row) {
    if (periodicNull(row, mapNullPct, salt)) {
      maps.push_back(std::nullopt);
      continue;
    }
    const auto mapSize = listSizeAt(row, shape, salt + 5);
    std::vector<std::pair<std::string, std::optional<int64_t>>> values;
    values.reserve(mapSize);
    for (vector_size_t i = 0; i < mapSize; ++i) {
      std::optional<int64_t> value;
      if (!periodicNull(row * 1'024 + i, valueNullPct, salt + 7)) {
        value = numericValue<int64_t>(row, i, salt);
      }
      values.push_back({fmt::format("k{}", (row + i + salt) % 97), value});
    }
    maps.push_back(std::move(values));
  }
  return maker.mapVector<std::string, int64_t>(maps, MAP(VARCHAR(), BIGINT()));
}

MapVectorPtr
makeStringStringMap(VectorMaker& maker, vector_size_t size, uint64_t salt) {
  std::vector<std::vector<std::pair<std::string, std::optional<std::string>>>>
      maps;
  maps.reserve(size);
  for (vector_size_t row = 0; row < size; ++row) {
    std::vector<std::pair<std::string, std::optional<std::string>>> values;
    const auto mapSize = listSizeAt(row, LengthShape::kFixedSmall, salt);
    values.reserve(mapSize);
    for (vector_size_t i = 0; i < mapSize; ++i) {
      values.push_back(
          {fmt::format("key_{}_{}", row % 997, i),
           fmt::format("string_value_{}_{}", row % 997, (i + salt) % 131)});
    }
    maps.push_back(std::move(values));
  }
  return maker.mapVector<std::string, std::string>(
      maps, MAP(VARCHAR(), VARCHAR()));
}

std::vector<std::string> allColumnNames(const DataSetSpec& spec) {
  std::vector<std::string> names;
  names.reserve(spec.columns.size());
  for (const auto& column : spec.columns) {
    names.push_back(column.name);
  }
  return names;
}

std::vector<std::string> projectedNames(
    const DataSetSpec& spec,
    ProjectionKind projection) {
  if (projection == ProjectionKind::kAll) {
    return allColumnNames(spec);
  }

  std::vector<std::string> names;
  for (const auto& column : spec.columns) {
    const auto& name = column.name;
    if (projection == ProjectionKind::kRepresentative) {
      if (name.find("fixed") != std::string::npos ||
          name.find("sparse") != std::string::npos ||
          name.find("nested") != std::string::npos ||
          name.find("map") != std::string::npos ||
          name.find("list_struct") != std::string::npos) {
        names.push_back(name);
      }
    } else if (projection == ProjectionKind::kLongOnly) {
      if (name.find("long") != std::string::npos ||
          name.find("nested") != std::string::npos) {
        names.push_back(name);
      }
    } else if (projection == ProjectionKind::kNullHeavy) {
      if (name.find("all_null") != std::string::npos ||
          name.find("top_null") != std::string::npos ||
          name.find("sparse") != std::string::npos) {
        names.push_back(name);
      }
    }
  }

  if (names.empty() && !spec.columns.empty()) {
    names.push_back(spec.columns.front().name);
  }
  return names;
}

RowTypePtr rowTypeFor(const std::vector<ColumnSpec>& columns) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(columns.size());
  types.reserve(columns.size());
  for (const auto& column : columns) {
    names.push_back(column.name);
    types.push_back(column.type);
  }
  return ROW(std::move(names), std::move(types));
}

DataSetSpec makeDataSetSpec(DataSetKind kind) {
  const auto bigintList = ROW({"value"}, {ARRAY(BIGINT())});
  const auto floatList = ROW({"value"}, {ARRAY(REAL())});
  const auto doubleList = ROW({"value"}, {ARRAY(DOUBLE())});
  const auto varcharList = ROW({"value"}, {ARRAY(VARCHAR())});
  const auto nestedList = ROW({"value"}, {ARRAY(ARRAY(BIGINT()))});
  const auto nestedStructList =
      ROW({"c0"}, {ROW({"value"}, {ARRAY(BIGINT())})});

  DataSetSpec spec{toString(kind), {}, true};
  auto add = [&](std::string name, TypePtr type) {
    spec.columns.push_back({std::move(name), std::move(type)});
  };

  switch (kind) {
    case DataSetKind::kControl:
      add("id", BIGINT());
      add("score", DOUBLE());
      add("name", VARCHAR());
      add("fixed_small_ids", ARRAY(BIGINT()));
      add("struct_value_fixed", bigintList);
      break;
    case DataSetKind::kList:
      add("list_values", ARRAY(BIGINT()));
      break;
    case DataSetKind::kMap:
      add("map_values", MAP(VARCHAR(), BIGINT()));
      break;
    case DataSetKind::kStringList:
      add("string_list_values", ARRAY(VARCHAR()));
      break;
    case DataSetKind::kStringMap:
      add("string_map_values", MAP(VARCHAR(), VARCHAR()));
      break;
    case DataSetKind::kNullable:
      add("all_null_ids", bigintList);
      add("top_null_sparse_ids", bigintList);
      add("element_null_ids", bigintList);
      add("sparse_ids", bigintList);
      add("fixed_label", floatList);
      break;
    case DataSetKind::kLongList:
      add("fixed_2k_ids", bigintList);
      add("long_tail_ids", nestedStructList);
      add("fixed_3035_label", floatList);
      add("sparse_ids", bigintList);
      break;
    case DataSetKind::kNested:
      add("nested_short_seq", nestedList);
      add("nested_sparse_seq", nestedList);
      add("list_struct_events",
          ARRAY(ROW({"id", "score"}, {BIGINT(), DOUBLE()})));
      add("fixed_label", floatList);
      break;
    case DataSetKind::kMapStruct:
      add("map_attrs", MAP(VARCHAR(), BIGINT()));
      add("list_struct_events",
          ARRAY(ROW({"id", "score"}, {BIGINT(), DOUBLE()})));
      add("binary_tags", varcharList);
      add("double_dense", doubleList);
      break;
    case DataSetKind::kWideFeature:
      add("id", BIGINT());
      for (int32_t i = 0; i < 48; ++i) {
        add(fmt::format("feature_ids_{:02d}", i), bigintList);
      }
      for (int32_t i = 0; i < 8; ++i) {
        add(fmt::format("feature_float_{:02d}", i), floatList);
      }
      for (int32_t i = 0; i < 4; ++i) {
        add(fmt::format("feature_nested_{:02d}", i), nestedList);
      }
      for (int32_t i = 0; i < 4; ++i) {
        add(fmt::format("feature_binary_{:02d}", i), varcharList);
      }
      add("all_null_ids", bigintList);
      add("long_tail_ids", nestedStructList);
      break;
  }
  return spec;
}

VectorPtr makeColumn(
    VectorMaker& maker,
    const std::string& name,
    const TypePtr& type,
    vector_size_t size,
    uint64_t salt) {
  if (type->kind() == TypeKind::BIGINT) {
    return maker.flatVector<int64_t>(size, [salt](vector_size_t row) {
      return numericValue<int64_t>(row, 0, salt);
    });
  }
  if (type->kind() == TypeKind::DOUBLE) {
    return maker.flatVector<double>(size, [salt](vector_size_t row) {
      return numericValue<double>(row, 0, salt);
    });
  }
  if (type->kind() == TypeKind::VARCHAR) {
    return maker.flatVector<std::string>(size, [salt](vector_size_t row) {
      return fmt::format("name_{}", (row + salt) % 1'003);
    });
  }
  if (type->kind() == TypeKind::ARRAY) {
    if (type->childAt(0)->kind() == TypeKind::ARRAY) {
      return makeNestedList(maker, size, 0, salt);
    }
    if (type->childAt(0)->kind() == TypeKind::BIGINT) {
      return makeNumericList<int64_t>(
          maker, size, LengthShape::kFixedSmall, 0, 0, salt, type);
    }
    if (type->childAt(0)->kind() == TypeKind::VARCHAR) {
      return makeStringList(maker, size, LengthShape::kFixedSmall, 0, salt);
    }
    if (type->childAt(0)->kind() == TypeKind::ROW) {
      return makeListOfStruct(maker, size, salt);
    }
  }
  if (type->kind() == TypeKind::MAP) {
    if (type->childAt(1)->kind() == TypeKind::VARCHAR) {
      return makeStringStringMap(maker, size, salt);
    }
    if (name == "map_values") {
      return makeStringBigintMap(
          maker, size, salt, LengthShape::kFixedSmall, 0, 0);
    }
    return makeStringBigintMap(
        maker, size, salt, LengthShape::kBounded, 15, 10);
  }

  BOLT_CHECK_EQ(type->kind(), TypeKind::ROW);
  const auto child = type->childAt(0);
  const auto& fieldName = type->asRow().nameOf(0);
  if (fieldName == "c0") {
    auto innerList = makeNumericList<int64_t>(
        maker, size, LengthShape::kLongTail, 87, 0, salt, child->childAt(0));
    auto innerStruct = makeStructValue(maker, "value", innerList, 87, salt + 3);
    return makeStructValue(maker, "c0", innerStruct, 87, salt + 5);
  }
  if (child->childAt(0)->kind() == TypeKind::ARRAY) {
    auto list = makeNestedList(maker, size, 0, salt);
    return makeStructValue(maker, "value", list, 0, salt);
  }

  if (name.find("all_null") != std::string::npos) {
    auto list = makeNumericList<int64_t>(
        maker, size, LengthShape::kFixedOne, 100, 0, salt, child);
    return makeStructValue(maker, "value", list, 100, salt);
  }
  if (name.find("top_null") != std::string::npos) {
    auto list = makeNumericList<int64_t>(
        maker, size, LengthShape::kSparse, 87, 0, salt, child);
    return makeStructValue(maker, "value", list, 87, salt);
  }
  if (name.find("element_null") != std::string::npos) {
    auto list = makeNumericList<int64_t>(
        maker, size, LengthShape::kBounded, 0, 20, salt, child);
    return makeStructValue(maker, "value", list, 0, salt);
  }
  if (name.find("sparse") != std::string::npos) {
    auto list = makeNumericList<int64_t>(
        maker, size, LengthShape::kSparse, 0, 0, salt, child);
    return makeStructValue(maker, "value", list, 0, salt);
  }
  if (name.find("fixed_2k") != std::string::npos) {
    auto list = makeNumericList<int64_t>(
        maker, size, LengthShape::kLong2K, 87, 0, salt, child);
    return makeStructValue(maker, "value", list, 87, salt);
  }
  if (name.find("3035") != std::string::npos) {
    auto list = makeNumericList<float>(
        maker, size, LengthShape::kFixedLarge, 0, 0, salt, child);
    return makeStructValue(maker, "value", list, 0, salt);
  }
  if (name.find("label") != std::string::npos || child->childAt(0)->isReal()) {
    auto list = makeNumericList<float>(
        maker, size, LengthShape::kFixedMedium, 0, 0, salt, child);
    return makeStructValue(maker, "value", list, 0, salt);
  }
  if (child->childAt(0)->isDouble()) {
    auto list = makeNumericList<double>(
        maker, size, LengthShape::kFixedMedium, 0, 0, salt, child);
    return makeStructValue(maker, "value", list, 0, salt);
  }
  if (child->childAt(0)->isVarchar()) {
    auto list = makeStringList(maker, size, LengthShape::kFixedOne, 0, salt);
    return makeStructValue(maker, "value", list, 0, salt);
  }
  auto list = makeNumericList<int64_t>(
      maker, size, LengthShape::kBounded, 0, 0, salt, child);
  return makeStructValue(maker, "value", list, 0, salt);
}

RowVectorPtr makeBatch(
    memory::MemoryPool* pool,
    const DataSetSpec& spec,
    vector_size_t size,
    int32_t batchIndex) {
  VectorMaker maker(pool);
  std::vector<std::string> names;
  std::vector<VectorPtr> children;
  names.reserve(spec.columns.size());
  children.reserve(spec.columns.size());
  for (int32_t i = 0; i < spec.columns.size(); ++i) {
    const auto& column = spec.columns[i];
    names.push_back(column.name);
    children.push_back(makeColumn(
        maker,
        column.name,
        column.type,
        size,
        kSeed + batchIndex * 997 + i * 67));
  }
  return maker.rowVector(std::move(names), children);
}

RowTypePtr projectedRowType(
    const RowTypePtr& fullType,
    const std::vector<std::string>& names) {
  std::vector<TypePtr> types;
  types.reserve(names.size());
  for (const auto& name : names) {
    types.push_back(fullType->findChild(name));
  }
  return ROW(std::vector<std::string>(names), std::move(types));
}

std::shared_ptr<bytedance::bolt::common::ScanSpec> makeScanSpec(
    const RowTypePtr& rowType,
    dwio::common::RuntimeStatistics* stats = nullptr) {
  auto scanSpec = std::make_shared<bytedance::bolt::common::ScanSpec>("");
  scanSpec->setRuntimeStatistics(stats);
  scanSpec->addAllChildFields(*rowType);
  return scanSpec;
}

class ComplexTypeBenchmark {
 public:
  explicit ComplexTypeBenchmark(DataSetKind kind)
      : spec_(makeDataSetSpec(kind)),
        rootPool_(memory::memoryManager()->addRootPool(
            fmt::format("ParquetComplexTypeBenchmark.{}", spec_.name))),
        leafPool_(rootPool_->addLeafChild("leaf")),
        fileFolder_(bytedance::bolt::exec::test::TempDirectoryPath::create()),
        filePath_(fileFolder_->path + "/" + spec_.name + ".parquet"),
        rowType_(rowTypeFor(spec_.columns)) {
    writeFile();
  }

  int64_t read(ProjectionKind projection, vector_size_t batchSize) {
    auto names = projectedNames(spec_, projection);
    auto projectedType = projectedRowType(rowType_, names);

    static std::atomic<uint64_t> scanId{0};
    auto scanRootPool = memory::memoryManager()->addRootPool(fmt::format(
        "ParquetComplexTypeBenchmark.{}.scan.{}",
        spec_.name,
        scanId.fetch_add(1)));
    auto scanLeafPool = scanRootPool->addLeafChild("leaf");
    dwio::common::ReaderOptions readerOptions{scanLeafPool.get()};
    auto input = std::make_unique<BufferedInput>(
        std::make_shared<LocalReadFile>(filePath_),
        readerOptions.getMemoryPool());
    auto reader =
        std::make_unique<ParquetReader>(std::move(input), readerOptions);

    dwio::common::RowReaderOptions rowReaderOptions;
    rowReaderOptions.select(
        std::make_shared<ColumnSelector>(projectedType, names));
    dwio::common::RuntimeStatistics stats;
    rowReaderOptions.setScanSpec(makeScanSpec(projectedType, &stats));
    rowReaderOptions.setParquetRepDefStreamingWindowSize(
        FLAGS_parquet_complex_repdef_streaming_window_size);
    auto rowReader = reader->createRowReader(rowReaderOptions);

    const auto collectMemoryStats = FLAGS_parquet_complex_print_memory_stats;
    const auto rssBeforeKb = collectMemoryStats ? processRssKb() : 0;
    auto rssPeakKb = rssBeforeKb;
    VectorPtr result = BaseVector::create(projectedType, 0, scanLeafPool.get());
    int64_t rows = 0;
    while (true) {
      const auto read = rowReader->next(batchSize, result);
      if (read == 0) {
        break;
      }
      rows += result->size();
      folly::doNotOptimizeAway(result->hashValueAt(0));
      if (collectMemoryStats) {
        rssPeakKb = std::max(rssPeakKb, processRssKb());
      }
    }

    rowReader->updateRuntimeStats(stats);
    if (FLAGS_parquet_complex_print_runtime_stats) {
      fmt::print(
          "PARQUET_COMPLEX_STATS dataset={} projection={} batch_size={} "
          "page_version={} streaming={} decompress_time_ns={} "
          "value_decode_time_ns={}\n",
          spec_.name,
          toString(projection),
          batchSize,
          FLAGS_parquet_complex_data_page_version,
          FLAGS_parquet_complex_repdef_streaming_window_size != 0,
          stats.decompressDataTimeNs,
          stats.decodeTimeNs);
    }
    if (FLAGS_parquet_complex_print_memory_stats) {
      fmt::print(
          "PARQUET_COMPLEX_MEMORY dataset={} projection={} batch_size={} "
          "page_version={} streaming={} rss_before_kb={} rss_peak_kb={} "
          "rss_delta_kb={} root_peak_bytes={} leaf_peak_bytes={}\n",
          spec_.name,
          toString(projection),
          batchSize,
          FLAGS_parquet_complex_data_page_version,
          FLAGS_parquet_complex_repdef_streaming_window_size != 0,
          rssBeforeKb,
          rssPeakKb,
          rssPeakKb - rssBeforeKb,
          scanRootPool->peakBytes(),
          scanLeafPool->peakBytes());
    }
    folly::doNotOptimizeAway(stats);
    BOLT_CHECK_GT(rows, 0, "Complex type benchmark read no rows");
    return rows;
  }

 private:
  void writeFile() {
    auto localWriteFile =
        std::make_unique<LocalWriteFile>(filePath_, true, false);
    auto sink =
        std::make_unique<WriteFileSink>(std::move(localWriteFile), filePath_);

    bytedance::bolt::parquet::WriterOptions options;
    options.enableDictionary = !spec_.disableDictionary;
    options.memoryPool = rootPool_.get();
    options.compression = bytedance::bolt::common::CompressionKind_ZSTD;
    BOLT_CHECK(
        FLAGS_parquet_complex_data_page_version == 1 ||
            FLAGS_parquet_complex_data_page_version == 2,
        "Unsupported Parquet data page version {}",
        FLAGS_parquet_complex_data_page_version);
    options.dataPageVersion = FLAGS_parquet_complex_data_page_version == 1
        ? bytedance::bolt::parquet::arrow::ParquetDataPageVersion::V1
        : bytedance::bolt::parquet::arrow::ParquetDataPageVersion::V2;
    options.dataPageSize = kDataPageSize;
    options.flushPolicyFactory = [] {
      return std::make_unique<DefaultFlushPolicy>(
          kRowsPerRowGroup, kBytesPerRowGroup);
    };

    bytedance::bolt::parquet::Writer writer(std::move(sink), options, rowType_);
    for (int32_t batch = 0; batch < kNumBatches; ++batch) {
      writer.write(makeBatch(leafPool_.get(), spec_, kRowsPerBatch, batch));
    }
    writer.flush();
    writer.close();
  }

  DataSetSpec spec_;
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
  std::shared_ptr<bytedance::bolt::exec::test::TempDirectoryPath> fileFolder_;
  std::string filePath_;
  RowTypePtr rowType_;
};

ComplexTypeBenchmark& benchmark(DataSetKind kind) {
  static std::map<DataSetKind, std::unique_ptr<ComplexTypeBenchmark>>
      benchmarks;
  auto& instance = benchmarks[kind];
  if (!instance) {
    instance = std::make_unique<ComplexTypeBenchmark>(kind);
  }
  return *instance;
}

void runComplexTypeBenchmark(
    uint32_t iters,
    DataSetKind kind,
    ProjectionKind projection,
    vector_size_t batchSize) {
  folly::BenchmarkSuspender suspender;
  auto& instance = benchmark(kind);
  suspender.dismiss();
  while (iters--) {
    folly::doNotOptimizeAway(instance.read(projection, batchSize));
  }
}

#define COMPLEX_BENCHMARK(name, kind, projection, batchSize)              \
  BENCHMARK(name##_##projection##_##batchSize, iters) {                   \
    runComplexTypeBenchmark(                                              \
        iters, DataSetKind::kind, ProjectionKind::projection, batchSize); \
  }

COMPLEX_BENCHMARK(control, kControl, kAll, 128);
COMPLEX_BENCHMARK(control, kControl, kAll, 4096);
BENCHMARK_DRAW_LINE();

COMPLEX_BENCHMARK(list, kList, kAll, 128);
COMPLEX_BENCHMARK(list, kList, kAll, 4096);
BENCHMARK_DRAW_LINE();

COMPLEX_BENCHMARK(map, kMap, kAll, 128);
COMPLEX_BENCHMARK(map, kMap, kAll, 4096);
BENCHMARK_DRAW_LINE();

COMPLEX_BENCHMARK(stringList, kStringList, kAll, 128);
COMPLEX_BENCHMARK(stringList, kStringList, kAll, 4096);
BENCHMARK_DRAW_LINE();

COMPLEX_BENCHMARK(stringMap, kStringMap, kAll, 128);
COMPLEX_BENCHMARK(stringMap, kStringMap, kAll, 4096);
BENCHMARK_DRAW_LINE();

COMPLEX_BENCHMARK(nullable, kNullable, kAll, 128);
COMPLEX_BENCHMARK(nullable, kNullable, kNullHeavy, 128);
COMPLEX_BENCHMARK(nullable, kNullable, kAll, 4096);
BENCHMARK_DRAW_LINE();

COMPLEX_BENCHMARK(longList, kLongList, kAll, 128);
COMPLEX_BENCHMARK(longList, kLongList, kLongOnly, 128);
COMPLEX_BENCHMARK(longList, kLongList, kAll, 4096);
BENCHMARK_DRAW_LINE();

COMPLEX_BENCHMARK(nested, kNested, kAll, 128);
COMPLEX_BENCHMARK(nested, kNested, kRepresentative, 4096);
BENCHMARK_DRAW_LINE();

COMPLEX_BENCHMARK(mapStruct, kMapStruct, kAll, 128);
COMPLEX_BENCHMARK(mapStruct, kMapStruct, kRepresentative, 4096);
BENCHMARK_DRAW_LINE();

COMPLEX_BENCHMARK(wideFeature, kWideFeature, kRepresentative, 128);
COMPLEX_BENCHMARK(wideFeature, kWideFeature, kAll, 1024);
COMPLEX_BENCHMARK(wideFeature, kWideFeature, kNullHeavy, 1024);

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize({});
  folly::runBenchmarks();
  return 0;
}
