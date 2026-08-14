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

#include "bolt/exec/benchmarks/RadixSortBenchmarkData.h"

#include <algorithm>
#include <string>

#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::exec::radixsort::benchmark {
namespace {

constexpr vector_size_t kRowsPerBatch = 2048;
constexpr uint32_t kEightKeyColumns = 8;
constexpr uint32_t kSixteenKeyColumns = 16;
constexpr uint32_t kFixedPayloadColumns = 16;
constexpr uint32_t kVeryWideFixedPayloadColumns = 64;
constexpr uint32_t kStringPayloadColumns = 8;

CompareFlags flags(bool ascending = true, bool nullsFirst = false) {
  return CompareFlags{
      .nullsFirst = nullsFirst,
      .ascending = ascending,
      .nullHandlingMode = CompareFlags::NullHandlingMode::kNullAsValue};
}

uint64_t randomBits(uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

template <typename T, typename ValueAt, typename IsNullAt>
FlatVectorPtr<T> makeFlatVector(
    memory::MemoryPool* pool,
    const TypePtr& type,
    vector_size_t size,
    ValueAt valueAt,
    IsNullAt isNullAt) {
  auto result = BaseVector::create<FlatVector<T>>(type, size, pool);
  for (vector_size_t row = 0; row < size; ++row) {
    if (isNullAt(row)) {
      result->setNull(row, true);
    } else {
      result->set(row, valueAt(row));
    }
  }
  return result;
}

template <typename ValueAt, typename IsNullAt>
FlatVectorPtr<StringView> makeStringVector(
    memory::MemoryPool* pool,
    vector_size_t size,
    ValueAt valueAt,
    IsNullAt isNullAt) {
  auto result =
      BaseVector::create<FlatVector<StringView>>(VARCHAR(), size, pool);
  for (vector_size_t row = 0; row < size; ++row) {
    if (isNullAt(row)) {
      result->setNull(row, true);
    } else {
      const auto value = valueAt(row);
      result->set(row, StringView(value));
    }
  }
  return result;
}

RowTypePtr rowTypeFor(ScenarioKind kind, ScenarioProfile profile) {
  switch (kind) {
    case ScenarioKind::kEightKeyInt64:
    case ScenarioKind::kSixteenKeyInt64: {
      const auto columns = kind == ScenarioKind::kEightKeyInt64
          ? kEightKeyColumns
          : kSixteenKeyColumns;
      std::vector<std::string> names;
      std::vector<TypePtr> types;
      names.reserve(columns + 1);
      types.reserve(columns + 1);
      for (uint32_t column = 0; column < columns; ++column) {
        names.push_back("key" + std::to_string(column));
        types.push_back(BIGINT());
      }
      names.push_back("id");
      types.push_back(BIGINT());
      return ROW(std::move(names), std::move(types));
    }
    case ScenarioKind::kMultiKeyNulls:
      return profile == ScenarioProfile::kSpill
          ? ROW({"key0", "key1", "key2", "payload", "id"},
                {BIGINT(), INTEGER(), VARCHAR(), BIGINT(), BIGINT()})
          : ROW({"key_i64", "key_i32", "key_double", "key_string", "id"},
                {BIGINT(), INTEGER(), DOUBLE(), VARCHAR(), BIGINT()});
    case ScenarioKind::kWideFixedPayload: {
      if (profile == ScenarioProfile::kSpill) {
        return ROW(
            {"key", "payload0", "payload1", "payload2", "payload3", "id"},
            {BIGINT(), BIGINT(), DOUBLE(), INTEGER(), BIGINT(), BIGINT()});
      }
      std::vector<std::string> names{"key"};
      std::vector<TypePtr> types{BIGINT()};
      for (uint32_t column = 0; column < kFixedPayloadColumns; ++column) {
        names.push_back("fixed_" + std::to_string(column));
        types.push_back(BIGINT());
      }
      names.push_back("id");
      types.push_back(BIGINT());
      return ROW(std::move(names), std::move(types));
    }
    case ScenarioKind::kVeryWideFixedPayload: {
      std::vector<std::string> names{"key"};
      std::vector<TypePtr> types{BIGINT()};
      for (uint32_t column = 0; column < kVeryWideFixedPayloadColumns;
           ++column) {
        names.push_back("fixed_" + std::to_string(column));
        types.push_back(BIGINT());
      }
      names.push_back("id");
      types.push_back(BIGINT());
      return ROW(std::move(names), std::move(types));
    }
    case ScenarioKind::kWideStringPayload: {
      std::vector<std::string> names{"key"};
      std::vector<TypePtr> types{BIGINT()};
      const auto columns =
          profile == ScenarioProfile::kSpill ? 3 : kStringPayloadColumns;
      for (uint32_t column = 0; column < columns; ++column) {
        names.push_back("string_" + std::to_string(column));
        types.push_back(VARCHAR());
      }
      names.push_back("id");
      types.push_back(BIGINT());
      return ROW(std::move(names), std::move(types));
    }
    case ScenarioKind::kInlineVarchar:
    case ScenarioKind::kLongVarchar:
    case ScenarioKind::kVarcharCommonPrefix:
      return ROW({"key", "id"}, {VARCHAR(), BIGINT()});
    case ScenarioKind::kNullableFixedPayload:
      return ROW({"key", "payload", "id"}, {BIGINT(), BIGINT(), BIGINT()});
    default:
      return ROW({"key", "id"}, {BIGINT(), BIGINT()});
  }
}

void addKeyMetadata(
    ScenarioFixture& fixture,
    ScenarioKind kind,
    ScenarioProfile profile) {
  if (kind == ScenarioKind::kEightKeyInt64) {
    fixture.keyChannels = {0, 1, 2, 3, 4, 5, 6, 7};
    fixture.keyFlags = {
        flags(true, false),
        flags(false, true),
        flags(true, false),
        flags(false, false),
        flags(true, true),
        flags(false, true),
        flags(true, false),
        flags(false, false)};
  } else if (kind == ScenarioKind::kSixteenKeyInt64) {
    fixture.keyChannels.reserve(kSixteenKeyColumns);
    fixture.keyFlags.reserve(kSixteenKeyColumns);
    for (uint32_t column = 0; column < kSixteenKeyColumns; ++column) {
      fixture.keyChannels.push_back(column);
      fixture.keyFlags.push_back(flags(column % 3 != 1, column % 4 < 2));
    }
  } else if (kind == ScenarioKind::kMultiKeyNulls) {
    if (profile == ScenarioProfile::kSpill) {
      fixture.keyChannels = {0, 1, 2};
      fixture.keyFlags = {flags(), flags(), flags()};
    } else {
      fixture.keyChannels = {0, 1, 2, 3};
      fixture.keyFlags = {
          flags(true, false),
          flags(false, true),
          flags(true, false),
          flags(true, true)};
    }
  } else {
    fixture.keyChannels = {0};
    fixture.keyFlags = {flags()};
  }
}

void addIntegerKey(
    memory::MemoryPool* pool,
    const ScenarioSpec& spec,
    vector_size_t offset,
    vector_size_t size,
    std::vector<VectorPtr>& children) {
  children.push_back(makeFlatVector<int64_t>(
      pool,
      BIGINT(),
      size,
      [&](vector_size_t row) {
        const auto index = static_cast<uint64_t>(offset + row);
        switch (spec.kind) {
          case ScenarioKind::kSortedInt64:
            return static_cast<int64_t>(index);
          case ScenarioKind::kReverseSortedInt64:
            return static_cast<int64_t>(spec.rows - index);
          case ScenarioKind::kNearlySortedInt64: {
            const auto position = index % 4096;
            return static_cast<int64_t>(
                position < 32 ? index + 31 - 2 * position : index);
          }
          case ScenarioKind::kDuplicateInt64:
            return static_cast<int64_t>(randomBits(index) % 32);
          case ScenarioKind::kLowCardinalityInt64:
            return static_cast<int64_t>(index % 4);
          default:
            return static_cast<int64_t>(randomBits(index));
        }
      },
      [&](vector_size_t row) {
        return spec.kind == ScenarioKind::kNullHeavyInt64 &&
            (offset + row) % 3 == 0;
      }));
}

void addEightKeys(
    memory::MemoryPool* pool,
    vector_size_t offset,
    vector_size_t size,
    std::vector<VectorPtr>& children) {
  for (uint32_t column = 0; column < kEightKeyColumns; ++column) {
    children.push_back(makeFlatVector<int64_t>(
        pool,
        BIGINT(),
        size,
        [&](vector_size_t row) {
          const auto index = static_cast<uint64_t>(offset + row);
          return static_cast<int64_t>(
              column % 2 == 0 ? randomBits(index + column * 17)
                              : randomBits(index + column * 17) % 1024);
        },
        [&](vector_size_t row) {
          return column >= 4 && (offset + row + column) % 31 == 0;
        }));
  }
}

void addSixteenKeys(
    memory::MemoryPool* pool,
    vector_size_t offset,
    vector_size_t size,
    std::vector<VectorPtr>& children) {
  for (uint32_t column = 0; column < kSixteenKeyColumns; ++column) {
    children.push_back(makeFlatVector<int64_t>(
        pool,
        BIGINT(),
        size,
        [&](vector_size_t row) {
          const auto index = static_cast<uint64_t>(offset + row);
          const auto bits = randomBits(index + column * 31);
          if (column % 4 == 0) {
            return static_cast<int64_t>(bits);
          }
          if (column % 4 == 1) {
            return static_cast<int64_t>(bits % 4096);
          }
          if (column % 4 == 2) {
            return static_cast<int64_t>((index / 8 + column) % 8192);
          }
          return static_cast<int64_t>(bits % 64);
        },
        [&](vector_size_t row) {
          return column >= 8 && (offset + row + column) % 37 == 0;
        }));
  }
}

void addMultiKeyNulls(
    memory::MemoryPool* pool,
    ScenarioProfile profile,
    vector_size_t offset,
    vector_size_t size,
    std::vector<VectorPtr>& children) {
  children.push_back(makeFlatVector<int64_t>(
      pool,
      BIGINT(),
      size,
      [&](vector_size_t row) {
        return static_cast<int64_t>(randomBits(offset + row));
      },
      [&](vector_size_t row) { return (offset + row) % 17 == 0; }));
  children.push_back(makeFlatVector<int32_t>(
      pool,
      INTEGER(),
      size,
      [&](vector_size_t row) {
        return static_cast<int32_t>(randomBits(offset + row + 7) % 101);
      },
      [&](vector_size_t row) { return (offset + row) % 19 == 0; }));
  if (profile == ScenarioProfile::kInMemory) {
    children.push_back(makeFlatVector<double>(
        pool,
        DOUBLE(),
        size,
        [&](vector_size_t row) {
          return static_cast<int64_t>(
                     randomBits(offset + row + 13) % 1'000'000) /
              10.0;
        },
        [&](vector_size_t row) { return (offset + row) % 23 == 0; }));
  }
  children.push_back(makeStringVector(
      pool,
      size,
      [&](vector_size_t row) {
        return "group-" +
            std::to_string(
                   randomBits(
                       offset + row +
                       (profile == ScenarioProfile::kSpill ? 11 : 29)) %
                   (profile == ScenarioProfile::kSpill ? 4096 : 1024));
      },
      [&](vector_size_t row) {
        return profile == ScenarioProfile::kInMemory &&
            (offset + row) % 29 == 0;
      }));
  if (profile == ScenarioProfile::kSpill) {
    children.push_back(makeFlatVector<int64_t>(
        pool,
        BIGINT(),
        size,
        [&](vector_size_t row) {
          return static_cast<int64_t>(randomBits(offset + row + 13));
        },
        [](vector_size_t) { return false; }));
  }
}

void addStringKey(
    memory::MemoryPool* pool,
    const ScenarioSpec& spec,
    vector_size_t offset,
    vector_size_t size,
    std::vector<VectorPtr>& children) {
  if (spec.kind == ScenarioKind::kInlineVarchar) {
    children.push_back(makeStringVector(
        pool,
        size,
        [&](vector_size_t row) {
          return "k" + std::to_string(randomBits(offset + row) % 10000);
        },
        [](vector_size_t) { return false; }));
    return;
  }
  if (spec.kind == ScenarioKind::kLongVarchar) {
    children.push_back(makeStringVector(
        pool,
        size,
        [&](vector_size_t row) {
          return "long-" + std::to_string(randomBits(offset + row)) + "-" +
              std::string(160, static_cast<char>('a' + row % 26));
        },
        [](vector_size_t) { return false; }));
    return;
  }
  children.push_back(makeStringVector(
      pool,
      size,
      [&](vector_size_t row) {
        return std::string(64, 'p') + std::to_string(randomBits(offset + row));
      },
      [](vector_size_t) { return false; }));
}

void addWideFixedPayload(
    memory::MemoryPool* pool,
    ScenarioProfile profile,
    vector_size_t offset,
    vector_size_t size,
    std::vector<VectorPtr>& children) {
  if (profile == ScenarioProfile::kSpill) {
    children.push_back(makeFlatVector<int64_t>(
        pool,
        BIGINT(),
        size,
        [&](vector_size_t row) {
          return static_cast<int64_t>(randomBits(offset + row + 3));
        },
        [](vector_size_t) { return false; }));
    children.push_back(makeFlatVector<double>(
        pool,
        DOUBLE(),
        size,
        [&](vector_size_t row) {
          return static_cast<double>(randomBits(offset + row + 5) % 1'000'000);
        },
        [](vector_size_t) { return false; }));
    children.push_back(makeFlatVector<int32_t>(
        pool,
        INTEGER(),
        size,
        [&](vector_size_t row) {
          return static_cast<int32_t>(randomBits(offset + row + 7));
        },
        [](vector_size_t) { return false; }));
    children.push_back(makeFlatVector<int64_t>(
        pool,
        BIGINT(),
        size,
        [&](vector_size_t row) {
          return static_cast<int64_t>(randomBits(offset + row + 11));
        },
        [](vector_size_t) { return false; }));
    return;
  }
  for (uint32_t column = 0; column < kFixedPayloadColumns; ++column) {
    children.push_back(makeFlatVector<int64_t>(
        pool,
        BIGINT(),
        size,
        [&](vector_size_t row) {
          return static_cast<int64_t>(randomBits(offset + row + column * 131));
        },
        [](vector_size_t) { return false; }));
  }
}

void addVeryWideFixedPayload(
    memory::MemoryPool* pool,
    vector_size_t offset,
    vector_size_t size,
    std::vector<VectorPtr>& children) {
  for (uint32_t column = 0; column < kVeryWideFixedPayloadColumns; ++column) {
    children.push_back(makeFlatVector<int64_t>(
        pool,
        BIGINT(),
        size,
        [&](vector_size_t row) {
          return static_cast<int64_t>(randomBits(offset + row + column * 257));
        },
        [](vector_size_t) { return false; }));
  }
}

void addWideStringPayload(
    memory::MemoryPool* pool,
    ScenarioProfile profile,
    vector_size_t offset,
    vector_size_t size,
    std::vector<VectorPtr>& children) {
  const auto columns =
      profile == ScenarioProfile::kSpill ? 3 : kStringPayloadColumns;
  for (uint32_t column = 0; column < columns; ++column) {
    children.push_back(makeStringVector(
        pool,
        size,
        [&](vector_size_t row) {
          if (profile == ScenarioProfile::kSpill) {
            return "payload-" + std::to_string(column) + "-" +
                std::string(
                       96,
                       static_cast<char>('a' + (offset + row + column) % 26));
          }
          return "payload-" + std::to_string(column) + "-" +
              std::to_string(offset + row) + "-" + std::string(48, 'x');
        },
        [](vector_size_t) { return false; }));
  }
}

} // namespace

ScenarioFixture makeFixture(
    memory::MemoryPool* pool,
    const ScenarioSpec& spec) {
  return makeFixture(pool, spec, ScenarioProfile::kInMemory);
}

ScenarioFixture makeFixture(
    memory::MemoryPool* pool,
    const ScenarioSpec& spec,
    ScenarioProfile profile) {
  ScenarioFixture fixture;
  fixture.rowType = rowTypeFor(spec.kind, profile);
  addKeyMetadata(fixture, spec.kind, profile);
  fixture.idChannel = fixture.rowType->size() - 1;

  for (vector_size_t offset = 0; offset < spec.rows; offset += kRowsPerBatch) {
    const auto size = std::min(kRowsPerBatch, spec.rows - offset);
    std::vector<VectorPtr> children;
    switch (spec.kind) {
      case ScenarioKind::kRandomInt64:
      case ScenarioKind::kSortedInt64:
      case ScenarioKind::kReverseSortedInt64:
      case ScenarioKind::kNearlySortedInt64:
      case ScenarioKind::kDuplicateInt64:
      case ScenarioKind::kLowCardinalityInt64:
      case ScenarioKind::kNullHeavyInt64:
        addIntegerKey(pool, spec, offset, size, children);
        break;
      case ScenarioKind::kEightKeyInt64:
        addEightKeys(pool, offset, size, children);
        break;
      case ScenarioKind::kSixteenKeyInt64:
        addSixteenKeys(pool, offset, size, children);
        break;
      case ScenarioKind::kMultiKeyNulls:
        addMultiKeyNulls(pool, profile, offset, size, children);
        break;
      case ScenarioKind::kInlineVarchar:
      case ScenarioKind::kLongVarchar:
      case ScenarioKind::kVarcharCommonPrefix:
        addStringKey(pool, spec, offset, size, children);
        break;
      case ScenarioKind::kNullableFixedPayload:
        addIntegerKey(pool, spec, offset, size, children);
        children.push_back(makeFlatVector<int64_t>(
            pool,
            BIGINT(),
            size,
            [&](vector_size_t row) {
              return static_cast<int64_t>(randomBits(offset + row + 97));
            },
            [&](vector_size_t row) { return (offset + row) % 17 == 0; }));
        break;
      case ScenarioKind::kWideFixedPayload:
        addIntegerKey(pool, spec, offset, size, children);
        addWideFixedPayload(pool, profile, offset, size, children);
        break;
      case ScenarioKind::kVeryWideFixedPayload:
        addIntegerKey(pool, spec, offset, size, children);
        addVeryWideFixedPayload(pool, offset, size, children);
        break;
      case ScenarioKind::kWideStringPayload:
        addIntegerKey(pool, spec, offset, size, children);
        addWideStringPayload(pool, profile, offset, size, children);
        break;
    }
    children.push_back(makeFlatVector<int64_t>(
        pool,
        BIGINT(),
        size,
        [&](vector_size_t row) { return static_cast<int64_t>(offset + row); },
        [](vector_size_t) { return false; }));
    fixture.inputs.push_back(std::make_shared<RowVector>(
        pool, fixture.rowType, nullptr, size, std::move(children)));
  }
  return fixture;
}

} // namespace bytedance::bolt::exec::radixsort::benchmark
