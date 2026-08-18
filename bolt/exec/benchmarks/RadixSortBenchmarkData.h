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

#pragma once

#include <array>
#include <vector>

#include "bolt/common/memory/Memory.h"
#include "bolt/type/Type.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::exec::radixsort::benchmark {

enum class ScenarioKind : uint8_t {
  kRandomInt64,
  kSortedInt64,
  kReverseSortedInt64,
  kNearlySortedInt64,
  kDuplicateInt64,
  kLowCardinalityInt64,
  kNullHeavyInt64,
  kEightKeyInt64,
  kSixteenKeyInt64,
  kMultiKeyNulls,
  kInlineVarchar,
  kLongVarchar,
  kVarcharCommonPrefix,
  kNullableFixedPayload,
  kWideFixedPayload,
  kVeryWideFixedPayload,
  kWideStringPayload,
  kBucketWriteKeyOnly,
  kBucketWriteFixedPayload,
  kBucketWriteKeyStringFixedPayload,
  kBucketWriteStringPayload,
  kBucketWriteComplexPayload,
};

struct ScenarioSpec {
  const char* name;
  ScenarioKind kind;
  vector_size_t rows;
};

struct ScenarioFixture {
  RowTypePtr rowType;
  std::vector<column_index_t> keyChannels;
  std::vector<CompareFlags> keyFlags;
  column_index_t idChannel;
  std::vector<RowVectorPtr> inputs;
};

enum class ScenarioProfile : uint8_t {
  kInMemory,
  kSpill,
};

inline constexpr std::array<ScenarioSpec, 19> kInMemoryScenarioSpecs{{
    {"random_i64_narrow_256k", ScenarioKind::kRandomInt64, 256 * 1024},
    {"duplicate_i64_narrow_256k", ScenarioKind::kDuplicateInt64, 256 * 1024},
    {"low_cardinality_i64_256k",
     ScenarioKind::kLowCardinalityInt64,
     256 * 1024},
    {"null_heavy_i64_128k", ScenarioKind::kNullHeavyInt64, 128 * 1024},
    {"eight_key_i64_128k", ScenarioKind::kEightKeyInt64, 128 * 1024},
    {"inline_varchar_128k", ScenarioKind::kInlineVarchar, 128 * 1024},
    {"long_varchar_64k", ScenarioKind::kLongVarchar, 64 * 1024},
    {"varchar_common_prefix_128k",
     ScenarioKind::kVarcharCommonPrefix,
     128 * 1024},
    {"nullable_fixed_payload_128k",
     ScenarioKind::kNullableFixedPayload,
     128 * 1024},
    {"wide_fixed_payload_128k", ScenarioKind::kWideFixedPayload, 128 * 1024},
    {"very_wide_fixed_payload_64k",
     ScenarioKind::kVeryWideFixedPayload,
     64 * 1024},
    {"wide_string_payload_64k", ScenarioKind::kWideStringPayload, 64 * 1024},
    {"random_i64_narrow_1m", ScenarioKind::kRandomInt64, 1 * 1024 * 1024},
    {"low_cardinality_i64_1m",
     ScenarioKind::kLowCardinalityInt64,
     1 * 1024 * 1024},
    {"eight_key_i64_1m", ScenarioKind::kEightKeyInt64, 1 * 1024 * 1024},
    {"sixteen_key_i64_1m", ScenarioKind::kSixteenKeyInt64, 1 * 1024 * 1024},
    {"wide_fixed_payload_1m", ScenarioKind::kWideFixedPayload, 1 * 1024 * 1024},
    {"very_wide_fixed_payload_1m",
     ScenarioKind::kVeryWideFixedPayload,
     1 * 1024 * 1024},
    {"wide_string_payload_1m",
     ScenarioKind::kWideStringPayload,
     1 * 1024 * 1024},
}};

inline constexpr std::array<ScenarioSpec, 20> kSpillScenarioSpecs{{
    {"random_i64_256k_spill", ScenarioKind::kRandomInt64, 256 * 1024},
    {"duplicate_i64_256k_spill", ScenarioKind::kDuplicateInt64, 256 * 1024},
    {"null_heavy_i64_128k_spill", ScenarioKind::kNullHeavyInt64, 128 * 1024},
    {"eight_key_i64_128k_spill", ScenarioKind::kEightKeyInt64, 128 * 1024},
    {"inline_varchar_128k_spill", ScenarioKind::kInlineVarchar, 128 * 1024},
    {"long_varchar_64k_spill", ScenarioKind::kLongVarchar, 64 * 1024},
    {"varchar_common_prefix_128k_spill",
     ScenarioKind::kVarcharCommonPrefix,
     128 * 1024},
    {"wide_fixed_payload_128k_spill",
     ScenarioKind::kWideFixedPayload,
     128 * 1024},
    {"very_wide_fixed_payload_64k_spill",
     ScenarioKind::kVeryWideFixedPayload,
     64 * 1024},
    {"wide_string_payload_64k_spill",
     ScenarioKind::kWideStringPayload,
     64 * 1024},
    {"random_i64_1m_spill", ScenarioKind::kRandomInt64, 1 * 1024 * 1024},
    {"eight_key_i64_1m_spill", ScenarioKind::kEightKeyInt64, 1 * 1024 * 1024},
    {"sixteen_key_i64_1m_spill",
     ScenarioKind::kSixteenKeyInt64,
     1 * 1024 * 1024},
    {"wide_fixed_payload_1m_spill",
     ScenarioKind::kWideFixedPayload,
     1 * 1024 * 1024},
    {"wide_string_payload_1m_spill",
     ScenarioKind::kWideStringPayload,
     1 * 1024 * 1024},
    {"bucket_write_key_only_1m_spill",
     ScenarioKind::kBucketWriteKeyOnly,
     1 * 1024 * 1024},
    {"bucket_write_fixed_payload_1m_spill",
     ScenarioKind::kBucketWriteFixedPayload,
     1 * 1024 * 1024},
    {"bucket_write_key_string_fixed_payload_1m_spill",
     ScenarioKind::kBucketWriteKeyStringFixedPayload,
     1 * 1024 * 1024},
    {"bucket_write_string_payload_1m_spill",
     ScenarioKind::kBucketWriteStringPayload,
     1 * 1024 * 1024},
    {"bucket_write_complex_payload_1m_spill",
     ScenarioKind::kBucketWriteComplexPayload,
     1 * 1024 * 1024},
}};

inline constexpr std::array<ScenarioSpec, 5> kInMemoryLargeScenarioSpecs{{
    {"random_i64_narrow_10m", ScenarioKind::kRandomInt64, 10 * 1024 * 1024},
    {"eight_key_i64_10m", ScenarioKind::kEightKeyInt64, 10 * 1024 * 1024},
    {"sixteen_key_i64_10m", ScenarioKind::kSixteenKeyInt64, 10 * 1024 * 1024},
    {"wide_fixed_payload_10m",
     ScenarioKind::kWideFixedPayload,
     10 * 1024 * 1024},
    {"long_varchar_10m", ScenarioKind::kLongVarchar, 10 * 1024 * 1024},
}};

inline constexpr std::array<ScenarioSpec, 4> kSpillLargeScenarioSpecs{{
    {"random_i64_10m_spill", ScenarioKind::kRandomInt64, 10 * 1024 * 1024},
    {"eight_key_i64_10m_spill", ScenarioKind::kEightKeyInt64, 10 * 1024 * 1024},
    {"sixteen_key_i64_10m_spill",
     ScenarioKind::kSixteenKeyInt64,
     10 * 1024 * 1024},
    {"wide_fixed_payload_10m_spill",
     ScenarioKind::kWideFixedPayload,
     10 * 1024 * 1024},
}};

ScenarioFixture makeFixture(
    memory::MemoryPool* pool,
    const ScenarioSpec& spec,
    ScenarioProfile profile);

ScenarioFixture makeFixture(memory::MemoryPool* pool, const ScenarioSpec& spec);

} // namespace bytedance::bolt::exec::radixsort::benchmark
