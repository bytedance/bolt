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
 * 2026-11-19.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 *
 * Shared input construction and level-encoding helpers for tests and
 * benchmarks.
 */

#pragma once

#include "arrow/api.h"
#include "bolt/dwio/parquet/arrow/EncodingInternal.h"
#include "bolt/dwio/parquet/arrow/PathInternal.h"
#include "bolt/dwio/parquet/arrow/Properties.h"

namespace bytedance::bolt::parquet::arrow::test {

// Input construction is shared by correctness tests and the benchmark, and is
// always outside the timed region. All array storage uses the supplied pool.
inline std::shared_ptr<::arrow::Array> MakeLevelRunInput(
    const std::string& pattern,
    int64_t rows,
    ::arrow::MemoryPool* pool) {
  auto values = std::make_shared<::arrow::Int32Builder>(pool);
  ::arrow::ListBuilder lists(pool, values);
  const bool is_list = pattern == "long_list" || pattern == "short_list";
  uint32_t random = 42;
  for (int64_t row = 0; row < rows; ++row) {
    if (is_list) {
      if (row % 13 == 0) {
        PARQUET_THROW_NOT_OK(lists.AppendNull());
        continue;
      }
      PARQUET_THROW_NOT_OK(lists.Append());
      if (row % 11 == 0) {
        continue;
      }
      const int length = pattern == "long_list" ? 256 : 4;
      for (int i = 0; i < length; ++i) {
        if (i >= 128 && i < 192) {
          PARQUET_THROW_NOT_OK(values->AppendNull());
        } else {
          PARQUET_THROW_NOT_OK(values->Append(row * 256 + i));
        }
      }
    } else {
      random = random * 1664525U + 1013904223U;
      const bool null = pattern == "all_null" ||
          (pattern == "blocked_null" && row % 512 >= 256) ||
          (pattern == "alternating_null" && row % 2 == 0) ||
          (pattern == "random_null" && (random >> 31));
      if (null) {
        PARQUET_THROW_NOT_OK(values->AppendNull());
      } else {
        PARQUET_THROW_NOT_OK(values->Append(row));
      }
    }
  }
  std::shared_ptr<::arrow::Array> array;
  if (is_list) {
    PARQUET_THROW_NOT_OK(lists.Finish(&array));
  } else {
    PARQUET_THROW_NOT_OK(values->Finish(&array));
  }
  return array;
}

// The same forward cursor used by ColumnWriter; no benchmark-only run scan.
inline int64_t GenerateAndEncodeLevels(
    arrow::MultipathLevelBuilder& builder,
    ArrowWriteContext& context,
    IncrementalLevelRleEncoder& def_encoder,
    IncrementalLevelRleEncoder& rep_encoder,
    int16_t max_def_level) {
  int64_t bytes = 0;
  for (int leaf = 0; leaf < builder.GetLeafCount(); ++leaf) {
    PARQUET_THROW_NOT_OK(builder.Write(leaf, &context, [&](const auto& result) {
      if (!result.def_levels && !result.rep_levels) {
        return ::arrow::Status::OK();
      }
      // Use the input schema's width and the writer's usual batch size.
      if (result.def_levels) {
        def_encoder.Reset(max_def_level, 0);
      }
      if (result.rep_levels) {
        rep_encoder.Reset(1, 0);
      }
      LevelRunCursor def(result.level_runs.definition);
      LevelRunCursor rep(result.level_runs.repetition);
      for (int64_t offset = 0; offset < result.def_rep_level_count;
           offset += 1024) {
        const auto count =
            std::min<int64_t>(1024, result.def_rep_level_count - offset);
        if (result.def_levels) {
          def.Put(def_encoder, result.def_levels + offset, offset, count);
        }
        if (result.rep_levels) {
          rep.Put(rep_encoder, result.rep_levels + offset, offset, count);
        }
      }
      if (result.def_levels) {
        bytes += def_encoder.Flush();
      }
      if (result.rep_levels) {
        bytes += rep_encoder.Flush();
      }
      return ::arrow::Status::OK();
    }));
  }
  return bytes;
}

} // namespace bytedance::bolt::parquet::arrow::test
