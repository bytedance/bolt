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
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/dwio/parquet/reader/Statistics.h"

#include "bolt/dwio/common/Statistics.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt::parquet {

std::unique_ptr<dwio::common::ColumnStatistics> buildColumnStatisticsFromThrift(
    const thrift::Statistics& columnChunkStats,
    std::unique_ptr<BlockSplitBloomFilter> blockBloom,
    std::vector<std::pair<
        std::unique_ptr<struct NgramTokenExtractor>,
        std::unique_ptr<NGramBloomFilter>>> nGramStats,
    const bolt::Type& type,
    uint64_t numRowsInRowGroup) {
  const int64_t rawNullCount = columnChunkStats.null_count;
  const bool validNullCount = columnChunkStats.__isset.null_count &&
      rawNullCount >= 0 &&
      static_cast<uint64_t>(rawNullCount) <= numRowsInRowGroup;

  std::optional<uint64_t> nullCount = validNullCount
      ? std::optional<uint64_t>(static_cast<uint64_t>(rawNullCount))
      : std::nullopt;
  std::optional<uint64_t> valueCount = validNullCount
      ? std::optional<uint64_t>(
            numRowsInRowGroup - static_cast<uint64_t>(rawNullCount))
      : std::nullopt;
  std::optional<bool> hasNull =
      validNullCount ? std::optional<bool>(rawNullCount > 0) : std::nullopt;
  switch (type.kind()) {
    case TypeKind::BOOLEAN:
      return std::make_unique<dwio::common::BooleanColumnStatistics>(
          valueCount, hasNull, std::nullopt, std::nullopt, std::nullopt);
    case TypeKind::TINYINT:
      return std::make_unique<dwio::common::IntegerColumnStatistics>(
          valueCount,
          hasNull,
          std::nullopt,
          std::nullopt,
          getMin<int8_t>(columnChunkStats),
          getMax<int8_t>(columnChunkStats),
          std::nullopt);
    case TypeKind::SMALLINT:
      return std::make_unique<dwio::common::IntegerColumnStatistics>(
          valueCount,
          hasNull,
          std::nullopt,
          std::nullopt,
          getMin<int16_t>(columnChunkStats),
          getMax<int16_t>(columnChunkStats),
          std::nullopt);
    case TypeKind::INTEGER:
      return std::make_unique<dwio::common::IntegerColumnStatistics>(
          valueCount,
          hasNull,
          std::nullopt,
          std::nullopt,
          getMin<int32_t>(columnChunkStats),
          getMax<int32_t>(columnChunkStats),
          std::nullopt);
    case TypeKind::BIGINT:
      return std::make_unique<dwio::common::IntegerColumnStatistics>(
          valueCount,
          hasNull,
          std::nullopt,
          std::nullopt,
          getMin<int64_t>(columnChunkStats),
          getMax<int64_t>(columnChunkStats),
          std::nullopt,
          std::move(blockBloom));
    case TypeKind::REAL:
      return std::make_unique<dwio::common::DoubleColumnStatistics>(
          valueCount,
          hasNull,
          std::nullopt,
          std::nullopt,
          getMin<float>(columnChunkStats),
          getMax<float>(columnChunkStats),
          std::nullopt);
    case TypeKind::DOUBLE:
      return std::make_unique<dwio::common::DoubleColumnStatistics>(
          valueCount,
          hasNull,
          std::nullopt,
          std::nullopt,
          getMin<double>(columnChunkStats),
          getMax<double>(columnChunkStats),
          std::nullopt);
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return std::make_unique<dwio::common::StringColumnStatistics>(
          valueCount,
          hasNull,
          std::nullopt,
          std::nullopt,
          getMin<std::string>(columnChunkStats),
          getMax<std::string>(columnChunkStats),
          std::nullopt,
          std::move(blockBloom),
          std::move(nGramStats));

    default:
      return std::make_unique<dwio::common::ColumnStatistics>(
          valueCount, hasNull, std::nullopt, std::nullopt);
  }
}

} // namespace bytedance::bolt::parquet
