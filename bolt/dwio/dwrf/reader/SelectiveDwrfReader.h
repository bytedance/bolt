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

#pragma once

#include "bolt/dwio/common/SelectiveColumnReader.h"
#include "bolt/dwio/dwrf/reader/ColumnReader.h"
#include "bolt/dwio/dwrf/reader/DwrfData.h"
namespace bytedance::bolt::dwrf {

// Wrapper for static functions for making DWRF readers
class SelectiveDwrfReader {
 public:
  static std::unique_ptr<dwio::common::SelectiveColumnReader> build(
      const dwio::common::ColumnReaderOptions& columnReaderOptions,
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec,
      bool isRoot = false);

  // Compatibility wrapper for tests. Takes the components of DwrfParams as
  // separate.
  static std::unique_ptr<dwio::common::SelectiveColumnReader> build(
      const dwio::common::ColumnReaderOptions& columnReaderOptions,
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      StripeStreams& stripe,
      const StreamLabels& streamLabels,
      dwio::common::ColumnReaderStatistics& stats,
      common::ScanSpec* FOLLY_NONNULL scanSpec,
      FlatMapContext flatMapContext = {},
      bool isRoot = false) {
    auto params = DwrfParams(stripe, streamLabels, stats, flatMapContext);
    return build(
        columnReaderOptions,
        requestedType,
        fileType,
        params,
        *scanSpec,
        isRoot);
  }
};

} // namespace bytedance::bolt::dwrf
