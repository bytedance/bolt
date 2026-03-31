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
#include "bolt/dwio/parquet/reader/ParquetData.h"
namespace bytedance::bolt::parquet {

class StringColumnReader : public dwio::common::SelectiveColumnReader {
 public:
  using ValueType = StringView;
  StringColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      ParquetParams& params,
      common::ScanSpec& scanSpec);

  StringColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      ParquetParams& params,
      common::ScanSpec& scanSpec);

  bool hasBulkPath() const override {
    //  Non-dictionary encodings do not have fast path.
    return !formatData_->as<ParquetData>().isDeltaByteArray() &&
        scanState_.dictionary.values != nullptr;
  }

  void seekToRowGroup(int64_t index) override {
    SelectiveColumnReader::seekToRowGroup(index);
    scanState().clear();
    readOffset_ = 0;
    formatData_->as<ParquetData>().seekToRowGroup(index);
  }

  uint64_t skip(uint64_t numValues) override;

  void read(int64_t offset, const RowSet& rows, const uint64_t* incomingNulls)
      override;

  void getValues(const RowSet& rows, VectorPtr* result) override;

  void dedictionarize() override;

 private:
  template <bool hasNulls>
  void skipInDecode(int32_t numValues, int32_t current, const uint64_t* nulls);

  folly::StringPiece readValue(int32_t length);

  template <bool hasNulls, typename Visitor>
  void decode(const uint64_t* nulls, Visitor visitor);

  template <typename TVisitor>
  void readWithVisitor(const RowSet& rows, TVisitor visitor);

  template <typename TFilter, bool isDense, typename ExtractValues>
  void
  readHelper(common::Filter* filter, const RowSet& rows, ExtractValues values);

  template <bool isDense, typename ExtractValues>
  void processFilter(
      common::Filter* filter,
      RowSet rows,
      ExtractValues extractValues);
};

} // namespace bytedance::bolt::parquet
