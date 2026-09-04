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

#include "bolt/dwio/common/SelectiveColumnReaderInternal.h"
#include "bolt/dwio/dwrf/reader/DwrfData.h"
namespace bytedance::bolt::dwrf {

class SelectiveStringDirectColumnReader
    : public dwio::common::SelectiveColumnReader {
 public:
  using ValueType = StringView;
  SelectiveStringDirectColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec)
      : SelectiveStringDirectColumnReader(
            fileType,
            fileType,
            params,
            scanSpec) {}

  SelectiveStringDirectColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec);

  void seekToRowGroup(int64_t index) override {
    SelectiveColumnReader::seekToRowGroup(index);
    auto positionsProvider = formatData_->seekToRowGroup(index);
    blobStream_->seekToPosition(positionsProvider);
    lengthDecoder_->seekToRowGroup(positionsProvider);

    BOLT_CHECK(!positionsProvider.hasNext());

    bytesToSkip_ = 0;
    bufferStart_ = bufferEnd_;
  }

  uint64_t skip(uint64_t numValues) override;

  void read(int64_t offset, const RowSet& rows, const uint64_t* incomingNulls)
      override;

  void getValues(const RowSet& rows, VectorPtr* result) override {
    rawStringBuffer_ = nullptr;
    rawStringSize_ = 0;
    rawStringUsed_ = 0;
    getFlatValues<StringView, StringView>(rows, result, fileType_->type());
    if (requestedType()->isBoolean()) {
      convertStringValuesToBoolean(result);
    }
  }

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
      const RowSet& rows,
      ExtractValues extractValues);

  void extractCrossBuffers(
      const int32_t* lengths,
      const int32_t* starts,
      int32_t rowIndex,
      int32_t numValues);

  inline void makeSparseStarts(
      int32_t startRow,
      const int32_t* rows,
      int32_t numRows,
      int32_t* starts);

  inline void extractNSparse(const int32_t* rows, int32_t row, int numRows);

  void extractSparse(const int32_t* rows, int32_t numRows);

  template <bool scatter, bool skip>
  bool try8Consecutive(int32_t start, const int32_t* rows, int32_t row);

  template <bool kScatter, bool kGreaterThan4>
  bool
  try8ConsecutiveSmall(const char* data, const uint16_t* offsets, int startRow);

  std::unique_ptr<dwio::common::IntDecoder</*isSigned*/ false>> lengthDecoder_;
  std::unique_ptr<dwio::common::SeekableInputStream> blobStream_;
  const char* bufferStart_ = nullptr;
  const char* bufferEnd_ = nullptr;
  BufferPtr lengths_;
  int32_t lengthIndex_ = 0;
  const uint32_t* rawLengths_ = nullptr;
  int64_t bytesToSkip_ = 0;
  // Storage for a string straddling a buffer boundary. Needed for calling
  // the filter.
  std::string tempString_;
};

} // namespace bytedance::bolt::dwrf
