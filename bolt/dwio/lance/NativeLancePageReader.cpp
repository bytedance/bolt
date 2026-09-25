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

#include "bolt/dwio/lance/NativeLancePageReader.h"

#include <limits>

#include "bolt/common/base/Exceptions.h"
#include "bolt/dwio/lance/NativeLanceStructuralDecoder.h"

namespace bytedance::bolt::lance::reader {

NativeLanceStructuralPageReader::NativeLanceStructuralPageReader(
    NativeLanceStructuralPageRequest request,
    const NativeLanceMetadata& metadata,
    memory::MemoryPool& pool,
    std::shared_ptr<const NativeLanceBlobResolver> blobResolver,
    std::string_view sourceDataFile,
    Prefetch prefetch,
    Read read,
    std::shared_ptr<const NativeLanceStructuralPagePlan> plan)
    : request_(std::move(request)),
      metadata_(metadata),
      pool_(pool),
      blobResolver_(std::move(blobResolver)),
      sourceDataFile_(sourceDataFile),
      prefetch_(std::move(prefetch)),
      read_(std::move(read)),
      plan_(std::move(plan)),
      supportsRangeRead_(lanceStructuralPageSupportsRangeRead(
          request_.type,
          request_.fixedSizeDimensions,
          request_.packedChildLogicalTypes,
          metadata_.pageLayout(
              request_.key.physicalColumn,
              request_.key.pageIndex))),
      accessMode_(
          supportsRangeRead_ ? NativeLanceCodecAccess::kBlock
                             : NativeLanceCodecAccess::kWholeBuffer) {
  BOLT_CHECK_LT(request_.key.physicalColumn, metadata_.numPhysicalColumns());
  const auto& column = metadata_.column(request_.key.physicalColumn);
  BOLT_CHECK_GE(request_.key.pageIndex, 0);
  BOLT_CHECK_LT(request_.key.pageIndex, column.pages_size());
  BOLT_CHECK_LE(
      request_.localRowStart,
      std::numeric_limits<uint64_t>::max() - request_.rowCount);
}

void NativeLanceStructuralPageReader::decode() {
  if (state_ == NativeLancePageState::kFailed) {
    rethrowFailure();
  }
  BOLT_CHECK(
      state_ == NativeLancePageState::kCreated,
      "Structural Lance page can only decode from the created state");
  state_ = NativeLancePageState::kDecoding;
  try {
    const auto& column = metadata_.column(request_.key.physicalColumn);
    const auto& page = column.pages(request_.key.pageIndex);
    const auto& layout = metadata_.pageLayout(
        request_.key.physicalColumn, request_.key.pageIndex);
    result_ = decodeLanceStructuralPage(
        request_.type,
        request_.logicalType,
        request_.fixedSizeDimensions,
        request_.packedChildLogicalTypes,
        column,
        page,
        layout,
        request_.localRowStart,
        request_.rowCount,
        pool_,
        blobResolver_,
        sourceDataFile_,
        [&](const std::vector<std::pair<uint64_t, uint64_t>>& ranges) {
          state_ = NativeLancePageState::kPlanningPayload;
          prefetch_(ranges);
          state_ = NativeLancePageState::kDecoding;
        },
        read_,
        plan_.get());
    BOLT_CHECK_NOT_NULL(result_);
    state_ = NativeLancePageState::kReadyToEmit;
  } catch (...) {
    failure_ = std::current_exception();
    state_ = NativeLancePageState::kFailed;
    throw;
  }
}

VectorPtr NativeLanceStructuralPageReader::consume() {
  if (state_ == NativeLancePageState::kFailed) {
    rethrowFailure();
  }
  BOLT_CHECK(
      state_ == NativeLancePageState::kReadyToEmit,
      "Structural Lance page has no decoded result to consume");
  state_ = NativeLancePageState::kExhausted;
  return std::move(result_);
}

void NativeLanceStructuralPageReader::cancel() {
  if (state_ == NativeLancePageState::kExhausted ||
      state_ == NativeLancePageState::kFailed ||
      state_ == NativeLancePageState::kCancelled) {
    return;
  }
  result_.reset();
  state_ = NativeLancePageState::kCancelled;
}

void NativeLanceStructuralPageReader::rethrowFailure() const {
  BOLT_CHECK_NOT_NULL(failure_);
  std::rethrow_exception(failure_);
}

} // namespace bytedance::bolt::lance::reader
