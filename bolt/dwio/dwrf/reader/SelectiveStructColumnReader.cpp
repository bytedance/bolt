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

#include "bolt/dwio/dwrf/reader/SelectiveStructColumnReader.h"
#include "bolt/dwio/common/ColumnLoader.h"
#include "bolt/dwio/dwrf/reader/SelectiveDwrfReader.h"
#include "folly/Conv.h"
namespace bytedance::bolt::dwrf {

using namespace dwio::common;

SelectiveStructColumnReader::SelectiveStructColumnReader(
    const dwio::common::ColumnReaderOptions& columnReaderOptions,
    const std::shared_ptr<const TypeWithId>& requestedType,
    const std::shared_ptr<const TypeWithId>& fileType,
    DwrfParams& params,
    common::ScanSpec& scanSpec,
    bool isRoot)
    : SelectiveStructColumnReaderBase(
          columnReaderOptions,
          requestedType,
          fileType,
          params,
          scanSpec,
          isRoot) {
  EncodingKey encodingKey{fileType_->id(), params.flatMapContext().sequence};
  auto& stripe = params.stripeStreams();
  auto encoding = static_cast<int64_t>(stripe.getEncoding(encodingKey).kind());
  DWIO_ENSURE_EQ(
      encoding,
      proto::ColumnEncoding_Kind_DIRECT,
      "Unknown encoding for StructColumnReader");

  const auto& cs = stripe.getColumnSelector();
  // A reader tree may be constructed while the ScanSpec is being used
  // for another read. This happens when the next stripe is being
  // prepared while the previous one is reading.
  auto& childSpecs = scanSpec.stableChildren();
  for (auto i = 0; i < childSpecs.size(); ++i) {
    auto childSpec = childSpecs[i];
    if (isChildConstant(*childSpec)) {
      childSpec->setSubscript(kConstantChildSpecSubscript);
      continue;
    }
    const auto childIndex = childSpec->channel();
    auto childFileType = columnReaderOptions.useColumnNamesForColumnMapping_
        ? fileType_->childByName(childSpec->fieldName())
        : fileType_->childAt(childIndex);
    auto childRequestedType = requestedType_->childAt(childIndex);
    auto labels = params.streamLabels().append(folly::to<std::string>(i));
    auto childParams = DwrfParams(
        stripe,
        labels,
        params.runtimeStatistics(),
        FlatMapContext{
            .sequence = encodingKey.sequence(),
            .inMapDecoder = nullptr,
            .keySelectionCallback = nullptr});
    BOLT_CHECK(cs.shouldReadNode(childRequestedType->id()));
    addChild(
        SelectiveDwrfReader::build(
            columnReaderOptions,
            childRequestedType,
            childFileType,
            childParams,
            *childSpec));
    childSpec->setSubscript(children_.size() - 1);
  }
}

void SelectiveStructColumnReaderBase::seekTo(
    int64_t offset,
    bool readsNullsOnly) {
  if (offset == readOffset_) {
    return;
  }
  if (readOffset_ < offset) {
    if (numParentNulls_) {
      BOLT_CHECK_LE(
          parentNullsRecordedTo_,
          offset,
          "Must not seek to before parentNullsRecordedTo_");
    }
    auto distance = offset - readOffset_ - numParentNulls_;
    auto numNonNulls = formatData_->skipNulls(distance);
    // We inform children how many nulls there were between original position
    // and destination. The children will seek this many less. The
    // nulls include the nulls found here as well as the enclosing
    // level nulls reported to this by parents.
    for (auto& child : children_) {
      if (child) {
        child->addSkippedParentNulls(
            readOffset_, offset, numParentNulls_ + distance - numNonNulls);
      }
    }
    numParentNulls_ = 0;
    parentNullsRecordedTo_ = 0;
    readOffset_ = offset;
  } else {
    BOLT_FAIL("Seeking backward on a ColumnReader");
  }
}

} // namespace bytedance::bolt::dwrf
