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

#include "bolt/dwio/dwrf/reader/BinaryStreamReader.h"
#include <boost/sort/pdqsort/pdqsort.hpp>
#include "bolt/dwio/common/exception/Exception.h"
namespace bytedance::bolt::dwrf::detail {

namespace {
class UnsupportedStrideIndexProvider : public StrideIndexProvider {
  uint64_t getStrideIndex() const override {
    throw std::runtime_error("StripeStreams do not support stride operations");
  }
};
} // namespace

BinaryStripeStreams::BinaryStripeStreams(
    StripeReaderBase& stripeReader,
    const dwio::common::ColumnSelector& selector,
    const uint32_t stripeIndex)
    : preload_(true), // TODO: is preload required ?
      stripeInfo_{stripeReader.loadStripe(stripeIndex, preload_)},
      stripeStreams_{
          stripeReader,
          selector,
          options_,
          stripeInfo_.offset(),
          static_cast<int64_t>(stripeInfo_.numberOfRows()),
          UnsupportedStrideIndexProvider(),
          stripeIndex} {
  if (!preload_) {
    stripeStreams_.loadReadPlan();
  }
  encodingKeys_ = stripeStreams_.getEncodingKeys();
  nodeToStreamIdMap_ = stripeStreams_.getStreamIdentifiers();
}

std::vector<proto::ColumnEncoding> BinaryStripeStreams::getEncodings(
    const uint32_t nodeId) const {
  if (encodingKeys_.count(nodeId) == 0) {
    return {};
  }

  auto sequenceIds = encodingKeys_.at(nodeId);
  boost::sort::pdqsort(sequenceIds.begin(), sequenceIds.end());

  std::vector<proto::ColumnEncoding> encodings;
  encodings.reserve(sequenceIds.size());

  for (const auto sequenceId : sequenceIds) {
    auto encoding = stripeStreams_.getEncoding({nodeId, sequenceId});
    encoding.set_node(nodeId);
    encoding.set_sequence(sequenceId);
    encodings.push_back(encoding);
  }
  return encodings;
}

std::vector<DwrfStreamIdentifier> BinaryStripeStreams::getStreamIdentifiers(
    const uint32_t nodeId) const {
  if (nodeToStreamIdMap_.count(nodeId) == 0) {
    return {};
  }

  return nodeToStreamIdMap_.at(nodeId);
}

BinaryStreamReader::BinaryStreamReader(
    const std::shared_ptr<ReaderBase>& reader,
    const std::vector<uint64_t>& columnIds)
    : stripeReaderBase_{reader},
      columnSelector_{reader->getSchema(), columnIds},
      stripeIndex_{0},
      numStripes{folly::to<uint32_t>(reader->getFooter().stripesSize())} {
  DWIO_ENSURE(!reader->getFooter().hasEncryption(), "encryption not supported");
  DWIO_ENSURE(!columnIds.empty(), "At least one column expected to be read");
}

std::unique_ptr<BinaryStripeStreams> BinaryStreamReader::next() {
  if (stripeIndex_ >= numStripes) {
    return nullptr;
  }
  return std::make_unique<BinaryStripeStreams>(
      stripeReaderBase_, columnSelector_, stripeIndex_++);
}

std::unordered_map<uint32_t, proto::ColumnStatistics>
BinaryStreamReader::getStatistics() const {
  std::unordered_map<uint32_t, proto::ColumnStatistics> stats;
  auto footerStatsSize =
      stripeReaderBase_.getReader().getFooter().statisticsSize();
  auto typesSize = stripeReaderBase_.getReader().getFooter().typesSize();

  if (footerStatsSize == 0) {
    DWIO_ENSURE_EQ(
        numStripes,
        0,
        "Corrupted file detected, Footer stats are missing, but stripes are present");
    for (auto node = 0; node < typesSize; node++) {
      if (columnSelector_.shouldReadNode(node)) {
        stats[node] = proto::ColumnStatistics();
      }
    }
  } else {
    DWIO_ENSURE_EQ(
        footerStatsSize, typesSize, "different number of nodes and statistics");
    // Node 0 is always selected by ColumnSelector, though this can be
    // disabled for the current use cases.
    for (auto node = 0; node < footerStatsSize; node++) {
      if (columnSelector_.shouldReadNode(node)) {
        stats[node] =
            stripeReaderBase_.getReader().getFooter().statistics(node);
      }
    }
  }
  return stats;
}

uint32_t BinaryStreamReader::getStrideLen() const {
  return stripeReaderBase_.getReader().getFooter().rowIndexStride();
}

} // namespace bytedance::bolt::dwrf::detail
