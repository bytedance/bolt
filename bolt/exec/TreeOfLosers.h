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

#include <boost/sort/pdqsort/pdqsort.hpp>
#include <algorithm>
#include <memory>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/SimdUtil.h"

#include <folly/Function.h>
#include <folly/Likely.h>
namespace bytedance::bolt {

/// Abstract class defining the interface for a stream of values to be merged by
/// TreeOfLosers or MergeArray. In addition to these, the MergeStream must have
/// a way of accessing its first value and popping off the first value.
/// TreeOfLosers and similar do not call these, so these are left out of this
/// interface.
class MergeStream {
 public:
  virtual ~MergeStream() = default;

  /// True if this has a value. If this returns true, it is valid to call <. A
  /// false value means that there will not be any more data in 'this'.
  virtual bool hasData() const = 0;

  virtual bool isNextEqual() const {
    return false;
  }

  /// Returns true if the first element of 'this' is less than the first element
  /// of 'other'. hasData() must be true of 'this' and 'other'.
  virtual bool operator<(const MergeStream& other) const {
    return compare(other) < 0;
  }

  /// Returns < 0 if 'this' is < 'other, '0' if equal and > 0 otherwise. This is
  /// not required for TreeOfLosers::next() but is required for
  /// TreeOfLosers::nextWithEquals().
  virtual int32_t compare(const MergeStream& /*other*/) const {
    BOLT_UNSUPPORTED();
  }

 protected:
  uint64_t spillReadTimeUs_{0};
  uint64_t spillDecompressTimeUs_{0};
  uint64_t spillReadIOTimeUs_{0};
};

/// Implements a tree of losers algorithm for merging ordered streams. The
/// TreeOfLosers owns one or more instances of Stream. At each call of next(),
/// it returns the Stream that has the lowest value as first value from the set
/// of Streams. It returns nullptr when all Streams are at end. The order is
/// determined by Stream::operator<.
template <typename Stream, typename TIndex = uint16_t>
class TreeOfLosers {
 public:
  using IndexAndFlag = std::pair<TIndex, bool>;

  explicit TreeOfLosers(std::vector<std::unique_ptr<Stream>> streams)
      : streams_(std::move(streams)) {
    static_assert(std::is_base_of_v<MergeStream, Stream>);
    BOLT_CHECK_LT(streams_.size(), std::numeric_limits<TIndex>::max());
    BOLT_CHECK_GE(streams_.size(), 1);
    resetSelection();
  }

  const Stream* streamAt(size_t index) const {
    BOLT_CHECK_LT(index, streams_.size());
    return streams_[index].get();
  }

  void replaceStreams(
      size_t index,
      size_t count,
      folly::FunctionRef<std::vector<std::unique_ptr<Stream>>()>
          makeReplacements,
      folly::FunctionRef<void()> releaseOldBacking) {
    BOLT_CHECK_LE(index, streams_.size());
    BOLT_CHECK_LE(count, streams_.size() - index);
    BOLT_CHECK_GT(count, 0);
    for (size_t i = index; i < index + count; ++i) {
      streams_[i].reset();
    }
    try {
      releaseOldBacking();
      auto replacements = makeReplacements();
      BOLT_CHECK(!replacements.empty());
      streams_.erase(
          streams_.begin() + index, streams_.begin() + index + count);
      streams_.insert(
          streams_.begin() + index,
          std::make_move_iterator(replacements.begin()),
          std::make_move_iterator(replacements.end()));
      resetSelection();
    } catch (...) {
      streams_.clear();
      values_.clear();
      equals_.clear();
      lastIndex_ = kEmpty;
      firstStream_ = 0;
      throw;
    }
  }

 private:
  /// Rebuilds the loser-tree topology and selection state after replacement.
  void resetSelection() {
    int32_t size = 0;
    int32_t levelSize = 1;
    int32_t numStreams = streams_.size();
    while (numStreams > levelSize) {
      size += levelSize;
      levelSize *= 2;
    }

    if (numStreams == bits::nextPowerOfTwo(numStreams)) {
      // All leaves are on last level.
      firstStream_ = size;
    } else {
      // Some of the streams are on the last level and some on the level before.
      // The first stream follows the last inner node in the node numbering.

      auto secondLastSize = levelSize / 2;
      auto overflow = numStreams - secondLastSize;
      // Suppose 12 streams. The last level has 16 places, the second
      // last 8. If we fill the second last level we have 8 streams
      // and 4 left over. These 4 need parents on the second last
      // level. So, we end up with 4 inner nodes on the second last
      // level and 8 nodes on the last level. The streams at the left
      // of the second last level become inner nodes and their streams
      // move to the level below.
      firstStream_ = (size - secondLastSize) + overflow;
    }
    values_.assign(firstStream_, kEmpty);
    equals_.assign(firstStream_, false);
    lastIndex_ = kEmpty;
  }

 public:
  /// Returns the number of streams.
  size_t numStreams() const {
    return streams_.size();
  }

  /// Returns the stream with the lowest first element. The caller is expected
  /// to pop off the first element of the stream before calling this again.
  /// Returns nullptr when all streams are at end.
  Stream* next() {
    if (UNLIKELY(lastIndex_ == kEmpty)) {
      if (UNLIKELY(values_.empty())) {
        // Only one stream. We handle this off the common path.
        return streams_[0]->hasData() ? streams_[0].get() : nullptr;
      }
      lastIndex_ = first(0);
    } else {
      lastIndex_ = propagate(
          parent(firstStream_ + lastIndex_),
          streams_[lastIndex_]->hasData() ? lastIndex_ : kEmpty);
    }
    return lastIndex_ == kEmpty ? nullptr : streams_[lastIndex_].get();
  }

  /// Returns the stream with the lowest first element and a flag that is true
  /// if there is another equal value to come from some other stream. The
  /// streams should have ordered unique values when using this function. This
  /// is useful for merging aggregate states that are unique by their key in
  /// each stream.  The caller is expected to pop off the first element of the
  /// stream before calling this again. Returns {nullptr, false} when all
  /// streams are at end.
  std::pair<Stream*, bool> nextWithEquals() {
    IndexAndFlag result;
    if (UNLIKELY(lastIndex_ == kEmpty)) {
      // Only one stream. We handle this off the common path.
      if (values_.empty()) {
        return streams_[0]->hasData()
            ? std::make_pair(streams_[0].get(), streams_[0]->isNextEqual())
            : std::make_pair(nullptr, false);
      }
      result = firstWithEquals(0);
    } else {
      result = propagateWithEquals(
          parent(firstStream_ + lastIndex_),
          streams_[lastIndex_]->hasData() ? lastIndex_ : kEmpty);
    }
    lastIndex_ = result.first;

    return lastIndex_ == kEmpty
        ? std::make_pair(nullptr, false)
        : std::make_pair(streams_[lastIndex_].get(), result.second);
  }

  int64_t getSpillReadTime() const {
    int64_t timeUs = 0;
    timeUs = std::accumulate(
        streams_.begin(),
        streams_.end(),
        0,
        [](int64_t sum, const std::unique_ptr<Stream>& stream) {
          return sum + stream->getSpillReadTime();
        });
    return timeUs;
  }

  int64_t getSpillDecompressTime() const {
    int64_t timeUs = 0;
    timeUs = std::accumulate(
        streams_.begin(),
        streams_.end(),
        0,
        [](int64_t sum, const std::unique_ptr<Stream>& stream) {
          return sum + stream->getSpillDecompressTime();
        });
    return timeUs;
  }

  int64_t getSpillReadIOTime() const {
    int64_t timeUs = 0;
    timeUs = std::accumulate(
        streams_.begin(),
        streams_.end(),
        0,
        [](int64_t sum, const std::unique_ptr<Stream>& stream) {
          return sum + stream->getSpillReadIOTime();
        });
    return timeUs;
  }

 private:
  static constexpr TIndex kEmpty = std::numeric_limits<TIndex>::max();

  IndexAndFlag indexAndFlag(TIndex index, bool flag) {
    return std::pair<TIndex, bool>{index, flag};
  }

  TIndex first(size_t node) {
    if (node >= firstStream_) {
      return streams_[node - firstStream_]->hasData() ? node - firstStream_
                                                      : kEmpty;
    }
    auto left = first(leftChild(node));
    auto right = first(rightChild(node));
    if (left == kEmpty) {
      return right;
    } else if (right == kEmpty) {
      return left;
    } else if (*streams_[left] < *streams_[right]) {
      values_[node] = right;
      return left;
    } else {
      values_[node] = left;
      return right;
    }
  }

  FOLLY_ALWAYS_INLINE TIndex propagate(size_t node, TIndex value) {
    while (UNLIKELY(values_[node] == kEmpty)) {
      if (UNLIKELY(node == 0)) {
        return value;
      }
      node = parent(node);
    }
    for (;;) {
      if (UNLIKELY(values_[node] == kEmpty)) {
        // The value goes past the node and the node stays empty.
      } else if (UNLIKELY(value == kEmpty)) {
        value = values_[node];
        values_[node] = kEmpty;
      } else if (*streams_[values_[node]] < *streams_[value]) {
        // The node had the lower value, the value stays here and the previous
        // value goes up.
        std::swap(value, values_[node]);
      } else {
        // The value is less than the value in the node, No action, the value
        // goes up.
        ;
      }
      if (UNLIKELY(node == 0)) {
        return value;
      }
      node = parent(node);
    }
  }

  IndexAndFlag firstWithEquals(size_t node) {
    if (node >= firstStream_) {
      BOLT_DCHECK_LT(node - firstStream_, streams_.size());
      return streams_[node - firstStream_]->hasData()
          ? indexAndFlag(
                node - firstStream_,
                streams_[node - firstStream_]->isNextEqual())
          : indexAndFlag(kEmpty, false);
    }
    auto left = firstWithEquals(leftChild(node));
    auto right = firstWithEquals(rightChild(node));
    if (left.first == kEmpty) {
      return right;
    } else if (right.first == kEmpty) {
      return left;
    } else {
      auto comparison = streams_[left.first]->compare(*streams_[right.first]);
      if (comparison == 0) {
        values_[node] = right.first;
        equals_[node] = right.second;
        return indexAndFlag(left.first, true);
      } else if (comparison < 0) {
        values_[node] = right.first;
        equals_[node] = right.second;
        return left;
      } else {
        values_[node] = left.first;
        equals_[node] = left.second;
        return right;
      }
    }
  }

  FOLLY_ALWAYS_INLINE IndexAndFlag
  propagateWithEquals(size_t node, TIndex valueIndex) {
    auto value = indexAndFlag(
        valueIndex,
        valueIndex == kEmpty ? false : streams_[valueIndex]->isNextEqual());
    while (UNLIKELY(values_[node] == kEmpty)) {
      if (UNLIKELY(node == 0)) {
        return value;
      }
      node = parent(node);
    }
    for (;;) {
      if (UNLIKELY(values_[node] == kEmpty)) {
        // The value goes past the node and the node stays empty.
      } else if (UNLIKELY(value.first == kEmpty)) {
        value = indexAndFlag(values_[node], equals_[node]);
        values_[node] = kEmpty;
        equals_[node] = false;
      } else {
        auto comparison =
            streams_[values_[node]]->compare(*streams_[value.first]);
        if (comparison == 0) {
          // the value goes up with equals set.
          value.second = true;
        } else if (comparison < 0) {
          // The node had the lower value, the value stays here and the previous
          // value goes up.
          auto newValue = indexAndFlag(values_[node], equals_[node]);
          values_[node] = value.first;
          equals_[node] = value.second;
          value = newValue;
        } else {
          // The value is less than the value in the node, No action, the value
          // goes up.
          ;
        }
      }
      if (UNLIKELY(node == 0)) {
        return value;
      }
      node = parent(node);
    }
  }

  static size_t parent(size_t node) {
    return (node - 1) / 2;
  }

  static size_t leftChild(size_t node) {
    return node * 2 + 1;
  }

  static size_t rightChild(size_t node) {
    return node * 2 + 2;
  }

  std::vector<std::unique_ptr<Stream>> streams_;

  std::vector<TIndex> values_;
  // 'true' if the corresponding element of 'values_' has met an equal
  // element on its way to its present position. Used only in nextWithEquals().
  // A byte vector is in this case faster than one of bool.
  std::vector<uint8_t> equals_;
  TIndex lastIndex_ = kEmpty;
  size_t firstStream_;
};

// Array-based merging structure implementing the same interface as
// TreOfLosers. The streams are sorted on their first value. The
// first stream is returned and then reinserted in the array at the
// position corresponding to the new element after the caller has
// popped off the previous first value.
template <typename Stream>
class MergeArray {
 public:
  explicit MergeArray(std::vector<std::unique_ptr<Stream>> streams) {
    static_assert(std::is_base_of_v<MergeStream, Stream>);
    for (auto& stream : streams) {
      if (stream->hasData()) {
        streams_.push_back(std::move(stream));
      }
    }
    boost::sort::pdqsort(
        streams_.begin(),
        streams_.end(),
        [](const auto& left, const auto& right) { return *left < *right; });
  }

  // Returns the stream with the lowest first element. The caller is
  // expected to pop off the first element of the stream before
  // calling this again. Returns nullptr when all streams are at end.
  Stream* next() {
    if (UNLIKELY(isFirst_)) {
      isFirst_ = false;
      if (streams_.empty()) {
        return nullptr;
      }
      // stream has data, else it would not be here after construction.
      return streams_[0].get();
    }
    if (!streams_[0]->hasData()) {
      streams_.erase(streams_.begin());
      return streams_.empty() ? nullptr : streams_[0].get();
    }
    auto rawStreams = reinterpret_cast<Stream**>(streams_.data());
    auto first = rawStreams[0];
    auto it = std::lower_bound(
        rawStreams + 1,
        rawStreams + streams_.size(),
        first,
        [](const Stream* left, const Stream* right) { return *left < *right; });
    auto offset = it - rawStreams;
    if (offset > 1) {
      simd::memcpy(rawStreams, rawStreams + 1, (offset - 1) * sizeof(Stream*));
      it[-1] = first;
    }
    return streams_[0].get();
  }

 private:
  bool isFirst_{true};
  std::vector<std::unique_ptr<Stream>> streams_;
};

} // namespace bytedance::bolt
