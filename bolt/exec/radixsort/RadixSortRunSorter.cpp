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

#include "bolt/exec/radixsort/RadixSortRunSorter.h"

#include <algorithm>
#include <limits>
#include <list>

#include <boost/sort/pdqsort/pdqsort.hpp>

namespace bytedance::bolt::exec::radixsort {
namespace {

template <bool Variable>
class RadixSortKeyLessState {
 public:
  explicit RadixSortKeyLessState(const RadixSortKeyLayout& /*layout*/) {}
};

template <>
class RadixSortKeyLessState<true> {
 public:
  explicit RadixSortKeyLessState(const RadixSortKeyLayout& layout)
      : heapKeyOffset_(layout.heapKeyOffset()) {}

 protected:
  uint32_t heapKeyOffset() const {
    return heapKeyOffset_;
  }

 private:
  uint32_t heapKeyOffset_;
};

constexpr uint64_t kRunDetectionCutoff = 128;
constexpr uint64_t kComparisonFallbackCutoff = 128;
constexpr uint64_t kLargeBucketCutoff = 1024;
constexpr uint32_t kEffectiveRadixPassLimit = sizeof(uint64_t);
constexpr uint32_t kFreeRadixPassInformationBits = 2;
constexpr uint32_t kFreeRadixPassBucketLimit = uint32_t{1}
    << kFreeRadixPassInformationBits;

uint64_t floorLog2(uint64_t value) {
  uint64_t result = 0;
  while (value >>= 1) {
    ++result;
  }
  return result;
}

template <uint32_t REMAINING>
bool fixedSortKeyLess(const uint64_t* left, const uint64_t* right) {
  return (*left < *right) ||
      ((*left == *right) &&
       fixedSortKeyLess<REMAINING - 1>(left + 1, right + 1));
}

template <>
bool fixedSortKeyLess<1>(const uint64_t* left, const uint64_t* right) {
  return *left < *right;
}

template <RadixSortKeyLayoutKind KIND>
class RadixSortKeyLess
    : private RadixSortKeyLessState<RadixSortKeyTraits<KIND>::kVariable> {
 public:
  using Type = typename RadixSortKeyTraits<KIND>::Type;
  using Traits = RadixSortKeyTraits<KIND>;
  using State = RadixSortKeyLessState<Traits::kVariable>;

  explicit RadixSortKeyLess(const RadixSortKeyLayout& layout) : State(layout) {}

  bool operator()(const Type& left, const Type& right) const {
    if constexpr (Traits::kVariable) {
      return RadixSortKeyOps<KIND>::compare(
                 reinterpret_cast<const char*>(&left),
                 reinterpret_cast<const char*>(&right),
                 this->heapKeyOffset()) < 0;
    } else {
      return fixedSortKeyLess<Traits::kInlineWords>(&left.part0, &right.part0);
    }
  }
};

template <RadixSortKeyLayoutKind KIND>
class SegmentedKeyState {
 public:
  using Type = typename RadixSortKeyTraits<KIND>::Type;
  using BlockPointers = std::vector<char*>;

  explicit SegmentedKeyState(RadixSortRunStorage& arena)
      : divisor_(arena.keysPerBlock()),
        multiplier_(
            std::numeric_limits<uint64_t>::max() / divisor_ + uint64_t{1}) {
    blockPointers_.reserve(arena.keyBlocks().size());
    for (const auto& block : arena.keyBlocks()) {
      blockPointers_.push_back(block.base);
    }
  }

  void randomAccess(uint64_t& blockIndex, uint64_t& tupleIndex, uint64_t index)
      const {
    blockIndex = static_cast<uint64_t>(
        (static_cast<__uint128_t>(index) * multiplier_) >> 64);
    tupleIndex = index - blockIndex * divisor_;
  }

  uint64_t index(uint64_t blockIndex, uint64_t tupleIndex) const {
    return blockIndex * divisor_ + tupleIndex;
  }

  Type& value(uint64_t blockIndex, uint64_t tupleIndex) const {
    return reinterpret_cast<Type*>(blockPointers_[blockIndex])[tupleIndex];
  }

  uint64_t divisor() const {
    return divisor_;
  }

 private:
  BlockPointers blockPointers_;
  const uint64_t divisor_;
  const uint64_t multiplier_;
};

template <RadixSortKeyLayoutKind KIND>
class SegmentedKeyIterator {
 public:
  using Type = typename RadixSortKeyTraits<KIND>::Type;
  using State = SegmentedKeyState<KIND>;
  using iterator_category = std::random_access_iterator_tag;
  using value_type = Type;
  using difference_type = uint64_t;
  using reference = Type&;
  using pointer = Type*;

  SegmentedKeyIterator() = default;

  SegmentedKeyIterator(State& state, uint64_t index) : state_(&state) {
    setIndex(index);
  }

  SegmentedKeyIterator(const SegmentedKeyIterator&) = default;

  SegmentedKeyIterator& operator=(const SegmentedKeyIterator& other) {
    if (this != &other) {
      blockIndex_ = other.blockIndex_;
      tupleIndex_ = other.tupleIndex_;
    }
    return *this;
  }

  reference operator*() const {
    return state_->value(blockIndex_, tupleIndex_);
  }

  pointer operator->() const {
    return &operator*();
  }

  reference operator[](difference_type offset) const {
    uint64_t blockIndex;
    uint64_t tupleIndex;
    state_->randomAccess(blockIndex, tupleIndex, offset);
    return state_->value(blockIndex, tupleIndex);
  }

  inline SegmentedKeyIterator& operator++() {
    if (++tupleIndex_ == state_->divisor()) {
      ++blockIndex_;
      tupleIndex_ = 0;
    }
    return *this;
  }

  SegmentedKeyIterator operator++(int) {
    auto copy = *this;
    ++*this;
    return copy;
  }

  inline SegmentedKeyIterator& operator--() {
    if (tupleIndex_-- == 0) {
      --blockIndex_;
      tupleIndex_ = state_->divisor() - 1;
    }
    return *this;
  }

  SegmentedKeyIterator operator--(int) {
    auto copy = *this;
    --*this;
    return copy;
  }

  SegmentedKeyIterator& operator+=(difference_type offset) {
    tupleIndex_ += static_cast<uint64_t>(offset);
    if (tupleIndex_ >= state_->divisor()) {
      setIndex(index());
    }
    return *this;
  }

  SegmentedKeyIterator& operator-=(difference_type offset) {
    tupleIndex_ -= static_cast<uint64_t>(offset);
    if (tupleIndex_ >= state_->divisor()) {
      setIndex(index());
    }
    return *this;
  }

  friend SegmentedKeyIterator operator+(
      SegmentedKeyIterator iterator,
      difference_type offset) {
    iterator += offset;
    return iterator;
  }

  friend SegmentedKeyIterator operator+(
      difference_type offset,
      SegmentedKeyIterator iterator) {
    return iterator + offset;
  }

  friend SegmentedKeyIterator operator-(
      SegmentedKeyIterator iterator,
      difference_type offset) {
    iterator -= offset;
    return iterator;
  }

  friend difference_type operator-(
      const SegmentedKeyIterator& left,
      const SegmentedKeyIterator& right) {
    return static_cast<difference_type>(left.index()) -
        static_cast<difference_type>(right.index());
  }

  friend bool operator==(
      const SegmentedKeyIterator& left,
      const SegmentedKeyIterator& right) {
    return left.blockIndex_ == right.blockIndex_ &&
        left.tupleIndex_ == right.tupleIndex_;
  }

  friend bool operator!=(
      const SegmentedKeyIterator& left,
      const SegmentedKeyIterator& right) {
    return !(left == right);
  }

  friend bool operator<(
      const SegmentedKeyIterator& left,
      const SegmentedKeyIterator& right) {
    return left.blockIndex_ == right.blockIndex_
        ? left.tupleIndex_ < right.tupleIndex_
        : left.blockIndex_ < right.blockIndex_;
  }

  friend bool operator>(
      const SegmentedKeyIterator& left,
      const SegmentedKeyIterator& right) {
    return right < left;
  }

  friend bool operator<=(
      const SegmentedKeyIterator& left,
      const SegmentedKeyIterator& right) {
    return !(right < left);
  }

  friend bool operator>=(
      const SegmentedKeyIterator& left,
      const SegmentedKeyIterator& right) {
    return !(left < right);
  }

 private:
  uint64_t index() const {
    return state_->index(blockIndex_, tupleIndex_);
  }

  void setIndex(uint64_t index) {
    state_->randomAccess(blockIndex_, tupleIndex_, index);
  }

  State* state_{nullptr};
  uint64_t blockIndex_{0};
  uint64_t tupleIndex_{0};
};

template <RadixSortKeyLayoutKind KIND, bool CompletePrefixRadix>
class RadixSortRunSorterKernel {
 public:
  explicit RadixSortRunSorterKernel(RadixSortRunStorage& arena)
      : arena_(arena), less_(arena.layout()), iteratorState_(arena) {
    static_assert(!CompletePrefixRadix || Traits::kVariable);
    BOLT_DCHECK_EQ(arena.layout().radixWidth(), kRadixByteLimit);
  }

  void radixSort(
      uint64_t begin,
      uint64_t end,
      std::span<const uint32_t> skippableByteOffsets = {}) {
    skippableByteOffsets_ = skippableByteOffsets;
    if (end - begin < 2) {
      return;
    }
    auto first = Iterator(iteratorState_, begin);
    auto last = Iterator(iteratorState_, end);
    if constexpr (!CompletePrefixRadix) {
      if (last - first < static_cast<int64_t>(kComparisonFallbackCutoff)) {
        fullSort(first, last);
        return;
      }
    }
    sortRadixByte<0>(first, last);
  }

  void adaptiveSort(std::span<const uint32_t> skippableByteOffsets = {}) {
    skippableByteOffsets_ = skippableByteOffsets;
    auto first = Iterator(iteratorState_, 0);
    auto last = Iterator(iteratorState_, arena_.size());
    auto fallback = [&](Iterator begin, Iterator end) {
      radixSort(begin, end);
    };
    detectRunsAndSort(first, last, less_, fallback);
  }

 private:
  struct PartitionInfo {
    PartitionInfo() : count(0) {}

    union {
      uint64_t count;
      uint64_t offset;
    };
    uint64_t nextOffset;
  };

  static_assert(kLargeBucketCutoff <= std::numeric_limits<uint16_t>::max());

  using Iterator = SegmentedKeyIterator<KIND>;
  using Traits = RadixSortKeyTraits<KIND>;
  using Compare = RadixSortKeyLess<KIND>;
  using RunList = std::list<Iterator>;
  static constexpr bool kEnableSuffixRadix =
      !Traits::kVariable && !Traits::kHasPayload;
  static constexpr uint32_t kRadixByteLimit = CompletePrefixRadix
      ? Traits::kInlineCapacity
      : (kEnableSuffixRadix
             ? Traits::kInlineCapacity
             : std::min<uint32_t>(Traits::kInlineCapacity, sizeof(uint64_t)));
  static constexpr bool kRequiresFullKeyFallback =
      Traits::kVariable || kRadixByteLimit < Traits::kInlineCapacity;

  void comparisonSort(Iterator begin, Iterator end) {
    boost::sort::pdqsort_branchless(begin, end, less_);
  }

  void finishRadixSort(Iterator begin, Iterator end) {
    if constexpr (CompletePrefixRadix) {
      const auto heapKeyOffset = arena_.layout().heapKeyOffset();
      constexpr auto radixWidth = kRadixByteLimit;
      auto less = [heapKeyOffset, radixWidth](
                      const typename Traits::Type& left,
                      const typename Traits::Type& right) {
        return RadixSortKeyOps<KIND>::compareSuffix(
                   reinterpret_cast<const char*>(&left),
                   reinterpret_cast<const char*>(&right),
                   heapKeyOffset,
                   radixWidth) < 0;
      };
      boost::sort::pdqsort_branchless(begin, end, less);
    } else if constexpr (kRequiresFullKeyFallback) {
      fullSort(begin, end);
    }
  }

  template <typename Fallback>
  void detectRunsAndSort(
      Iterator first,
      Iterator last,
      Compare compare,
      Fallback fallback) {
    const auto size = last - first;
    if (size < static_cast<int64_t>(kRunDetectionCutoff)) {
      fallback(first, last);
      return;
    }

    const auto unstableLimit = size / floorLog2(size);
    RunList runs;
    auto beginUnstable = last;
    auto current = first;
    auto next = first + 1;

    while (true) {
      const auto beginRange = current;
      if ((last - next) <= unstableLimit) {
        if (beginUnstable == last) {
          beginUnstable = beginRange;
        }
        break;
      }

      current += unstableLimit;
      next += unstableLimit;
      auto currentForward = current;
      auto nextForward = next;

      if (compare(*next, *current)) {
        do {
          --current;
          --next;
          if (compare(*current, *next)) {
            break;
          }
        } while (current != beginRange);
        if (compare(*current, *next)) {
          ++current;
        }

        ++currentForward;
        ++nextForward;
        while (nextForward != last) {
          if (compare(*currentForward, *nextForward)) {
            break;
          }
          ++currentForward;
          ++nextForward;
        }

        if ((nextForward - current) >= unstableLimit) {
          std::reverse(current, nextForward);
          if (current != beginRange && beginUnstable == last) {
            beginUnstable = beginRange;
          }
          if (beginUnstable != last) {
            fallback(beginUnstable, current);
            runs.push_back(current);
            beginUnstable = last;
          }
          runs.push_back(nextForward);
        } else if (beginUnstable == last) {
          beginUnstable = beginRange;
        }
      } else {
        do {
          --current;
          --next;
          if (compare(*next, *current)) {
            break;
          }
        } while (current != beginRange);
        if (compare(*next, *current)) {
          ++current;
        }

        ++currentForward;
        ++nextForward;
        while (nextForward != last) {
          if (compare(*nextForward, *currentForward)) {
            break;
          }
          ++currentForward;
          ++nextForward;
        }

        if ((nextForward - current) >= unstableLimit) {
          if (current != beginRange && beginUnstable == last) {
            beginUnstable = beginRange;
          }
          if (beginUnstable != last) {
            fallback(beginUnstable, current);
            runs.push_back(current);
            beginUnstable = last;
          }
          runs.push_back(nextForward);
        } else if (beginUnstable == last) {
          beginUnstable = beginRange;
        }
      }

      if (nextForward == last) {
        break;
      }
      current = currentForward + 1;
      next = nextForward + 1;
    }

    if (beginUnstable != last) {
      runs.push_back(last);
      fallback(beginUnstable, last);
    }
    if (runs.size() < 2) {
      return;
    }

    do {
      auto begin = first;
      for (auto iterator = runs.begin();
           iterator != runs.end() && std::next(iterator) != runs.end();
           ++iterator) {
        const auto middle = *iterator;
        const auto end = *std::next(iterator);
        std::inplace_merge(begin, middle, end, compare);
        iterator = runs.erase(iterator);
        begin = *iterator;
      }
    } while (runs.size() > 1);
  }

  void radixSort(Iterator begin, Iterator end) {
    if (end - begin < 2) {
      return;
    }
    if constexpr (!CompletePrefixRadix) {
      if (end - begin < static_cast<int64_t>(kComparisonFallbackCutoff)) {
        fullSort(begin, end);
        return;
      }
    }
    sortRadixByte<0>(begin, end);
  }

  inline __attribute__((always_inline)) void fullSort(
      Iterator begin,
      Iterator end) {
    if (end - begin < 2) {
      return;
    }
    if (end - begin < kRunDetectionCutoff) {
      comparisonSort(begin, end);
      return;
    }
    auto fallback = [&](Iterator fallbackBegin, Iterator fallbackEnd) {
      comparisonSort(fallbackBegin, fallbackEnd);
    };
    detectRunsAndSort(begin, end, less_, fallback);
  }

  bool byteIsSkippable(uint32_t byteOffset) const {
    return std::find(
               skippableByteOffsets_.begin(),
               skippableByteOffsets_.end(),
               byteOffset) != skippableByteOffsets_.end();
  }

  template <uint32_t OFFSET>
  uint8_t radixByte(const typename Traits::Type& key) const {
    static_assert(OFFSET < Traits::kInlineCapacity);
    if constexpr (Traits::kVariable) {
      return reinterpret_cast<const uint8_t*>(&key)[OFFSET];
    } else {
      const auto* words = &key.part0;
      constexpr auto kWord = OFFSET / sizeof(uint64_t);
      constexpr auto kByte = OFFSET % sizeof(uint64_t);
      return static_cast<uint8_t>(
          words[kWord] >> ((sizeof(uint64_t) - kByte - 1) * 8));
    }
  }

  template <uint32_t OFFSET>
  bool singleRadixBucket(Iterator begin, Iterator end) const {
    const auto byte = radixByte<OFFSET>(*begin);
    for (auto iterator = begin + 1; iterator != end; ++iterator) {
      if (radixByte<OFFSET>(*iterator) != byte) {
        return false;
      }
    }
    return true;
  }

  bool isEffectiveRadixPass(uint32_t bucketCount) const {
    return bucketCount > kFreeRadixPassBucketLimit;
  }

  template <uint32_t OFFSET>
  inline void
  sortBucket(Iterator begin, Iterator end, uint32_t effectivePasses) {
    const auto count = end - begin;
    if (count <= 1) {
      return;
    }
    if constexpr (!CompletePrefixRadix) {
      if (count < kComparisonFallbackCutoff) {
        fullSort(begin, end);
        return;
      }
    }
    sortRadixByte<OFFSET>(begin, end, effectivePasses);
  }

  template <uint32_t OFFSET>
  void cycleBucketSort(Iterator begin, Iterator end, uint32_t effectivePasses) {
    uint16_t counts[256];
    uint16_t offsets[256];
    uint16_t ends[256];
    uint8_t remaining[256];
    uint64_t occupied[4]{};
    auto* remainingEnd = remaining;
    if constexpr (!CompletePrefixRadix) {
      std::fill(std::begin(counts), std::end(counts), uint16_t{0});
    }
    for (auto iterator = begin; iterator != end; ++iterator) {
      const auto bucket = radixByte<OFFSET>(*iterator);
      if constexpr (CompletePrefixRadix) {
        const auto mask = uint64_t{1} << (bucket % 64);
        auto& word = occupied[bucket / 64];
        if ((word & mask) == 0) {
          word |= mask;
          counts[bucket] = 1;
        } else {
          ++counts[bucket];
        }
      } else {
        ++counts[bucket];
      }
    }

    uint16_t total = 0;
    if constexpr (CompletePrefixRadix) {
      for (uint32_t wordIndex = 0; wordIndex < 4; ++wordIndex) {
        auto word = occupied[wordIndex];
        while (word != 0) {
          const auto bit = static_cast<uint32_t>(__builtin_ctzll(word));
          *remainingEnd++ = static_cast<uint8_t>(wordIndex * 64 + bit);
          word &= word - 1;
        }
      }
    } else {
      for (uint32_t bucket = 0; bucket < 256; ++bucket) {
        if (counts[bucket] != 0) {
          *remainingEnd++ = static_cast<uint8_t>(bucket);
        }
      }
    }
    for (auto* bucket = remaining; bucket != remainingEnd; ++bucket) {
      offsets[*bucket] = total;
      total += counts[*bucket];
      ends[*bucket] = total;
    }
    auto nextPasses = effectivePasses;
    if constexpr (!CompletePrefixRadix) {
      const auto bucketCount = static_cast<uint32_t>(remainingEnd - remaining);
      nextPasses += isEffectiveRadixPass(bucketCount);
      if (nextPasses > kEffectiveRadixPassLimit) {
        fullSort(begin, end);
        return;
      }
    }
    if (remainingEnd - remaining > 1) {
      auto* currentBucket = remaining;
      const auto* lastBucket = remainingEnd - 1;
      auto iterator = begin + offsets[*currentBucket];
      auto blockEnd = begin + ends[*currentBucket];
      const auto lastElement = end - 1;
      while (true) {
        const auto destinationBucket = radixByte<OFFSET>(*iterator);
        if (destinationBucket == *currentBucket) {
          ++iterator;
          if (iterator == lastElement) {
            break;
          }
          if (iterator == blockEnd) {
            while (true) {
              if (++currentBucket == lastBucket) {
                goto recurse;
              }
              if (offsets[*currentBucket] != ends[*currentBucket]) {
                break;
              }
            }
            iterator = begin + offsets[*currentBucket];
            blockEnd = begin + ends[*currentBucket];
          }
        } else {
          auto destination = begin + offsets[destinationBucket]++;
          std::iter_swap(iterator, destination);
        }
      }
    }

  recurse:
    if constexpr (!kRequiresFullKeyFallback && OFFSET + 1 == kRadixByteLimit) {
      return;
    }
    uint64_t startOffset = 0;
    for (auto* bucket = remaining; bucket != remainingEnd; ++bucket) {
      const auto endOffset = ends[*bucket];
      sortBucket<OFFSET + 1>(
          begin + startOffset, begin + endOffset, nextPasses);
      startOffset = endOffset;
    }
  }

  template <typename Predicate>
  uint8_t*
  partitionBucketIds(uint8_t* begin, uint8_t* end, Predicate&& predicate) {
    while (begin != end && predicate(*begin)) {
      ++begin;
    }
    if (begin == end) {
      return end;
    }
    for (auto iterator = begin + 1; iterator != end; ++iterator) {
      if (predicate(*iterator)) {
        std::iter_swap(begin++, iterator);
      }
    }
    return begin;
  }

  template <typename Function>
  inline void
  unrollLoopFourTimes(Iterator begin, uint64_t count, Function&& function) {
    auto loopCount = count / 4;
    const auto remainder = count - loopCount * 4;
    while (loopCount-- != 0) {
      function(begin++);
      function(begin++);
      function(begin++);
      function(begin++);
    }
    switch (remainder) {
      case 3:
        function(begin++);
        [[fallthrough]];
      case 2:
        function(begin++);
        [[fallthrough]];
      case 1:
        function(begin);
        [[fallthrough]];
      case 0:
        break;
    }
  }

  template <uint32_t OFFSET>
  void largeBucketSort(Iterator begin, Iterator end, uint32_t effectivePasses) {
    PartitionInfo partitions[256];
    for (auto iterator = begin; iterator != end; ++iterator) {
      ++partitions[radixByte<OFFSET>(*iterator)].count;
    }

    uint8_t remaining[256];
    uint64_t total = 0;
    auto* remainingEnd = remaining;
    for (uint32_t bucket = 0; bucket < 256; ++bucket) {
      auto& partition = partitions[bucket];
      const auto count = partition.count;
      if (count != 0) {
        partition.offset = total;
        total += count;
        *remainingEnd++ = static_cast<uint8_t>(bucket);
      }
      partition.nextOffset = total;
    }
    auto nextPasses = effectivePasses;
    if constexpr (!CompletePrefixRadix) {
      const auto bucketCount = static_cast<uint32_t>(remainingEnd - remaining);
      nextPasses += bucketCount > kFreeRadixPassBucketLimit;
      if (nextPasses > kEffectiveRadixPassLimit) {
        fullSort(begin, end);
        return;
      }
    }
    auto* unfinished = remainingEnd;
    while (unfinished > remaining + 1) {
      unfinished =
          partitionBucketIds(remaining, unfinished, [&](uint8_t bucket) {
            const auto beginOffset = partitions[bucket].offset;
            const auto endOffset = partitions[bucket].nextOffset;
            if (beginOffset == endOffset) {
              return false;
            }
            auto& partitionCopy = partitions;
            unrollLoopFourTimes(
                begin + beginOffset,
                endOffset - beginOffset,
                [&](Iterator iterator) {
                  const auto destinationBucket = radixByte<OFFSET>(*iterator);
                  auto destination =
                      begin + partitionCopy[destinationBucket].offset++;
                  std::iter_swap(iterator, destination);
                });
            return partitions[bucket].offset != partitions[bucket].nextOffset;
          });
    }

    if constexpr (!kRequiresFullKeyFallback && OFFSET + 1 == kRadixByteLimit) {
      return;
    }
    for (auto* bucketIterator = remainingEnd; bucketIterator != remaining;
         --bucketIterator) {
      const auto bucket = bucketIterator[-1];
      const auto startOffset =
          bucket == 0 ? uint64_t{0} : partitions[bucket - 1].nextOffset;
      const auto endOffset = partitions[bucket].nextOffset;
      sortBucket<OFFSET + 1>(
          begin + startOffset, begin + endOffset, nextPasses);
    }
  }

  template <uint32_t OFFSET>
  __attribute__((noinline)) void
  sortRadixByte(Iterator begin, Iterator end, uint32_t effectivePasses = 0) {
    if constexpr (OFFSET == kRadixByteLimit) {
      finishRadixSort(begin, end);
      return;
    } else {
      if (byteIsSkippable(OFFSET)) {
        sortRadixByte<OFFSET + 1>(begin, end, effectivePasses);
        return;
      }
      if constexpr (CompletePrefixRadix) {
        if (end - begin < kLargeBucketCutoff) {
          cycleBucketSort<OFFSET>(begin, end, effectivePasses);
        } else {
          largeBucketSort<OFFSET>(begin, end, effectivePasses);
        }
        return;
      } else {
        if (singleRadixBucket<OFFSET>(begin, end)) {
          sortRadixByte<OFFSET + 1>(begin, end, effectivePasses);
          return;
        }
        if (end - begin < kLargeBucketCutoff) {
          cycleBucketSort<OFFSET>(begin, end, effectivePasses);
        } else {
          largeBucketSort<OFFSET>(begin, end, effectivePasses);
        }
      }
    }
  }

  RadixSortRunStorage& arena_;
  Compare less_;
  SegmentedKeyState<KIND> iteratorState_;
  std::span<const uint32_t> skippableByteOffsets_;
};

template <typename Function>
void dispatchRadixSortKeyLayout(
    RadixSortKeyLayoutKind kind,
    Function&& function) {
  switch (kind) {
    case RadixSortKeyLayoutKind::kKeyOnlyFixed8:
      return function
          .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed8>();
    case RadixSortKeyLayoutKind::kKeyOnlyFixed16:
      return function
          .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed16>();
    case RadixSortKeyLayoutKind::kKeyOnlyFixed24:
      return function
          .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed24>();
    case RadixSortKeyLayoutKind::kKeyOnlyFixed32:
      return function
          .template operator()<RadixSortKeyLayoutKind::kKeyOnlyFixed32>();
    case RadixSortKeyLayoutKind::kKeyOnlyVariable32:
      return function
          .template operator()<RadixSortKeyLayoutKind::kKeyOnlyVariable32>();
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed16:
      return function.template
      operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed16>();
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed24:
      return function.template
      operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed24>();
    case RadixSortKeyLayoutKind::kKeyWithPayloadFixed32:
      return function.template
      operator()<RadixSortKeyLayoutKind::kKeyWithPayloadFixed32>();
    case RadixSortKeyLayoutKind::kKeyWithPayloadVariable32:
      return function.template
      operator()<RadixSortKeyLayoutKind::kKeyWithPayloadVariable32>();
    case RadixSortKeyLayoutKind::kInvalid:
      BOLT_UNREACHABLE("Cannot sort an invalid radix sort key layout");
  }
  BOLT_UNREACHABLE("Unknown radix sort key layout");
}

} // namespace

RadixSortRunSorter::RadixSortRunSorter(RadixSortRunStorage& arena)
    : arena_(arena) {}

void RadixSortRunSorter::sort(std::span<const uint32_t> skippableByteOffsets) {
  dispatchRadixSortKeyLayout(
      arena_.layout().kind(), [&]<RadixSortKeyLayoutKind KIND>() {
        using Traits = RadixSortKeyTraits<KIND>;
        if constexpr (Traits::kVariable) {
          if (arena_.layout().radixWidth() == Traits::kInlineCapacity) {
            RadixSortRunSorterKernel<KIND, true> sorter(arena_);
            sorter.adaptiveSort(skippableByteOffsets);
            return;
          }
        }
        RadixSortRunSorterKernel<KIND, false> sorter(arena_);
        sorter.adaptiveSort(skippableByteOffsets);
      });
}

} // namespace bytedance::bolt::exec::radixsort
