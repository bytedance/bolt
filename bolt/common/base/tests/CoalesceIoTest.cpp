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

#include "bolt/common/base/CoalesceIo.h"

#include <gtest/gtest.h>
#include <utility>
using namespace bytedance::bolt;

// Mockup of a cache entry: Offset, size, number of elements in a
// chained buffer.
struct IoUnit {
  explicit IoUnit(
      int64_t _offset,
      int32_t _size,
      int32_t _numBuffers,
      bool _mayCoalesce = true)
      : offset(_offset),
        size(_size),
        numBuffers(_numBuffers),
        mayCoalesce(_mayCoalesce) {}
  const int64_t offset;
  const int32_t size;
  const int32_t numBuffers;
  const bool mayCoalesce;
};

// Represents a data transfer. size is the number of bytes. numBuffers
// is the number of chained buffers to process. If this is 0, Range
// represents a skip of size bytes. This is for illustration only, so
// does not mention the actual buffers.
struct Range {
  Range(int64_t _size, int32_t _numBuffers)
      : size(_size), numBuffers(_numBuffers) {}
  const int64_t size;
  const int32_t numBuffers;
};

TEST(CoalesceIoTest, basic) {
  std::vector<IoUnit> data;

  //
  // The first 3 are in close proximity. But the 4th is apart because
  // bundling this with the 2 first would exceed the max batch size of 4.
  data.emplace_back(1000, 100000, 3);
  data.emplace_back(105000, 100000, 3);
  data.emplace_back(220000, 100000, 3);
  data.emplace_back(330000, 100000, 3);
  data.emplace_back(700000, 100000, 3);
  data.emplace_back(810000, 1000000, 1, false);
  std::vector<int64_t> offsets;
  // Vector where each element has the indices of the IoUnits to
  // process together.
  std::vector<std::vector<int32_t>> ioGroups;

  auto stats = coalesceIo<IoUnit, Range>(
      data,
      20000,
      8,
      [&](int32_t index) { return data[index].offset; },
      [&](int32_t index) { return data[index].size; },
      [&](int32_t index) {
        return data[index].mayCoalesce ? data[index].numBuffers : kNoCoalesce;
      },
      [&](const IoUnit& item, std::vector<Range>& ranges) {
        // Add as many ranges as there are buffers. Make all but the last be 0
        // bytes.
        for (auto i = 0; i < item.numBuffers - 1; ++i) {
          ranges.emplace_back(0, 0);
        }
        ranges.emplace_back(item.size, item.numBuffers);
      },
      [&](int32_t skip, std::vector<Range>& ranges) {
        ranges.emplace_back(skip, 0);
      },
      [&](const std::vector<IoUnit>& items,
          int32_t begin,
          int32_t end,
          uint64_t offset,
          const std::vector<Range>& rages) {
        offsets.push_back(offset);
        ioGroups.emplace_back();
        for (auto i = begin; i < end; ++i) {
          ioGroups.back().push_back(i);
        }
      });

  // We expect 4 IOs. The two first are coalesced because of a gap of
  // under 30000. The third is not coalesced even though it is near
  // because of the ranges per IO limit of 10. The fourth is too far
  // from the third to coalesce.
  EXPECT_EQ(4, stats.numIos);
  EXPECT_EQ(1500000, stats.payloadBytes);
  EXPECT_EQ(14000, stats.extraBytes);

  std::vector<int64_t> expectedOffsets{1000, 220000, 700000, 810000};
  EXPECT_EQ(expectedOffsets, offsets);
  EXPECT_EQ(2, ioGroups[0].size());
  EXPECT_EQ(2, ioGroups[1].size());
  EXPECT_EQ(1, ioGroups[2].size());
  EXPECT_EQ(1, ioGroups[3].size());
}

namespace {

// Checks group boundaries, transferred ranges, and callback order together.
void checkGroups(
    const std::vector<IoUnit>& data,
    const std::vector<int32_t>& expectedEnds,
    int64_t expectedExtraBytes,
    int32_t maxGap = 100,
    int32_t rangesPerIo = 100,
    int64_t byteLimit = 0) {
  std::vector<int32_t> ends;
  std::vector<int32_t> events;
  std::vector<int64_t> skips;
  std::vector<int64_t> expectedSkips;
  int64_t coalescedBytes = 0;
  int64_t payloadBytes = 0;
  for (const auto& item : data) {
    payloadBytes += item.size;
  }
  const auto stats = coalesceIo<IoUnit, Range>(
      data,
      maxGap,
      rangesPerIo,
      [&](int32_t index) { return data.at(index).offset; },
      [&](int32_t index) {
        events.push_back(3 * index);
        coalescedBytes += data.at(index).size;
        return data.at(index).size;
      },
      [&](int32_t index) {
        events.push_back(3 * index + 1);
        if (byteLimit > 0 && coalescedBytes > byteLimit) {
          coalescedBytes = 0;
          return kNoCoalesce;
        }
        return data.at(index).mayCoalesce ? 1 : kNoCoalesce;
      },
      [&](const IoUnit& item, std::vector<Range>& ranges) {
        const auto index = &item - data.data();
        events.push_back(3 * index + 2);
        ranges.emplace_back(item.size, 1);
      },
      [&](int64_t gap, std::vector<Range>& ranges) {
        skips.push_back(gap);
        ranges.emplace_back(gap, 0);
      },
      [&](const auto&,
          int32_t begin,
          int32_t end,
          int64_t offset,
          const std::vector<Range>& ranges) {
        ASSERT_LT(ends.size(), expectedEnds.size());
        const auto expectedBegin =
            ends.empty() ? 0 : expectedEnds[ends.size() - 1];
        const auto expectedEnd = expectedEnds[ends.size()];
        ends.push_back(end);
        EXPECT_EQ(expectedBegin, begin);
        EXPECT_EQ(expectedEnd, end);
        EXPECT_EQ(data.at(expectedBegin).offset, offset);
        std::vector<std::pair<int64_t, int32_t>> expectedRanges;
        for (auto i = expectedBegin; i < expectedEnd; ++i) {
          if (i > expectedBegin) {
            const auto gap =
                data[i].offset - data[i - 1].offset - data[i - 1].size;
            if (gap > 0) {
              expectedRanges.emplace_back(gap, 0);
              expectedSkips.push_back(gap);
            }
          }
          expectedRanges.emplace_back(data[i].size, 1);
        }
        std::vector<std::pair<int64_t, int32_t>> actualRanges;
        for (const auto& range : ranges) {
          actualRanges.emplace_back(range.size, range.numBuffers);
        }
        EXPECT_EQ(expectedRanges, actualRanges);
      });
  EXPECT_EQ(expectedEnds, ends);
  EXPECT_EQ(expectedEnds.size(), stats.numIos);
  EXPECT_EQ(payloadBytes, stats.payloadBytes);
  EXPECT_EQ(expectedExtraBytes, stats.extraBytes);
  EXPECT_EQ(expectedSkips, skips);
  std::vector<int32_t> expectedEvents;
  for (int32_t i = 0; i < 3 * data.size(); ++i) {
    expectedEvents.push_back(i);
  }
  EXPECT_EQ(expectedEvents, events);
}

} // namespace

TEST(CoalesceIoTest, noCoalesce) {
  struct TestCase {
    std::vector<bool> mayCoalesce;
    std::vector<int32_t> ends;
  };
  const std::vector<TestCase> cases{
      {{true, true}, {2}},
      {{false, true}, {1, 2}},
      {{true, true, false, true, true}, {2, 3, 5}},
      {{true, false, false, true, true}, {1, 2, 3, 5}},
      {{false, false, false}, {1, 2, 3}},
      {{true, true, false}, {2, 3}},
      {{false}, {1}},
      {{true}, {1}}};
  for (const auto gap : {0, 5}) {
    for (const auto& test : cases) {
      SCOPED_TRACE(
          testing::Message() << "gap=" << gap << " flags="
                             << testing::PrintToString(test.mayCoalesce));
      std::vector<IoUnit> data;
      for (int32_t i = 0; i < test.mayCoalesce.size(); ++i) {
        data.emplace_back(100 + i * (10 + gap), 10, 1, test.mayCoalesce[i]);
      }
      checkGroups(data, test.ends, (data.size() - test.ends.size()) * gap);
    }
  }
}

TEST(CoalesceIoTest, ordinaryBoundaries) {
  // The range limit splits when adding an item would reach the limit.
  checkGroups({IoUnit(0, 10, 1), IoUnit(10, 10, 1)}, {1, 2}, 0, 100, 2);
  for (const auto gap : {99, 100, 101, -1}) {
    SCOPED_TRACE(gap);
    const bool coalesces = gap == 99;
    checkGroups(
        {IoUnit(0, 10, 1), IoUnit(10 + gap, 10, 1)},
        coalesces ? std::vector<int32_t>{2} : std::vector<int32_t>{1, 2},
        coalesces ? gap : 0);
  }
}

TEST(CoalesceIoTest, statefulByteLimit) {
  // Match the readers: size accumulates bytes, numRanges resets on overflow.
  // The third item exceeds the limit and must remain separate from the fourth.
  checkGroups(
      {IoUnit(0, 10, 1),
       IoUnit(10, 10, 1),
       IoUnit(20, 10, 1),
       IoUnit(30, 10, 1)},
      {2, 3, 4},
      0,
      100,
      100,
      20);
}
