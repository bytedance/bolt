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

#include "bolt/exec/bm/tests/BmRowContainerTestBase.h"

namespace bytedance::bolt::exec::bm {
namespace {

using bytedance::bolt::memory::bm::MemoryTag;

TEST_F(BmRowContainerTest, PartitionCanFlushMultipleSegments) {
  BmRowContainer container(
      {BIGINT()}, {false}, 0, bufferManager_, MemoryTag::kTesting);
  auto input = makeRowVector({makeFlatVector<int64_t>({1, 2})});
  SelectivityVector rows(input->size());
  DecodedVector decoded;
  decoded.decode(*input->childAt(0), rows);

  auto firstContext = container.appendRow(7);
  container.store(firstContext, decoded, 0, 0);
  auto firstSegment = container.spillActivePartitionSegment(7);

  auto secondContext = container.appendRow(7);
  container.store(secondContext, decoded, 1, 0);
  auto secondSegment = container.spillActivePartitionSegment(7);

  EXPECT_NE(firstSegment, secondSegment);
  EXPECT_EQ(
      SegmentState::kFinalizedFlushed, container.segmentState(firstSegment));
  EXPECT_EQ(
      SegmentState::kFinalizedFlushed, container.segmentState(secondSegment));
  EXPECT_EQ(2, container.segmentsForPartition(7).size());
}

TEST_F(BmRowContainerTest, PartitionUsesFixedVectorLimit) {
  BmRowContainer container(
      {BIGINT()}, {false}, 0, bufferManager_, MemoryTag::kTesting);
  auto input = makeRowVector({makeFlatVector<int64_t>({42})});
  SelectivityVector rows(input->size());
  DecodedVector decoded;
  decoded.decode(*input->childAt(0), rows);

  auto context = container.appendRow(255);
  container.store(context, decoded, 0, 0);
  auto segment = container.spillActivePartitionSegment(255);

  EXPECT_EQ(1, container.segmentsForPartition(255).size());
  EXPECT_EQ(segment, container.segmentsForPartition(255)[0]);
  EXPECT_THROW({ (void)container.appendRow(256); }, BoltRuntimeError);
}

} // namespace
} // namespace bytedance::bolt::exec::bm
