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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "bolt/common/memory/CompactDoubleList.h"
using namespace bytedance::bolt;

namespace {
int32_t listSize(const CompactDoubleList& list) {
  int32_t count = 0;
  for (auto element = list.next(); element != &list;
       element = element->next()) {
    ++count;
  }
  return count;
}
} // namespace

TEST(CompactDoubleListTest, basic) {
  constexpr int32_t kNumElements = 10;
  CompactDoubleList list;
  std::array<CompactDoubleList, kNumElements> elements;
  EXPECT_TRUE(list.empty());
  for (auto i = 0; i < kNumElements; ++i) {
    EXPECT_EQ(i, listSize(list));
    list.insert(&elements[i]);
  }
  for (auto i = 0; i < kNumElements; ++i) {
    EXPECT_EQ(kNumElements - i, listSize(list));
    EXPECT_EQ(i == kNumElements - 1, elements[i].remove());
  }
  EXPECT_TRUE(list.empty());
}

TEST(CompactDoubleListTest, popFront) {
  CompactDoubleList list;
  CompactDoubleList first;
  CompactDoubleList second;
  list.insert(&second);
  list.insert(&first);

  bool listIsEmpty = true;
  EXPECT_EQ(&first, list.popFront(listIsEmpty));
  EXPECT_FALSE(listIsEmpty);
  EXPECT_EQ(&second, list.next());

  EXPECT_EQ(&second, list.popFront(listIsEmpty));
  EXPECT_TRUE(listIsEmpty);
  EXPECT_TRUE(list.empty());

  EXPECT_EQ(nullptr, list.popFront(listIsEmpty));
  EXPECT_TRUE(listIsEmpty);
}
