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

#include "bolt/common/memory/bm/io/DrrDispatcher.h"

#include <array>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

TEST(DrrDispatcherTest, RestoresDeficitWhenRequestReturnsToFront) {
  DrrDispatcher dispatcher({1, 1, 1});
  const std::array<size_t, kIoPriorityCount> queueSizes{1, 1, 0};

  const auto first = dispatcher.pick(queueSizes);
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(*first, priorityIndex(IoPriority::High));

  dispatcher.restore(*first);

  const auto retried = dispatcher.pick(queueSizes);
  ASSERT_TRUE(retried.has_value());
  EXPECT_EQ(*retried, priorityIndex(IoPriority::High));
}

TEST(DrrDispatcherTest, ResetsDeficitWhenQueueDrains) {
  DrrDispatcher dispatcher({2, 1, 1});
  std::array<size_t, kIoPriorityCount> queueSizes{1, 0, 0};

  const auto first = dispatcher.pick(queueSizes);
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(*first, priorityIndex(IoPriority::High));

  dispatcher.reset(*first);
  queueSizes = {0, 1, 0};

  const auto second = dispatcher.pick(queueSizes);
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(*second, priorityIndex(IoPriority::Medium));
}
