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
