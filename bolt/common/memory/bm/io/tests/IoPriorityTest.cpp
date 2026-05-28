#include "bolt/common/memory/bm/io/IoPriority.h"

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

TEST(IoPriorityTest, priorityCountTracksPriorityEnumSentinel) {
  EXPECT_EQ(static_cast<size_t>(IoPriority::Count), kIoPriorityCount);
}
