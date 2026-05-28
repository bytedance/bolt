#include "bolt/common/memory/bm/io/DiskIoScheduler.h"

#include <type_traits>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

static_assert(!std::is_default_constructible_v<DiskIoScheduler>);
static_assert(!std::is_copy_constructible_v<DiskIoScheduler>);
static_assert(!std::is_copy_assignable_v<DiskIoScheduler>);

TEST(DiskIoSchedulerTest, globalAccessorReturnsStableFacade) {
  EXPECT_EQ(&diskIoScheduler(), &diskIoScheduler());
}
