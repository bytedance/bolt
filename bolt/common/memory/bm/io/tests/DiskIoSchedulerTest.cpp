#include "bolt/common/memory/bm/io/DiskIoScheduler.h"

#include <string>
#include <type_traits>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

static_assert(!std::is_default_constructible_v<DiskIoScheduler>);
static_assert(!std::is_copy_constructible_v<DiskIoScheduler>);
static_assert(!std::is_copy_assignable_v<DiskIoScheduler>);

TEST(DiskIoSchedulerTest, globalAccessorReturnsStableFacade) {
  EXPECT_EQ(&diskIoScheduler(), &diskIoScheduler());
}

TEST(DiskIoSchedulerTest, ensureReadyIsExplicitInitializationEntry) {
  try {
    diskIoScheduler().ensureReady();
  } catch (const std::exception& e) {
    if (std::string(e.what()).find("io_uring_queue_init failed") !=
        std::string::npos) {
      GTEST_SKIP() << "io_uring is not permitted in this runtime";
    }
    throw;
  }
}
