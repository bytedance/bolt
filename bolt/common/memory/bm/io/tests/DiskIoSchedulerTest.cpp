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
