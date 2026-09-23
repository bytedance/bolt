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

#include "bolt/common/process/ProcessBase.h"
#include <gtest/gtest.h>
#include "bolt/common/flags/BoltFlags.h"

#if defined(__aarch64__) && defined(__linux__)
#include <asm/hwcap.h>
#include <sys/auxv.h>
#endif

namespace bytedance::bolt::process {

TEST(ProcessBaseTest, simdCapabilities) {
  gflags::FlagSaver flags;
  for (bool enabled : {false, true}) {
    FLAGS_bolt_enable_avx2 = enabled;
#if defined(__aarch64__)
    EXPECT_EQ(hasSimd(), hasNeon());
#else
    EXPECT_EQ(hasSimd(), hasAvx2());
    if (!enabled) {
      EXPECT_FALSE(hasSimd());
    }
#endif
  }
}

TEST(ProcessBaseTest, sveCapabilities) {
  gflags::FlagSaver flags;
  FLAGS_bolt_enable_sve = false;
  EXPECT_FALSE(hasSve());
  FLAGS_bolt_enable_sve = true;
#if defined(__aarch64__) && defined(__linux__)
  EXPECT_EQ(hasSve(), (getauxval(AT_HWCAP) & HWCAP_SVE) != 0);
#else
  EXPECT_FALSE(hasSve());
#endif
}

} // namespace bytedance::bolt::process
