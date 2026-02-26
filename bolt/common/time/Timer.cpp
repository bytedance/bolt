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

#include "bolt/common/time/Timer.h"

#ifndef NDEBUG
#include "bolt/common/testutil/ScopedTestTime.h"
#endif
namespace bytedance::bolt {

using namespace std::chrono;

#ifndef NDEBUG
using common::testutil::ScopedTestTime;
#endif

#ifndef NDEBUG

uint64_t getCurrentTimeSec() {
  return ScopedTestTime::getCurrentTestTimeSec().value_or(
      duration_cast<seconds>(system_clock::now().time_since_epoch()).count());
}

uint64_t getCurrentTimeMs() {
  return ScopedTestTime::getCurrentTestTimeMs().value_or(
      duration_cast<milliseconds>(system_clock::now().time_since_epoch())
          .count());
}

uint64_t getCurrentTimeMicro() {
  return ScopedTestTime::getCurrentTestTimeMicro().value_or(
      duration_cast<microseconds>(system_clock::now().time_since_epoch())
          .count());
}

uint64_t getCurrentTimeNano() {
  return ScopedTestTime::getCurrentTestTimeNano().value_or(
      duration_cast<nanoseconds>(system_clock::now().time_since_epoch())
          .count());
}
#else

uint64_t getCurrentTimeSec() {
  return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

uint64_t getCurrentTimeMs() {
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch())
      .count();
}

uint64_t getCurrentTimeMicro() {
  return duration_cast<microseconds>(system_clock::now().time_since_epoch())
      .count();
}

uint64_t getCurrentTimeNano() {
  return duration_cast<nanoseconds>(system_clock::now().time_since_epoch())
      .count();
}
#endif

} // namespace bytedance::bolt
