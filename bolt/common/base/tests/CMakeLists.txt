# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# --------------------------------------------------------------------------
# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0
#
# This file has been modified by ByteDance Ltd. and/or its affiliates on
# 2025-11-11.
#
# Original file was released under the Apache License 2.0,
# with the full license text available at:
#     http://www.apache.org/licenses/LICENSE-2.0
#
# This modified file is released under the same license.
# --------------------------------------------------------------------------


add_executable(
  bolt_base_test
  AsyncSourceTest.cpp
  BitUtilTest.cpp
  BloomFilterTest.cpp
  CoalesceIoTest.cpp
  ConcurrentCounterTest.cpp
  ExceptionTest.cpp
  FsTest.cpp
  RangeTest.cpp
  RuntimeMetricsTest.cpp
  ScopedLockTest.cpp
  SemaphoreTest.cpp
  SimdUtilTest.cpp
  SpillConfigTest.cpp
  SpillStatsTest.cpp
  StatsReporterTest.cpp
  StatusTest.cpp
  SuccinctPrinterTest.cpp
  IdMapTest.cpp
)

add_test(bolt_base_test bolt_base_test)

target_link_libraries(
  bolt_base_test
  PRIVATE bolt_testutils
          bolt_common_base
          GTest::gtest
          GTest::gmock
          GTest::gtest_main
)

add_executable(bolt_memcpy_meter MemcpyTest.cpp)
target_link_libraries(
  bolt_memcpy_meter PRIVATE
  bolt_testutils
  GTest::gtest
  GTest::gtest_main
  pthread
)
