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

if(${BOLT_ENABLE_METRICS})
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
  )
else()
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
  )
endif()

add_test(bolt_base_test bolt_base_test)

target_link_libraries(
  bolt_base_test
  PRIVATE bolt_common_base
          bolt_time
          bolt_status
          bolt_exception
          bolt_temp_path
          bolt_type
          Boost::filesystem
          Boost::headers
          Folly::folly
          fmt::fmt
          gflags::gflags
          GTest::gtest
          GTest::gmock
          GTest::gtest_main
)

add_executable(bolt_id_map_test IdMapTest.cpp)

add_test(bolt_id_map_test bolt_id_map_test)

target_link_libraries(
  bolt_id_map_test
  bolt_id_map
  bolt_common_base
  bolt_memory
  Boost::headers
  gflags::gflags
  glog::glog
  GTest::gtest
  GTest::gtest_main
  pthread
)

add_executable(bolt_memcpy_meter Memcpy.cpp)
target_link_libraries(
  bolt_memcpy_meter PRIVATE bolt_common_base bolt_exception bolt_time
                            bolt_process Folly::folly gflags::gflags
)
