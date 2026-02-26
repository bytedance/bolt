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

include(GoogleTest)

add_executable(
  bolt_memory_test
  AllocationPoolTest.cpp
  AllocationTest.cpp
  ArbitrationParticipantTest.cpp
  ByteStreamTest.cpp
  CompactDoubleListTest.cpp
  HashStringAllocatorTest.cpp
  MemoryAllocatorTest.cpp
  MemoryArbitratorTest.cpp
  MemoryCapExceededTest.cpp
  MemoryManagerTest.cpp
  MemoryPoolTest.cpp
  MockSharedArbitratorTest.cpp
  # MemoryAllocationListenerTest.cpp
  RawVectorTest.cpp
  ScratchTest.cpp
  SharedArbitratorTest.cpp
  StreamArenaTest.cpp
)

target_compile_features(bolt_memory_test PRIVATE cxx_std_20)

if(NOT APPLE)
  find_package(jemalloc CONFIG REQUIRED)
endif()

target_link_libraries(
  bolt_memory_test
  PRIVATE bolt_testutils
          GTest::gmock
          GTest::gtest
          GTest::gtest_main
          $<$<NOT:$<PLATFORM_ID:Darwin>>:jemalloc::jemalloc>
          pthread
          dl
)

gtest_add_tests(bolt_memory_test "" AUTO)

add_executable(bolt_fragmentation_benchmark FragmentationBenchmark.cpp)

target_link_libraries(
  bolt_fragmentation_benchmark
  PRIVATE bolt_testutils
          $<$<NOT:$<PLATFORM_ID:Darwin>>:jemalloc::jemalloc>
          gflags::gflags glog::glog dl
)

add_executable(bolt_concurrent_allocation_benchmark ConcurrentAllocationBenchmark.cpp)

target_link_libraries(bolt_concurrent_allocation_benchmark PRIVATE bolt_testutils)
