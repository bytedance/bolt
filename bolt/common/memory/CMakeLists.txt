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

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
  add_subdirectory(sparksql/tests)
endif()

add_library(
  bolt_memory
  Allocation.cpp
  AllocationPool.cpp
  ArbitrationOperation.cpp
  ArbitrationParticipant.cpp
  ByteStream.cpp
  HashStringAllocator.cpp
  MallocAllocator.cpp
  Memory.cpp
  MemoryAllocationListener.cpp
  MemoryAllocator.cpp
  MemoryArbitrator.cpp
  MemoryPool.cpp
  MemoryPoolForGluten.cpp
  MemoryUtils.cpp
  MmapAllocator.cpp
  MmapArena.cpp
  RawVector.cpp
  SharedArbitrator.cpp
  sparksql/AllocationListener.cpp
  sparksql/ExecutionMemoryPool.cpp
  sparksql/MemoryConsumer.cpp
  sparksql/MemoryTarget.cpp
  sparksql/MemoryUsageStats.cpp
  sparksql/NativeMemoryManagerFactory.cpp
  sparksql/OomPrinter.cpp
  sparksql/Spiller.cpp
  sparksql/TaskMemoryManager.cpp
  StreamArena.cpp
  # file for sparksql
)

if(NOT APPLE)
  include_directories(${JEMALLOC_INCLUDE_DIRS})
endif()

find_package(GTest CONFIG REQUIRED)
find_package(Arrow CONFIG REQUIRED)

target_link_libraries(
  bolt_memory
  PUBLIC bolt_common_base
         bolt_common_config
         bolt_exception
         bolt_flag_definitions
         bolt_time
         bolt_type_base
         ${FOLLY_WITH_DEPENDENCIES}
         ${FMT}
         gflags::gflags
         glog::glog
         arrow::arrow
  PRIVATE bolt_test_util re2::re2
)
