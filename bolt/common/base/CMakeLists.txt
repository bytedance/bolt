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

bolt_add_library(bolt_exception BoltException.cpp Exceptions.cpp Exceptions.h)
target_link_libraries(
  bolt_exception PUBLIC bolt_flag_definitions bolt_process Folly::folly fmt::fmt gflags::gflags
                        glog::glog
)

bolt_add_library(
  bolt_common_base
  BitUtil.cpp
  BloomFilter.cpp
  BoltLibSignalHandler.cpp
  Counters.cpp
  Fs.cpp
  Md5.cpp
  RandomUtil.cpp
  RuntimeMetrics.cpp
  SimdUtil.cpp
  SpillConfig.cpp
  SpillStats.cpp
  StatsReporter.cpp
  SuccinctPrinter.cpp
  Uuid.cpp
)

find_package(ryu CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)

add_compile_definitions(FOLLY_HAVE_INT128_T=1)
target_link_libraries(
  bolt_common_base
  PUBLIC bolt_exception
         Folly::folly
         fmt::fmt
         xsimd::xsimd
         cityhash::cityhash
         xxHash::xxhash
         ryu::ryu
         date::date
         openssl::openssl
  PRIVATE bolt_common_compression
          bolt_process
          glog::glog
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()

bolt_add_library(bolt_id_map BigintIdMap.cpp)
target_link_libraries(
  bolt_id_map
  bolt_memory
  bolt_flag_definitions
  bolt_process
  glog::glog
  Folly::folly
  ${FMT}
  gflags::gflags
)

bolt_add_library(bolt_status Status.cpp)
target_link_libraries(
  bolt_status
  PUBLIC fmt::fmt Folly::folly
  PRIVATE glog::glog
)
