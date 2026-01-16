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

add_executable(simple_lru_cache_test SimpleLRUCacheTest.cpp)
add_test(simple_lru_cache_test simple_lru_cache_test)
target_link_libraries(
  simple_lru_cache_test
  PRIVATE ${FOLLY_WITH_DEPENDENCIES} glog::glog bolt_process
          GTest::gtest GTest::gtest_main
)

add_executable(
  bolt_cache_test AsyncDataCacheTest.cpp CacheTTLControllerTest.cpp SsdFileTest.cpp
                  SsdFileTrackerTest.cpp StringIdMapTest.cpp
)
add_test(bolt_cache_test bolt_cache_test)
target_link_libraries(
  bolt_cache_test
  PRIVATE bolt_caching
          bolt_file
          bolt_memory
          bolt_temp_path
          ${FOLLY_WITH_DEPENDENCIES}
          bolt_process
          glog::glog
          GTest::gtest
          GTest::gtest_main
)

add_executable(cached_factory_test CachedFactoryTest.cpp)
add_test(cached_factory_test cached_factory_test)
target_link_libraries(
  cached_factory_test PRIVATE ${FOLLY_WITH_DEPENDENCIES} bolt_process glog::glog GTest::gtest
                              GTest::gtest_main
)
