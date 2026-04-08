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

bolt_add_library(
  bolt_functions_iceberg_hash
  Murmur3Hash32.cpp
)

target_link_libraries(
  bolt_functions_iceberg_hash
  bolt_common_base
  bolt_functions_util
)

bolt_add_library(
  bolt_functions_iceberg
  BucketFunction.cpp
  DateTimeFunctions.cpp
  Register.cpp
  Truncate.cpp
)

target_link_libraries(
  bolt_functions_iceberg
  bolt_functions_iceberg_hash
  bolt_functions_lib
  Folly::folly
)

if(NOT BOLT_MONO_LIBRARY)
  set_property(
    TARGET bolt_functions_iceberg
    PROPERTY JOB_POOL_COMPILE high_memory_pool
  )
endif()

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
