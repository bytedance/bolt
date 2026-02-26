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

set(BOLT_BENCHMARK_DEPS
    bolt_vector_fuzzer
    bolt_testutils
    ${FOLLY_BENCHMARK}
)

add_executable(bolt_memory_alloc_benchmark MemoryAllocationBenchmark.cpp)
add_executable(bolt_memory_alloc_contiguous_benchmark AllocateContiguousBenchmark.cpp)
add_executable(bolt_memory_realloc_benchmark ReallocateBenchmark.cpp)
add_executable(bolt_memory_realloc_contiguous_benchmark ReallocateContiguousBenchmark.cpp)

target_link_libraries(bolt_memory_alloc_benchmark ${BOLT_BENCHMARK_DEPS} bolt_memory pthread)
target_link_libraries(
  bolt_memory_alloc_contiguous_benchmark ${BOLT_BENCHMARK_DEPS} bolt_memory pthread
)
target_link_libraries(bolt_memory_realloc_benchmark ${BOLT_BENCHMARK_DEPS} bolt_memory pthread)

target_link_libraries(
  bolt_memory_realloc_contiguous_benchmark ${BOLT_BENCHMARK_DEPS} bolt_memory pthread
)
