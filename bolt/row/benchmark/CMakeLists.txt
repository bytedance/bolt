#
# Copyright (c) ByteDance Ltd. and/or its affiliates
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

add_executable(unsafe_row_serialize_benchmark UnsafeRowSerializeBenchmark.cpp)
add_executable(dense_row_serialize_benchmark DenseRowSerializeBenchmark.cpp)

target_link_libraries(
  unsafe_row_serialize_benchmark
  bolt_row_fast
  bolt_vector_fuzzer
  bolt_testutils
  ${FOLLY_BENCHMARK}
  GTest::gtest
)

target_link_libraries(
  dense_row_serialize_benchmark
  bolt_row_fast
  bolt_vector_fuzzer
  bolt_testutils
  ${FOLLY_BENCHMARK}
  GTest::gtest
)
