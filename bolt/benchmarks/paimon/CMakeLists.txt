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

add_library(bolt_paimon_benchmark_lib PaimonBenchmark.cpp)

target_link_libraries(
  bolt_paimon_benchmark_lib
  bolt_query_benchmark
  bolt_aggregates
  bolt_exec
  bolt_exec_test_lib
  bolt_dwio_common
  bolt_dwio_common_exception
  bolt_dwio_parquet_reader
  bolt_dwio_common_test_utils
  bolt_hive_connector
  bolt_exception
  bolt_memory
  bolt_process
  bolt_serialization
  bolt_encode
  bolt_type
  bolt_type_fbhive
  bolt_caching
  bolt_vector_test_lib
  bolt_test_util_gperf
  Folly::folly
  ${FOLLY_BENCHMARK}
  fmt::fmt)

add_executable(bolt_paimon_benchmark PaimonBenchmarkMain.cpp)

target_link_libraries(bolt_paimon_benchmark bolt_paimon_benchmark_lib)
