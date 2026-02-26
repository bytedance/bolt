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

add_executable(bolt_cudf_benchmark Main.cpp)

target_link_libraries(
  bolt_cudf_benchmark PRIVATE bolt_tpch_gen bolt_testutils ${FOLLY_BENCHMARK}
)

# This linker option eliminates the following warning. /usr/bin/ld: warning: XXX.o: missing
# .note.GNU-stack section implies executable stack /usr/bin/ld: NOTE: This behaviour is deprecated
# and will be removed in a future version of the linker
target_link_options(bolt_cudf_benchmark PRIVATE -z execstack)

add_test(NAME bolt_cudf_orderby_benchmark_test COMMAND bolt_cudf_benchmark -scaleFactor 1 -opType
                                                       orderby
)

add_test(NAME bolt_cudf_aggregation_benchmark_test COMMAND bolt_cudf_benchmark -scaleFactor 1
                                                           -opType aggregation
)
