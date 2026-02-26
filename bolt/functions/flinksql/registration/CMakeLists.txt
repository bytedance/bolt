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

find_package(simdjson CONFIG REQUIRED)

bolt_add_library(bolt_functions_flink Register.cpp)

# GCC 12 has a bug where it does not respect "pragma ignore" directives and ends up failing
# compilation in an openssl header included by a hash-related function.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(bolt_functions_flink PRIVATE -Wno-deprecated-declarations)
endif()

target_link_libraries(bolt_functions_flink bolt_functions_flink_impl simdjson::simdjson)

set_property(TARGET bolt_functions_flink PROPERTY JOB_POOL_COMPILE high_memory_pool)
