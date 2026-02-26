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

bolt_add_library(
  bolt_thrustjit ThrustJIT.cpp
                 RowContainer/RowContainerCodeGenerator.cpp
                 RowContainer/RowEqVectorsCodeGenerator.cpp
)

target_link_libraries(bolt_thrustjit PUBLIC llvm-core::llvm-core date::date fmt::fmt Folly::folly)

target_compile_options(
  bolt_thrustjit PRIVATE $<$<COMPILE_LANG_AND_ID:CXX,Clang,GNU>:-Werror=return-type>
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
