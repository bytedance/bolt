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
endif()

bison_target(
  SignatureParser SignatureParser.yy ${CMAKE_CURRENT_BINARY_DIR}/SignatureParser.yy.cc
  DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/SignatureParser.yy.h
  COMPILE_FLAGS "-Werror -Wno-deprecated"
)

flex_target(
  SignatureParserScanner SignatureParser.ll ${CMAKE_CURRENT_BINARY_DIR}/Scanner.cpp
  COMPILE_FLAGS "-Cf --prefix=boltsp"
)

add_flex_bison_dependency(SignatureParserScanner SignatureParser)

include_directories(${PROJECT_BINARY_DIR})
include_directories(${FLEX_INCLUDE_DIRS})
bolt_add_library(
  bolt_signature_parser ${BISON_SignatureParser_OUTPUTS} ${FLEX_SignatureParserScanner_OUTPUTS}
                        ParseUtil.cpp
)
target_link_libraries(bolt_signature_parser bolt_common_base bolt_type_base)
