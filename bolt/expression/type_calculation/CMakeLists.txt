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

# find_package(BISON REQUIRED) find_package(FLEX REQUIRED) set(BISON_VARS BISON_FOUND
# BISON_EXECUTABLE BISON_VERSION )

bison_target(
  TypeCalculationParser TypeCalculation.yy ${CMAKE_CURRENT_BINARY_DIR}/TypeCalculation.yy.cc
  DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/TypeCalculation.yy.h
)

flex_target(
  TypeCalculationScanner TypeCalculation.ll ${CMAKE_CURRENT_BINARY_DIR}/Scanner.cpp
  COMPILE_FLAGS "-Cf --prefix=bolttc"
)

add_flex_bison_dependency(TypeCalculationScanner TypeCalculationParser)

include_directories(${PROJECT_BINARY_DIR})
include_directories(${FLEX_INCLUDE_DIRS})
bolt_add_library(
  bolt_type_calculation ${BISON_TypeCalculationParser_OUTPUTS}
                        ${FLEX_TypeCalculationScanner_OUTPUTS} Scanner.h TypeCalculation.h
)
target_link_libraries(bolt_type_calculation bolt_common_base)
