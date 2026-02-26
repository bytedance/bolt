# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
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

find_package(Thrift CONFIG REQUIRED)

find_program(THRIFT_COMPILER thrift REQUIRED)

set(THRIFT_FILE "${CMAKE_CURRENT_SOURCE_DIR}/parquet.thrift")

# Specify the directory where the generated files will be placed
set(GENERATED_DIR "${CMAKE_CURRENT_SOURCE_DIR}/codegen/")

# Ensure the directory exists
file(MAKE_DIRECTORY ${GENERATED_DIR})

# Make the GENERATED_DIR variable available globally
set(GENERATED_H_FILE "${GENERATED_DIR}/parquet_types.h")
set(GENERATED_CPP_FILE "${GENERATED_DIR}/parquet_types.cpp")

# Output a status message about the Thrift compilation
message(STATUS "Running Thrift compiler for ${THRIFT_FILE}")

add_custom_command(
  OUTPUT ${GENERATED_H_FILE} ${GENERATED_CPP_FILE}
  COMMAND ${THRIFT_COMPILER} --gen cpp -out ${GENERATED_DIR} ${THRIFT_FILE}
  DEPENDS ${THRIFT_FILE}
  COMMENT "Generating Thrift files from ${THRIFT_FILE}"
  VERBATIM
)

add_custom_target(
  generate_parquet_thrift
  DEPENDS ${GENERATED_H_FILE} ${GENERATED_CPP_FILE}
  COMMENT "Generating Parquet Thrift files"
)

bolt_add_library(bolt_dwio_parquet_thrift ${GENERATED_CPP_FILE})
add_dependencies(bolt_dwio_parquet_thrift generate_parquet_thrift)
target_include_directories(bolt_dwio_parquet_thrift PUBLIC ${GENERATED_DIR})

target_link_libraries(bolt_dwio_parquet_thrift arrow::arrow thrift::thrift fmt::fmt Boost::headers)
