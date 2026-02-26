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


set(BOLT_BASE_OBJECT_TARGETS "" CACHE INTERNAL "")
set(BOLT_EXECUTION_OBJECT_TARGETS "" CACHE INTERNAL "")
set(BOLT_FUNCTIONS_OBJECT_TARGETS "" CACHE INTERNAL "")
set(BOLT_IO_OBJECT_TARGETS "" CACHE INTERNAL "")
set(BOLT_SUBSTRAIT_OBJECT_TARGETS "" CACHE INTERNAL "")
set(BOLT_SHUFFLE_OBJECT_TARGETS "" CACHE INTERNAL "")
set(BOLT_TESTUTILS_OBJECT_TARGETS "" CACHE INTERNAL "")

function(bolt_add_library target_name)
  set(options OBJECT STATIC SHARED INTERFACE)
  set(oneValueArgs)
  set(multiValueArgs)
  cmake_parse_arguments(BOLT "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  list(REMOVE_ITEM ARGN OBJECT)
  list(REMOVE_ITEM ARGN STATIC)
  list(REMOVE_ITEM ARGN SHARED)
  list(REMOVE_ITEM ARGN INTERFACE)

  if(BOLT_STATIC OR BOLT_SHARED)
    message(FATAL_ERROR "Do NOT specify shared/static type, it will be decided automatically by conan")
  endif()

  # Get current directory relative path
  get_filename_component(CURRENT_DIR_ABSOLUTE ${CMAKE_CURRENT_LIST_DIR} ABSOLUTE)
  get_filename_component(SOURCE_DIR_ABSOLUTE ${CMAKE_SOURCE_DIR} ABSOLUTE)
  file(RELATIVE_PATH CURRENT_RELATIVE_PATH ${SOURCE_DIR_ABSOLUTE}/bolt ${CURRENT_DIR_ABSOLUTE})

  string(REPLACE "/" ";" PATH_SEGMENTS "${CURRENT_RELATIVE_PATH}")
  list(GET PATH_SEGMENTS 0 FIRST_LEVEL_DIR)

  # these are test utils are scattered in different directories
  # TODO: Move these targets to bolt_testutils subdirectory, and adjust the header files accordingly.
  set(TESTUTIL_PATH_LIST
      "common/file/tests"
      "vector/fuzzer"
      "vector/tests/utils"
      "expression/tests"
      "expression/fuzzer"
      "parse"
      "dwio/common/tests/utils"
      "dwio/common/fuzzer"
      "dwio/parquet/arrow/tests"
      "tpch/gen/dbgen"
      "tpch/gen"
      "functions/lib/aggregates/tests/utils"
      "functions/lib/window/tests"
      "functions/prestosql/tests/utils"
      "connectors/fuzzer"
      "connectors/tpch"
      "exec/fuzzer"
      "exec/tests/utils"
      "exec/prefixsort/tests/utils"
      "duckdb/conversion")

  set(BASE_COMP_LIST "buffer" "core" "type" "common" "vector" "row" "serializers" "version" "flag_definitions")

  set(EXECUTION_COMP_LIST "exec" "jit" "expression")

  set(FUNCTIONS_COMP_LIST "functions")

  set(IO_COMP_LIST "connectors" "dwio" "external")

  set(SUBSTRAIT_COMP_LIST "substrait")

  set(SHUFFLE_COMP_LIST "shuffle")

  set(EXTENSIONS_COMP_LIST "torch" "cudf" "python")

  if (BOLT_INTERFACE)
    add_library(${target_name} INTERFACE ${ARGN})
    return()
  endif()

  # OBJECT libraries are used to aggregate source files without creating a library file.
  add_library(${target_name} OBJECT ${ARGN})

  if(CURRENT_RELATIVE_PATH IN_LIST TESTUTIL_PATH_LIST)
    # Note, this if-branch should be the first to match to filter utils/fuzzer.
    # TODO: Move these targets to bolt_testutils subdirectory?
    list(APPEND BOLT_TESTUTILS_OBJECT_TARGETS $<TARGET_OBJECTS:${target_name}>)
    set(BOLT_TESTUTILS_OBJECT_TARGETS ${BOLT_TESTUTILS_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()


  if(FIRST_LEVEL_DIR IN_LIST BASE_COMP_LIST)
    list(APPEND BOLT_BASE_OBJECT_TARGETS $<TARGET_OBJECTS:${target_name}>)
    set(BOLT_BASE_OBJECT_TARGETS ${BOLT_BASE_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()

  if(FIRST_LEVEL_DIR IN_LIST EXECUTION_COMP_LIST)
    list(APPEND BOLT_EXECUTION_OBJECT_TARGETS $<TARGET_OBJECTS:${target_name}>)
    set(BOLT_EXECUTION_OBJECT_TARGETS ${BOLT_EXECUTION_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()

  if(FIRST_LEVEL_DIR IN_LIST FUNCTIONS_COMP_LIST)
    list(APPEND BOLT_FUNCTIONS_OBJECT_TARGETS $<TARGET_OBJECTS:${target_name}>)
    set(BOLT_FUNCTIONS_OBJECT_TARGETS ${BOLT_FUNCTIONS_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()

  if(FIRST_LEVEL_DIR IN_LIST IO_COMP_LIST)
    list(APPEND BOLT_IO_OBJECT_TARGETS $<TARGET_OBJECTS:${target_name}>)
    set(BOLT_IO_OBJECT_TARGETS ${BOLT_IO_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()

  if(FIRST_LEVEL_DIR IN_LIST SUBSTRAIT_COMP_LIST)
    list(APPEND BOLT_SUBSTRAIT_OBJECT_TARGETS $<TARGET_OBJECTS:${target_name}>)
    set(BOLT_SUBSTRAIT_OBJECT_TARGETS ${BOLT_SUBSTRAIT_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()

  if(FIRST_LEVEL_DIR IN_LIST SHUFFLE_COMP_LIST)
    list(APPEND BOLT_SHUFFLE_OBJECT_TARGETS $<TARGET_OBJECTS:${target_name}>)
    set(BOLT_SHUFFLE_OBJECT_TARGETS ${BOLT_SHUFFLE_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()

  if(FIRST_LEVEL_DIR IN_LIST EXTENSIONS_COMP_LIST)
    list(APPEND EXTENSIONS_OBJECT_TARGETS $<TARGET_OBJECTS:${target_name}>)
    set(EXTENSIONS_OBJECT_TARGETS ${EXTENSIONS_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()

  message(FATAL_ERROR "New component ${target_name} in ${CURRENT_RELATIVE_PATH}?")
endfunction()

add_subdirectory(buffer)
add_subdirectory(common)
add_subdirectory(core)
add_subdirectory(serializers)
add_subdirectory(type)
add_subdirectory(vector)
add_subdirectory(row)
add_subdirectory(version)
add_subdirectory(flag_definitions)

if(${BOLT_ENABLE_SPARK_COMPATIBLE})
  add_subdirectory(shuffle)
endif()

if(${ENABLE_BOLT_JIT})
  add_subdirectory(jit)
endif()

add_subdirectory(external/hdfs)
include_directories(SYSTEM external)

# examples depend on expression
if(${BOLT_ENABLE_EXAMPLES} AND ${BOLT_ENABLE_EXPRESSION})
  add_subdirectory(examples)
endif()

if(${BOLT_BUILD_BENCHMARKS} OR ${BOLT_BUILD_BENCHMARKS_BASIC})
  add_subdirectory(benchmarks)
endif()

if(${BOLT_ENABLE_EXPRESSION})
  add_subdirectory(expression)
endif()

if(${BOLT_ENABLE_PARSE})
  add_subdirectory(parse)
endif()

# hive connector depends on dwio
if(${BOLT_ENABLE_HIVE_CONNECTOR})
  add_subdirectory(dwio)
endif()

if(${BOLT_ENABLE_TPCH_CONNECTOR})
  add_subdirectory(tpch/gen)
endif()

add_subdirectory(functions) # depends on md5 (postgresql)
add_subdirectory(connectors)

if(${BOLT_ENABLE_EXEC})
  add_subdirectory(exec)
endif()

if(${BOLT_ENABLE_DUCKDB})
  add_subdirectory(duckdb)
endif()

# substrait converter
if(${BOLT_ENABLE_SUBSTRAIT})
  add_subdirectory(substrait)
endif()

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tool)
endif()

if(${BOLT_ENABLE_TORCH})
  add_subdirectory(torch)
endif()

if(${BOLT_ENABLE_CUDF})
  add_subdirectory(cudf)
endif()

if(${BOLT_BUILD_PYTHON_PACKAGE})
  add_subdirectory(python)
endif()

add_library(bolt_engine
  ${BOLT_FUNCTIONS_OBJECT_TARGETS}
  ${BOLT_EXECUTION_OBJECT_TARGETS}
  ${BOLT_BASE_OBJECT_TARGETS}
  ${BOLT_IO_OBJECT_TARGETS}
  ${BOLT_SUBSTRAIT_OBJECT_TARGETS}
  ${BOLT_SHUFFLE_OBJECT_TARGETS})

target_link_libraries(bolt_engine PUBLIC
                                  $<$<CONFIG:Debug>:bolt_debug_util>
                                  arrow::arrow
                                  llvm-core::llvm-core
                                  Folly::folly
                                  gfx::timsort
                                  date::date
                                  re2::re2
                                  ryu::ryu
                                  fmt::fmt
                                  thrift::thrift
                                  simdjson::simdjson
                                  sonic-cpp::sonic-cpp
                                  xxHash::xxhash
                                  cityhash::cityhash
                                  utf8proc::utf8proc
                                  icu::icu
                                  date::date
                                  roaring::roaring
                                  Boost::boost
                                  Boost::url
                                  cpr::cpr
                                  $<$<BOOL:${BOLT_ENABLE_SPARK_COMPATIBLE}>:celeborn-cpp-client::celeborn-cpp-client>
                                  $<$<BOOL:${BOLT_ENABLE_S3}>:aws-sdk-cpp::aws-sdk-cpp>
                                  CURL::libcurl
)

if(${BOLT_BUILD_TEST_UTILS})
  add_library(bolt_testutils ${BOLT_TESTUTILS_OBJECT_TARGETS})
  target_link_libraries(bolt_testutils PUBLIC
                                  bolt_engine
                                  $<$<CONFIG:Debug>:bolt_debug_util>
                                  duckdb::duckdb
                                  GTest::gtest
  )
  target_include_directories(bolt_testutils PUBLIC ${CMAKE_BINARY_DIR})
endif()
