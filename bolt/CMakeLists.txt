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
set(BOLT_EXTENSIONS_OBJECT_TARGETS "" CACHE INTERNAL "")
set(BOLT_TESTUTILS_OBJECT_TARGETS "" CACHE INTERNAL "")

# Test targets are defined throughout the source tree, before the concrete
# engine and testutils variants are assembled below.  Create the stable test
# entry points up front so CMake records target dependencies (rather than
# treating their names as plain linker items).
if(BOLT_BUILD_TEST_UTILS)
  add_library(bolt_engine_for_tests INTERFACE)
  add_library(bolt_testutils INTERFACE)
endif()

function(bolt_add_library target_name)
  set(
    options
    OBJECT
    STATIC
    SHARED
    INTERFACE
  )
  set(oneValueArgs)
  set(multiValueArgs)
  cmake_parse_arguments(BOLT "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  list(REMOVE_ITEM ARGN OBJECT)
  list(REMOVE_ITEM ARGN STATIC)
  list(REMOVE_ITEM ARGN SHARED)
  list(REMOVE_ITEM ARGN INTERFACE)

  if(BOLT_STATIC OR BOLT_SHARED)
    message(
      FATAL_ERROR
      "Do NOT specify shared/static type, it will be decided automatically by conan"
    )
  endif()

  # Get current directory relative path
  get_filename_component(CURRENT_DIR_ABSOLUTE ${CMAKE_CURRENT_LIST_DIR} ABSOLUTE)
  get_filename_component(SOURCE_DIR_ABSOLUTE ${CMAKE_SOURCE_DIR} ABSOLUTE)
  file(RELATIVE_PATH CURRENT_RELATIVE_PATH ${SOURCE_DIR_ABSOLUTE}/bolt ${CURRENT_DIR_ABSOLUTE})

  string(REPLACE "/" ";" PATH_SEGMENTS "${CURRENT_RELATIVE_PATH}")
  list(GET PATH_SEGMENTS 0 FIRST_LEVEL_DIR)

  # these are test utils are scattered in different directories
  # TODO: Move these targets to bolt_testutils subdirectory, and adjust the header files accordingly.
  set(
    TESTUTIL_PATH_LIST
    "common/file/tests"
    "vector/fuzzer"
    "vector/tests/utils"
    "expression/tests"
    "expression/fuzzer"
    "parse"
    "dwio/common/tests/Lemire/FastPFor"
    "dwio/common/tests/utils"
    "dwio/common/fuzzer"
    "dwio/dwrf/test/utils"
    "dwio/parquet/arrow/tests"
    "tpch/gen/dbgen"
    "tpch/gen"
    "functions/lib/aggregates/tests/utils"
    "functions/lib/window/tests"
    "functions/prestosql/tests/utils"
    "connectors/fuzzer"
    "connectors/tpch"
    "cudf/tests"
    "exec/fuzzer"
    "exec/tests/utils"
    "exec/prefixsort/tests/utils"
    "shuffle/sparksql/tests"
    "duckdb/conversion"
  )

  set(
    BASE_COMP_LIST
    "buffer"
    "core"
    "type"
    "common"
    "vector"
    "row"
    "serializers"
    "version"
  )

  set(
    EXECUTION_COMP_LIST
    "exec"
    "jit"
    "expression"
  )

  set(FUNCTIONS_COMP_LIST "functions")

  set(
    IO_COMP_LIST
    "connectors"
    "dwio"
    "external"
  )

  set(SUBSTRAIT_COMP_LIST "substrait")

  set(SHUFFLE_COMP_LIST "shuffle")

  set(
    EXTENSIONS_COMP_LIST
    "torch"
    "cudf"
    "python"
  )

  if(BOLT_INTERFACE)
    add_library(${target_name} INTERFACE ${ARGN})
    return()
  endif()

  # OBJECT libraries are used to aggregate source files without creating a library file.
  add_library(${target_name} OBJECT ${ARGN})

  if(CURRENT_RELATIVE_PATH IN_LIST TESTUTIL_PATH_LIST)
    # Note, this if-branch should be the first to match to filter utils/fuzzer.
    # TODO: Move these targets to bolt_testutils subdirectory?
    list(APPEND BOLT_TESTUTILS_OBJECT_TARGETS ${target_name})
    set(BOLT_TESTUTILS_OBJECT_TARGETS ${BOLT_TESTUTILS_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()

  if(FIRST_LEVEL_DIR IN_LIST BASE_COMP_LIST)
    list(APPEND BOLT_BASE_OBJECT_TARGETS ${target_name})
    set(BOLT_BASE_OBJECT_TARGETS ${BOLT_BASE_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()

  if(FIRST_LEVEL_DIR IN_LIST EXECUTION_COMP_LIST)
    list(APPEND BOLT_EXECUTION_OBJECT_TARGETS ${target_name})
    set(BOLT_EXECUTION_OBJECT_TARGETS ${BOLT_EXECUTION_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()

  if(FIRST_LEVEL_DIR IN_LIST FUNCTIONS_COMP_LIST)
    list(APPEND BOLT_FUNCTIONS_OBJECT_TARGETS ${target_name})
    set(BOLT_FUNCTIONS_OBJECT_TARGETS ${BOLT_FUNCTIONS_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()

  if(FIRST_LEVEL_DIR IN_LIST IO_COMP_LIST)
    list(APPEND BOLT_IO_OBJECT_TARGETS ${target_name})
    set(BOLT_IO_OBJECT_TARGETS ${BOLT_IO_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()

  if(FIRST_LEVEL_DIR IN_LIST SUBSTRAIT_COMP_LIST)
    list(APPEND BOLT_SUBSTRAIT_OBJECT_TARGETS ${target_name})
    set(BOLT_SUBSTRAIT_OBJECT_TARGETS ${BOLT_SUBSTRAIT_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()

  if(FIRST_LEVEL_DIR IN_LIST SHUFFLE_COMP_LIST)
    list(APPEND BOLT_SHUFFLE_OBJECT_TARGETS ${target_name})
    set(BOLT_SHUFFLE_OBJECT_TARGETS ${BOLT_SHUFFLE_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()

  if(FIRST_LEVEL_DIR IN_LIST EXTENSIONS_COMP_LIST)
    list(APPEND BOLT_EXTENSIONS_OBJECT_TARGETS ${target_name})
    set(BOLT_EXTENSIONS_OBJECT_TARGETS ${BOLT_EXTENSIONS_OBJECT_TARGETS} CACHE INTERNAL "")
    return()
  endif()

  message(FATAL_ERROR "New component ${target_name} in ${CURRENT_RELATIVE_PATH}?")
endfunction()

find_package(libbacktrace REQUIRED CONFIG)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  find_package(libunwind REQUIRED CONFIG)
endif()

add_subdirectory(buffer)
add_subdirectory(common)
add_subdirectory(core)
add_subdirectory(serializers)
add_subdirectory(type)
add_subdirectory(vector)
add_subdirectory(row)
add_subdirectory(version)
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
if(${BOLT_ENABLE_PAIMON})
  find_package(Paimon CONFIG REQUIRED)
endif()
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

set(
  BOLT_ENGINE_OBJECT_TARGETS
  ${BOLT_FUNCTIONS_OBJECT_TARGETS}
  ${BOLT_EXECUTION_OBJECT_TARGETS}
  ${BOLT_BASE_OBJECT_TARGETS}
  ${BOLT_IO_OBJECT_TARGETS}
  ${BOLT_SUBSTRAIT_OBJECT_TARGETS}
  ${BOLT_SHUFFLE_OBJECT_TARGETS}
  ${BOLT_EXTENSIONS_OBJECT_TARGETS}
)

function(bolt_configure_engine target_name)
  target_link_libraries(
    ${target_name}
    PUBLIC
      $<$<CONFIG:Debug>:bolt_debug_util>
      arrow::arrow
      $<$<BOOL:${ENABLE_BOLT_JIT}>:llvm-core::llvm-core>
      Folly::folly
      gflags::gflags
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
      $<$<BOOL:${BOLT_BUILD_TEST_UTILS}>:cpr::cpr>
      $<$<BOOL:${BOLT_ENABLE_S3}>:aws-sdk-cpp::aws-sdk-cpp>
      $<$<BOOL:${BOLT_ENABLE_GCS}>:google-cloud-cpp::storage>
      $<$<BOOL:${BOLT_ENABLE_ABFS}>:Azure::azure-storage-blobs
      Azure::azure-storage-files-datalake
      Azure::azure-identity>
  )

  if(BOLT_ENABLE_SPARK_COMPATIBLE)
    get_target_property(bolt_engine_type ${target_name} TYPE)
    if(bolt_engine_type STREQUAL "SHARED_LIBRARY")
      # Celeborn defines process-wide gflags. Keep one complete copy in the
      # shared engine instead of pulling its static archives into each DSO or
      # test executable.
      get_target_property(
        bolt_celeborn_libraries
        celeborn-cpp-client::celeborn-cpp-client
        INTERFACE_LINK_LIBRARIES
      )
      target_link_libraries(
        ${target_name}
        PRIVATE
          "$<LINK_LIBRARY:WHOLE_ARCHIVE,${bolt_celeborn_libraries}>"
      )
    else()
      target_link_libraries(${target_name} PRIVATE celeborn-cpp-client::celeborn-cpp-client)
    endif()
  endif()

  if(${BOLT_ENABLE_TORCH})
    target_link_libraries(${target_name} PUBLIC ${TORCH_LIBRARIES})
    target_compile_definitions(${target_name} PUBLIC BOLT_HAS_TORCH=1)
  endif()

  if(${BOLT_BUILD_PYTHON_PACKAGE})
    target_link_libraries(${target_name} PUBLIC pybind11::pybind11)
    target_compile_definitions(${target_name} PUBLIC BOLT_HAS_PYTHON=1)
  endif()

  if(${BOLT_ENABLE_CUDF})
    target_link_libraries(${target_name} PUBLIC cudf::cudf)
    target_compile_definitions(
      ${target_name}
      PUBLIC
        BOLT_HAS_CUDF=1
        LIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE
    )
  endif()

  if(${BOLT_ENABLE_PAIMON})
    get_target_property(BOLT_ENGINE_TYPE ${target_name} TYPE)
    if(BOLT_ENGINE_TYPE STREQUAL "SHARED_LIBRARY")
      set(PAIMON_ENGINE_LINK_SCOPE PRIVATE)
    else()
      set(PAIMON_ENGINE_LINK_SCOPE PUBLIC)
    endif()
    target_link_libraries(
      ${target_name}
      PUBLIC
        Paimon::core
        ${PAIMON_ENGINE_LINK_SCOPE}
        $<LINK_LIBRARY:WHOLE_ARCHIVE,Paimon::format_avro>
        $<LINK_LIBRARY:WHOLE_ARCHIVE,Paimon::fs_local>
      PRIVATE
        Paimon::libavrocpp
    )
  endif()

  target_link_libraries(${target_name} PUBLIC libbacktrace::libbacktrace PRIVATE ${CMAKE_DL_LIBS})
  if(CMAKE_SYSTEM_NAME STREQUAL "Linux" OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    target_link_libraries(${target_name} PUBLIC libunwind::libunwind)
  endif()
endfunction()

set(BOLT_BUILD_STATIC_VARIANT OFF)
set(BOLT_BUILD_SHARED_VARIANT OFF)
if(BUILD_SHARED_LIBS)
  set(BOLT_BUILD_SHARED_VARIANT ON)
else()
  set(BOLT_BUILD_STATIC_VARIANT ON)
endif()
if(BOLT_BUILD_TEST_UTILS)
  if(BOLT_TEST_LINKAGE STREQUAL "shared")
    set(BOLT_BUILD_SHARED_VARIANT ON)
  else()
    set(BOLT_BUILD_STATIC_VARIANT ON)
  endif()
endif()

if(BOLT_BUILD_STATIC_VARIANT)
  add_library(bolt_engine STATIC)
  target_link_libraries(bolt_engine PUBLIC ${BOLT_ENGINE_OBJECT_TARGETS})
  bolt_configure_engine(bolt_engine)
endif()
if(BOLT_BUILD_SHARED_VARIANT)
  add_library(bolt_engine_shared SHARED)
  target_link_libraries(bolt_engine_shared PUBLIC ${BOLT_ENGINE_OBJECT_TARGETS})
  set_target_properties(
    bolt_engine_shared
    PROPERTIES
      OUTPUT_NAME
        bolt_engine
  )
  bolt_configure_engine(bolt_engine_shared)
endif()

function(bolt_configure_testutils target_name engine_target)
  target_link_libraries(
    ${target_name}
    PUBLIC
      ${engine_target}
      duckdb::duckdb
      gflags::gflags
      GTest::gtest
  )
  if(BOLT_ENABLE_SPARK_COMPATIBLE)
    get_target_property(bolt_test_engine_type ${engine_target} TYPE)
    if(bolt_test_engine_type STREQUAL "STATIC_LIBRARY")
      target_link_libraries(${target_name} PUBLIC celeborn-cpp-client::celeborn-cpp-client)
    endif()
  endif()
  target_include_directories(${target_name} PUBLIC ${CMAKE_BINARY_DIR})
endfunction()

if(BOLT_BUILD_TEST_UTILS)
  if(BOLT_BUILD_STATIC_VARIANT)
    add_library(bolt_testutils_static STATIC)
    target_link_libraries(bolt_testutils_static PUBLIC ${BOLT_TESTUTILS_OBJECT_TARGETS})
    set_target_properties(
      bolt_testutils_static
      PROPERTIES
        OUTPUT_NAME
          bolt_testutils
    )
    bolt_configure_testutils(bolt_testutils_static bolt_engine)
  endif()
  if(BOLT_BUILD_SHARED_VARIANT)
    add_library(bolt_testutils_shared SHARED)
    target_link_libraries(bolt_testutils_shared PUBLIC ${BOLT_TESTUTILS_OBJECT_TARGETS})
    set_target_properties(
      bolt_testutils_shared
      PROPERTIES
        OUTPUT_NAME
          bolt_testutils
    )
    bolt_configure_testutils(bolt_testutils_shared bolt_engine_shared)
  endif()

  if(BOLT_TEST_LINKAGE STREQUAL "shared")
    set(BOLT_TEST_ENGINE_TARGET bolt_engine_shared)
    set(BOLT_TESTUTILS_TARGET bolt_testutils_shared)
  else()
    set(BOLT_TEST_ENGINE_TARGET bolt_engine)
    set(BOLT_TESTUTILS_TARGET bolt_testutils_static)
  endif()
  if(BOLT_NEEDS_SEPARATE_TEST_RUNTIME)
    set_property(
      TARGET
        ${BOLT_TEST_ENGINE_TARGET}
        ${BOLT_TESTUTILS_TARGET}
      PROPERTY
        BOLT_USE_SEPARATE_TEST_RUNTIME
          ON
    )
  endif()
  target_link_libraries(bolt_engine_for_tests INTERFACE ${BOLT_TEST_ENGINE_TARGET})
  target_link_libraries(bolt_testutils INTERFACE ${BOLT_TESTUTILS_TARGET})
endif()

if(BUILD_SHARED_LIBS)
  set(BOLT_INSTALL_TARGETS bolt_engine_shared)
  if(BOLT_BUILD_TEST_UTILS)
    list(APPEND BOLT_INSTALL_TARGETS bolt_testutils_shared)
  endif()
else()
  set(BOLT_INSTALL_TARGETS bolt_engine)
  if(BOLT_BUILD_TEST_UTILS)
    list(APPEND BOLT_INSTALL_TARGETS bolt_testutils_static)
  endif()
endif()

install(
  TARGETS
    ${BOLT_INSTALL_TARGETS}
  ARCHIVE
    DESTINATION lib
  LIBRARY
    DESTINATION lib
  RUNTIME
    DESTINATION bin
)
