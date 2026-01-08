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
# Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

cmake_minimum_required(VERSION 3.14)

# The policy allows us to change options without caching.
cmake_policy(SET CMP0077 NEW)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)


if(POLICY CMP0177)
    cmake_policy(SET CMP0177 NEW)
    set(CMAKE_POLICY_WARNING_CMP0177 OFF)
endif()

set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS ON CACHE BOOL "Suppress developer warnings")

# Sets new behavior for CMP0135, which controls how timestamps are extracted
# when using ExternalProject_Add():
# https://cmake.org/cmake/help/latest/policy/CMP0135.html
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
  set(CMAKE_POLICY_DEFAULT_CMP0135 NEW)
endif()

# set the project name
project(bolt CXX)

# If we are in an active conda env disable search in system paths and add env to
# prefix path if(DEFINED ENV{CONDA_PREFIX}) if(NOT DEFINED
# ENV{BOLT_DEPENDENCY_SOURCE} OR "$ENV{BOLT_DEPENDENCY_SOURCE}" STREQUAL
# "CONDA") message(STATUS "Using Conda environment: $ENV{CONDA_PREFIX}")
# set(CMAKE_FIND_USE_SYSTEM_ENVIRONMENT_PATH FALSE) list(APPEND
# CMAKE_PREFIX_PATH "$ENV{CONDA_PREFIX}") # Override in case it was set to CONDA
# set(ENV{BOLT_DEPENDENCY_SOURCE} AUTO) elseif(DEFINED
# ENV{BOLT_DEPENDENCY_SOURCE} AND NOT "$ENV{BOLT_DEPENDENCY_SOURCE}" STREQUAL
# "CONDA") message(STATUS "Overriding Conda environment: $ENV{CONDA_PREFIX}")
# endif() endif()

# set platforms
if(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64|x86_64")
  set(ARCH_AMD64 1)
endif()
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*|arm64.*)")
  set(ARCH_AARCH64 1)
endif()
if(ARCH_AARCH64 OR CMAKE_SYSTEM_PROCESSOR MATCHES "arm")
  set(ARCH_ARM 1)
endif()
if(CMAKE_LIBRARY_ARCHITECTURE MATCHES "i386")
  set(ARCH_I386 1)
endif()
if((ARCH_ARM AND NOT ARCH_AARCH64) OR ARCH_I386)
  message(FATAL_ERROR "32bit platforms are not supported")
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(ppc64le.*|PPC64LE.*)")
  set(ARCH_PPC64LE 1)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  set(OS_LINUX 1)
  add_definitions(-D OS_LINUX)
elseif(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  set(OS_MACOSX 1)
  add_definitions(-D OS_MACOSX)
endif()

list(PREPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/CMake"
     "${PROJECT_SOURCE_DIR}/CMake/third-party")

# Include our ThirdPartyToolchain dependencies macros include(ResolveDependency)

# set_with_default(BOLT_DEPENDENCY_SOURCE_DEFAULT BOLT_DEPENDENCY_SOURCE AUTO)
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# Add all options below
option(
  BOLT_BUILD_TESTING
  "Enable Bolt tests. This will enable all other build options automatically."
  ON)
option(
  BOLT_BUILD_MINIMAL
  "Build a minimal set of components only. This will override other build options."
  OFF)
option(BOLT_BUILD_TESTING_WITH_COVERAGE
       "Enable Bolt tests with coverage statistics." OFF)
# option() always creates a BOOL variable so we have to use a normal cache
# variable with STRING type for this option.
#
# * AUTO: Try SYSTEM first fall back to BUNDLED.
# * SYSTEM: Use installed dependencies via find_package.
# * BUNDLED: Build dependencies from source. set(BOLT_DEPENDENCY_SOURCE
#   ${BOLT_DEPENDENCY_SOURCE_DEFAULT} CACHE STRING "Default source for all
#   dependencies with source builds enabled: AUTO SYSTEM BUNDLED." )

option(BOLT_ENABLE_ADDRESS_SANITIZER
       "Enable Address Sanitizer to check for memory related errors" OFF)
option(BOLT_ENABLE_DUCKDB "Build duckDB to enable differential testing." ON)
option(BOLT_ENABLE_EXEC "Build exec." ON)
option(BOLT_ENABLE_AGGREGATES "Build aggregates." ON)
option(BOLT_ENABLE_HIVE_CONNECTOR "Build Hive connector." ON)
option(BOLT_ENABLE_TPCH_CONNECTOR "Build TPC-H connector." ON)
option(BOLT_ENABLE_ARROW_CONNECTOR "Build Arrow Memory connector." ON)
option(BOLT_ENABLE_PRESTO_FUNCTIONS "Build Presto SQL functions." ON)
option(BOLT_ENABLE_SPARK_FUNCTIONS "Build Spark SQL functions." ON)
option(BOLT_ENABLE_FLINK_FUNCTIONS "Build Flink SQL functions." ON)
option(BOLT_ENABLE_SKETCH_FUNCTIONS "Build DataSketches SQL functions." OFF)
option(BOLT_ENABLE_COLOCATE_FUNCTIONS "Enable colocate function support" OFF)
option(ALLOW_REMOTE_COLOCATE_REGISTER
       "Enable remote colocate function register" OFF)
option(BOLT_ENABLE_EXPRESSION "Build expression." ON)
option(BOLT_ENABLE_PARSE "Build parser used for unit tests." ON)
option(BOLT_ENABLE_EXAMPLES
       "Build examples. This will enable BOLT_ENABLE_EXPRESSION automatically."
       OFF)
option(BOLT_ENABLE_SUBSTRAIT "Build Substrait-to-Bolt converter." OFF)
option(BOLT_ENABLE_BENCHMARKS "Enable Bolt top level benchmarks." OFF)
option(BOLT_ENABLE_BENCHMARKS_BASIC "Enable Bolt basic benchmarks." OFF)
option(BOLT_ENABLE_S3 "Build S3 Connector" OFF)
option(BOLT_ENABLE_GCS "Build GCS Connector" OFF)
option(BOLT_ENABLE_ABFS "Build Abfs Connector" OFF)
option(BOLT_ENABLE_HDFS "Build Hdfs Connector" ON)

option(BOLT_ENABLE_PARQUET "Enable Parquet support" OFF)
option(BOLT_ENABLE_ORC "Enable ORC support" OFF)
option(BOLT_ENABLE_TXT "Enable TXT read support" OFF)
option(BOLT_ENABLE_ARROW "Enable Arrow support" OFF)
option(BOLT_ENABLE_REMOTE_FUNCTIONS "Enable remote function support" OFF)
option(BOLT_ENABLE_CCACHE "Use ccache if installed." ON)

option(BOLT_BUILD_TEST_UTILS "Builds Bolt test utilities" OFF)
option(BOLT_BUILD_PYTHON_PACKAGE "Builds Bolt Python bindings" OFF)
option(BOLT_BUILD_BENCHMARKS "Builds Bolt benchmarks" OFF)
option(
  BOLT_ENABLE_INT64_BUILD_PARTITION_BOUND
  "make buildPartitionBounds_ a vector int64 instead of int32 to avoid integer overflow when the hashtable has billions of records"
  OFF)

option(BOLT_ENABLE_SPARK_COMPATIBLE "Enable spark compatible setting" OFF)
option(ENABLE_EXCEPTION_TRACE "Enable exception trace" ON)
option(BOLT_ON_TCE "Build Bolt on Bytedance TCE Environment" OFF)
option(BOLT_ENABLE_TBB "Enable oneTBB support" OFF)
option(BOLT_ENABLE_TORCH "Enable torch script support" OFF)
option(BOLT_ENABLE_CUDF "Enable cuDF support" OFF)

if (BOLT_ENABLE_SPARK_COMPATIBLE)
  set(SPARK_VERSION "3.5" CACHE STRING "Spark major.minor version")
  set_property(CACHE SPARK_VERSION PROPERTY STRINGS "3.2" "3.3" "3.4" "3.5")
  message("Build with SPARK_VERSION ${SPARK_VERSION}")
  string(REPLACE "." "_" SPARK_VERSION_ENUM ${SPARK_VERSION})
  set(BOLT_SPARK_VERSION "SPARK_${SPARK_VERSION_ENUM}")
else()
  set(BOLT_SPARK_VERSION "UNDEFINED")
endif()

# Enable address sanitizer, this needs to precede the addition of all targets to
# be enabled for all subdirectories
if(BOLT_ENABLE_ADDRESS_SANITIZER)
  # To ensure compatibility with folly, we add other sanitizers
  set(CMAKE_CXX_FLAGS
      "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -fsanitize=address")
  set(CMAKE_LINKER_FLAGS
      "${CMAKE_LINKER_FLAGS} -fno-omit-frame-pointer -fsanitize=address")
endif()

# Explicitly force compilers to generate colored output. Compilers usually do
# this by default if they detect the output is a terminal, but this assumption
# is broken if you use ninja.
option(BOLT_FORCE_COLORED_OUTPUT
       "Always produce ANSI-colored output (GNU/Clang only)." OFF)

option(KERNEL_SUPPORTS_IO_URING "IO_URING is supported by Linux kernel" ON)

option(BOLT_ON_TCE "Build Bolt on Bytedance TCE Environment" OFF)

if(${BOLT_ON_TCE})
  set(BOLT_ENABLE_METRICS ON)
endif()

# bytedance presto coordinator do not support crc
option(BOLT_ENABLE_CRC "Enable crc in presto serialize" OFF)
option(BOLT_ENABLE_PERF "Enable perf support" OFF)

if(${BOLT_ENABLE_CRC})
  add_definitions(-DBOLT_ENABLE_CRC)
endif()

if(${BOLT_BUILD_MINIMAL})
  # Enable and disable components for bolt base build
  set(BOLT_BUILD_TESTING OFF)
  set(BOLT_ENABLE_PRESTO_FUNCTIONS ON)
  set(BOLT_ENABLE_DUCKDB OFF)
  set(BOLT_ENABLE_EXPRESSION ON)
  set(BOLT_ENABLE_PARSE OFF)
  set(BOLT_ENABLE_EXEC OFF)
  set(BOLT_ENABLE_AGGREGATES OFF)
  set(BOLT_ENABLE_HIVE_CONNECTOR OFF)
  set(BOLT_ENABLE_TPCH_CONNECTOR OFF)
  set(BOLT_ENABLE_ARROW_CONNECTOR OFF)
  set(BOLT_ENABLE_SPARK_FUNCTIONS OFF)
  set(BOLT_ENABLE_FLINK_FUNCTIONS OFF)
  set(BOLT_ENABLE_SKETCH_FUNCTIONS OFF)
  set(BOLT_ENABLE_EXAMPLES OFF)
  set(BOLT_ENABLE_S3 OFF)
  set(BOLT_ENABLE_GCS OFF)
  set(BOLT_ENABLE_ABFS OFF)
  set(BOLT_ENABLE_SUBSTRAIT OFF)
  set(BOLT_CODEGEN_SUPPORT OFF)
endif()

if(${BOLT_BUILD_TESTING})
  # Enable all components to build testing binaries
  set(BOLT_ENABLE_PRESTO_FUNCTIONS ON)
  set(BOLT_ENABLE_DUCKDB ON)
  set(BOLT_ENABLE_EXPRESSION ON)
  set(BOLT_ENABLE_PARSE ON)
  set(BOLT_ENABLE_EXEC ON)
  set(BOLT_ENABLE_AGGREGATES ON)
  set(BOLT_ENABLE_HIVE_CONNECTOR ON)
  set(BOLT_ENABLE_TPCH_CONNECTOR ON)
  # set(BOLT_ENABLE_ARROW_CONNECTOR ON)
  set(BOLT_ENABLE_SPARK_FUNCTIONS ON)
  set(BOLT_ENABLE_FLINK_FUNCTIONS ON)
  set(BOLT_ENABLE_TEST_UTILS OFF)
  set(BOLT_ENABLE_EXAMPLES ON)
else()
  set(BOLT_BUILD_TESTING_WITH_COVERAGE OFF)
endif()

if(${BOLT_ENABLE_EXAMPLES})
  set(BOLT_ENABLE_EXPRESSION ON)
  set(BOLT_ENABLE_TEST_UTILS ON)
endif()

if(${BOLT_BUILD_BENCHMARKS})
  set(BOLT_ENABLE_BENCHMARKS ON)
  set(BOLT_ENABLE_BENCHMARKS_BASIC ON)
  set(BOLT_ENABLE_DUCKDB ON)
  set(BOLT_ENABLE_PARSE ON)
  set(BOLT_ENABLE_PARQUET ON)
  set(BOLT_ENABLE_ORC ON)
  set(BOLT_BUILD_TEST_UTILS ON)
  set(BOLT_BUILD_TESTING OFF)
  set(BOLT_ENABLE_EXAMPLES OFF)
  set(BOLT_ENABLE_GCS OFF)
  set(BOLT_ENABLE_ABFS OFF)
  set(BOLT_ENABLE_SUBSTRAIT OFF)
  set(BOLT_CODEGEN_SUPPORT OFF)
endif()

# enable test coverage
if(BOLT_BUILD_TESTING_WITH_COVERAGE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage")
  add_compile_options(-fprofile-arcs -ftest-coverage
                      -DGTEST_USE_OWN_TR1_TUPLE=0)
  set(CMAKE_EXE_LINKER_FLAGS
      "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs -ftest-coverage")
  if(NOT OS_MACOSX)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lgcov")
  endif()
  if(ARCH_ARM)
    add_compile_options(-ffp-contract=off)
  endif()
endif()

if(${BOLT_ENABLE_BENCHMARKS})
  set(BOLT_ENABLE_BENCHMARKS_BASIC ON)
endif()

if(BOLT_ENABLE_BENCHMARKS_BASIC)
  set(BOLT_BUILD_TEST_UTILS ON)
endif()

if(BOLT_BUILD_TESTING OR BOLT_BUILD_TEST_UTILS)
  find_package(cpr CONFIG REQUIRED)
  set(BOLT_ENABLE_DUCKDB ON)
  set(BOLT_ENABLE_PARSE ON)
endif()

if(${BOLT_BUILD_PYTHON_PACKAGE})
  set(BOLT_ENABLE_PRESTO_FUNCTIONS ON)
  set(BOLT_ENABLE_DUCKDB ON)
  set(BOLT_ENABLE_EXPRESSION ON)
  set(BOLT_ENABLE_PARSE ON)
  set(BOLT_ENABLE_EXEC ON)
  set(BOLT_ENABLE_AGGREGATES ON)
  set(BOLT_ENABLE_HIVE_CONNECTOR ON)
  set(BOLT_ENABLE_TPCH_CONNECTOR ON)
  set(BOLT_ENABLE_SPARK_FUNCTIONS ON)
  set(BOLT_ENABLE_FLINK_FUNCTIONS ON)
  set(BOLT_ENABLE_EXAMPLES OFF)
  set(BOLT_ENABLE_S3 OFF)
  set(BOLT_ENABLE_GCS OFF)
  set(BOLT_ENABLE_ABFS OFF)
  set(BOLT_ENABLE_SUBSTRAIT ON)
  set(BOLT_CODEGEN_SUPPORT OFF)
  set(BOLT_ENABLE_BENCHMARKS_BASIC OFF)
  set(BOLT_ENABLE_BENCHMARKS OFF)
  set(BOLT_ENABLE_PARQUET ON)
  set(BOLT_ENABLE_HDFS ON)
endif()

if(BOLT_ENABLE_CCACHE
   AND NOT CMAKE_C_COMPILER_LAUNCHER
   AND NOT CMAKE_CXX_COMPILER_LAUNCHER)

  find_program(CCACHE_FOUND ccache)

  if(CCACHE_FOUND)
    message(STATUS "Using ccache: ${CCACHE_FOUND}")
    set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE_FOUND})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_FOUND})
    # keep comments as they might matter to the compiler
    set(ENV{CCACHE_COMMENTS} "1")
  endif()
endif()

if(${BOLT_FORCE_COLORED_OUTPUT})
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    add_compile_options(-fdiagnostics-color=always)
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang"
         OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    add_compile_options(-fcolor-diagnostics)
  endif()
endif()

# At the moment we prefer static linking but by default cmake looks for shared
# libs first. This will still fallback to shared libs when static ones are not
# found
list(INSERT CMAKE_FIND_LIBRARY_SUFFIXES 0 a)
if(BOLT_ENABLE_S3)
  # Set AWS_ROOT_DIR if you have a custom install location of AWS SDK CPP.
  if(AWSSDK_ROOT_DIR)
    set(CMAKE_PREFIX_PATH ${AWSSDK_ROOT_DIR})
  endif()
  find_package(AWSSDK REQUIRED COMPONENTS s3;identity-management)
  add_definitions(-DBOLT_ENABLE_S3)
endif()

if(BOLT_ENABLE_GCS)
  # Set GCS_ROOT_DIR if you have a custom install location of GCS SDK CPP.
  if(GCSSDK_ROOT_DIR)
    list(APPEND CMAKE_PREFIX_PATH ${GCSSDK_ROOT_DIR})
  endif()
  find_package(google_cloud_cpp_storage REQUIRED)
  add_definitions(-DBOLT_ENABLE_GCS)
endif()

if(BOLT_ENABLE_ABFS)
  # Set AZURESDK_ROOT_DIR if you have a custom install location of Azure Storage
  # SDK CPP.
  if(AZURESDK_ROOT_DIR)
    list(APPEND CMAKE_PREFIX_PATH ${AZURESDK_ROOT_DIR})
  endif()
  # files-datalake is built on blobs
  find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
  find_package(azure-storage-files-datalake-cpp CONFIG REQUIRED)
  add_definitions(-DBOLT_ENABLE_ABFS)
endif()

if(BOLT_ENABLE_HDFS)
  find_package(Arrow REQUIRED)
  add_definitions(-DBOLT_ENABLE_HDFS3)
endif()

if(BOLT_ENABLE_PARQUET)
  add_definitions(-DBOLT_ENABLE_PARQUET)
  # Native Parquet reader requires Apache Thrift and Arrow Parquet writer, which
  # are included in Arrow.
  set(BOLT_ENABLE_ARROW ON)
endif()

if(BOLT_ENABLE_ORC)
  add_definitions(-DBOLT_ENABLE_ORC)
  # Native Parquet reader requires Apache Thrift and Arrow Parquet writer, which
  # are included in Arrow.
  set(BOLT_ENABLE_ARROW ON)
endif()

if(BOLT_ENABLE_TXT)
  add_definitions(-DBOLT_ENABLE_TXT)
  # TXT reader requires Apache Arrow CSV reader, which
  # are included in Arrow.
  set(BOLT_ENABLE_ARROW ON)
endif()

# if(BOLT_ENABLE_ARROW) find_package(Thrift CONFIG REQUIRED) endif()


# define processor variable for conditional compilation
if(${BOLT_CODEGEN_SUPPORT})
  add_compile_definitions(CODEGEN_ENABLED=1)
endif()

# make buildPartitionBounds_ a vector int64 instead of int32 to avoid integer
# overflow
if(${BOLT_ENABLE_INT64_BUILD_PARTITION_BOUND})
  add_compile_definitions(BOLT_ENABLE_INT64_BUILD_PARTITION_BOUND)
endif()

# MacOSX enables two-level namespace by default:
# http://mirror.informatimago.com/next/developer.apple.com/releasenotes/DeveloperTools/TwoLevelNamespaces.html
# Enables -flat_namespace so type_info can be deudplicated across .so boundaries
if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  add_link_options("-Wl,-flat_namespace")
endif()

if(UNIX AND NOT APPLE)
  # codegen linker flags, -export-dynamic for rtti
  add_link_options("-Wl,-export-dynamic")
endif()

if(ENABLE_EXCEPTION_TRACE)
  add_link_options("-Wl,-wrap=__cxa_throw")
endif()

if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm64" AND ${CMAKE_SYSTEM_NAME} MATCHES
                                                 "Darwin")
  set(ON_APPLE_M1 True)
endif()

# Required so bolt code can be used in a dynamic library
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# execute_process(
#   COMMAND
#     bash -c
#     "( source ${CMAKE_CURRENT_SOURCE_DIR}/scripts/setup-helper-functions.sh && echo -n $(get_cxx_flags $ENV{CPU_TARGET}))"
#   OUTPUT_VARIABLE SCRIPT_CXX_FLAGS
#   RESULT_VARIABLE COMMAND_STATUS)

if(COMMAND_STATUS EQUAL "1")
  message(FATAL_ERROR "Unable to determine compiler flags!")
endif()
message("Setting CMAKE_CXX_FLAGS=${SCRIPT_CXX_FLAGS}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D USE_BOLT_COMMON_BASE")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D HAS_UNCAUGHT_EXCEPTIONS")
if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsigned-char")
endif()

# Under Ninja, we are able to designate certain targets large enough to require
# restricted parallelism.
if("${MAX_HIGH_MEM_JOBS}")
  set_property(GLOBAL PROPERTY JOB_POOLS
                               "high_memory_pool=${MAX_HIGH_MEM_JOBS}")
else()
  set_property(GLOBAL PROPERTY JOB_POOLS high_memory_pool=1000)
endif()

set(MAX_LINK_JOBS
    4
    CACHE STRING "")
if("${MAX_LINK_JOBS}")
  set_property(GLOBAL APPEND PROPERTY JOB_POOLS
                                      "link_job_pool=${MAX_LINK_JOBS}")
  set(CMAKE_JOB_POOL_LINK link_job_pool)
endif()

if("${ENABLE_ALL_WARNINGS}")
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(KNOWN_COMPILER_SPECIFIC_WARNINGS
        "-Wno-range-loop-analysis \
         -Wno-mismatched-tags \
         -Wno-nullability-completeness")
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(KNOWN_COMPILER_SPECIFIC_WARNINGS
        "-Wno-implicit-fallthrough \
         -Wno-empty-body \
         -Wno-class-memaccess \
         -Wno-comment \
         -Wno-int-in-bool-context \
         -Wno-redundant-move \
         -Wno-array-bounds \
         -Wno-maybe-uninitialized \
         -Wno-unused-result \
         -Wno-format-overflow \
         -Wno-strict-aliasing \
         -Wno-type-limits \
         -Wno-stringop-overflow \
         -Wno-stringop-overread \
         -Wno-return-type")
  endif()

  set(KNOWN_WARNINGS
      "-Wno-unused \
       -Wno-unused-parameter \
       -Wno-sign-compare \
       -Wno-ignored-qualifiers \
       -Wno-psabi \
       ${KNOWN_COMPILER_SPECIFIC_WARNINGS}")

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra ${KNOWN_WARNINGS}")
endif()

message("FINAL CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")

if(${BOLT_ENABLE_GPU})
  enable_language(CUDA)
  # Determine CUDA_ARCHITECTURES automatically.
  cmake_policy(SET CMP0104 NEW)
  if(CMAKE_BUILD_TYPE MATCHES Debug)
    add_compile_options("$<$<COMPILE_LANGUAGE:CUDA>:-G>")
  endif()
  include_directories("${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES}")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Boost CONFIG REQUIRED)

# Range-v3 will be enable when the codegen code actually lands keeping it here
# for reference. find_package(range-v3)

find_package(gflags CONFIG REQUIRED)
# set_source(gflags) resolve_dependency(gflags COMPONENTS shared)
if(NOT TARGET gflags::gflags)
  # This is a bit convoluted, but we want to be able to use gflags::gflags as a
  # target even when bolt is built as a subproject which uses
  # `find_package(gflags)` which does not create a globally imported target that
  # we can ALIAS.
  add_library(gflags_gflags INTERFACE)
  target_link_libraries(gflags_gflags INTERFACE gflags)
  add_library(gflags::gflags ALIAS gflags_gflags)
endif()

# if(${gflags_SOURCE} STREQUAL "BUNDLED") # we force glog from source to avoid
# issues with a system version built # against another gflags version (which is
# likely) set(glog_SOURCE BUNDLED) else() set(glog_SOURCE SYSTEM) endif()
# resolve_dependency(glog)

# if(${BOLT_ENABLE_DUCKDB}) set_source(DuckDB) resolve_dependency(DuckDB)
# endif()

# set_source(fmt) resolve_dependency(fmt)

find_library(EVENT event)

if(${BOLT_ENABLE_TBB})
  # find_library(TBB tbb)
  find_package(TBB REQUIRED)
endif()

find_library(DOUBLE_CONVERSION double-conversion)

# Required for boost.
find_package(ZLIB)

if(NOT ${BOLT_BUILD_MINIMAL})
  find_package(ZLIB CONFIG REQUIRED)
  find_package(lz4 CONFIG REQUIRED)
  find_package(zstd CONFIG REQUIRED)
  find_package(Snappy CONFIG REQUIRED)
  find_package(zstd CONFIG REQUIRED)
  if(NOT TARGET zstd::zstd)
    if(TARGET zstd::libzstd_static)
      set(ZSTD_TYPE static)
    else()
      set(ZSTD_TYPE shared)
    endif()
    add_library(zstd::zstd ALIAS zstd::libzstd_${ZSTD_TYPE})
  endif()
endif()

find_package(re2 CONFIG REQUIRED)
find_package(ICU CONFIG REQUIRED)

find_package(sonic-cpp CONFIG REQUIRED)
find_package(simdjson CONFIG REQUIRED)
find_package(sonic-cpp CONFIG REQUIRED)
find_package(folly CONFIG REQUIRED)

# We should just use Folly::folly, all the dependencies should be managed by
# target Folly::folly But, there is an issue in folly recipe:
# transitive_headers=True doesn't work in conan 1.x bug? Note: it is not elegant
# solution, just a workaround. It should be ok in conan 2.x
set(FOLLY_WITH_DEPENDENCIES Folly::folly fmt::fmt)

if(BOLT_ENABLE_REMOTE_FUNCTIONS)
  # TODO: Move this to use resolve_dependency(). For some reason, FBThrift
  # requires clients to explicitly install fizz and wangle.
  find_package(fizz CONFIG REQUIRED)
  find_package(wangle CONFIG REQUIRED)
  find_package(FBThrift CONFIG REQUIRED)
endif()

if(BOLT_ENABLE_SPARK_COMPATIBLE)
  message("Enable spark compatible setting.")
  add_definitions(-DSPARK_COMPATIBLE)
endif()

if(KERNEL_SUPPORTS_IO_URING)
  message("IO_URING is supported by Linux kernel in building")
  add_definitions(-DIO_URING_SUPPORTED)
endif()

if(DEFINED FOLLY_BENCHMARK_STATIC_LIB)
  set(FOLLY_BENCHMARK ${FOLLY_BENCHMARK_STATIC_LIB} GTest::gtest
                      GTest::gtest_main)
else()
  set(FOLLY_BENCHMARK Folly::follybenchmark GTest::gtest GTest::gtest_main)
endif()

# if(NOT ${BOLT_BUILD_MINIMAL}) # Locate or build protobuf.
# set_source(Protobuf) resolve_dependency(Protobuf 3.21 EXACT)
# include_directories(${Protobuf_INCLUDE_DIRS}) endif()

find_package(Protobuf CONFIG REQUIRED)

# GCC needs to link a library to enable std::filesystem.
if("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  set(FILESYSTEM "stdc++fs")

  # Ensure we have gcc at least 9+.
  if(CMAKE_CXX_COMPILER_VERSION LESS 9.0)
    message(
      FATAL_ERROR "BOLT requires gcc > 9. Found ${CMAKE_CXX_COMPILER_VERSION}")
  endif()

  # Find Threads library
  find_package(Threads REQUIRED)
else()
  set(FILESYSTEM "")
endif()

if(BOLT_BUILD_TESTING AND NOT BOLT_ENABLE_DUCKDB)
  message(
    FATAL_ERROR
      "Unit tests require duckDB to be enabled (BOLT_ENABLE_DUCKDB=ON or BOLT_BUILD_TESTING=OFF)"
  )
endif()

set(BOLT_DISABLE_GOOGLETEST OFF)
if(NOT BOLT_BUILD_TEST_UTILS AND NOT BOLT_BUILD_TESTING)
  set(BOLT_DISABLE_GOOGLETEST ON)
  add_definitions(-DBOLT_DISABLE_GOOGLETEST)
endif()

find_package(ryu CONFIG REQUIRED)

if(BOLT_ENABLE_COLOCATE_FUNCTIONS)
  find_package(Protobuf REQUIRED)
  find_package(gRPC REQUIRED)
  find_package(Arrow REQUIRED)
  set(BOLT_ENABLE_ARROW ON)
endif()

# On macOS, search Homebrew for keg-only versions of Bison and Flex. Xcode does
# not provide new enough versions for us to use.
if(CMAKE_HOST_SYSTEM_NAME MATCHES "Darwin")
  execute_process(
    COMMAND brew --prefix bison
    RESULT_VARIABLE BREW_BISON
    OUTPUT_VARIABLE BREW_BISON_PREFIX
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(BREW_BISON EQUAL 0 AND EXISTS "${BREW_BISON_PREFIX}")
    message(
      STATUS "Found Bison keg installed by Homebrew at ${BREW_BISON_PREFIX}")
    set(BISON_EXECUTABLE "${BREW_BISON_PREFIX}/bin/bison")
  endif()

  execute_process(
    COMMAND brew --prefix flex
    RESULT_VARIABLE BREW_FLEX
    OUTPUT_VARIABLE BREW_FLEX_PREFIX
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(BREW_FLEX EQUAL 0 AND EXISTS "${BREW_FLEX_PREFIX}")
    message(
      STATUS "Found Flex keg installed by Homebrew at ${BREW_FLEX_PREFIX}")
    set(FLEX_EXECUTABLE "${BREW_FLEX_PREFIX}/bin/flex")
    set(FLEX_INCLUDE_DIR "${BREW_FLEX_PREFIX}/include")
  endif()
endif()

find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)
find_package(Arrow CONFIG REQUIRED)
find_package(xxHash CONFIG REQUIRED)

include_directories(SYSTEM bolt)

# these were previously vendored in third-party/
if(NOT BOLT_DISABLE_GOOGLETEST)
  # set(gtest_SOURCE AUTO) resolve_dependency(gtest) set(BOLT_GTEST_INCUDE_DIR
  # "${gtest_SOURCE_DIR}/googletest/include" PARENT_SCOPE)
  find_package(GTest CONFIG REQUIRED)
endif()

find_package(xsimd CONFIG REQUIRED)
add_library(xsimd::xsimd ALIAS xsimd)

find_package(gfx-timsort CONFIG REQUIRED)

find_package(utf8proc REQUIRED)

find_package(date REQUIRED)

include(CTest) # include after project() but before add_subdirectory()
# if(BOLT_BUILD_TESTING) set(BUILD_TESTING ON) include(CTest) # include after
# project() but before add_subdirectory() endif()

include_directories(.)

# Adding this down here prevents warnings in dependencies from stopping the
# build
if("${TREAT_WARNINGS_AS_ERRORS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()

# It would be better to only expose public headers Currently, just expose all
# the headers
install(
  DIRECTORY "${CMAKE_SOURCE_DIR}/bolt" # source directory
  DESTINATION "include" # target directory
  FILES_MATCHING # install only matched files
  # PATTERN "test" EXCLUDE
  PATTERN "benchmarks" EXCLUDE
  PATTERN "benchmark" EXCLUDE
  PATTERN "*.h" # select header files
  PATTERN "*.hpp" # select header files
)

install(FILES "${CMAKE_BINARY_DIR}/deps/dep.json" DESTINATION deps)

if(${BOLT_ENABLE_SUBSTRAIT})
  install(
    DIRECTORY "${CMAKE_BINARY_DIR}/gen_cpp/bolt" # source directory
    DESTINATION "include" # target directory
    FILES_MATCHING # install only matched files
    # PATTERN "test" EXCLUDE
    PATTERN "benchmarks" EXCLUDE
    PATTERN "benchmark" EXCLUDE
    PATTERN "*.h" # select header files
    PATTERN "*.hpp" # select header files
  )
endif()

file(GLOB_RECURSE SHARED_LIBS
     "${CMAKE_BINARY_DIR}/bolt/*${CMAKE_SHARED_LIBRARY_SUFFIX}")
file(GLOB_RECURSE STATIC_LIBS
     "${CMAKE_BINARY_DIR}/bolt/*${CMAKE_STATIC_LIBRARY_SUFFIX}")

list(APPEND ALL_LIBS ${SHARED_LIBS} ${STATIC_LIBS})

list(FILTER ALL_LIBS EXCLUDE REGEX "benchmarks")
list(FILTER ALL_LIBS EXCLUDE REGEX "benchmark")

install(FILES ${ALL_LIBS} DESTINATION "lib")

find_package(cityhash REQUIRED CONFIG)

# Adding this down here prevents warnings in dependencies from stopping the
# build
if("${TREAT_WARNINGS_AS_ERRORS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()

find_package(duckdb REQUIRED CONFIG)

# if both JIT and TORCH are enabled, torch includes
# a llvm header as a transitive header included by jit.
# Therefore, LLVM definitions need to be available to
# torch module and not just the jit module.
if (ENABLE_BOLT_JIT)
  find_package(LLVM CONFIG REQUIRED)
endif()

if (BOLT_ENABLE_TORCH)
  find_package(Torch CONFIG REQUIRED)
endif()


if (BOLT_ENABLE_CUDF)
  set(BOLT_ENABLE_ARROW ON)
  find_package(cudf CONFIG REQUIRED)
endif ()


# print all configs

message("---- All CMake Configuration Variables ----")
get_cmake_property(vars CACHE_VARIABLES)
foreach(var ${vars})
  message("${var} = ${${var}}")
endforeach()
message("-------------------------------------------------")

# Function: Generate template specialization files
# Parameters:
#   OUTPUT_VAR - Output variable name that will store the list of generated files
#   TEMPLATE_FILE - Template file path (.cpp.in)
#   TYPES - List of types to specialize
function(specialize_template OUTPUT_VAR TEMPLATE_FILE)
    # Parse remaining arguments as type list
    set(TYPES ${ARGN})
    
    # Extract class name from template file path (remove path and .cpp.in)
    get_filename_component(TEMPLATE_NAME "${TEMPLATE_FILE}" NAME)
    string(REGEX REPLACE "\\.cpp\\.in$" "" CLASS_NAME "${TEMPLATE_NAME}")
    
    # Get template file's relative path (relative to source directory)
    file(RELATIVE_PATH REL_PATH "${CMAKE_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")
    
    # Build output directory path (in build/gen_cpp/ while maintaining the same relative path)
    set(OUTPUT_DIR "${CMAKE_BINARY_DIR}/gen_cpp/${REL_PATH}")
    
    # Ensure output directory exists
    file(MAKE_DIRECTORY ${OUTPUT_DIR})
    
    # Initialize output file list
    set(GENERATED_FILES "")
    
    # Generate specialization files for each type
    foreach(TYPE ${TYPES})
        # Process special characters in type names, replace with file-friendly forms
        string(REPLACE " " "_" SAFE_TYPE "${TYPE}")
        string(REPLACE "<" "_" SAFE_TYPE "${SAFE_TYPE}")
        string(REPLACE ">" "_" SAFE_TYPE "${SAFE_TYPE}")
        string(REPLACE "::" "_" SAFE_TYPE "${SAFE_TYPE}")
        
        # Set output file path
        set(OUTPUT_FILE "${OUTPUT_DIR}/${CLASS_NAME}_${SAFE_TYPE}.cpp")
        
        # Configure file generation
        configure_file(
            "${TEMPLATE_FILE}"
            "${OUTPUT_FILE}"
            @ONLY
        )
        
        # Add to generated files list
        list(APPEND GENERATED_FILES "${OUTPUT_FILE}")
    endforeach()
    
    # Set output variable in parent scope
    set(${OUTPUT_VAR} ${GENERATED_FILES} PARENT_SCOPE)
    
    # Print generation info (optional)
    message(STATUS "Generated ${CLASS_NAME} specializations in ${OUTPUT_DIR}")
endfunction()

add_subdirectory(bolt)
