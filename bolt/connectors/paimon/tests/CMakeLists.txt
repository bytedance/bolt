# Copyright (c) ByteDance Ltd. and/or its affiliates.
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

# PaimonConnectorTest creates its tables with the pypaimon Python package.
# Only check for it here; the test fails at runtime when it is missing.

find_package(Python3 REQUIRED COMPONENTS Interpreter)

execute_process(
  COMMAND
    ${Python3_EXECUTABLE} -c "import pypaimon"
  RESULT_VARIABLE _paimon_check_result
  ERROR_QUIET
  OUTPUT_QUIET
)
if(NOT _paimon_check_result EQUAL 0)
  message(
    WARNING
    "Python package pypaimon is not available for ${Python3_EXECUTABLE}; "
    "PaimonConnectorTest will fail. Install it with: "
    "${Python3_EXECUTABLE} -m pip install pypaimon==1.4.2"
  )
endif()

set(
  PAIMON_CONNECTOR_TEST_SOURCES
  PaimonConnectorTest.cpp
  PaimonConfigTest.cpp
  PaimonFilterPushdownTest.cpp
  PaimonFilterTranslatorTest.cpp
  PaimonFileSystemTest.cpp
  PaimonParquetReaderTest.cpp
)

if(BOLT_ENABLE_HDFS)
  list(
    APPEND PAIMON_CONNECTOR_TEST_SOURCES
    PaimonHdfsFileSystemTest.cpp
    HdfsContainerMiniCluster.cpp
  )
endif()

add_executable(bolt_paimon_connector_test ${PAIMON_CONNECTOR_TEST_SOURCES})

add_test(bolt_paimon_connector_test bolt_paimon_connector_test)
set_tests_properties(
  bolt_paimon_connector_test
  PROPERTIES
    ENVIRONMENT
      "PAIMON_TEST_SCRIPT_DIR=${CMAKE_CURRENT_SOURCE_DIR};PAIMON_TEST_PYTHON=${Python3_EXECUTABLE}"
)

set(PAIMON_CONNECTOR_TEST_REGISTRATION_LIBRARIES)
set(PAIMON_CONNECTOR_TEST_EARLY_LIBRARIES)
add_library(paimon_connector_test_compile_deps INTERFACE)
target_link_libraries(paimon_connector_test_compile_deps INTERFACE bolt_connector_paimon)
set(PAIMON_CONNECTOR_TEST_CONNECTOR_LIBRARY paimon_connector_test_compile_deps)
if(NOT BUILD_SHARED_LIBS AND BOLT_TEST_LINKAGE STREQUAL "static")
  set(PAIMON_CONNECTOR_TEST_CONNECTOR_LIBRARY bolt_connector_paimon)
  list(
    APPEND PAIMON_CONNECTOR_TEST_REGISTRATION_LIBRARIES
    $<LINK_LIBRARY:WHOLE_ARCHIVE,paimon_avro_file_format>
  )
endif()

if(BOLT_TEST_LINKAGE STREQUAL "shared" AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
  # Unit tests are split across multiple shared libraries to reduce disk usage.
  # As a result, parts of the static Paimon archive are linked into both this
  # executable and the shared Bolt engine. Paimon implementation types have
  # hidden RTTI, so ELF symbol preemption can create an object using the
  # executable's RTTI and later attempt to cast it using the engine's distinct
  # RTTI, causing the cast to fail. Load the archive early for test-side
  # serialization, then use a version script to keep the affected engine-side
  # conversion entry points from being preempted by the executable's copy.
  get_target_property(PAIMON_CORE_LINK_LIBRARIES Paimon::core INTERFACE_LINK_LIBRARIES)
  list(
    APPEND PAIMON_CONNECTOR_TEST_EARLY_LIBRARIES
    fmt::fmt
    ${PAIMON_CORE_LINK_LIBRARIES}
  )
  set(PAIMON_TEST_VERSION_SCRIPT "${CMAKE_CURRENT_BINARY_DIR}/paimon-test.map")
  file(
    GENERATE OUTPUT "${PAIMON_TEST_VERSION_SCRIPT}"
    CONTENT
      [[{
  local:
    _ZN6paimon5Split11DeserializeE*;
    _ZN6paimon16PredicateBuilder*;
};
]]
  )
  target_link_options(
    bolt_paimon_connector_test
    PRIVATE
      "LINKER:--version-script=${PAIMON_TEST_VERSION_SCRIPT}"
  )
  set_property(
    TARGET
      bolt_paimon_connector_test
    APPEND
    PROPERTY
      LINK_DEPENDS
        "${PAIMON_TEST_VERSION_SCRIPT}"
  )
endif()

target_link_libraries(
  bolt_paimon_connector_test
  PUBLIC
    ${PAIMON_CONNECTOR_TEST_EARLY_LIBRARIES}
    ${PAIMON_CONNECTOR_TEST_CONNECTOR_LIBRARY}
    Paimon::core
    roaring::roaring
    arrow::arrow
    Snappy::snappy
    thrift::thrift
    protobuf::protobuf
    zstd::libzstd_static
    LZ4::lz4_static
    bolt_gtest_main
    glog::glog
    gflags::gflags
    Folly::folly
  PRIVATE
    ${PAIMON_CONNECTOR_TEST_REGISTRATION_LIBRARIES}
    Paimon::tbb
    Paimon::libavrocpp
)

if(BOLT_ENABLE_HDFS)
  target_link_libraries(bolt_paimon_connector_test PRIVATE bolt_hdfs)
endif()

if(BOLT_ENABLE_GCS)
  target_link_libraries(
    bolt_paimon_connector_test
    PRIVATE
      bolt_gcs
      GTest::gmock
  )
endif()
