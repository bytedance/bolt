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

# Ensure paimon Python package is available for tests that use it.
# Only runs during cmake configuration; skips if already installed.

find_package(
        Python3
        REQUIRED
        COMPONENTS Interpreter
)

execute_process(
  COMMAND ${Python3_EXECUTABLE} -c "import pypaimon"
  RESULT_VARIABLE _paimon_check_result
  ERROR_QUIET
  OUTPUT_QUIET
)
if(NOT _paimon_check_result EQUAL 0)
  message(STATUS "Installing paimon Python package for integration tests...")
  execute_process(
    COMMAND ${Python3_EXECUTABLE} -m pip install
      "git+https://github.com/apache/paimon@release-1.4.1-rc2#subdirectory=paimon-python"
    RESULT_VARIABLE _paimon_install_result
    OUTPUT_VARIABLE _paimon_install_output
    ERROR_VARIABLE _paimon_install_error
  )
  if(NOT _paimon_install_result EQUAL 0)
    message(WARNING "Failed to install paimon Python package: ${_paimon_install_error}. Please install at least version 1.4.1")
  else()
    message(STATUS "paimon Python package installed successfully")
  endif()
endif()

set(PAIMON_CONNECTOR_TEST_SOURCES
    PaimonConnectorTest.cpp
    PaimonConfigTest.cpp
    PaimonFilterPushdownTest.cpp
    PaimonFilterTranslatorTest.cpp
    PaimonParquetReaderTest.cpp)

if(BOLT_ENABLE_HDFS)
  list(APPEND PAIMON_CONNECTOR_TEST_SOURCES
      PaimonHdfsFileSystemTest.cpp
      HdfsContainerMiniCluster.cpp)
endif()

add_executable(bolt_paimon_connector_test ${PAIMON_CONNECTOR_TEST_SOURCES})

add_test(bolt_paimon_connector_test bolt_paimon_connector_test)
set_tests_properties(bolt_paimon_connector_test PROPERTIES
  ENVIRONMENT "PAIMON_TEST_SCRIPT_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
)

target_link_libraries(bolt_paimon_connector_test
    PUBLIC
    bolt_connector_paimon
    bolt_testutils
    bolt_engine
    bolt_process
    Paimon::core
    roaring::roaring
    arrow::arrow
    Snappy::snappy
    thrift::thrift
    protobuf::protobuf
    zstd::libzstd_static
    LZ4::lz4_static
    GTest::gtest
    GTest::gtest_main
    glog::glog
    gflags::gflags
    Folly::folly
    PRIVATE
    $<LINK_LIBRARY:WHOLE_ARCHIVE,paimon_avro_file_format>
    $<LINK_LIBRARY:WHOLE_ARCHIVE,paimon_local_file_system>
    Paimon::tbb
    Paimon::libavrocpp
)
