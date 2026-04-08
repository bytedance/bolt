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

set(
  ICEBERG_SOURCES
  IcebergConfig.cpp
  IcebergColumnHandle.cpp
  IcebergConnector.cpp
  IcebergDataSink.cpp
  IcebergDataSource.cpp
  IcebergPartitionName.cpp
  IcebergSplit.cpp
  IcebergSplitReader.cpp
  PartitionSpec.cpp
  PositionalDeleteFileReader.cpp
  TransformEvaluator.cpp
  TransformExprBuilder.cpp
)

bolt_add_library(
  bolt_hive_iceberg_splitreader
  ${ICEBERG_SOURCES}
)

target_link_libraries(
  bolt_hive_iceberg_splitreader
  PUBLIC
    bolt_connector
    bolt_functions_iceberg
    Folly::folly
)

# Iceberg split reader transitively includes exec/jit headers when ENABLE_BOLT_JIT is on.
# Ensure LLVM headers are available for this target.
if(ENABLE_BOLT_JIT)
  find_package(LLVM CONFIG REQUIRED)
  target_include_directories(
    bolt_hive_iceberg_splitreader
    SYSTEM
    PRIVATE
      ${LLVM_INCLUDE_DIRS}
  )
endif()
