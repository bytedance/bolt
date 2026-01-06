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


add_subdirectory(util)
add_subdirectory(generated)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

add_library(
  bolt_dwio_arrow_parquet_writer_lib
  ArrowSchema.cpp
  ArrowSchemaInternal.cpp
  ColumnWriter.cpp
  Encoding.cpp
  Encryption.cpp
  EncryptionInternal.cpp
  Exception.cpp
  FileEncryptorInternal.cpp
  FileDecryptorInternal.cpp
  FileWriter.cpp
  LevelComparison.cpp
  LevelComparisonAvx2.cpp
  LevelConversion.cpp
  Metadata.cpp
  PageIndex.cpp
  PathInternal.cpp
  Platform.cpp
  Properties.cpp
  Schema.cpp
  Statistics.cpp
  Types.cpp
  Writer.cpp)

target_link_libraries(
  bolt_dwio_arrow_parquet_writer_lib
  bolt_dwio_arrow_parquet_writer_util_lib
  bolt_dwio_arrow_parquet_writer_thrift_lib
  bolt_dwio_common
  bolt_arrow_bridge
  bolt_process
  arrow::arrow
  ${FMT})
