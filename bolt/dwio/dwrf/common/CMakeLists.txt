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

find_package(zstd CONFIG REQUIRED)

# Keep SVE instructions out of the baseline decoder and dispatch at runtime.
# This also keeps the public headers usable with the ordinary ARM target.
set(rlev2_sve_sources)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
  include(CheckCXXCompilerFlag)
  check_cxx_compiler_flag(
    "-march=armv8-a+sve"
    BOLT_COMPILER_SUPPORTS_RLEV2_SVE
  )
  if(BOLT_COMPILER_SUPPORTS_RLEV2_SVE)
    list(APPEND rlev2_sve_sources RLEv2SVE.cpp)
    set_source_files_properties(
      RLEv2SVE.cpp
      PROPERTIES
        COMPILE_OPTIONS
          "-march=armv8-a+sve"
    )
    set_property(
      SOURCE
        RLEv2.cpp
      APPEND
      PROPERTY
        COMPILE_DEFINITIONS
          BOLT_RLEV2_SVE
    )
  endif()
endif()

bolt_add_library(
  bolt_dwio_dwrf_common
  ByteRLE.cpp
  Common.cpp
  Config.cpp
  Decryption.cpp
  Encryption.cpp
  EncryptionSpecification.cpp
  FileMetadata.cpp
  IntEncoder.cpp
  RLEv1.cpp
  RLEv2.cpp
  ${rlev2_sve_sources}
  Statistics.cpp
)

add_dependencies(bolt_dwio_dwrf_common bolt_dwio_dwrf_proto)

target_link_libraries(
  bolt_dwio_dwrf_common
  bolt_common_base
  bolt_common_compression
  bolt_common_config
  bolt_dwio_common
  bolt_dwio_dwrf_proto
  bolt_caching
  Snappy::snappy
  zstd::libzstd_static
)
