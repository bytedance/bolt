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

bolt_add_library(
  bolt_dwio_dwrf_reader
  BinaryStreamReader.cpp
  ColumnReader.cpp
  DwrfData.cpp
  DwrfReader.cpp
  FlatMapColumnReader.cpp
  ReaderBase.cpp
  SelectiveDecimalColumnReader.cpp
  SelectiveDwrfReader.cpp
  SelectiveFlatMapColumnReader.cpp
  SelectiveIntegerDictionaryColumnReader.cpp
  SelectiveIntegerDirectColumnReader.cpp
  SelectiveRepeatedColumnReader.cpp
  SelectiveStringDictionaryColumnReader.cpp
  SelectiveStringDirectColumnReader.cpp
  SelectiveStructColumnReader.cpp
  SelectiveTimestampColumnReader.cpp
  StreamLabels.cpp
  StripeDictionaryCache.cpp
  StripeReaderBase.cpp
  StripeStream.cpp
)

target_link_libraries(
  bolt_dwio_dwrf_reader
  bolt_dwio_common
  bolt_dwio_dwrf_common
  bolt_caching
  bolt_dwio_dwrf_utils
  fmt::fmt
)

install(
  TARGETS bolt_dwio_dwrf_reader
          ARCHIVE
          DESTINATION
          ${CMAKE_INSTALL_LIBDIR}
          LIBRARY
          DESTINATION
          ${CMAKE_INSTALL_LIBDIR}
)
