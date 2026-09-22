# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

add_subdirectory(proto)

bolt_add_library(
  bolt_dwio_lance_native
  NativeLanceBitmap.cpp
  NativeLanceBitpack.cpp
  NativeLanceColumnReader.cpp
  NativeLanceListOffsets.cpp
  NativeLancePackedStruct.cpp
  NativeLanceReadPlan.cpp
  NativeLanceReader.cpp
  NativeLanceStructuralDecoder.cpp
  NativeLanceTypeAdapter.cpp
  NativeLanceDecoder.cpp
  NativeLanceMetadata.cpp
  RegisterNativeLanceReader.cpp
)
target_link_libraries(
  bolt_dwio_lance_native
  PUBLIC
    bolt_dwio_common
    bolt_dwio_lance_proto
    lz4::lz4
    protobuf::libprotobuf
    zstd::libzstd_static
)

if(BOLT_BUILD_TESTING)
  add_subdirectory(tests)
endif()
