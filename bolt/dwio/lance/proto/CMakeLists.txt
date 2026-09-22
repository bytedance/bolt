# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

set(
  LANCE_PROTO_FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/lance_file.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/lance_file_v2.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/lance_encodings_v2_0.proto
  ${CMAKE_CURRENT_SOURCE_DIR}/lance_encodings_v2_1.proto
)

protobuf_generate_cpp(
  LANCE_PROTO_SRCS
  LANCE_PROTO_HDRS
  ${LANCE_PROTO_FILES}
)
set_source_files_properties(
  ${LANCE_PROTO_SRCS}
  ${LANCE_PROTO_HDRS}
  PROPERTIES
    GENERATED
      TRUE
)

bolt_add_library(
  bolt_dwio_lance_proto
  ${LANCE_PROTO_SRCS}
  ${LANCE_PROTO_HDRS}
)
target_link_libraries(bolt_dwio_lance_proto PUBLIC protobuf::libprotobuf)
target_include_directories(
  bolt_dwio_lance_proto
  PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
    $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
)

install(FILES ${LANCE_PROTO_HDRS} DESTINATION include/bolt/dwio/lance/proto)
