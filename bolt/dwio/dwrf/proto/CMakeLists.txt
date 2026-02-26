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

# Set up Proto file( GLOB PROTO_FILES RELATIVE ${PROJECT_SOURCE_DIR}
# ${CMAKE_CURRENT_SOURCE_DIR}/*.proto)
set(PROTO_FILES ${CMAKE_CURRENT_SOURCE_DIR}/dwrf_schema.proto
                ${CMAKE_CURRENT_SOURCE_DIR}/orc_schema.proto
)

set(PROTO_SRCS)
set(PROTO_HDRS)
foreach(proto ${PROTO_FILES})
  protobuf_generate_cpp(BOLT_PROTO_SRCS BOLT_PROTO_HDRS ${proto})
  list(APPEND PROTO_SRCS ${BOLT_PROTO_SRCS})
  list(APPEND PROTO_HDRS ${BOLT_PROTO_HDRS})
endforeach()
set(PROTO_OUTPUT_FILES ${PROTO_HDRS} ${PROTO_SRCS})
set_source_files_properties(${PROTO_OUTPUT_FILES} PROPERTIES GENERATED TRUE)

# add_custom_command( OUTPUT ${PROTO_OUTPUT_FILES} COMMAND ${Protobuf_PROTOC_EXECUTABLE}
# --proto_path ${CMAKE_SOURCE_DIR}/ --proto_path ${Protobuf_INCLUDE_DIRS} --cpp_out
# ${CMAKE_BINARY_DIR} ${PROTO_FILES_FULL} DEPENDS ${Protobuf_PROTOC_EXECUTABLE} COMMENT "Running
# proto compiler" VERBATIM) add_custom_target(dwio_proto ALL DEPENDS ${PROTO_OUTPUT_FILES})

bolt_add_library(bolt_dwio_dwrf_proto ${PROTO_SRCS} ${PROTO_HDRS})

target_link_libraries(bolt_dwio_dwrf_proto protobuf::libprotobuf)
target_include_directories(bolt_dwio_dwrf_proto PUBLIC ${CMAKE_BINARY_DIR})

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/dwrf_schema.pb.h
        ${CMAKE_CURRENT_BINARY_DIR}/orc_schema.pb.h DESTINATION "include/bolt/dwio/dwrf/proto/"
)

install(
  TARGETS bolt_dwio_dwrf_proto
          # PUBLIC_HEADER DESTINATION include/bolt/dwio/dwrf/proto/
          ARCHIVE
          DESTINATION
          ${CMAKE_INSTALL_LIBDIR}
          LIBRARY
          DESTINATION
          ${CMAKE_INSTALL_LIBDIR}
)
