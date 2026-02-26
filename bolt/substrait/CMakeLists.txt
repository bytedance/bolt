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

# Set up Proto

# Find the protobuf compiler
if(NOT Protobuf_PROTOC_EXECUTABLE)
  message(FATAL_ERROR "Protobuf compiler not found")
endif()

message(
  STATUS
    "Found Protobuf compiler: ${Protobuf_PROTOC_EXECUTABLE} (found version ${Protobuf_VERSION})"
)

message(STATUS "Proto directory path: ${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(SUBSTRAIT_PROTO_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/proto/substrait)
set(PROTO_OUTPUT_DIR "${CMAKE_BINARY_DIR}/gen_cpp")
file(MAKE_DIRECTORY ${PROTO_OUTPUT_DIR})

# Glob all the .proto files
file(GLOB PROTO_FILES ${SUBSTRAIT_PROTO_DIRECTORY}/*.proto
     ${SUBSTRAIT_PROTO_DIRECTORY}/extensions/*.proto ${SUBSTRAIT_PROTO_DIRECTORY}/bolt/*.proto
)

set(PROTO_SRCS "")
set(PROTO_HDRS "")

foreach(proto ${PROTO_FILES})
  file(RELATIVE_PATH REL_PROTO ${SUBSTRAIT_PROTO_DIRECTORY} ${proto})
  string(REGEX REPLACE "\\.proto" "" PROTO_NAME ${REL_PROTO})

  set(PROTO_SRC "${PROTO_OUTPUT_DIR}/bolt/substrait/proto/substrait/${PROTO_NAME}.pb.cc")
  set(PROTO_HDR "${PROTO_OUTPUT_DIR}/bolt/substrait/proto/substrait/${PROTO_NAME}.pb.h")

  list(APPEND PROTO_SRCS ${PROTO_SRC})
  list(APPEND PROTO_HDRS ${PROTO_HDR})
  message(STATUS "Compiling proto file: ${proto}")
  # Generate commands for each .proto file
  add_custom_command(
    OUTPUT ${PROTO_SRC} ${PROTO_HDR}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE} ARGS --proto_path=${PROJECT_SOURCE_DIR}/
            --cpp_out=${PROTO_OUTPUT_DIR} ${proto}
    DEPENDS ${proto}
    COMMENT "Running proto compiler for ${proto}"
    VERBATIM
  )
endforeach()

set(PROTO_OUTPUT_FILES ${PROTO_HDRS} ${PROTO_SRCS})
set_source_files_properties(${PROTO_OUTPUT_FILES} PROPERTIES GENERATED TRUE)

# Custom target to handle all the generated .pb.cc and .pb.h files
add_custom_target(
  substrait_proto ALL
  DEPENDS ${PROTO_OUTPUT_FILES}
  COMMENT "Generating substrait proto files"
)
add_dependencies(substrait_proto protobuf::libprotobuf)

set(SRCS
    ${PROTO_SRCS}
    SubstraitParser.cpp
    SubstraitToBoltExpr.cpp
    SubstraitToBoltPlan.cpp
    TypeUtils.cpp
    SubstraitExtensionCollector.cpp
    BoltToSubstraitExpr.cpp
    BoltToSubstraitPlan.cpp
    BoltToSubstraitType.cpp
    BoltSubstraitSignature.cpp
    VariantToVectorConverter.cpp
)

bolt_add_library(bolt_substrait_plan_converter ${SRCS})
target_include_directories(bolt_substrait_plan_converter PUBLIC ${PROTO_OUTPUT_DIR}/)
target_link_libraries(bolt_substrait_plan_converter bolt_connector bolt_dwio_dwrf_common)

if(${BOLT_ENABLE_TORCH})
  target_compile_definitions(bolt_substrait_plan_converter PRIVATE BOLT_HAS_TORCH=1)
  target_include_directories(bolt_substrait_plan_converter SYSTEM PRIVATE ${TORCH_INCLUDE_DIRS})
  target_link_libraries(bolt_substrait_plan_converter bolt_torch)
endif()

# Force export this library
add_custom_target(
  dummy_substrait_target
  DEPENDS bolt_substrait_plan_converter
  COMMENT "Exporting substrait plan converter library"
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
