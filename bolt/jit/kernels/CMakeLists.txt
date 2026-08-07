#
# Copyright (c) ByteDance Ltd. and/or its affiliates
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

message(STATUS "LLVM_TOOLS_BINARY_DIR = ${LLVM_TOOLS_BINARY_DIR}")
find_program(LLVM_CLANG clang PATHS ${LLVM_TOOLS_BINARY_DIR} NO_DEFAULT_PATH)
if(NOT LLVM_CLANG)
  find_program(LLVM_CLANG clang REQUIRED)
endif()
message(STATUS "Using clang: ${LLVM_CLANG}")

# Verify clang version matches LLVM to ensure bitcode compatibility.
execute_process(
  COMMAND ${LLVM_CLANG} --version
  OUTPUT_VARIABLE _CLANG_VERSION_OUTPUT
  OUTPUT_STRIP_TRAILING_WHITESPACE
)
string(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]+" _CLANG_VERSION "${_CLANG_VERSION_OUTPUT}")
if(LLVM_PACKAGE_VERSION AND _CLANG_VERSION AND
    NOT _CLANG_VERSION VERSION_EQUAL LLVM_PACKAGE_VERSION)
  message(WARNING
    "Clang version (${_CLANG_VERSION}) does not match "
    "LLVM version (${LLVM_PACKAGE_VERSION}). "
    "Pre-built bitcode may have compatibility issues.")
endif()

set(KERNEL_SRC ${CMAKE_CURRENT_SOURCE_DIR}/kernels.cpp)
set(KERNEL_BC ${CMAKE_CURRENT_BINARY_DIR}/kernels.bc)
set(KERNEL_HEADER ${CMAKE_CURRENT_BINARY_DIR}/kernels_bc.h)

# Generate kernel compile flags from CMake target properties.
# Add more targets if future kernels need additional headers.
set(KERNEL_DEP_TARGETS bolt_vector)
set(KERNEL_FLAGS_RSP ${CMAKE_CURRENT_BINARY_DIR}/kernel_flags.rsp)

# Merge INCLUDE_DIRECTORIES and COMPILE_DEFINITIONS from all dep targets.
set(_INC "")
set(_DEF "")
foreach(_target ${KERNEL_DEP_TARGETS})
  list(APPEND _INC "$<TARGET_PROPERTY:${_target},INCLUDE_DIRECTORIES>")
  list(APPEND _DEF "$<TARGET_PROPERTY:${_target},COMPILE_DEFINITIONS>")
endforeach()

# Deduplicate, add -I/-D prefixes, filter empty entries, join with newlines
set(_INC_DEDUP  "$<REMOVE_DUPLICATES:${_INC}>")
set(_INC_FLAGS  "$<LIST:TRANSFORM,${_INC_DEDUP},PREPEND,-I>")

set(_DEF_DEDUP  "$<REMOVE_DUPLICATES:${_DEF}>")
set(_DEF_CLEAN  "$<LIST:FILTER,${_DEF_DEDUP},EXCLUDE,^$>")
set(_DEF_FLAGS  "$<LIST:TRANSFORM,${_DEF_CLEAN},PREPEND,-D>")

file(GENERATE OUTPUT ${KERNEL_FLAGS_RSP} CONTENT
  "$<LIST:JOIN,${_INC_FLAGS},\n>\n$<LIST:JOIN,${_DEF_FLAGS},\n>\n-DNDEBUG\n"
)

# Compile kernels.cpp to LLVM bitcode with bolt headers
add_custom_command(
  OUTPUT ${KERNEL_BC}
  COMMAND ${LLVM_CLANG} -emit-llvm -c -O2 -std=c++17
    @${KERNEL_FLAGS_RSP}
    -o ${KERNEL_BC} ${KERNEL_SRC}
  DEPENDS ${KERNEL_SRC} ${KERNEL_FLAGS_RSP}
  COMMENT "Compiling JIT kernels to LLVM bitcode"
)

# Embed bitcode as C byte array header
add_custom_command(
  OUTPUT ${KERNEL_HEADER}
  COMMAND xxd -i kernels.bc > ${KERNEL_HEADER}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  DEPENDS ${KERNEL_BC}
  COMMENT "Embedding JIT kernel bitcode as C header"
)

add_custom_target(jit_kernels_bc DEPENDS ${KERNEL_HEADER})
