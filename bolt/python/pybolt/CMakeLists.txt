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
#
# --------------------------------------------------------------------------
# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0
#
# This file has been modified by ByteDance Ltd. and/or its affiliates on
# 2026-10-10.
#
# Original file was released under the Apache License 2.0,
# with the full license text available at:
#     http://www.apache.org/licenses/LICENSE-2.0
#
# This modified file is released under the same license.
# --------------------------------------------------------------------------

message("Creating pybolt module")

# Define our Python module:
pybind11_add_module(
  pybolt
  MODULE
  arrowAdapter.cpp
  context.cpp
  dataType.cpp
  executor.cpp
  fileFormat.cpp
  fileSystem.cpp
  joinType.cpp
  planBuilder.cpp
  planFragment.cpp
  planNode.cpp
  planNodeId.cpp
  pybolt.cpp
  sortOrder.cpp
  split.cpp
  substrait.cpp
  tpch.cpp
  vector.cpp
)

# Wheel builds select BOLT_TEST_LINKAGE=static so the wheel contains
# its Bolt implementation. Explicit shared linkage requires the matching
# Bolt engine and testutils shared libraries in the runtime environment.
# Use the public facade to honor either selection without duplicating objects.
target_link_libraries(
  pybolt
  PRIVATE
    ${PYTHON_LIBRARIES}
    ${Arrow_LIBRARIES}
    pybind11::module
    bolt_testutils
)

# Include generated Substrait protobuf headers for substrait.cpp
target_include_directories(pybolt PRIVATE ${CMAKE_BINARY_DIR}/gen_cpp/)

# Only export the Python module init symbol. In the default static build,
# protobuf 3.21.4 is included via the Bolt engine, while pyarrow embeds
# protobuf 5.29.5 with an incompatible ABI. Restricting exports prevents
# those internal symbols from colliding under -flat_namespace. This does
# not control symbols exported by external DSOs in explicit shared builds.
if(APPLE)
  file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/pybolt_exports.txt "_PyInit_pybolt\n")
  target_link_options(
    pybolt
    PRIVATE
      "-Wl,-exported_symbols_list,${CMAKE_CURRENT_BINARY_DIR}/pybolt_exports.txt"
  )
elseif(UNIX)
  file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/pybolt.map "{ global: PyInit_pybolt; local: *; };\n")
  target_link_options(pybolt PRIVATE "-Wl,--version-script=${CMAKE_CURRENT_BINARY_DIR}/pybolt.map")
endif()

# Use the compiler's shared C++ and exception runtimes. A private static libgcc,
# hidden by the export map above, can conflict with shared libstdc++ during
# exception unwinding. Keep the Linux compiler runtime directory in the rpath;
# macOS uses libc++ from the SDK.
if(NOT APPLE)
  execute_process(
    COMMAND
      ${CMAKE_CXX_COMPILER} -print-file-name=libstdc++.so.6
    OUTPUT_VARIABLE STDLIBCXX
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  execute_process(
    COMMAND
      dirname ${STDLIBCXX}
    OUTPUT_VARIABLE STDLIBCXX_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  link_directories(${STDLIBCXX_DIR})
  set_target_properties(
    pybolt
    PROPERTIES
      BUILD_RPATH
        "${STDLIBCXX_DIR}"
      INSTALL_RPATH
        "${STDLIBCXX_DIR}"
      # Bake the install rpath at link time. Without this CMake's install
      # step tries to relink to swap BUILD_RPATH→INSTALL_RPATH, but the
      # Ninja generator only supports that on ELF/XCOFF platforms — and
      # CMake's platform detection occasionally trips on nested-docker /
      # non-standard sysroot setups (saw "is not supported with the Ninja
      # generator unless on an ELF-based or XCOFF-based platform" inside
      # the boltml dev container). Since BUILD_RPATH and INSTALL_RPATH
      # carry the same value here, baking the install rpath upfront has
      # zero behavioural difference but skips the relink step entirely.
      BUILD_WITH_INSTALL_RPATH
        TRUE
      LINK_FLAGS
        "-Wl,-rpath,${STDLIBCXX_DIR} -Wl,--disable-new-dtags"
  )
endif()

target_compile_definitions(pybolt PRIVATE BOLT_HAS_PYTHON=1)
if(SKBUILD)
  install(
    TARGETS
      pybolt
    LIBRARY
      DESTINATION pybolt
      COMPONENT bolt_python_wheel
    RUNTIME
      DESTINATION pybolt
      COMPONENT bolt_python_wheel
  )
else()
  install(TARGETS pybolt LIBRARY DESTINATION .)
endif()
