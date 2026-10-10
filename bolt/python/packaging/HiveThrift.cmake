# Copyright (c) ByteDance Ltd. and/or its affiliates.
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

# Reuse the compiler supplied by Conan for Parquet's Thrift bindings.
find_program(THRIFT_COMPILER thrift REQUIRED)

set(BOLT_HIVE_THRIFT_DOWNLOAD_DIR "${CMAKE_CURRENT_BINARY_DIR}/hive_thrift_idl/downloads")
set(BOLT_HIVE_THRIFT_SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/hive_thrift_idl/source")
file(
  MAKE_DIRECTORY
    "${BOLT_HIVE_THRIFT_DOWNLOAD_DIR}"
    "${BOLT_HIVE_THRIFT_SOURCE_DIR}/share/fb303/if"
)

function(bolt_prepare_hive_idl relative_path url sha256 python_namespace)
  set(download "${BOLT_HIVE_THRIFT_DOWNLOAD_DIR}/${sha256}.thrift")
  message(STATUS "Hive Thrift IDL: ${url}")
  # CMake reuses an existing file only when its hash matches, without networking.
  file(
    DOWNLOAD
      "${url}"
    "${download}"
    EXPECTED_HASH "SHA256=${sha256}"
    TLS_VERIFY ON
    TIMEOUT 60
    INACTIVITY_TIMEOUT 20
  )

  # Explicit namespaces are required for Thrift's Python package_prefix option.
  # Keep the downloaded originals intact so their hashes remain verifiable.
  file(READ "${download}" source)
  string(
    REPLACE
    "namespace java "
    "namespace py ${python_namespace}\nnamespace java "
    source
    "${source}"
  )
  file(CONFIGURE OUTPUT "${BOLT_HIVE_THRIFT_SOURCE_DIR}/${relative_path}" CONTENT "${source}" @ONLY)
endfunction()

bolt_prepare_hive_idl(
  "hive_metastore.thrift"
  "https://raw.githubusercontent.com/apache/hive/92dd0159f440ca7863be3232f3a683a510a62b9d/metastore/if/hive_metastore.thrift"
  "6e35b250523ad17ff62de2f38ca39f73d8a9b4659003d4e0295b2719e2942ca2"
  "hive_metastore"
)
bolt_prepare_hive_idl(
  "share/fb303/if/fb303.thrift"
  "https://raw.githubusercontent.com/apache/thrift/53dd39833a08ce33582e5ff31fa18bb4735d6731/contrib/fb303/if/fb303.thrift"
  "925771d4a9a4288ec95926ccc3f7a8feab68368171ae9efe34bacf21703f8547"
  "fb303"
)

if(SKBUILD AND NOT SKBUILD_STATE STREQUAL "editable")
  set(BOLT_HIVE_THRIFT_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/hive_thrift")
else()
  # Keep generated Python visible to IDEs during editable/source development.
  get_filename_component(
    BOLT_HIVE_THRIFT_OUTPUT_DIR
    "${CMAKE_CURRENT_LIST_DIR}/../boltml/_generated"
    ABSOLUTE
  )
endif()

set(
  BOLT_HIVE_THRIFT_PYTHON_FILES
  "${BOLT_HIVE_THRIFT_OUTPUT_DIR}/__init__.py"
  "${BOLT_HIVE_THRIFT_OUTPUT_DIR}/fb303/__init__.py"
  "${BOLT_HIVE_THRIFT_OUTPUT_DIR}/fb303/constants.py"
  "${BOLT_HIVE_THRIFT_OUTPUT_DIR}/fb303/ttypes.py"
  "${BOLT_HIVE_THRIFT_OUTPUT_DIR}/fb303/FacebookService.py"
  "${BOLT_HIVE_THRIFT_OUTPUT_DIR}/hive_metastore/__init__.py"
  "${BOLT_HIVE_THRIFT_OUTPUT_DIR}/hive_metastore/constants.py"
  "${BOLT_HIVE_THRIFT_OUTPUT_DIR}/hive_metastore/ttypes.py"
  "${BOLT_HIVE_THRIFT_OUTPUT_DIR}/hive_metastore/ThriftHiveMetastore.py"
)
add_custom_command(
  OUTPUT
    ${BOLT_HIVE_THRIFT_PYTHON_FILES}
  BYPRODUCTS
    "${BOLT_HIVE_THRIFT_OUTPUT_DIR}/fb303/FacebookService-remote"
    "${BOLT_HIVE_THRIFT_OUTPUT_DIR}/hive_metastore/ThriftHiveMetastore-remote"
  COMMAND
    ${CMAKE_COMMAND} -E make_directory "${BOLT_HIVE_THRIFT_OUTPUT_DIR}"
  COMMAND
    "${THRIFT_COMPILER}" -r --gen py:package_prefix=boltml._generated. -out
    "${BOLT_HIVE_THRIFT_OUTPUT_DIR}" "${BOLT_HIVE_THRIFT_SOURCE_DIR}/hive_metastore.thrift"
  DEPENDS
    "${BOLT_HIVE_THRIFT_SOURCE_DIR}/hive_metastore.thrift"
    "${BOLT_HIVE_THRIFT_SOURCE_DIR}/share/fb303/if/fb303.thrift"
  COMMENT "Generating Hive Metastore Python bindings"
  VERBATIM
)
add_custom_target(generate_hive_thrift DEPENDS ${BOLT_HIVE_THRIFT_PYTHON_FILES})
add_dependencies(pybolt generate_hive_thrift)

if(SKBUILD)
  install(
    DIRECTORY
      "${BOLT_HIVE_THRIFT_OUTPUT_DIR}/"
    DESTINATION boltml/_generated
    COMPONENT bolt_python_wheel
    FILES_MATCHING
    PATTERN "*.py"
  )

  set(BOLT_PYTHON_LICENSE_DIR "${CMAKE_SOURCE_DIR}")
  if(NOT EXISTS "${BOLT_PYTHON_LICENSE_DIR}/LICENSE")
    # Source distributions place licenses beside the native/ source tree.
    get_filename_component(BOLT_PYTHON_LICENSE_DIR "${CMAKE_SOURCE_DIR}" DIRECTORY)
  endif()
  install(
    FILES
      "${BOLT_PYTHON_LICENSE_DIR}/LICENSE"
      "${BOLT_PYTHON_LICENSE_DIR}/NOTICE.txt"
    DESTINATION "${SKBUILD_METADATA_DIR}/licenses"
    COMPONENT bolt_python_wheel
  )
  install(
    FILES
      "${CMAKE_CURRENT_LIST_DIR}/../boltml/third_party/hive/NOTICE.txt"
    DESTINATION "${SKBUILD_METADATA_DIR}/licenses/hive"
    COMPONENT bolt_python_wheel
  )
endif()
