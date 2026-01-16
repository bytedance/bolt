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

add_library(bolt_link_libs INTERFACE)
target_link_libraries(
  bolt_link_libs
  INTERFACE bolt_caching
            bolt_dwio_catalog_fbhive
            bolt_dwio_common
            bolt_dwio_common_exception
            bolt_encode
            bolt_exception
            bolt_memory
            bolt_process
            bolt_serialization
            bolt_type
            bolt_type_fbhive
            bolt_vector
            Folly::folly
            fmt::fmt
)

add_subdirectory(common)
add_subdirectory(catalog)
add_subdirectory(dwrf)
add_subdirectory(orc)
add_subdirectory(txt)
add_subdirectory(parquet)
add_subdirectory(paimon)
