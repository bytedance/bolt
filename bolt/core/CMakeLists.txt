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

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()

bolt_add_library(bolt_config Config.cpp Context.cpp)
target_link_libraries(bolt_config bolt_exception Folly::folly)

bolt_add_library(
  bolt_core Expressions.cpp PlanFragment.cpp PlanNode.cpp QueryConfig.cpp QueryCtx.cpp
            SimpleFunctionMetadata.cpp
)

target_link_libraries(
  bolt_core
  PUBLIC bolt_arrow_bridge
         bolt_caching
         bolt_config
         bolt_common_config
         bolt_connector
         bolt_exception
         bolt_expression_functions
         bolt_memory
         bolt_type_base
         bolt_vector
         Boost::headers
         Folly::folly
         fmt::fmt
  PRIVATE bolt_encode
)

if(${BOLT_ENABLE_TORCH})
  target_compile_definitions(bolt_core PRIVATE BOLT_HAS_TORCH=1)
  target_include_directories(bolt_core SYSTEM PRIVATE ${TORCH_INCLUDE_DIRS})
  target_link_libraries(bolt_core PUBLIC bolt_torch)
endif()

if(${BOLT_BUILD_PYTHON_PACKAGE})
  target_compile_definitions(bolt_core PRIVATE BOLT_HAS_PYTHON=1)
  target_include_directories(bolt_core SYSTEM PRIVATE ${pybind11_INCLUDE_DIRS})
  target_link_libraries(bolt_core PUBLIC bolt_python)
endif()

if(${BOLT_ENABLE_CUDF})
  target_compile_definitions(bolt_core PRIVATE BOLT_HAS_CUDF=1)
  target_include_directories(bolt_core SYSTEM PRIVATE ${cudf_INCLUDE_DIRS})
  target_link_libraries(bolt_core PRIVATE bolt_cudf_exec cudf::cudf)
endif()
