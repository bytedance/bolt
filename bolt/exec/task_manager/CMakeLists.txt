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
# 2026-04-07.
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

bolt_add_library(
  bolt_exec_task_manager
  TaskManager.cpp
  BoltVectorQueue.cpp
  Utils.cpp
  WrappedTask.cpp
)

target_link_libraries(
  bolt_exec_task_manager
  bolt_core
  bolt_exception
  bolt_hive_connector
  bolt_type
  bolt_vector
  bolt_arrow_bridge
  arrow::arrow
  bolt_exec
  bolt_dwio_common
)

if(BOLT_ENABLE_PARQUET)
  target_link_libraries(bolt_exec_task_manager bolt_dwio_parquet_reader)
endif()

if(BOLT_ENABLE_HDFS)
  target_link_libraries(bolt_exec_task_manager bolt_hdfs)
endif()
