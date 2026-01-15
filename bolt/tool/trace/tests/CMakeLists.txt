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

add_executable(
  bolt_tool_trace_test
  AggregationReplayerTest.cpp
  FilterProjectReplayerTest.cpp
  HashJoinReplayerTest.cpp
  PartitionedOutputReplayerTest.cpp
  TableScanReplayerTest.cpp
  TableWriterReplayerTest.cpp
  TraceFileToolTest.cpp
)

add_test(
  NAME bolt_tool_trace_test
  COMMAND bolt_tool_trace_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

set_tests_properties(bolt_tool_trace_test PROPERTIES TIMEOUT 3000)

target_link_libraries(
  bolt_tool_trace_test
  bolt_query_trace_replayer_base
  bolt_trace_file_tool_base
  bolt_testutils
  GTest::gtest_main
  GTest::gmock
  Folly::folly
  gflags::gflags
  glog::glog
  fmt::fmt
)
