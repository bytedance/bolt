/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <utility>
#include "bolt/common/file/FileSystems.h"
#include "bolt/tool/trace/OperatorReplayerBase.h"

#include <folly/executors/IOThreadPoolExecutor.h>
#include "bolt/exec/TaskTraceReader.h"

DECLARE_string(bolt_trace_replay_root_dir);
DECLARE_bool(bolt_trace_replay_summary);
DECLARE_bool(bolt_trace_replay_short_summary);
DECLARE_string(bolt_trace_replay_query_id);
DECLARE_string(bolt_trace_replay_task_id);
DECLARE_string(bolt_trace_replay_node_id);
DECLARE_string(bolt_trace_replay_driver_ids);
DECLARE_string(bolt_trace_replay_table_writer_output_dir);
DECLARE_double(bolt_trace_replay_hive_connector_executor_hw_multiplier);
DECLARE_int32(bolt_trace_replay_shuffle_serialization_format);
DECLARE_uint64(bolt_trace_replay_query_memory_capacity_mb);
DECLARE_double(bolt_trace_replay_driver_cpu_executor_hw_multiplier);
DECLARE_string(bolt_trace_replay_memory_arbitrator_type);
DECLARE_bool(bolt_trace_replay_copy_results);
DECLARE_string(bolt_trace_replay_function_prefix);
namespace bytedance::bolt::tool::trace {

/// The trace replay runner. It is configured through a set of gflags passed
/// from replayer tool command line.
class TraceReplayRunner {
 public:
  TraceReplayRunner();
  virtual ~TraceReplayRunner() = default;

  /// Initializes the trace replay runner by setting the bolt runtime
  /// environment for the trace replay. It is invoked before run().
  virtual void init();

  /// Runs the trace replay with a set of gflags passed from replayer tool.
  virtual void run();

 protected:
  std::unique_ptr<tool::trace::OperatorReplayerBase> createReplayer() const;

  const std::unique_ptr<folly::CPUThreadPoolExecutor> cpuExecutor_;
  const std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  std::shared_ptr<filesystems::FileSystem> fs_;
  std::unique_ptr<exec::trace::TaskTraceMetadataReader>
      taskTraceMetadataReader_;
  core::PlanNodePtr tracePlanNode_;
};

} // namespace bytedance::bolt::tool::trace
