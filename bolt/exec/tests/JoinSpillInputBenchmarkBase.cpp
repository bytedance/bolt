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

#include <deque>
#include "bolt/serializers/PrestoSerializer.h"

#include "bolt/exec/tests/JoinSpillInputBenchmarkBase.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::common;
using namespace bytedance::bolt::memory;
using namespace bytedance::bolt::exec;
namespace bytedance::bolt::exec::test {
namespace {
const int numSampleVectors = 100;
} // namespace

void JoinSpillInputBenchmarkBase::setUp() {
  SpillerBenchmarkBase::setUp();
  spillConfig_.getSpillDirPathCb = [&]() -> const std::string& {
    return spillDir_;
  };
  spillConfig_.updateAndCheckSpillLimitCb = [&](uint64_t) {};
  spillConfig_.fileNamePrefix = FLAGS_bolt_benchmark_spiller_name;
  spillConfig_.writeBufferSize = FLAGS_bolt_benchmark_spiller_write_buffer_size;
  spillConfig_.executor = executor_.get();
  spillConfig_.compressionKind =
      stringToCompressionKind(FLAGS_bolt_benchmark_spiller_compression_kind);
  spillConfig_.maxSpillRunRows = 0;
  spillConfig_.fileCreateConfig = {};

  spiller_ = std::make_unique<Spiller>(
      exec::Spiller::Type::kHashJoinProbe,
      rowType_,
      HashBitRange{29, 29},
      &spillConfig_,
      FLAGS_bolt_benchmark_spiller_max_spill_file_size);
  spiller_->setPartitionsSpilled({0});
  spiller_->setSpillConfig(&spillConfig_);
}

void JoinSpillInputBenchmarkBase::run() {
  MicrosecondTimer timer(&executionTimeUs_);
  for (auto i = 0; i < numInputVectors_; ++i) {
    spiller_->spill(0, rowVectors_[i % numSampleVectors]);
  }
}

} // namespace bytedance::bolt::exec::test
