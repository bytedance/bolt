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

#include "bolt/exec/tests/AggregateSpillBenchmarkBase.h"
#include "bolt/serializers/PrestoSerializer.h"

#include <gflags/gflags.h>
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  serializer::presto::PrestoVectorSerde::registerVectorSerde();
  filesystems::registerLocalFileSystem();

  auto spillerTypeName = FLAGS_spiller_benchmark_spiller_type;
  std::transform(
      spillerTypeName.begin(),
      spillerTypeName.end(),
      spillerTypeName.begin(),
      [](unsigned char c) { return std::toupper(c); });
  Spiller::Type spillerType;
  if (spillerTypeName == Spiller::typeName(Spiller::Type::kAggregateInput)) {
    spillerType = Spiller::Type::kAggregateInput;
  } else if (
      spillerTypeName == Spiller::typeName(Spiller::Type::kAggregateOutput)) {
    spillerType = Spiller::Type::kAggregateOutput;
  } else {
    BOLT_UNSUPPORTED(
        "The spiller type {} is not one of [AGGREGATE_INPUT, AGGREGATE_OUTPUT], the aggregate spiller dose not support it.",
        spillerTypeName);
  }
  auto test =
      std::make_unique<exec::test::AggregateSpillBenchmarkBase>(spillerType);
  test->setUp();
  test->run();
  test->printStats();
  test->cleanup();

  return 0;
}
