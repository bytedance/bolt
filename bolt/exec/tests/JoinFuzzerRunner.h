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

#include <gtest/gtest.h>

#include "bolt/common/base/SparkCompatibility.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/tests/JoinFuzzer.h"
#include "bolt/serializers/PrestoSerializer.h"
#include "bolt/serializers/UnsafeRowSerializer.h"

/// Join FuzzerRunner leverages JoinFuzzer and VectorFuzzer to
/// automatically generate and execute join tests. It works by:
///
///  1. Picking a random join type.
///  2. Generating a random set of input data (vector), with a variety of
///     encodings and data layouts.
///  3. Executing a variety of logically equivalent query plans and
///     asserting results are the same.
///  4. Rinse and repeat.
///
/// The common usage pattern is as following:
///
///  $ ./bolt_join_fuzzer_test --bolt_fuzzer_steps 10000
///
/// The important flags that control JoinFuzzer's behavior are:
///
///  --bolt_fuzzer_steps: how many iterations to run.
///  --bolt_fuzzer_duration_sec: alternatively, for how many seconds it should
///  run (takes
///          precedence over --bolt_fuzzer_steps).
///  --bolt_fuzzer_seed: pass a deterministic seed to reproduce the behavior
///  (each iteration
///          will print a seed as part of the logs).
///  --v=1: verbose logging; print a lot more details about the execution.
///  --bolt_fuzzer_batch_size: size of input vector batches generated.
///  --bolt_fuzzer_num_batches: number if input vector batches to generate.
///
/// e.g:
///
///  $ ./bolt_join_fuzzer_test \
///         --bolt_fuzzer_steps 10000 \
///         --bolt_fuzzer_seed 123 \
///         --v=1

class JoinFuzzerRunner {
 public:
  static int run(size_t seed) {
    bytedance::bolt::memory::MemoryManager::initialize({});
    if constexpr (::bytedance::bolt::kSparkCompatible) {
      bytedance::bolt::serializer::spark::UnsafeRowVectorSerde::
          registerVectorSerde();
    } else {
      bytedance::bolt::serializer::presto::PrestoVectorSerde::
          registerVectorSerde();
    }
    bytedance::bolt::filesystems::registerLocalFileSystem();

    bytedance::bolt::exec::test::joinFuzzer(seed);
    return RUN_ALL_TESTS();
  }
};
