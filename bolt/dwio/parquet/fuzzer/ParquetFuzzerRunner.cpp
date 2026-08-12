/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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
 */

#include "bolt/dwio/parquet/fuzzer/ParquetFuzzerRunner.h"
#include "bolt/exec/fuzzer/FuzzerFlags.h"
namespace bytedance::bolt::dwio::fuzzer {
namespace {
VectorFuzzer::Options getVectorFuzzerOptions() {
  VectorFuzzer::Options opts;
  opts.stringVariableLength = true;
  opts.fuzzForArrowFlatten = true;
  opts.stringLength = 100;
  opts.nullRatio = FLAGS_bolt_fuzzer_null_ratio;
  opts.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;
  return opts;
}

DwioFuzzer::Options getDwioFuzzerOptions() {
  DwioFuzzer::Options opts;
  opts.steps = FLAGS_bolt_fuzzer_steps;
  opts.durationSeconds = FLAGS_bolt_fuzzer_duration_sec;
  opts.isAssertArrowData = false;
  opts.arrowFlattenDictionary = true;
  opts.arrowFlattenConstant = true;
  opts.vectorFuzzerOptions = getVectorFuzzerOptions();
  return opts;
}
} // namespace

// static
int ParquetFuzzerRunner::run() {
  memory::MemoryManager::initialize({});
  size_t seed =
      FLAGS_bolt_fuzzer_seed == 0 ? std::time(nullptr) : FLAGS_bolt_fuzzer_seed;
  DwioFuzzer(seed, getDwioFuzzerOptions(), common::FileFormat::PARQUET).go();
  return RUN_ALL_TESTS();
}
} // namespace bytedance::bolt::dwio::fuzzer
