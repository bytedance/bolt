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

DEFINE_int64(
    seed,
    0,
    "Initial seed for random number generator used to reproduce previous "
    "results (0 means start with random seed).");

DEFINE_int32(steps, 0, "Number of expressions to generate and execute.");

DEFINE_int32(
    duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --steps iterations and exits.");

DEFINE_double(
    lazy_vector_generation_ratio,
    0.0,
    "Specifies the probability with which columns in the input row "
    "vector will be selected to be wrapped in lazy encoding "
    "(expressed as double from 0 to 1).");

DEFINE_double(
    null_ratio,
    0.1,
    "Chance of adding a null constant to the plan, or null value in a vector "
    "(expressed as double from 0 to 1).");
namespace bytedance::bolt::dwio::fuzzer {
namespace {
VectorFuzzer::Options getVectorFuzzerOptions() {
  VectorFuzzer::Options opts;
  opts.stringVariableLength = true;
  opts.fuzzForArrowFlatten = true;
  opts.stringLength = 100;
  opts.nullRatio = FLAGS_null_ratio;
  opts.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;
  return opts;
}

DwioFuzzer::Options getDwioFuzzerOptions() {
  DwioFuzzer::Options opts;
  opts.steps = FLAGS_steps;
  opts.durationSeconds = FLAGS_duration_sec;
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
  size_t seed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;
  DwioFuzzer(seed, getDwioFuzzerOptions(), common::FileFormat::PARQUET).go();
  return RUN_ALL_TESTS();
}
} // namespace bytedance::bolt::dwio::fuzzer
