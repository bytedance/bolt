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

#include <folly/init/Init.h>
#include <gtest/gtest.h>

#include "bolt/dwio/parquet/fuzzer/ParquetFuzzerRunner.h"
#include "bolt/exec/fuzzer/FuzzerFlags.h"

int main(int argc, char** argv) {
  bytedance::bolt::fuzzer::setParquetFuzzerFlagDefaults();
  // FLAGS_v = 10;
  FLAGS_minloglevel = 1;
  FLAGS_logtostderr = true;
  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  return bytedance::bolt::dwio::fuzzer::ParquetFuzzerRunner::run();
}
