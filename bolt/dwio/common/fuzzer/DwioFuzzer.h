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

#pragma once

#include <boost/random/uniform_int_distribution.hpp>
#include <chrono>
#include "bolt/dwio/common/Options.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "gtest/gtest.h"
namespace bytedance::bolt::dwio {
namespace common {
class Writer;
class Reader;
class RowReader;
class FileSink;
}; // namespace common

namespace fuzzer {
class DwioFuzzer {
 public:
  struct Options;

  DwioFuzzer(
      size_t initialSeed,
      const Options& options,
      common::FileFormat fileFormat);

  // This function starts the test that is performed by the
  // ExpressionFuzzerVerifier which is generating random expressions and
  // verifying them.
  void go();

  struct Options {
    // Number of expressions to generate and execute.
    int32_t steps = 0;

    // For how long it should run (in seconds). If zero it executes exactly
    // --bolt_fuzzer_steps iterations and exits.
    int32_t durationSeconds = 0;

    bool isAssertArrowData = false;

    // Close file after writing `maxBatchNum` batches
    int32_t maxBatchNum = 10;

    // The max number of elements on each generated vector.
    int32_t maxBatchSize = 1024;

    int32_t maxSchemaColumnNum = 30;

    // max parquet rowgroup size
    int64_t maxBlockSize = 32 * 1024 * 1024;

    bool arrowFlattenDictionary = false;

    bool arrowFlattenConstant = false;

    // Specifies the probability with which columns in the input row vector will
    // be selected to be wrapped in lazy encoding (expressed as double from 0 to
    // 1).
    double lazyVectorGenerationRatio = 0.0;

    VectorFuzzer::Options vectorFuzzerOptions;
  };

 private:
  void seed(size_t seed);

  void reSeed();

  /// Returns true n% of times (`n` is a double between 0 and 1).
  bool coinToss(double n);

  template <typename T>
  T rand(T min, T max) {
    return boost::random::uniform_int_distribution<T>(min, max)(rng_);
  }

  template <typename T>
  bool isDone(size_t i, T startTime) const;

  RowTypePtr fuzzSchema(int32_t columnNum);

  std::vector<RowVectorPtr> fuzzData(
      int32_t batchNums,
      const RowTypePtr& schema);

  void assertArrowData(const std::vector<RowVectorPtr>& data);

  void writeToFile(
      const std::string& path,
      const RowTypePtr& schema,
      const std::vector<RowVectorPtr>& data);

  std::unique_ptr<common::Writer> createFuzzerParuqetWriter(
      std::unique_ptr<common::FileSink> sink,
      RowTypePtr schema);

  std::unique_ptr<common::Reader> createLocalReader(
      const std::string& filePath);

  std::unique_ptr<common::RowReader> createRowReaderWithSchema(
      const std::unique_ptr<common::Reader> reader,
      const RowTypePtr& rowType);

  std::shared_ptr<exec::test::TempDirectoryPath> tempPath_;

  Options options_;

  std::chrono::steady_clock::time_point lastLogTime_;

  common::FileFormat fileFormat_;

  FuzzerGenerator rng_;

  size_t currentSeed_{0};

  std::shared_ptr<memory::MemoryPool> aggregatePool_;

  std::shared_ptr<memory::MemoryPool> pool_;

  std::shared_ptr<VectorFuzzer> vectorFuzzer_;
};
} // namespace fuzzer
} // namespace bytedance::bolt::dwio
