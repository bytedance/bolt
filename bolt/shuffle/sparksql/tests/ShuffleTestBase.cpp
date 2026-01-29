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

#include "bolt/shuffle/sparksql/tests/ShuffleTestBase.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/sparksql/ConfigurationResolver.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/common/memory/sparksql/NativeMemoryManagerFactory.h"
#include "bolt/common/memory/sparksql/tests/MemoryTestUtils.h"
#include "bolt/core/PlanNode.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/exec/tests/utils/LocalExchangeSource.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/QueryAssertions.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/ShuffleReaderNode.h"
#include "bolt/shuffle/sparksql/ShuffleWriterNode.h"
#include "bolt/shuffle/sparksql/tests/LocalFileReaderStreamIterator.h"
#include "bolt/shuffle/sparksql/tests/MemoryReaderStreamIterator.h"
#include "bolt/shuffle/sparksql/tests/MockRssClient.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

#include <cstring>
#include <filesystem>
#include <utility>

#include <fmt/format.h>

using namespace bytedance::bolt;
using namespace bytedance::bolt::test;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::shuffle::sparksql;
using namespace bytedance::bolt::shuffle::sparksql::test;
using namespace bytedance::bolt::memory;
using namespace bytedance::bolt::memory::sparksql;
using namespace bytedance::bolt::memory::sparksql::test;

namespace bytedance::bolt::shuffle::sparksql::test {

std::string dataTypeGroupToString(DataTypeGroup group) {
  switch (group) {
    case DataTypeGroup::kInteger:
      return "Integer";
    case DataTypeGroup::kFloat:
      return "Float";
    case DataTypeGroup::kString:
      return "String";
    case DataTypeGroup::kLargeString:
      return "LargeString";
    case DataTypeGroup::kDecimal:
      return "Decimal";
    case DataTypeGroup::kDateTime:
      return "DateTime";
    case DataTypeGroup::kComplex:
      return "Complex";
    case DataTypeGroup::kMix:
      return "Mix";
    case DataTypeGroup::kHighNulls:
      return "HighNulls";
    case DataTypeGroup::kEmpty:
      return "Empty";
    default:
      BOLT_UNREACHABLE();
      return "Unknown";
  }
}

std::string writerTypeToString(PartitionWriterType t) {
  switch (t) {
    case PartitionWriterType::kLocal:
      return "Local";
    case PartitionWriterType::kCeleborn:
      return "Celeborn";
    default:
      BOLT_UNREACHABLE();
      return "Unknown";
  }
}

std::string shuffleModeToString(int mode) {
  switch (mode) {
    case 0:
      return "Adaptive";
    case 1:
      return "V1";
    case 2:
      return "V2";
    case 3:
      return "RowBased";
    default:
      BOLT_UNREACHABLE();
      return "Unknown";
  }
}

constexpr uint32_t kPidSeed = 42;

const std::vector<DataTypeGroup> dataGroups = {
    DataTypeGroup::kInteger,
    DataTypeGroup::kFloat,
    DataTypeGroup::kString,
    DataTypeGroup::kLargeString,
    DataTypeGroup::kDecimal,
    DataTypeGroup::kDateTime,
    DataTypeGroup::kComplex,
    DataTypeGroup::kMix,
    DataTypeGroup::kHighNulls,
    DataTypeGroup::kEmpty};

MockRowVector::MockRowVector(
    memory::MemoryPool* pool,
    std::shared_ptr<const Type> type,
    BufferPtr nulls,
    size_t length,
    std::vector<VectorPtr> children,
    uint64_t flatSize)
    : RowVector(pool, type, nulls, length, children), flatSize_(flatSize) {}

bool MockRowVector::isMockRowVector(const RowVectorPtr& rv) {
  return dynamic_cast<MockRowVector*>(rv.get()) != nullptr;
}

uint64_t MockRowVector::estimateFlatSize() const {
  return flatSize_;
}

std::string ShuffleTestParam::toString() const {
  static constexpr const char* units[] = {
      "B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"};
  const int maxUnit = (int)(sizeof(units) / sizeof(units[0])) - 1;

  int64_t v = memoryLimit;
  int u = 0;
  while (v >= 1024 && u < maxUnit) {
    v /= 1024;
    ++u;
  }
  auto memStr = fmt::format("{}{}", v, units[u]);

  return fmt::format(
      "{}_{}_{}_{}_M{}_P{}_{}",
      partitioning,
      shuffleModeToString(shuffleMode),
      writerTypeToString(writerType),
      dataTypeGroupToString(dataTypeGroup),
      numMappers,
      numPartitions,
      memStr);
}

bool ShuffleTestParam::isSupported() const {
  if (partitioning == Partitioning::kSingle) {
    return numPartitions == 1 && shuffleMode <= 1;
  }
  if (partitioning == "rr") {
    return shuffleMode <= 1;
  }
  return true;
}

void ShuffleTestBase::SetUpTestCase() {
  OperatorTestBase::SetUpTestCase();
  // Register Operators
  bytedance::bolt::exec::Operator::registerOperator(
      std::make_unique<SparkShuffleWriterTranslator>());
  bytedance::bolt::exec::Operator::registerOperator(
      std::make_unique<SparkShuffleReaderTranslator>());
  filesystems::registerLocalFileSystem();
}

void ShuffleTestBase::TearDownTestCase() {
  OperatorTestBase::TearDownTestCase();
}

void ShuffleTestBase::SetUp() {}

void ShuffleTestBase::TearDown() {}

ShuffleInputData ShuffleTestBase::makeInputData(const ShuffleTestParam& param) {
  ShuffleInputData data;

  VectorFuzzer::Options opts;
  opts.nullRatio = 0.1;
  opts.stringVariableLength = true;
  opts.vectorSize = param.batchSize;
  opts.timestampPrecision =
      VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;

  auto numBatches = param.numBatches;

  auto generateRandomData = [&](const RowTypePtr& rowType,
                                const VectorFuzzer::Options& opts,
                                int32_t numBatches) {
    std::vector<RowVectorPtr> batches;
    if (numBatches == 0) {
      batches.push_back(std::dynamic_pointer_cast<RowVector>(
          BaseVector::create(rowType, 0, pool())));
      return batches;
    }

    batches.reserve(numBatches);
    for (int32_t i = 0; i < numBatches; ++i) {
      VectorFuzzer fuzzer(opts, pool());
      batches.push_back(fuzzer.fuzzInputRow(rowType));
    }
    return batches;
  };

  RowTypePtr rowType;
  switch (param.dataTypeGroup) {
    case DataTypeGroup::kInteger: {
      rowType =
          ROW({"c0", "c1", "c2", "c3", "c4"},
              {BOOLEAN(), TINYINT(), SMALLINT(), INTEGER(), BIGINT()});
      break;
    }
    case DataTypeGroup::kFloat: {
      rowType = ROW({"c0", "c1"}, {REAL(), DOUBLE()});
      break;
    }
    case DataTypeGroup::kString: {
      opts.stringLength = 8;
      rowType = ROW({"c0", "c1"}, {VARCHAR(), VARBINARY()});
      break;
    }
    case DataTypeGroup::kLargeString: {
      opts.stringLength = 1024;
      rowType = ROW({"c0", "c1"}, {VARCHAR(), VARBINARY()});
      break;
    }
    case DataTypeGroup::kDecimal: {
      rowType = ROW({"c0", "c1"}, {DECIMAL(10, 2), DECIMAL(38, 18)});
      break;
    }
    case DataTypeGroup::kDateTime: {
      rowType = ROW({"c0", "c1"}, {TIMESTAMP(), DATE()});
      break;
    }
    case DataTypeGroup::kComplex: {
      rowType = ROW({"c0", "c1"}, {ARRAY(INTEGER()), MAP(VARCHAR(), BIGINT())});
      break;
    }
    case DataTypeGroup::kMix: {
      rowType =
          ROW({"c0",
               "c1",
               "c2",
               "c3",
               "c4",
               "c5",
               "c6",
               "c7",
               "c8",
               "c9",
               "c10",
               "c11",
               "c12",
               "c13",
               "c14",
               "c15"},
              {BOOLEAN(),
               TINYINT(),
               SMALLINT(),
               INTEGER(),
               BIGINT(),
               DECIMAL(10, 2),
               DECIMAL(38, 18),
               REAL(),
               DOUBLE(),
               VARCHAR(),
               VARBINARY(),
               DATE(),
               TIMESTAMP(),
               ARRAY(INTEGER()),
               MAP(VARCHAR(), BIGINT()),
               ROW({"f0", "f1"}, {INTEGER(), VARCHAR()})});
      break;
    }
    case DataTypeGroup::kHighNulls: {
      opts.nullRatio = 1;
      rowType = ROW({"c0", "c1", "c2"}, {INTEGER(), DOUBLE(), VARCHAR()});
      break;
    }
    case DataTypeGroup::kEmpty: {
      rowType = ROW({"c0", "c1"}, {BIGINT(), VARCHAR()});
      numBatches = 0;
      break;
    }
  }

  data.inputsPerMapper.reserve(param.numMappers);
  for (int i = 0; i < param.numMappers; ++i) {
    data.inputsPerMapper.push_back(
        generateRandomData(rowType, opts, numBatches));
  }
  return data;
}

size_t countRows(const std::vector<RowVectorPtr>& batches) {
  size_t count = 0;
  for (const auto& batch : batches) {
    count += batch->size();
  }
  return count;
}

RowVectorPtr ShuffleTestBase::prependPidColumn(
    const RowVectorPtr& input,
    int32_t numPartitions,
    std::mt19937& rng) {
  const auto numRows = input->size();
  const auto rowType = input->type()->asRow();
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(rowType.size() + 1);
  types.reserve(rowType.size() + 1);
  names.push_back("pid");
  types.push_back(INTEGER());
  for (size_t i = 0; i < rowType.size(); ++i) {
    names.push_back(rowType.nameOf(i));
    types.push_back(rowType.childAt(i));
  }

  std::vector<int32_t> pids(numRows);
  std::uniform_int_distribution<int32_t> dist(0, numPartitions - 1);
  for (int32_t row = 0; row < numRows; ++row) {
    pids[row] = dist(rng);
  }
  auto pidVector =
      makeFlatVector<int32_t>(numRows, [&](auto row) { return pids[row]; });

  std::vector<VectorPtr> children;
  children.reserve(input->childrenSize() + 1);
  children.push_back(pidVector);
  for (size_t i = 0; i < input->childrenSize(); ++i) {
    children.push_back(input->childAt(i));
  }
  if (MockRowVector::isMockRowVector(input)) {
    // Wrap in MockRowVector to preserve estimateFlatSize() behavior
    uint64_t flatSize = input->estimateFlatSize();
    return std::make_shared<MockRowVector>(
        pool(),
        ROW(std::move(names), std::move(types)),
        nullptr,
        numRows,
        std::move(children),
        flatSize);
  }
  return std::make_shared<RowVector>(
      pool(),
      ROW(std::move(names), std::move(types)),
      nullptr,
      numRows,
      std::move(children));
}

std::vector<RowVectorPtr> ShuffleTestBase::prependPidBatches(
    const std::vector<RowVectorPtr>& batches,
    int32_t numPartitions) {
  std::mt19937 rng(kPidSeed);
  std::vector<RowVectorPtr> withPid;
  withPid.reserve(batches.size());
  for (const auto& batch : batches) {
    withPid.push_back(prependPidColumn(batch, numPartitions, rng));
  }
  return withPid;
}

std::vector<std::vector<RowVectorPtr>> ShuffleTestBase::splitInputByPid(
    const std::vector<RowVectorPtr>& writerInput,
    int32_t numPartitions) {
  std::vector<std::vector<RowVectorPtr>> result(numPartitions);

  for (const auto& batch : writerInput) {
    auto pidVec = batch->childAt(0)->as<SimpleVector<int32_t>>();
    auto rowType = std::dynamic_pointer_cast<const RowType>(batch->type());

    // Target type is rowType without the first column
    auto targetNames = rowType->names();
    targetNames.erase(targetNames.begin());
    auto targetTypes = rowType->children();
    targetTypes.erase(targetTypes.begin());
    auto targetRowType = ROW(std::move(targetNames), std::move(targetTypes));

    std::vector<std::vector<vector_size_t>> indices(numPartitions);
    for (int i = 0; i < batch->size(); ++i) {
      int32_t p = pidVec->valueAt(i);
      if (p >= 0 && p < numPartitions) {
        indices[p].push_back(i);
      }
    }

    for (int p = 0; p < numPartitions; ++p) {
      if (indices[p].empty()) {
        continue;
      }

      auto indicesBuffer = allocateIndices(indices[p].size(), pool());
      auto rawIndices = indicesBuffer->asMutable<vector_size_t>();
      std::memcpy(
          rawIndices,
          indices[p].data(),
          indices[p].size() * sizeof(vector_size_t));

      std::vector<VectorPtr> children;
      children.reserve(batch->childrenSize() - 1);
      for (size_t c = 1; c < batch->childrenSize(); ++c) {
        children.push_back(BaseVector::wrapInDictionary(
            nullptr, indicesBuffer, indices[p].size(), batch->childAt(c)));
      }

      auto partBatch = std::make_shared<RowVector>(
          pool(),
          targetRowType,
          nullptr,
          indices[p].size(),
          std::move(children));
      result[p].push_back(partBatch);
    }
  }
  return result;
}

ShuffleRunResult ShuffleTestBase::runShuffle(
    const std::vector<std::vector<RowVectorPtr>>& inputsPerMapper,
    const RowTypePtr& outputType,
    const ShuffleTestParam& param,
    const ShuffleInputData& inputData) {
  auto memoryManagerHolder = TestMemoryManagerHolder::create(param.memoryLimit);
  ShuffleRunResult result;

  auto tempDir = exec::test::TempDirectoryPath::create();
  std::string localDir = tempDir->path + "/local_dir";
  std::filesystem::create_directories(localDir);

  std::shared_ptr<MockRssClient> mockRssClient;
  if (param.writerType == PartitionWriterType::kCeleborn) {
    mockRssClient = std::make_shared<MockRssClient>();
  }

  std::vector<ShuffleWriterMetrics> mapperMetrics;
  std::vector<std::string> mapperDataFiles;

  for (int m = 0; m < param.numMappers; ++m) {
    std::string dataFile =
        tempDir->path + "/shuffle_data_" + std::to_string(m) + ".bin";
    mapperDataFiles.push_back(dataFile);

    ShuffleWriterOptions writerOptions;
    writerOptions.partitioning = toPartitioning(param.partitioning);
    writerOptions.sort_before_repartition = false;
    writerOptions.partitionWriterOptions.numPartitions = param.numPartitions;
    writerOptions.forceShuffleWriterType = param.shuffleMode;
    writerOptions.partitionWriterOptions.partitionWriterType = param.writerType;
    writerOptions.taskAttemptId = memoryManagerHolder->taskAttemptId();
    writerOptions.partitionWriterOptions.shuffleBufferSize =
        param.shuffleBufferSize;

    if (param.writerType == PartitionWriterType::kCeleborn) {
      writerOptions.partitionWriterOptions.rssClient = mockRssClient;
    } else {
      writerOptions.partitionWriterOptions.dataFile = dataFile;
      writerOptions.partitionWriterOptions.configuredDirs = {localDir};
      writerOptions.partitionWriterOptions.numSubDirs = 1;
    }

    core::PlanNodeId writerId("writer");
    auto planBuilder = PlanBuilder();
    auto sourceNode = planBuilder.values(inputsPerMapper[m]).planNode();

    ShuffleWriterMetrics metrics;
    auto reportCallback = [&](const ShuffleWriterMetrics& m) { metrics = m; };

    auto writerNode = std::make_shared<SparkShuffleWriterNode>(
        writerId, writerOptions, reportCallback, sourceNode);

    CursorParameters params;
    params.planNode = writerNode;
    params.serialExecution = true;
    params.queryCtx = core::QueryCtx::create(
        nullptr,
        core::QueryConfig{{}},
        {},
        cache::AsyncDataCache::getInstance(),
        memoryManagerHolder->rootPool());

    auto cursor = TaskCursor::create(params);
    while (cursor->moveNext()) {
    }
    mapperMetrics.push_back(metrics);
  }

  if (!mapperMetrics.empty()) {
    result.metrics = mapperMetrics[0];
    // We aggregate totalBytesWritten for validity check
    for (size_t i = 1; i < mapperMetrics.size(); ++i) {
      result.metrics.totalBytesWritten += mapperMetrics[i].totalBytesWritten;
    }
  }

  result.partitionOutputs.resize(param.numPartitions);
  for (int i = 0; i < param.numPartitions; ++i) {
    std::shared_ptr<ReaderStreamIterator> streamIter;
    if (param.writerType == PartitionWriterType::kCeleborn) {
      auto it = mockRssClient->getData().find(i);
      if (it == mockRssClient->getData().end() || it->second.empty()) {
        continue;
      }
      streamIter = std::make_shared<MemoryReaderStreamIterator>(
          std::vector<std::vector<char>>{it->second});
    } else {
      std::vector<SegmentInfo> segments;
      for (int m = 0; m < param.numMappers; ++m) {
        const auto& metrics = mapperMetrics[m];
        if (metrics.partitionLengths.empty()) {
          continue;
        }
        int64_t length = metrics.partitionLengths[i];
        if (length == 0) {
          continue;
        }
        int64_t offset = 0;
        for (int j = 0; j < i; ++j) {
          offset += metrics.partitionLengths[j];
        }
        segments.push_back({mapperDataFiles[m], offset, length});
      }
      if (segments.empty()) {
        continue;
      }
      streamIter =
          std::make_shared<LocalFileReaderStreamIterator>(std::move(segments));
    }

    ShuffleReaderOptions readerOptions;
    readerOptions.numPartitions = param.numPartitions;
    readerOptions.forceShuffleWriterType = param.shuffleMode;
    readerOptions.partitionShortName = param.partitioning;
    readerOptions.shuffleBatchByteSize = 1024 * 1024; // 1MB

    core::PlanNodeId readerId("reader_" + std::to_string(i));
    auto readerNode = std::make_shared<SparkShuffleReaderNode>(
        readerId, outputType, readerOptions, streamIter);

    CursorParameters readerParams;
    readerParams.planNode = readerNode;
    readerParams.serialExecution = true;
    readerParams.queryCtx = core::QueryCtx::create(
        nullptr,
        core::QueryConfig{{}},
        {},
        cache::AsyncDataCache::getInstance(),
        memoryManagerHolder->rootPool());

    auto readerCursor = TaskCursor::create(readerParams);
    while (readerCursor->moveNext()) {
      auto curBatch = readerCursor->current();
      // deep copy to avoid hold shuffle reader memory
      VectorPtr copy =
          BaseVector::create(curBatch->type(), curBatch->size(), pool());
      copy->copy(curBatch.get(), 0, 0, curBatch->size());
      result.partitionOutputs[i].push_back(
          std::dynamic_pointer_cast<RowVector>(copy));
      readerCursor->current().reset();
    }
  }

  return result;
}

void ShuffleTestBase::executeTestWithCustomInput(
    const ShuffleTestParam& param,
    ShuffleInputData& inputData) {
  const bool needsPid =
      (param.partitioning == "hash" || param.partitioning == "range");

  std::vector<std::vector<RowVectorPtr>> writerInputs;
  std::vector<RowVectorPtr> allBaseBatches;

  for (const auto& mapperBatches : inputData.inputsPerMapper) {
    allBaseBatches.insert(
        allBaseBatches.end(), mapperBatches.begin(), mapperBatches.end());
    auto input = needsPid
        ? prependPidBatches(mapperBatches, param.numPartitions)
        : mapperBatches;
    writerInputs.push_back(input);
  }

  BOLT_CHECK(!allBaseBatches.empty(), "Input batches should not be empty");
  auto outputType =
      std::dynamic_pointer_cast<const RowType>(allBaseBatches[0]->type());

  auto result = runShuffle(writerInputs, outputType, param, inputData);

  int64_t totalRows = 0;
  for (const auto& batch : allBaseBatches) {
    totalRows += batch->size();
  }

  EXPECT_EQ(result.metrics.partitionLengths.size(), param.numPartitions);
  if (totalRows > 0) {
    EXPECT_GT(result.metrics.totalBytesWritten, 0);
  }

  if (needsPid) {
    std::vector<std::vector<RowVectorPtr>> expectedPartitions(
        param.numPartitions);
    for (const auto& mapperInput : writerInputs) {
      auto mapperSplit = splitInputByPid(mapperInput, param.numPartitions);
      for (int p = 0; p < param.numPartitions; ++p) {
        expectedPartitions[p].insert(
            expectedPartitions[p].end(),
            mapperSplit[p].begin(),
            mapperSplit[p].end());
      }
    }

    for (int i = 0; i < param.numPartitions; ++i) {
      assertEqualTypeAndNumRows(
          outputType,
          countRows(expectedPartitions[i]),
          result.partitionOutputs[i]);
      ASSERT_TRUE(assertEqualResults(
          expectedPartitions[i], result.partitionOutputs[i]));
    }
  } else {
    // Flatten all outputs
    std::vector<RowVectorPtr> allOutputs;
    for (const auto& partBatches : result.partitionOutputs) {
      allOutputs.insert(
          allOutputs.end(), partBatches.begin(), partBatches.end());
    }
    assertEqualTypeAndNumRows(outputType, totalRows, allOutputs);
    ASSERT_TRUE(assertEqualResults(allBaseBatches, allOutputs));
  }

  result.partitionOutputs.clear();
}

void ShuffleTestBase::executeTest(const ShuffleTestParam& param) {
  auto inputData = makeInputData(param);
  executeTestWithCustomInput(param, inputData);
}

} // namespace bytedance::bolt::shuffle::sparksql::test
