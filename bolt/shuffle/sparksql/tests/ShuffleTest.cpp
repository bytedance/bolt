#include "bolt/common/file/FileSystems.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
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

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>

using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::shuffle::sparksql;
using namespace bytedance::bolt::shuffle::sparksql::test;
using namespace bytedance::bolt::memory::sparksql;
using testing::NiceMock;

namespace {

enum class DataTypeGroup {
  kPrimitive,
  kComplex,
  kLargeString,
  kHighNulls,
  kEmpty
};

enum class MemoryPressure { kNormal, kLow };

std::string dataTypeGroupToString(DataTypeGroup group) {
  switch (group) {
    case DataTypeGroup::kPrimitive:
      return "Primitive";
    case DataTypeGroup::kComplex:
      return "Complex";
    case DataTypeGroup::kLargeString:
      return "LargeString";
    case DataTypeGroup::kHighNulls:
      return "HighNulls";
    case DataTypeGroup::kEmpty:
      return "Empty";
    default:
      return "Unknown";
  }
}

std::string memoryPressureToString(MemoryPressure pressure) {
  switch (pressure) {
    case MemoryPressure::kNormal:
      return "NormalMem";
    case MemoryPressure::kLow:
      return "LowMem";
    default:
      return "UnknownMem";
  }
}

std::string partitioningToString(Partitioning p) {
  switch (p) {
    case Partitioning::kSingle:
      return "Single";
    case Partitioning::kRoundRobin:
      return "RoundRobin";
    case Partitioning::kHash:
      return "Hash";
    case Partitioning::kRange:
      return "Range";
    default:
      return "Unknown";
  }
}

std::string partitionShortName(Partitioning p) {
  switch (p) {
    case Partitioning::kSingle:
      return "single";
    case Partitioning::kRoundRobin:
      return "rr";
    case Partitioning::kHash:
      return "hash";
    case Partitioning::kRange:
      return "range";
    default:
      return "";
  }
}

std::string writerTypeToString(PartitionWriterType t) {
  switch (t) {
    case PartitionWriterType::kLocal:
      return "Local";
    case PartitionWriterType::kCeleborn:
      return "Celeborn";
    default:
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
      return "Unknown";
  }
}

std::string keyChannelsToString(
    const std::vector<column_index_t>& keyChannels) {
  if (keyChannels.empty()) {
    return "KeysNone";
  }
  std::ostringstream out;
  out << "Keys";
  for (size_t i = 0; i < keyChannels.size(); ++i) {
    out << keyChannels[i];
    if (i + 1 != keyChannels.size()) {
      out << "_";
    }
  }
  return out.str();
}

struct ShuffleTestParam {
  Partitioning partitioning;
  int32_t shuffleMode; // 0: Adaptive, 1: V1, 2: V2, 3: RowBased
  PartitionWriterType writerType;
  DataTypeGroup dataTypeGroup;
  MemoryPressure memoryPressure;
  int32_t numPartitions;
  int32_t numRows;
  int32_t numBatches;
  bool prependPid;
  std::vector<column_index_t> keyChannels;

  friend std::ostream& operator<<(std::ostream& os, const ShuffleTestParam& p) {
    return os << partitioningToString(p.partitioning) << "_"
              << shuffleModeToString(p.shuffleMode) << "_"
              << writerTypeToString(p.writerType) << "_"
              << dataTypeGroupToString(p.dataTypeGroup) << "_"
              << memoryPressureToString(p.memoryPressure) << "_P"
              << p.numPartitions << "_R" << p.numRows << "_B" << p.numBatches
              << "_" << (p.prependPid ? "Pid" : "NoPid") << "_"
              << keyChannelsToString(p.keyChannels);
  }
};

struct ShuffleInputData {
  std::vector<RowVectorPtr> baseBatches;
  int32_t bufferSize{4096};
  int32_t shuffleBatchByteSize{4 * 1024 * 1024};
};

struct ShuffleRunResult {
  ShuffleWriterMetrics metrics;
  std::vector<std::unique_ptr<TaskCursor>> readerCursors;
  std::vector<RowVectorPtr> outputBatches;
};

std::vector<ShuffleTestParam> buildShuffleParams() {
  std::vector<ShuffleTestParam> params;
  auto add = [&](Partitioning partitioning,
                 int32_t shuffleMode,
                 PartitionWriterType writerType,
                 DataTypeGroup dataTypeGroup,
                 MemoryPressure memoryPressure,
                 int32_t numPartitions,
                 int32_t numRows,
                 int32_t numBatches,
                 bool prependPid,
                 std::vector<column_index_t> keyChannels) {
    ShuffleTestParam p{
        partitioning,
        shuffleMode,
        writerType,
        dataTypeGroup,
        memoryPressure,
        numPartitions,
        numRows,
        numBatches,
        prependPid,
        std::move(keyChannels)};
    params.push_back(std::move(p));
  };

  add(Partitioning::kSingle,
      1,
      PartitionWriterType::kLocal,
      DataTypeGroup::kPrimitive,
      MemoryPressure::kNormal,
      1,
      1000,
      1,
      false,
      {});
  add(Partitioning::kRoundRobin,
      1,
      PartitionWriterType::kLocal,
      DataTypeGroup::kPrimitive,
      MemoryPressure::kNormal,
      4,
      1000,
      3,
      false,
      {});
  add(Partitioning::kHash,
      1,
      PartitionWriterType::kLocal,
      DataTypeGroup::kPrimitive,
      MemoryPressure::kNormal,
      4,
      1000,
      1,
      true,
      {0});
  add(Partitioning::kRange,
      1,
      PartitionWriterType::kLocal,
      DataTypeGroup::kPrimitive,
      MemoryPressure::kNormal,
      4,
      1000,
      1,
      true,
      {0});
  add(Partitioning::kRoundRobin,
      2,
      PartitionWriterType::kLocal,
      DataTypeGroup::kPrimitive,
      MemoryPressure::kNormal,
      4,
      1000,
      2,
      false,
      {});
  add(Partitioning::kHash,
      2,
      PartitionWriterType::kLocal,
      DataTypeGroup::kPrimitive,
      MemoryPressure::kNormal,
      8,
      1000,
      1,
      true,
      {0});
  add(Partitioning::kHash,
      3,
      PartitionWriterType::kLocal,
      DataTypeGroup::kPrimitive,
      MemoryPressure::kNormal,
      4,
      1000,
      1,
      true,
      {0});
  add(Partitioning::kRange,
      3,
      PartitionWriterType::kLocal,
      DataTypeGroup::kPrimitive,
      MemoryPressure::kNormal,
      4,
      1000,
      1,
      true,
      {0});
  add(Partitioning::kRoundRobin,
      3,
      PartitionWriterType::kLocal,
      DataTypeGroup::kPrimitive,
      MemoryPressure::kNormal,
      4,
      1000,
      1,
      true,
      {});
  add(Partitioning::kHash,
      1,
      PartitionWriterType::kLocal,
      DataTypeGroup::kComplex,
      MemoryPressure::kNormal,
      4,
      200,
      1,
      true,
      {0});
  add(Partitioning::kRoundRobin,
      2,
      PartitionWriterType::kLocal,
      DataTypeGroup::kComplex,
      MemoryPressure::kNormal,
      4,
      200,
      1,
      false,
      {});
  add(Partitioning::kRoundRobin,
      1,
      PartitionWriterType::kLocal,
      DataTypeGroup::kHighNulls,
      MemoryPressure::kNormal,
      4,
      1000,
      1,
      false,
      {});
  add(Partitioning::kHash,
      1,
      PartitionWriterType::kLocal,
      DataTypeGroup::kLargeString,
      MemoryPressure::kLow,
      4,
      200,
      1,
      true,
      {0});
  add(Partitioning::kHash,
      2,
      PartitionWriterType::kLocal,
      DataTypeGroup::kLargeString,
      MemoryPressure::kLow,
      4,
      200,
      1,
      true,
      {0});
  add(Partitioning::kHash,
      3,
      PartitionWriterType::kLocal,
      DataTypeGroup::kLargeString,
      MemoryPressure::kLow,
      4,
      200,
      1,
      true,
      {0});
  add(Partitioning::kHash,
      1,
      PartitionWriterType::kLocal,
      DataTypeGroup::kEmpty,
      MemoryPressure::kNormal,
      4,
      0,
      1,
      true,
      {0});
  add(Partitioning::kSingle,
      1,
      PartitionWriterType::kLocal,
      DataTypeGroup::kEmpty,
      MemoryPressure::kNormal,
      1,
      0,
      1,
      false,
      {});
  add(Partitioning::kHash,
      1,
      PartitionWriterType::kCeleborn,
      DataTypeGroup::kPrimitive,
      MemoryPressure::kNormal,
      4,
      1000,
      1,
      true,
      {0});
  add(Partitioning::kRoundRobin,
      2,
      PartitionWriterType::kCeleborn,
      DataTypeGroup::kPrimitive,
      MemoryPressure::kNormal,
      4,
      1000,
      1,
      false,
      {});
  add(Partitioning::kHash,
      3,
      PartitionWriterType::kCeleborn,
      DataTypeGroup::kPrimitive,
      MemoryPressure::kNormal,
      4,
      1000,
      1,
      true,
      {0});
  add(Partitioning::kRoundRobin,
      0,
      PartitionWriterType::kLocal,
      DataTypeGroup::kPrimitive,
      MemoryPressure::kNormal,
      4,
      1000,
      2,
      false,
      {});

  return params;
}

} // namespace

class ShuffleTest : public OperatorTestBase {
 protected:
  static std::atomic<int64_t> taskAttemptIdCounter;
  static std::atomic<int32_t> suiteCounter_;
  static std::once_flag setupOnce_;

  static void SetUpTestCase() {
    suiteCounter_.fetch_add(1);
    std::call_once(setupOnce_, [] { OperatorTestBase::SetUpTestCase(); });
  }

  static void TearDownTestCase() {
    if (suiteCounter_.fetch_sub(1) == 1) {
      OperatorTestBase::TearDownTestCase();
    }
  }

  void SetUp() override {
    OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();

    // Register Operators
    bytedance::bolt::exec::Operator::registerOperator(
        std::make_unique<SparkShuffleWriterTranslator>());
    bytedance::bolt::exec::Operator::registerOperator(
        std::make_unique<SparkShuffleReaderTranslator>());

    // Initialize ExecutionMemoryPool
    if (!ExecutionMemoryPool::inited()) {
      ExecutionMemoryPool::init(true, 1L * 1024 * 1024 * 1024, 1, {}, 1000);
    }
  }

  void TearDown() override {
    waitForAllTasksToBeDeleted();
    testingShutdownLocalExchangeSource();
    pool_.reset();
    rootPool_.reset();
  }

  ShuffleInputData makeInputData(const ShuffleTestParam& param) {
    ShuffleInputData data;
    data.bufferSize =
        (param.memoryPressure == MemoryPressure::kLow) ? 256 : 4096;
    data.shuffleBatchByteSize = (param.memoryPressure == MemoryPressure::kLow)
        ? 256 * 1024
        : 4 * 1024 * 1024;

    VectorFuzzer::Options opts;
    opts.nullRatio = 0.1;
    opts.stringVariableLength = true;
    opts.stringLength = 20;

    RowTypePtr rowType;
    switch (param.dataTypeGroup) {
      case DataTypeGroup::kPrimitive:
        rowType =
            ROW({"c0", "c1", "c2", "c3", "c4"},
                {INTEGER(), BIGINT(), VARCHAR(), BOOLEAN(), DOUBLE()});
        break;
      case DataTypeGroup::kComplex:
        rowType =
            ROW({"c0", "c1", "c2"},
                {INTEGER(), ARRAY(VARCHAR()), MAP(INTEGER(), VARCHAR())});
        break;
      case DataTypeGroup::kLargeString:
        rowType = ROW({"c0"}, {VARCHAR()});
        opts.stringLength = 1024;
        if (param.memoryPressure == MemoryPressure::kNormal) {
          data.bufferSize = 1024;
        }
        break;
      case DataTypeGroup::kHighNulls:
        rowType = ROW({"c0", "c1"}, {INTEGER(), VARCHAR()});
        opts.nullRatio = 1.0;
        break;
      case DataTypeGroup::kEmpty:
        rowType = ROW({"c0"}, {INTEGER()});
        break;
    }

    const int32_t numRows =
        (param.dataTypeGroup == DataTypeGroup::kEmpty) ? 0 : param.numRows;
    const int32_t numBatches =
        (numRows == 0) ? 1 : std::max(1, param.numBatches);

    if (numRows == 0) {
      data.baseBatches.push_back(std::dynamic_pointer_cast<RowVector>(
          BaseVector::create(rowType, 0, pool())));
      return data;
    }

    const int32_t baseSize = numRows / numBatches;
    const int32_t remainder = numRows % numBatches;
    data.baseBatches.reserve(numBatches);
    for (int32_t i = 0; i < numBatches; ++i) {
      opts.vectorSize = baseSize + ((i < remainder) ? 1 : 0);
      VectorFuzzer fuzzer(opts, pool());
      data.baseBatches.push_back(fuzzer.fuzzInputRow(rowType));
    }
    return data;
  }

  RowVectorPtr prependPidColumn(
      const RowVectorPtr& input,
      const std::vector<column_index_t>& keyChannels,
      int32_t numPartitions) {
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

    auto rows = materialize(input);
    auto pidVector = makeFlatVector<int32_t>(numRows, [&](auto row) {
      uint64_t hash = 0;
      if (keyChannels.empty()) {
        hash = static_cast<uint64_t>(row);
      } else {
        for (auto idx : keyChannels) {
          BOLT_CHECK(
              idx < rows[row].size(), "Key column index out of range: {}", idx);
          hash = hash * 31 + rows[row][idx].hash();
        }
      }
      if (numPartitions == 0) {
        return 0;
      }
      return static_cast<int32_t>(hash % numPartitions);
    });

    std::vector<VectorPtr> children;
    children.reserve(input->childrenSize() + 1);
    children.push_back(pidVector);
    for (size_t i = 0; i < input->childrenSize(); ++i) {
      children.push_back(input->childAt(i));
    }
    return std::make_shared<RowVector>(
        pool(),
        ROW(std::move(names), std::move(types)),
        nullptr,
        numRows,
        std::move(children));
  }

  std::vector<RowVectorPtr> prependPidBatches(
      const std::vector<RowVectorPtr>& batches,
      const std::vector<column_index_t>& keyChannels,
      int32_t numPartitions) {
    std::vector<RowVectorPtr> withPid;
    withPid.reserve(batches.size());
    for (const auto& batch : batches) {
      withPid.push_back(prependPidColumn(batch, keyChannels, numPartitions));
    }
    return withPid;
  }

  ShuffleRunResult runShuffle(
      const std::vector<RowVectorPtr>& writerInput,
      const RowTypePtr& outputType,
      const ShuffleTestParam& param,
      const ShuffleInputData& inputData) {
    ShuffleRunResult result;

    auto tempDir = exec::test::TempDirectoryPath::create();
    std::string dataFile = tempDir->path + "/shuffle_data.bin";
    std::string localDir = tempDir->path + "/local_dir";
    std::filesystem::create_directories(localDir);

    ShuffleWriterOptions writerOptions;
    writerOptions.bufferSize = inputData.bufferSize;
    writerOptions.partitioning = param.partitioning;
    writerOptions.sort_before_repartition = param.prependPid;
    writerOptions.partitionWriterOptions.numPartitions = param.numPartitions;
    writerOptions.forceShuffleWriterType = param.shuffleMode;
    writerOptions.partitionWriterOptions.partitionWriterType = param.writerType;
    writerOptions.partitionWriterOptions.shuffleBufferSize =
        inputData.shuffleBatchByteSize;
    writerOptions.taskAttemptId = taskAttemptIdCounter++;

    std::shared_ptr<NiceMock<MockRssClient>> mockRssClient;
    if (param.writerType == PartitionWriterType::kCeleborn) {
      mockRssClient = std::make_shared<NiceMock<MockRssClient>>();
      mockRssClient->delegateToFake();
      writerOptions.partitionWriterOptions.rssClient = mockRssClient;
    } else {
      writerOptions.partitionWriterOptions.dataFile = dataFile;
      writerOptions.partitionWriterOptions.configuredDirs = {localDir};
      writerOptions.partitionWriterOptions.numSubDirs = 1;
    }

    core::PlanNodeId writerId("writer");
    auto planBuilder = PlanBuilder();
    auto sourceNode = planBuilder.values(writerInput).planNode();

    ShuffleWriterMetrics metrics;
    auto reportCallback = [&](const ShuffleWriterMetrics& m) { metrics = m; };

    auto writerNode = std::make_shared<SparkShuffleWriterNode>(
        writerId, writerOptions, reportCallback, sourceNode);

    CursorParameters params;
    params.planNode = writerNode;
    params.serialExecution = true;
    params.queryCtx = core::QueryCtx::create();

    auto cursor = TaskCursor::create(params);
    while (cursor->moveNext()) {
    }

    result.metrics = metrics;

    for (int i = 0; i < param.numPartitions; ++i) {
      std::shared_ptr<ReaderStreamIterator> streamIter;
      if (param.writerType == PartitionWriterType::kCeleborn) {
        auto it = mockRssClient->data_.find(i);
        if (it == mockRssClient->data_.end() || it->second.empty()) {
          continue;
        }
        streamIter = std::make_shared<MemoryReaderStreamIterator>(
            std::vector<std::vector<char>>{it->second});
      } else {
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
        SegmentInfo seg{dataFile, offset, length};
        streamIter = std::make_shared<LocalFileReaderStreamIterator>(
            std::vector<SegmentInfo>{seg});
      }

      ShuffleReaderOptions readerOptions;
      readerOptions.shuffleBatchByteSize = inputData.shuffleBatchByteSize;
      readerOptions.numPartitions = param.numPartitions;
      readerOptions.forceShuffleWriterType = param.shuffleMode;
      readerOptions.partitionShortName = partitionShortName(param.partitioning);

      core::PlanNodeId readerId("reader_" + std::to_string(i));
      auto readerNode = std::make_shared<SparkShuffleReaderNode>(
          readerId, outputType, readerOptions, streamIter);

      CursorParameters readerParams;
      readerParams.planNode = readerNode;
      readerParams.serialExecution = true;
      readerParams.queryCtx = core::QueryCtx::create();

      auto readerCursor = TaskCursor::create(readerParams);
      while (readerCursor->moveNext()) {
        result.outputBatches.push_back(readerCursor->current());
        readerCursor->current().reset();
      }
      result.readerCursors.push_back(std::move(readerCursor));
    }

    return result;
  }

  void executeTestWithInput(
      const ShuffleTestParam& param,
      const ShuffleInputData& inputData) {
    if (param.partitioning == Partitioning::kSingle) {
      if (param.shuffleMode == 2 || param.shuffleMode == 3) {
        GTEST_SKIP() << "Single partitioning not supported for V2/RowBased";
      }
    }
    if ((param.partitioning == Partitioning::kHash ||
         param.partitioning == Partitioning::kRange) &&
        !param.prependPid) {
      GTEST_SKIP() << "Hash/Range partitioning requires PID column";
    }
    if (param.shuffleMode == 3 && !param.prependPid) {
      GTEST_SKIP() << "RowBased shuffle requires PID column";
    }

    const auto& baseBatches = inputData.baseBatches;
    BOLT_CHECK(!baseBatches.empty(), "Input batches should not be empty");

    auto writerInput = param.prependPid
        ? prependPidBatches(baseBatches, param.keyChannels, param.numPartitions)
        : baseBatches;
    auto outputType =
        std::dynamic_pointer_cast<const RowType>(baseBatches[0]->type());

    auto result = runShuffle(writerInput, outputType, param, inputData);

    int64_t totalRows = 0;
    for (const auto& batch : baseBatches) {
      totalRows += batch->size();
    }

    EXPECT_EQ(result.metrics.partitionLengths.size(), param.numPartitions);
    if (totalRows > 0) {
      EXPECT_GT(result.metrics.totalBytesWritten, 0);
    }

    assertEqualTypeAndNumRows(outputType, totalRows, result.outputBatches);
    ASSERT_TRUE(assertEqualResults(baseBatches, result.outputBatches));
    result.outputBatches.clear();
    result.readerCursors.clear();
  }

  void executeTest(const ShuffleTestParam& param) {
    auto inputData = makeInputData(param);
    executeTestWithInput(param, inputData);
  }
};

std::atomic<int64_t> ShuffleTest::taskAttemptIdCounter{0};
std::atomic<int32_t> ShuffleTest::suiteCounter_{0};
std::once_flag ShuffleTest::setupOnce_;

class ShuffleTestP : public ShuffleTest,
                     public testing::WithParamInterface<ShuffleTestParam> {};

TEST_P(ShuffleTestP, RoundTrip) {
  executeTest(GetParam());
}

INSTANTIATE_TEST_SUITE_P(
    ShuffleMatrix,
    ShuffleTestP,
    testing::ValuesIn(buildShuffleParams()),
    [](const testing::TestParamInfo<ShuffleTestParam>& info) {
      std::stringstream ss;
      ss << info.param;
      return ss.str();
    });

TEST_F(ShuffleTest, HashPartitionMultiKey) {
  ShuffleTestParam param{
      Partitioning::kHash,
      1,
      PartitionWriterType::kLocal,
      DataTypeGroup::kPrimitive,
      MemoryPressure::kNormal,
      4,
      500,
      2,
      true,
      {0, 2}};
  executeTest(param);
}

TEST_F(ShuffleTest, SharedDatasetAcrossModes) {
  ShuffleTestParam base{
      Partitioning::kHash,
      1,
      PartitionWriterType::kLocal,
      DataTypeGroup::kPrimitive,
      MemoryPressure::kNormal,
      4,
      1000,
      2,
      true,
      {0}};

  auto inputData = makeInputData(base);
  for (int mode : {1, 2, 3}) {
    auto param = base;
    param.shuffleMode = mode;
    executeTestWithInput(param, inputData);
  }
}
