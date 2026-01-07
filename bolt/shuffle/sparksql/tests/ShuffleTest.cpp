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
#include <random>
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
  kInteger,
  kFloat,
  kString,
  kLargeString,
  kDecimal,
  kDateTime,
  kComplex,
  kMix,
  kHighNulls,
  kEmpty
};

std::vector<DataTypeGroup> dataGroups = {
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

constexpr int32_t kBatchSize = 100;
constexpr int32_t kNumBatches = 10;
constexpr uint32_t kPidSeed = 42;

bool needsPidColumn(Partitioning partitioning) {
  return partitioning == Partitioning::kHash ||
      partitioning == Partitioning::kRange;
}

struct ShuffleTestParam {
  std::string partitioning;
  int32_t shuffleMode; // 0: Adaptive, 1: V1, 2: V2, 3: RowBased
  PartitionWriterType writerType;
  DataTypeGroup dataTypeGroup;
  int32_t numPartitions;

  std::string toString() const {
    return fmt::format(
        "{}_{}_{}_{}_P{}",
        partitioning,
        shuffleModeToString(shuffleMode),
        writerTypeToString(writerType),
        dataTypeGroupToString(dataTypeGroup),
        numPartitions);
  }

  bool isSupported() const {
    if (partitioning == Partitioning::kSingle) {
      return numPartitions == 1 && shuffleMode <= 1;
    }
    return true;
  }
};

struct ShuffleInputData {
  std::vector<RowVectorPtr> baseBatches;
};

struct ShuffleRunResult {
  ShuffleWriterMetrics metrics;
  std::vector<std::unique_ptr<TaskCursor>> readerCursors;
  std::vector<RowVectorPtr> outputBatches;
};

std::vector<ShuffleTestParam> buildShuffleParams() {
  std::vector<ShuffleTestParam> params;
  const std::vector<std::string> partitionings = {
      "single", "rr", "hash", "range"};
  const std::vector<int32_t> shuffleModes = {0, 1, 2, 3};
  const std::vector<int32_t> partitionNumbers = {1, 4, 16, 128};

  const std::vector<PartitionWriterType> writerTypes = {
      PartitionWriterType::kLocal, PartitionWriterType::kCeleborn};

  for (auto partitioning : partitionings) {
    for (auto shuffleMode : shuffleModes) {
      for (auto writerType : writerTypes) {
        for (auto dataTypeGroup : dataGroups) {
          for (auto numPartitions : partitionNumbers) {
            auto param = ShuffleTestParam{
                partitioning,
                shuffleMode,
                writerType,
                dataTypeGroup,
                numPartitions};
            if (param.isSupported()) {
              params.push_back(param);
            }
          }
        }
      }
    }
  }

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

    VectorFuzzer::Options opts;
    opts.nullRatio = 0.1;
    opts.stringVariableLength = true;
    opts.vectorSize = kBatchSize;

    auto numBatches = kNumBatches;

    auto generateRandomData = [&](const RowTypePtr& rowType,
                                  const VectorFuzzer::Options& opts,
                                  int32_t numBatches) {
      ShuffleInputData data;

      if (numBatches == 0) {
        data.baseBatches.push_back(std::dynamic_pointer_cast<RowVector>(
            BaseVector::create(rowType, 0, pool())));
        return data;
      }

      data.baseBatches.reserve(numBatches);
      for (int32_t i = 0; i < numBatches; ++i) {
        VectorFuzzer fuzzer(opts, pool());
        data.baseBatches.push_back(fuzzer.fuzzInputRow(rowType));
      }
      return data;
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
        rowType = ROW({"c0", "c1", "c2"}, {REAL(), DOUBLE()});
        break;
      }
      case DataTypeGroup::kString: {
        opts.stringLength = 8;
        rowType = ROW({"c0", "c1", "c2"}, {VARCHAR(), VARBINARY()});
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
        rowType =
            ROW({"c0", "c1"}, {ARRAY(INTEGER()), MAP(VARCHAR(), BIGINT())});
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
    return generateRandomData(rowType, opts, numBatches);
  }

  RowVectorPtr prependPidColumn(
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
    if (numPartitions <= 0) {
      std::fill(pids.begin(), pids.end(), 0);
    } else {
      std::uniform_int_distribution<int32_t> dist(0, numPartitions - 1);
      for (int32_t row = 0; row < numRows; ++row) {
        pids[row] = dist(rng);
      }
    }
    auto pidVector =
        makeFlatVector<int32_t>(numRows, [&](auto row) { return pids[row]; });

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
      int32_t numPartitions) {
    std::mt19937 rng(kPidSeed);
    std::vector<RowVectorPtr> withPid;
    withPid.reserve(batches.size());
    for (const auto& batch : batches) {
      withPid.push_back(prependPidColumn(batch, numPartitions, rng));
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
    writerOptions.partitioning = toPartitioning(param.partitioning);
    writerOptions.sort_before_repartition = false;
    writerOptions.partitionWriterOptions.numPartitions = param.numPartitions;
    writerOptions.forceShuffleWriterType = param.shuffleMode;
    writerOptions.partitionWriterOptions.partitionWriterType = param.writerType;
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
      readerOptions.numPartitions = param.numPartitions;
      readerOptions.forceShuffleWriterType = param.shuffleMode;
      readerOptions.partitionShortName = param.partitioning;

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

  void executeTest(const ShuffleTestParam& param) {
    auto inputData = makeInputData(param);
    const bool needsPid = param.partitioning == Partitioning::kHash ||
        param.partitioning == Partitioning::kRange;
    const auto& baseBatches = inputData.baseBatches;
    BOLT_CHECK(!baseBatches.empty(), "Input batches should not be empty");

    auto writerInput = needsPid
        ? prependPidBatches(baseBatches, param.numPartitions)
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
      return info.param.toString();
    });
