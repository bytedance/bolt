#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <vector>

#include "Type.h"
#include "bolt/common/memory/sparksql/tests/MemoryTestUtils.h"
#include "bolt/core/PlanNode.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/ShuffleReaderNode.h"
#include "bolt/shuffle/sparksql/tests/LocalFileReaderStreamIterator.h"
#include "bolt/type/parser/TypeParser.h"
#include "bolt/vector/VectorPrinter.h"
#include "vector/VectorPrinter.h"

DEFINE_string(index_file_path, "", "Path to the index file");
DEFINE_string(data_file_path, "", "Path to the data file");
DEFINE_string(
    data_type,
    "",
    "The schema of shuffle file, for example row(bigint,varchar,map(bigint, varchar))");
DEFINE_int32(
    shuffle_writer_type,
    0,
    "The shuffle file is written by which shuffle writer");
DEFINE_string(
    shuffle_type,
    "hash",
    "Please chose one from [hash, single, range, rr]");
DEFINE_string(
    compress_type,
    "zstd",
    "The compression type which shuffle adopted");
// same conf with bytedance online env
DEFINE_int32(max_batch_size, 32 * 1024, "");
DEFINE_int32(max_batch_byte_size, 40 * 1024 * 1024, "");
DEFINE_string(
    filter,
    "",
    "string to filter result, if toString(vector) contains filter, will output");

namespace {
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::shuffle::sparksql;
using namespace bytedance::bolt::memory::sparksql::test;
using namespace bytedance::bolt::shuffle::sparksql::test;

int64_t swapEndian(uint64_t val) {
  return ((val & 0x00000000000000FFULL) << 56) |
      ((val & 0x000000000000FF00ULL) << 40) |
      ((val & 0x0000000000FF0000ULL) << 24) |
      ((val & 0x00000000FF000000ULL) << 8) |
      ((val & 0x000000FF00000000ULL) >> 8) |
      ((val & 0x0000FF0000000000ULL) >> 24) |
      ((val & 0x00FF000000000000ULL) >> 40) |
      ((val & 0xFF00000000000000ULL) >> 56);
}

std::vector<SegmentInfo> getSegmentInfo(
    const std::string& indexFilePath,
    const std::string& dataFilePath) {
  std::vector<SegmentInfo> segs;
  std::ifstream file(indexFilePath, std::ios::binary);
  if (!file) {
    std::cerr << "Can't open file: " << indexFilePath << std::endl;
    exit(1);
  }
  int64_t buffer = 0;
  int64_t prev = 0;
  int64_t partitionCount = 0;
  file.read(reinterpret_cast<char*>(&prev), sizeof(int64_t));
  while (file.read(reinterpret_cast<char*>(&buffer), sizeof(int64_t))) {
    auto actualValue = swapEndian(buffer);
    auto offset = prev;
    auto length = actualValue - prev;
    segs.push_back(SegmentInfo{dataFilePath, offset, length});
    prev = actualValue;
    partitionCount++;
  }
  std::cout << "PartitionNum=" << partitionCount << std::endl;

  return segs;
}

arrow::Compression::type parseCompressionType(const std::string& type) {
  auto inputType = type;
  boost::to_lower(inputType);
  if (inputType == "zstd") {
    return arrow::Compression::ZSTD;
  }
  if (inputType == "lz4") {
    return arrow::Compression::LZ4;
  }
  std::cout << "Can't parse your input type " << type << std::endl;
  exit(1);
}

void printShuffleFile(
    const std::string& indexFilePath,
    const std::string& dataFilePath,
    const RowTypePtr& outputType,
    const int32_t& shuffleWriterType,
    const std::string& partitionShortName,
    const std::string& compressType,
    const int32_t maxBatchSize,
    const int32_t maxBatchByteSize,
    const std::string& filter) {
  auto segments = getSegmentInfo(indexFilePath, dataFilePath);
  auto streamIter =
      std::make_shared<LocalFileReaderStreamIterator>(std::move(segments));

  ShuffleReaderOptions readerOptions;
  readerOptions.compressionType = parseCompressionType(compressType);
  readerOptions.batchSize = maxBatchSize;
  readerOptions.shuffleBatchByteSize = maxBatchByteSize;
  readerOptions.numPartitions = segments.size();
  readerOptions.partitionShortName = partitionShortName;
  readerOptions.forceShuffleWriterType = shuffleWriterType;

  bytedance::bolt::exec::Operator::registerOperator(
      std::make_unique<SparkShuffleReaderTranslator>());

  core::PlanNodeId readerId("shuffle_reader");
  auto readerNode = std::make_shared<SparkShuffleReaderNode>(
      readerId, outputType, readerOptions, streamIter);

  auto memoryManagerHolder =
      TestMemoryManagerHolder::create(500 * 1024 * 1024L /*500MB*/);
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
    auto str = printVector(*curBatch);
    if (filter.empty() ||
        (!filter.empty() && str.find(filter) != std::string::npos)) {
      // avoid using LOG(INFO), which will be truncated
      std::cout << printVector(*curBatch) << std::endl;
    }
    readerCursor->current().reset();
  }
}

} // namespace

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto outputType = parseType(FLAGS_data_type);
  auto rowType = std::dynamic_pointer_cast<const RowType>(outputType);
  if (rowType == nullptr) {
    std::cout << "Please ensure data_type is like row(...)" << std::endl;
    exit(1);
  }
  std::cout << "schema is:" << rowType->toString() << std::endl;
  if (FLAGS_index_file_path.empty() || FLAGS_data_file_path.empty()) {
    std::cout << "Please set index_file_path or data_file_path" << std::endl;
    exit(1);
  }
  printShuffleFile(
      FLAGS_index_file_path,
      FLAGS_data_file_path,
      rowType,
      FLAGS_shuffle_writer_type,
      FLAGS_shuffle_type,
      FLAGS_compress_type,
      FLAGS_max_batch_size,
      FLAGS_max_batch_byte_size,
      FLAGS_filter);
}
