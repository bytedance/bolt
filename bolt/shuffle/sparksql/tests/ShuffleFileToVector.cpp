#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <vector>

#include "Type.h"
#include "bolt/common/memory/sparksql/tests/MemoryTestUtils.h"
#include "bolt/core/PlanNode.h"
#include "bolt/exec/tests/utils/Cursor.h"
#include "bolt/shuffle/sparksql/Options.h"
#include "bolt/shuffle/sparksql/ShuffleReaderNode.h"
#include "bolt/shuffle/sparksql/tests/LocalFileReaderStreamIterator.h"
#include "bolt/vector/VectorPrinter.h"
#include "vector/VectorPrinter.h"

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
  LOG(INFO) << "PartitionNum=" << partitionCount;

  return segs;
}

void printShuffleFile(
    const std::string& indexFilePath,
    const std::string& dataFilePath,
    const RowTypePtr& outputType,
    const int32_t& shuffleWriterType = 0 /*adaptive shuffle write type*/,
    const std::string& partitionShortName = "hash") {
  auto segments = getSegmentInfo(indexFilePath, dataFilePath);
  auto streamIter =
      std::make_shared<LocalFileReaderStreamIterator>(std::move(segments));

  ShuffleReaderOptions readerOptions;
  readerOptions.numPartitions = segments.size();
  readerOptions.forceShuffleWriterType = shuffleWriterType;
  readerOptions.partitionShortName = partitionShortName;
  readerOptions.shuffleBatchByteSize = 1024 * 1024; // 1MB
  readerOptions.compressionType = arrow::Compression::ZSTD;

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
    // avoid using LOG(INFO), which will be truncated
    std::cout << printVector(*curBatch) << std::endl;
    readerCursor->current().reset();
  }
}
} // namespace

int main() {
  const std::string shuffleFileBasePath = "/home/xxx/shuffle_data";
  const std::string shuffleId = "15";
  const std::string taskId = "90";
  RowTypePtr outputType = ROW(
      {{"l_l_task_id#3900L", BIGINT()},
       {"l_l_deadline_item_vv_td#3901L", BIGINT()},
       {"l_l_deadline_item_interact_pv_td#3902L", BIGINT()},
       {"l_l_deadline_item_product_show_pv_td#3903L", BIGINT()},
       {"l_l_deadline_item_life_anchor_entrance_click_cnt_td#3904L", BIGINT()},
       {"l_l_deadline_item_pay_amount_td#3905L", BIGINT()},
       {"l_l_deadline_whole_verify_amount#3906L", BIGINT()},
       {"l_l_guarantee_deadline#3907", VARCHAR()},
       {"l_l_task_create_date#3908", VARCHAR()},
       {"l_l_item_vv_td_create2deadline#3909", VARCHAR()},
       {"l_l_item_interact_pv_td_create2deadline#3910", VARCHAR()},
       {"l_l_item_product_show_pv_td_create2deadline#3911", VARCHAR()},
       {"l_l_item_life_anchor_entrance_click_cnt_td_create2deadline#3912",
        VARCHAR()},
       {"l_l_whole_sale_amount_create2deadline#3913", VARCHAR()},
       {"l_l_whole_verify_amount_create2deadline#3914", VARCHAR()},
       {"l_l_life_account_pay_amount_create2deadline_nd#3915L", BIGINT()},
       {"l_l_estimated_item_vv_td#3916L", BIGINT()},
       {"l_l_estimated_item_interact_pv_td#3917L", BIGINT()},
       {"l_l_estimated_item_product_show_pv_td#3918L", BIGINT()},
       {"l_l_estimated_item_life_anchor_entrance_click_cnt_td#3919L", BIGINT()},
       {"l_l_estimated_item_pay_amount_td#3920L", BIGINT()},
       {"l_l_estimated_whole_verify_amount#3921L", BIGINT()},
       {"l_l_deadline_dingxiang_item_pay_amount_td#3922L", BIGINT()},
       {"l_r_compensate_amount#3924L", BIGINT()},
       {"l_r_refund_amount#3925L", BIGINT()},
       {"l_r_pay_amount#3926L", BIGINT()}});

  const std::string indexFilePath = fmt::format(
      "{}/{}/{}/-1/shuffle_{}_{}_0.index",
      shuffleFileBasePath,
      shuffleId,
      taskId,
      shuffleId,
      taskId);
  const std::string dataFilePath = fmt::format(
      "{}/{}/{}/-1/shuffle_{}_{}_0.data",
      shuffleFileBasePath,
      shuffleId,
      taskId,
      shuffleId,
      taskId);
  printShuffleFile(indexFilePath, dataFilePath, outputType, 0, "hash");
}
