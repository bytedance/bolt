#pragma once

#include "bolt/common/memory/bm/io/DiskIoSchedulerStats.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/Spill.h"
#include "bolt/exec/bm/BmRowContainer.h"
#include "bolt/vector/ComplexVector.h"

#include <memory>
#include <string>
#include <vector>

namespace bytedance::bolt::exec::bm::benchmarks {

enum class DatasetKind {
  kFixed,
  kVariable,
};

enum class SpillCompressionKind {
  kRaw,
  kLz4,
  kZstd,
};

struct BenchmarkOptions {
  DatasetKind dataset{DatasetKind::kFixed};
  uint64_t dataBytes{0};
  vector_size_t batchRows{0};
  uint32_t stringLength{0};
  SpillCompressionKind compression{SpillCompressionKind::kZstd};
};

struct ReusableInputBatches {
  std::vector<RowVectorPtr> batches;
  uint64_t rows{0};
};

struct BenchmarkContext {
  explicit BenchmarkContext(
      const std::string& name,
      uint64_t dataBytes,
      uint32_t memoryMultiplier = 0,
      SpillCompressionKind compression = SpillCompressionKind::kZstd);
  ~BenchmarkContext();

  std::shared_ptr<memory::MemoryPool> rootPool;
  std::shared_ptr<memory::MemoryPool> pool;
  std::shared_ptr<memory::bm::BufferManager> bufferManager;
  std::string spillDir;
  SpillCompressionKind compression{SpillCompressionKind::kZstd};
};

struct OldStoredRows {
  std::unique_ptr<RowContainer> container;
  std::vector<char*> rows;
};

struct BmStoredRows {
  std::unique_ptr<BmRowContainer> container;
  std::vector<char*> rows;
};

struct OldSpillData {
  std::unique_ptr<RowFormatInfo> rowFormat;
  SpillPartition partition;
};

struct BmSpillData {
  std::unique_ptr<BmRowContainer> container;
  SegmentId segment{0};
};

struct OldSpillWriteMetrics {
  uint64_t spillNs{0};
  uint64_t rows{0};
  uint64_t spillBytes{0};
  uint64_t files{0};
};

struct OldSpillReadMetrics {
  uint64_t createReaderNs{0};
  uint64_t nextBatchNs{0};
  uint64_t copyRowsNs{0};
  uint64_t listRowsNs{0};
  uint64_t batches{0};
  uint64_t rows{0};
  uint64_t serializedBytes{0};
};

struct BmSpillReadMetrics {
  uint64_t beginNs{0};
  uint64_t listRowsNs{0};
  uint64_t windowLoadNs{0};
  uint64_t rows{0};
  uint64_t rowIds{0};
  uint64_t windows{0};
  bool resultPointers{false};
  BulkLoadMetrics bulkLoad;
  memory::bm::BufferManagerStats statsDelta;
  memory::bm::DiskIoSchedulerStats ioStatsDelta;
};

BenchmarkOptions options(
    DatasetKind dataset,
    uint64_t dataBytes,
    SpillCompressionKind compression = SpillCompressionKind::kZstd);

uint64_t rowCount(const BenchmarkOptions& options);

void checkOldRowBasedSpillBenchmarkSupported(
    const BenchmarkOptions& options);

uint64_t benchmarkNowNs();

double nsToMs(uint64_t ns);

uint64_t counterDelta(uint64_t before, uint64_t after);

const char* datasetName(DatasetKind dataset);

const char* spillCompressionName(SpillCompressionKind compression);

bool shouldPrintSpillMetrics(
    const char* benchmark,
    DatasetKind dataset,
    SpillCompressionKind compression = SpillCompressionKind::kZstd);

std::vector<TypePtr> columnTypes(DatasetKind dataset);

RowTypePtr rowType(DatasetKind dataset);

RowVectorPtr makeInputBatch(
    memory::MemoryPool* pool,
    const BenchmarkOptions& options,
    uint64_t startRow,
    vector_size_t size);

ReusableInputBatches makeReusableInputBatches(
    memory::MemoryPool* pool,
    const BenchmarkOptions& options);

void storeInputBatchOld(
    RowContainer& container,
    const RowVectorPtr& batch,
    std::vector<char*>* rows = nullptr);

void storeInputBatchBm(
    BmRowContainer& container,
    const RowVectorPtr& batch,
    std::vector<char*>* rows = nullptr,
    BmStoreMetrics* metrics = nullptr);

void storeReusableInputBatchesOld(
    RowContainer& container,
    const ReusableInputBatches& input,
    const BenchmarkOptions& options,
    std::vector<char*>* rows = nullptr);

void storeReusableInputBatchesBm(
    BmRowContainer& container,
    const ReusableInputBatches& input,
    const BenchmarkOptions& options,
    std::vector<char*>* rows = nullptr,
    BmStoreMetrics* metrics = nullptr);

std::unique_ptr<RowContainer> makeOldRowContainer(
    DatasetKind dataset,
    memory::MemoryPool* pool);

std::unique_ptr<BmRowContainer> makeBmRowContainer(
    DatasetKind dataset,
    const std::shared_ptr<memory::bm::BufferManager>& bufferManager);

OldStoredRows storeOldRows(
    BenchmarkContext& context,
    const BenchmarkOptions& options,
    bool keepRows);

BmStoredRows storeBmRows(
    BenchmarkContext& context,
    const BenchmarkOptions& options,
    bool keepRows,
    BmStoreMetrics* metrics = nullptr);

void storeOldRowsOnly(
    RowContainer& container,
    memory::MemoryPool* pool,
    const BenchmarkOptions& options,
    std::vector<char*>* rows = nullptr);

void storeBmRowsOnly(
    BmRowContainer& container,
    memory::MemoryPool* pool,
    const BenchmarkOptions& options,
    std::vector<char*>* rows = nullptr,
    BmStoreMetrics* metrics = nullptr);

void extractOldRows(
    RowContainer& container,
    const std::vector<char*>& rows,
    const BenchmarkOptions& options,
    memory::MemoryPool* pool);

void extractBmRowsResident(
    BmRowContainer& container,
    const std::vector<char*>& rows,
    const BenchmarkOptions& options,
    memory::MemoryPool* pool);

void extractBmRowsResident(
    BmRowContainer& container,
    const std::vector<const char*>& rows,
    const BenchmarkOptions& options,
    memory::MemoryPool* pool);

void readBmSpill(
    BmRowContainer& container,
    SegmentId segment,
    const BenchmarkOptions& options,
    BmSpillReadMetrics* metrics = nullptr);

OldSpillData spillOldRows(
    BenchmarkContext& context,
    RowContainer& container,
    DatasetKind dataset,
    OldSpillWriteMetrics* metrics = nullptr);

BmSpillData spillBmRows(
    BenchmarkContext& context,
    const BenchmarkOptions& options);

std::unique_ptr<RowContainer> readOldSpillIntoNewRowContainer(
    BenchmarkContext& context,
    OldSpillData& spillData,
    DatasetKind dataset,
    OldSpillReadMetrics* metrics = nullptr,
    std::vector<char*>* restoredRows = nullptr);

void warmupStoreOld(const BenchmarkOptions& options);

void warmupStoreBm(const BenchmarkOptions& options);

void warmupReadOld(const BenchmarkOptions& options);

void warmupReadBm(const BenchmarkOptions& options);

void warmupSpillWriteOld(const BenchmarkOptions& options);

void warmupSpillWriteBm(const BenchmarkOptions& options);

void warmupSpillReadOld(const BenchmarkOptions& options);

void warmupSpillReadBm(const BenchmarkOptions& options);

} // namespace bytedance::bolt::exec::bm::benchmarks
