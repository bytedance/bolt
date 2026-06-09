#pragma once

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

struct BenchmarkOptions {
  DatasetKind dataset{DatasetKind::kFixed};
  uint64_t dataBytes{0};
  vector_size_t batchRows{0};
  uint32_t stringLength{0};
};

struct BenchmarkContext {
  explicit BenchmarkContext(const std::string& name, uint64_t dataBytes);
  ~BenchmarkContext();

  std::shared_ptr<memory::MemoryPool> rootPool;
  std::shared_ptr<memory::MemoryPool> pool;
  std::shared_ptr<memory::bm::BufferManager> bufferManager;
  std::string spillDir;
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

BenchmarkOptions options(DatasetKind dataset, uint64_t dataBytes);

uint64_t rowCount(const BenchmarkOptions& options);

std::vector<TypePtr> columnTypes(DatasetKind dataset);

RowTypePtr rowType(DatasetKind dataset);

RowVectorPtr makeInputBatch(
    memory::MemoryPool* pool,
    const BenchmarkOptions& options,
    uint64_t startRow,
    vector_size_t size);

void storeInputBatchOld(
    RowContainer& container,
    const RowVectorPtr& batch,
    std::vector<char*>* rows = nullptr);

void storeInputBatchBm(
    BmRowContainer& container,
    const RowVectorPtr& batch,
    std::vector<char*>* rows = nullptr);

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
    bool keepRows);

void storeOldRowsOnly(
    RowContainer& container,
    memory::MemoryPool* pool,
    const BenchmarkOptions& options,
    std::vector<char*>* rows = nullptr);

void storeBmRowsOnly(
    BmRowContainer& container,
    memory::MemoryPool* pool,
    const BenchmarkOptions& options,
    std::vector<char*>* rows = nullptr);

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

void readBmSpill(
    BmRowContainer& container,
    SegmentId segment,
    const BenchmarkOptions& options);

OldSpillData spillOldRows(
    BenchmarkContext& context,
    RowContainer& container,
    DatasetKind dataset);

BmSpillData spillBmRows(
    BenchmarkContext& context,
    const BenchmarkOptions& options);

std::unique_ptr<RowContainer> readOldSpillIntoNewRowContainer(
    BenchmarkContext& context,
    OldSpillData& spillData,
    DatasetKind dataset);

} // namespace bytedance::bolt::exec::bm::benchmarks
