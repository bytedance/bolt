#pragma once

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/Spiller.h"
#include "bolt/exec/bm/BmRowContainer.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace bytedance::bolt::exec {

inline constexpr vector_size_t kBmRowContainerBenchmarkBatchRows = 16'384;
enum class DatasetKind {
  kFixedInt64,
  kMixedFixed,
  kVarcharPayload,
};

struct Dataset {
  std::string name;
  std::vector<TypePtr> keyTypes;
  std::vector<TypePtr> dependentTypes;
  std::vector<VectorPtr> vectors;
  vector_size_t rows{0};
  vector_size_t startRow{0};
  uint64_t logicalBytes{0};
};

struct DatasetSpec {
  std::string name;
  DatasetKind kind;
  std::vector<TypePtr> keyTypes;
  std::vector<TypePtr> dependentTypes;
  uint64_t estimatedBytesPerRow;
};

struct RowContainerReadSpillStats {
  uint64_t rows{0};
  uint64_t readTimeUs{0};
  uint64_t decompressTimeUs{0};
  uint64_t readIoTimeUs{0};
};

std::vector<DatasetSpec> makeDatasetSpecs();

uint64_t benchmarkPoolCapacityBytes();

std::shared_ptr<memory::bm::BufferManager> makeBufferManager(
    memory::MemoryPool& root,
    const std::string& name);

common::SpillConfig makeRowContainerSpillConfig(const std::string& name);

vector_size_t rowsForTargetBytes(uint64_t estimatedBytesPerRow);

void forEachBatch(
    memory::MemoryPool* pool,
    const DatasetSpec& spec,
    const std::function<void(const Dataset&, bool)>& callback);

std::vector<char*> appendRowContainerBatchReturningRows(
    RowContainer& container,
    const Dataset& dataset);

void appendRowContainerBatch(RowContainer& container, const Dataset& dataset);

std::vector<RowId> appendBmRowContainerBatchReturningRows(
    BmRowContainer& container,
    const Dataset& dataset);

void appendBmRowContainerBatch(BmRowContainer& container, const Dataset& dataset);

void readBackRowContainer(
    RowContainer& container,
    const std::vector<char*>& rows,
    const TypePtr& type,
    memory::MemoryPool* pool);

void readBackBmRowContainer(
    BmRowContainer& container,
    const std::vector<RowId>& rows,
    const TypePtr& type,
    memory::MemoryPool* pool);

std::unique_ptr<Spiller> makeRowContainerSpiller(
    RowContainer& container,
    const DatasetSpec& spec,
    common::SpillConfig& config);

RowContainerReadSpillStats readRowBasedSpillPartition(
    SpillPartition& partition,
    RowContainer& container,
    memory::MemoryPool* pool);

void printRowSpillStats(
    const std::string& name,
    const common::SpillStats& stats,
    const SpillPartition* partition = nullptr,
    const RowContainerReadSpillStats* readStats = nullptr);

void printBmStats(
    const std::string& name,
    const memory::bm::BufferManagerStats& stats);

void registerWriteBenchmarks(const std::vector<DatasetSpec>& specs);
void registerSpillBenchmarks(const std::vector<DatasetSpec>& specs);
void registerReadMemoryBenchmarks(const std::vector<DatasetSpec>& specs);
void registerReadSpillBenchmarks(const std::vector<DatasetSpec>& specs);

} // namespace bytedance::bolt::exec
