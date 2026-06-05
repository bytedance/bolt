#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkUtil.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"
#include "bolt/vector/FlatVector.h"

#include <filesystem>
#include <iostream>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <gflags/gflags.h>

DEFINE_double(
    bm_row_container_data_gib,
    1.0,
    "Logical input data size per benchmark dataset, in GiB.");

DEFINE_bool(
    bm_row_container_print_stats,
    false,
    "Print detailed RowContainer spiller and BufferManager stats for bm row "
    "container benchmarks.");

namespace bytedance::bolt::exec {
namespace {

constexpr uint64_t kGiB = 1024ULL * 1024 * 1024;

template <typename T>
VectorPtr makeFlatVector(
    memory::MemoryPool* pool,
    const TypePtr& type,
    vector_size_t size,
    const std::function<std::optional<T>(vector_size_t)>& valueAt) {
  auto vector = BaseVector::create(type, size, pool);
  auto* flat = vector->asFlatVector<T>();
  for (vector_size_t i = 0; i < size; ++i) {
    auto value = valueAt(i);
    if (value.has_value()) {
      flat->set(i, value.value());
    } else {
      flat->setNull(i, true);
    }
  }
  return vector;
}

bool shouldPrintStats(const std::string& name) {
  if (!FLAGS_bm_row_container_print_stats) {
    return false;
  }
  static std::mutex mutex;
  static std::unordered_set<std::string> printed;
  std::lock_guard<std::mutex> lock(mutex);
  return printed.insert(name).second;
}

RowTypePtr rowTypeFor(const DatasetSpec& spec) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(spec.keyTypes.size() + spec.dependentTypes.size());
  types.reserve(spec.keyTypes.size() + spec.dependentTypes.size());
  for (auto i = 0; i < spec.keyTypes.size(); ++i) {
    names.push_back(fmt::format("k{}", i));
    types.push_back(spec.keyTypes[i]);
  }
  for (auto i = 0; i < spec.dependentTypes.size(); ++i) {
    names.push_back(fmt::format("d{}", i));
    types.push_back(spec.dependentTypes[i]);
  }
  return ROW(std::move(names), std::move(types));
}

uint64_t targetLogicalBytes() {
  return std::max<uint64_t>(
      1, static_cast<uint64_t>(FLAGS_bm_row_container_data_gib * kGiB));
}

std::string randomPayloadForRow(uint64_t row, size_t size) {
  std::string payload(size, '\0');
  uint64_t state = row + 0x9e3779b97f4a7c15ULL;
  for (auto& c : payload) {
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    const auto value = state * 0x2545f4914f6cdd1dULL;
    c = static_cast<char>(' ' + (value % 95));
  }
  return payload;
}

VectorPtr makeRandomPayloadVector(
    memory::MemoryPool* pool,
    vector_size_t startRow,
    vector_size_t rows) {
  auto vector = BaseVector::create(VARCHAR(), rows, pool);
  auto* flat = vector->asFlatVector<StringView>();
  for (vector_size_t i = 0; i < rows; ++i) {
    const auto row = static_cast<uint64_t>(startRow + i);
    if (row % 10 == 0) {
      flat->setNull(i, true);
      continue;
    }
    const auto payload = randomPayloadForRow(row, 1024);
    flat->set(i, StringView(payload));
  }
  return vector;
}

Dataset makeDataset(
    memory::MemoryPool* pool,
    const DatasetSpec& spec,
    vector_size_t startRow,
    vector_size_t rows) {
  switch (spec.kind) {
    case DatasetKind::kFixedInt64:
      return Dataset{
          spec.name,
          spec.keyTypes,
          spec.dependentTypes,
          {makeFlatVector<int64_t>(
              pool, BIGINT(), rows, [startRow](auto i) {
                return (startRow + i) % 10 == 0
                    ? std::optional<int64_t>{}
                    : std::optional<int64_t>{startRow + i};
              })},
          rows,
          startRow,
          static_cast<uint64_t>(rows) * spec.estimatedBytesPerRow};
    case DatasetKind::kMixedFixed:
      return Dataset{
          spec.name,
          spec.keyTypes,
          spec.dependentTypes,
          {makeFlatVector<int32_t>(
               pool, INTEGER(), rows, [startRow](auto i) {
                 return static_cast<int32_t>(startRow + i);
               }),
           makeFlatVector<int64_t>(
               pool, BIGINT(), rows, [startRow](auto i) {
                 return static_cast<int64_t>((startRow + i) * 7);
               }),
           makeFlatVector<double>(
               pool, DOUBLE(), rows, [startRow](auto i) {
                 return static_cast<double>(startRow + i) / 3;
               }),
           makeFlatVector<bool>(
               pool, BOOLEAN(), rows, [startRow](auto i) {
                 return ((startRow + i) & 1) == 0;
               })},
          rows,
          startRow,
          static_cast<uint64_t>(rows) * spec.estimatedBytesPerRow};
    case DatasetKind::kVarcharPayload:
      return Dataset{
          spec.name,
          spec.keyTypes,
          spec.dependentTypes,
          {makeFlatVector<int64_t>(
               pool, BIGINT(), rows, [startRow](auto i) {
                 return static_cast<int64_t>(startRow + i);
               }),
           makeRandomPayloadVector(pool, startRow, rows)},
          rows,
          startRow,
          static_cast<uint64_t>(rows) * spec.estimatedBytesPerRow};
  }
  BOLT_UNREACHABLE();
}

} // namespace

std::vector<DatasetSpec> makeDatasetSpecs() {
  return {
      DatasetSpec{"fixed_int64", DatasetKind::kFixedInt64, {BIGINT()}, {}, 8},
      DatasetSpec{
          "mixed_fixed",
          DatasetKind::kMixedFixed,
          {INTEGER(), BIGINT()},
          {DOUBLE(), BOOLEAN()},
          4 + 8 + 8 + 1},
      DatasetSpec{
          "varchar_payload",
          DatasetKind::kVarcharPayload,
          {BIGINT()},
          {VARCHAR()},
          8 + 1024},
  };
}

std::shared_ptr<memory::bm::BufferManager> makeBufferManager(
    memory::MemoryPool& root,
    const std::string& name) {
  const auto directory =
      memory::bm::test::UniqueTempDir(fmt::format("bm-row-benchmark-{}", name));
  std::filesystem::remove_all(directory);

  memory::bm::BufferManagerConfig config;
  config.poolName = fmt::format("bm-row-benchmark-{}", name);
  config.spillStoreConfig.compressionConfig.kind =
      memory::bm::compress::CompressionKind::kZstdFrame;
  config.spillStoreConfig.fileAllocatorConfig =
      memory::bm::test::ValidConfigWithDirectory(directory);
  return memory::bm::BufferManager::Create(root, std::move(config));
}

common::SpillConfig makeRowContainerSpillConfig(const std::string& name) {
  auto directory =
      memory::bm::test::UniqueTempDir(fmt::format("row-container-spill-{}", name));
  std::filesystem::remove_all(directory);
  std::filesystem::create_directories(directory);

  common::SpillConfig config;
  config.getSpillDirPathCb = [directory]() -> const std::string& {
    static thread_local std::string path;
    path = directory;
    return path;
  };
  config.updateAndCheckSpillLimitCb = [](uint64_t) {};
  config.fileNamePrefix = name;
  config.maxFileSize = 0;
  config.spillUringEnabled = false;
  config.writeBufferSize = 1 << 20;
  config.executor = nullptr;
  config.maxSpillRunRows = 0;
  config.compressionKind = common::CompressionKind_ZSTD;
  config.fileCreateConfig = {};
  config.rowBasedSpillMode = common::RowBasedSpillMode::COMPRESSION;
  return config;
}

vector_size_t rowsForTargetBytes(uint64_t estimatedBytesPerRow) {
  BOLT_CHECK_GT(estimatedBytesPerRow, 0);
  return static_cast<vector_size_t>(
      std::max<uint64_t>(1, targetLogicalBytes() / estimatedBytesPerRow));
}

void forEachBatch(
    memory::MemoryPool* pool,
    const DatasetSpec& spec,
    const std::function<void(const Dataset&, bool)>& callback) {
  const auto totalRows = rowsForTargetBytes(spec.estimatedBytesPerRow);
  for (vector_size_t start = 0; start < totalRows;) {
    const auto batchRows = std::min<vector_size_t>(
        kBmRowContainerBenchmarkBatchRows, totalRows - start);
    auto dataset = makeDataset(pool, spec, start, batchRows);
    callback(dataset, start + batchRows == totalRows);
    start += batchRows;
  }
}

std::vector<char*> appendRowContainerBatchReturningRows(
    RowContainer& container,
    const Dataset& dataset) {
  std::vector<DecodedVector> decoded;
  decoded.reserve(dataset.vectors.size());
  for (const auto& vector : dataset.vectors) {
    decoded.emplace_back(*vector);
  }

  std::vector<char*> rows;
  rows.reserve(dataset.rows);
  for (vector_size_t i = 0; i < dataset.rows; ++i) {
    auto* row = container.newRow();
    for (auto column = 0; column < decoded.size(); ++column) {
      container.store(decoded[column], i, row, column);
    }
    rows.push_back(row);
  }
  return rows;
}

void appendRowContainerBatch(RowContainer& container, const Dataset& dataset) {
  std::vector<DecodedVector> decoded;
  decoded.reserve(dataset.vectors.size());
  for (const auto& vector : dataset.vectors) {
    decoded.emplace_back(*vector);
  }

  for (vector_size_t i = 0; i < dataset.rows; ++i) {
    auto* row = container.newRow();
    for (auto column = 0; column < decoded.size(); ++column) {
      container.store(decoded[column], i, row, column);
    }
  }
}

std::vector<RowId> appendBmRowContainerBatchReturningRows(
    BmRowContainer& container,
    const Dataset& dataset) {
  std::vector<DecodedVector> decoded;
  decoded.reserve(dataset.vectors.size());
  for (const auto& vector : dataset.vectors) {
    decoded.emplace_back(*vector);
  }

  std::vector<RowId> rows;
  rows.reserve(dataset.rows);
  for (vector_size_t i = 0; i < dataset.rows; ++i) {
    auto row = container.newRow();
    for (auto column = 0; column < decoded.size(); ++column) {
      container.store(decoded[column], i, row, column);
    }
    rows.push_back(row);
  }
  return rows;
}

void appendBmRowContainerBatch(BmRowContainer& container, const Dataset& dataset) {
  std::vector<DecodedVector> decoded;
  decoded.reserve(dataset.vectors.size());
  for (const auto& vector : dataset.vectors) {
    decoded.emplace_back(*vector);
  }

  for (vector_size_t i = 0; i < dataset.rows; ++i) {
    auto row = container.newRow();
    for (auto column = 0; column < decoded.size(); ++column) {
      container.store(decoded[column], i, row, column);
    }
  }
}

void readBackRowContainer(
    RowContainer& container,
    const std::vector<char*>& rows,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  auto result = BaseVector::create(
      type,
      std::min<vector_size_t>(kBmRowContainerBenchmarkBatchRows, rows.size()),
      pool);
  for (size_t offset = 0; offset < rows.size();) {
    const auto batchRows = std::min<vector_size_t>(
        kBmRowContainerBenchmarkBatchRows, rows.size() - offset);
    container.extractColumn(rows.data() + offset, batchRows, 0, result);
    offset += batchRows;
  }
  folly::doNotOptimizeAway(result);
}

void readBackBmRowContainer(
    BmRowContainer& container,
    const std::vector<RowId>& rows,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  auto result = BaseVector::create(
      type,
      std::min<vector_size_t>(kBmRowContainerBenchmarkBatchRows, rows.size()),
      pool);
  for (size_t offset = 0; offset < rows.size();) {
    const auto batchRows = std::min<vector_size_t>(
        kBmRowContainerBenchmarkBatchRows, rows.size() - offset);
    container.extractColumn(rows.data() + offset, batchRows, 0, result);
    offset += batchRows;
  }
  folly::doNotOptimizeAway(result);
}

std::unique_ptr<Spiller> makeRowContainerSpiller(
    RowContainer& container,
    const DatasetSpec& spec,
    common::SpillConfig& config) {
  return std::make_unique<Spiller>(
      Spiller::Type::kAggregateOutput, &container, rowTypeFor(spec), &config);
}

RowContainerReadSpillStats readRowBasedSpillPartition(
    SpillPartition& partition,
    RowContainer& container,
    memory::MemoryPool* pool) {
  auto reader = partition.createRowBasedOrderedReader(
      pool, &container, false, false);
  RowContainerReadSpillStats stats;
  while (auto* stream = reader->next()) {
    const auto& currentBatch = stream->current();
    const auto index = stream->currentIndex();
    folly::doNotOptimizeAway(currentBatch[index]);
    ++stats.rows;
    stream->pop();
  }
  stats.readTimeUs = reader->getSpillReadTime();
  stats.decompressTimeUs = reader->getSpillDecompressTime();
  stats.readIoTimeUs = reader->getSpillReadIOTime();
  folly::doNotOptimizeAway(stats.rows);
  return stats;
}

void printRowSpillStats(
    const std::string& name,
    const common::SpillStats& stats,
    const SpillPartition* partition,
    const RowContainerReadSpillStats* readStats) {
  if (!shouldPrintStats(name)) {
    return;
  }
  std::cerr << "[bm-row-benchmark-stats] " << name << " row_spill"
            << " runs=" << stats.spillRuns
            << " input_bytes=" << stats.spilledInputBytes
            << " spilled_bytes=" << stats.spilledBytes
            << " rows=" << stats.spilledRows
            << " files=" << stats.spilledFiles
            << " writes=" << stats.spillWrites
            << " total_us=" << stats.spillTotalTimeUs
            << " fill_us=" << stats.spillFillTimeUs
            << " sort_us=" << stats.spillSortTimeUs
            << " convert_us=" << stats.spillConvertTimeUs
            << " serialization_us=" << stats.spillSerializationTimeUs
            << " flush_us=" << stats.spillFlushTimeUs
            << " write_us=" << stats.spillWriteTimeUs;
  if (partition != nullptr) {
    std::cerr << " partition_bytes=" << partition->size()
              << " partition_rows=" << partition->rowCount()
              << " partition_files=" << partition->numFiles();
  }
  if (readStats != nullptr) {
    std::cerr << " read_rows=" << readStats->rows
              << " read_us=" << readStats->readTimeUs
              << " decompress_us=" << readStats->decompressTimeUs
              << " read_io_us=" << readStats->readIoTimeUs;
  }
  std::cerr << std::endl;
}

void printBmStats(
    const std::string& name,
    const memory::bm::BufferManagerStats& stats) {
  if (!shouldPrintStats(name)) {
    return;
  }
  std::cerr << "[bm-row-benchmark-stats] " << name << " bm"
            << " allocated_blocks=" << stats.allocatedBlocks
            << " live_blocks=" << stats.liveBlocks
            << " pin_count=" << stats.pinCount
            << " pin_in_memory=" << stats.pinInMemoryCount
            << " pin_read=" << stats.pinReadCount
            << " batch_pin=" << stats.batchPinCount
            << " spill_write_count=" << stats.spillWriteCount
            << " spill_read_count=" << stats.spillReadCount
            << " spill_write_bytes=" << stats.spillWriteBytes
            << " spill_read_bytes=" << stats.spillReadBytes
            << " spill_physical_write_bytes="
            << stats.spillPhysicalWriteBytes
            << " spill_physical_read_bytes=" << stats.spillPhysicalReadBytes
            << " compressed_blocks=" << stats.spillCompressedBlocks
            << " compression_us=" << stats.spillCompressionTimeUs
            << " decompression_us=" << stats.spillDecompressionTimeUs
            << " pinned_resident_bytes=" << stats.pinnedResidentBytes
            << " unpinned_resident_bytes=" << stats.unpinnedResidentBytes
            << " spilled_bytes=" << stats.spilledBytes
            << " prefetching_bytes=" << stats.prefetchingBytes
            << " spilling_bytes=" << stats.spillingBytes
            << " reclaimed_bytes=" << stats.reclaimedBytes
            << " eviction_queue_size=" << stats.evictionQueueSize
            << " eviction_queue_stale=" << stats.evictionQueueStaleEntries
            << std::endl;
}

} // namespace bytedance::bolt::exec
