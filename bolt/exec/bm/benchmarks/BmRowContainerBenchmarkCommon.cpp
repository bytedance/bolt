#include "bolt/exec/bm/benchmarks/BmRowContainerBenchmarkCommon.h"

#include "bolt/common/base/SpillConfig.h"
#include "bolt/common/compression/Compression.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/compress/CompressionConfig.h"
#include "bolt/exec/HashBitRange.h"
#include "bolt/exec/Spiller.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/tests/utils/VectorMaker.h"

#include <fmt/core.h>
#include <folly/Benchmark.h>
#include <folly/portability/Unistd.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <random>

DEFINE_uint64(
    bm_row_container_data_bytes,
    1ULL << 30,
    "Logical input bytes processed by BM row container benchmarks.");
DEFINE_uint32(
    bm_row_container_batch_rows,
    4096,
    "Input rows per generated batch.");
DEFINE_uint32(
    bm_row_container_string_length,
    1024,
    "Fixed string length for variable BM row container benchmarks.");
DEFINE_uint64(
    bm_row_container_spill_write_buffer_bytes,
    4ULL << 20,
    "Write buffer size used by old RowContainer Spiller benchmarks.");

namespace bytedance::bolt::exec::bm::benchmarks {
namespace {

std::atomic<uint64_t> benchmarkId{0};

uint64_t splitMix64(uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

std::string randomString(uint64_t row, uint32_t length) {
  static constexpr char kAlphabet[] =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  std::string value;
  value.resize(length);
  auto state = splitMix64(row);
  for (uint32_t i = 0; i < length; ++i) {
    if ((i & 7) == 0) {
      state = splitMix64(state + i);
    }
    value[i] = kAlphabet[state % (sizeof(kAlphabet) - 1)];
    state >>= 8;
  }
  return value;
}

uint64_t logicalRowBytes(const BenchmarkOptions& options) {
  uint64_t bytes = sizeof(int64_t) + sizeof(int32_t) + sizeof(double);
  if (options.dataset == DatasetKind::kVariable) {
    bytes += options.stringLength;
  }
  return bytes;
}

void decodeBatch(
    const RowVectorPtr& batch,
    std::vector<DecodedVector>& decoded) {
  SelectivityVector rows(batch->size());
  decoded.resize(batch->childrenSize());
  for (auto column = 0; column < batch->childrenSize(); ++column) {
    decoded[column].decode(*batch->childAt(column), rows);
  }
}

common::SpillConfig makeOldSpillConfig(const std::string& spillDir) {
  common::SpillConfig config;
  config.getSpillDirPathCb = [&spillDir]() -> const std::string& {
    return spillDir;
  };
  config.updateAndCheckSpillLimitCb = [](uint64_t) {};
  config.fileNamePrefix = "bm-row-container-old";
  config.maxFileSize = 0;
  config.spillUringEnabled = false;
  config.writeBufferSize = FLAGS_bm_row_container_spill_write_buffer_bytes;
  config.compressionKind = common::CompressionKind_ZSTD;
  config.rowBasedSpillMode = common::RowBasedSpillMode::COMPRESSION;
  config.maxSpillRunRows = 0;
  return config;
}

VectorPtr makeResultVector(
    const TypePtr& type,
    vector_size_t size,
    memory::MemoryPool* pool) {
  return BaseVector::create(type, size, pool);
}

} // namespace

BenchmarkContext::BenchmarkContext(
    const std::string& name,
    uint64_t dataBytes) {
  const auto id = benchmarkId.fetch_add(1);
  const auto poolName = fmt::format("bm-row-container-benchmark-{}-{}", name, id);
  rootPool = memory::memoryManager()->addRootPool(
      poolName,
      std::max<uint64_t>(dataBytes * 3, 1ULL << 30),
      memory::MemoryReclaimer::create());
  pool = rootPool->addLeafChild("rows");
  spillDir = (std::filesystem::temp_directory_path() /
              fmt::format("{}-{}", poolName, getpid()))
                 .string();
  std::filesystem::create_directories(spillDir);

  memory::bm::BufferManagerConfig config;
  config.poolName = "buffer-manager";
  config.spillStoreConfig.fileAllocatorConfig.directory = spillDir;
  config.spillStoreConfig.fileAllocatorConfig.bucket_sizes = {
      4 * 1024, 8 * 1024, 16 * 1024};
  config.spillStoreConfig.fileAllocatorConfig.file_size_limit_bytes =
      256 * 1024 * 1024;
  config.spillStoreConfig.fileAllocatorConfig.max_open_files_per_bucket = 16;
  config.spillStoreConfig.compressionConfig.kind =
      memory::bm::compress::CompressionKind::kZstdFrame;
  config.spillStoreConfig.compressionConfig.minCompressBytes = 1;
  config.spillStoreConfig.compressionConfig.zstd.strategy =
      memory::bm::compress::ZstdStrategy::kPooledContext;
  config.spillStoreConfig.compressionConfig.zstd.compressionLevel = 3;
  bufferManager = memory::bm::BufferManager::Create(*rootPool, std::move(config));
}

BenchmarkContext::~BenchmarkContext() {
  bufferManager.reset();
  pool.reset();
  rootPool.reset();
  std::error_code error;
  std::filesystem::remove_all(spillDir, error);
}

BenchmarkOptions options(DatasetKind dataset, uint64_t dataBytes) {
  return BenchmarkOptions{
      .dataset = dataset,
      .dataBytes = dataBytes,
      .batchRows = static_cast<vector_size_t>(
          FLAGS_bm_row_container_batch_rows),
      .stringLength = FLAGS_bm_row_container_string_length};
}

uint64_t rowCount(const BenchmarkOptions& options) {
  return std::max<uint64_t>(
      1, (options.dataBytes + logicalRowBytes(options) - 1) /
          logicalRowBytes(options));
}

std::vector<TypePtr> columnTypes(DatasetKind dataset) {
  std::vector<TypePtr> types{BIGINT(), INTEGER(), DOUBLE()};
  if (dataset == DatasetKind::kVariable) {
    types.push_back(VARCHAR());
  }
  return types;
}

RowTypePtr rowType(DatasetKind dataset) {
  std::vector<std::string> names{"c0", "c1", "c2"};
  if (dataset == DatasetKind::kVariable) {
    names.push_back("c3");
  }
  return ROW(std::move(names), columnTypes(dataset));
}

RowVectorPtr makeInputBatch(
    memory::MemoryPool* pool,
    const BenchmarkOptions& options,
    uint64_t startRow,
    vector_size_t size) {
  test::VectorMaker maker(pool);
  std::vector<VectorPtr> children;
  children.push_back(maker.flatVector<int64_t>(
      size, [startRow](vector_size_t row) {
        return static_cast<int64_t>(splitMix64(startRow + row));
      }));
  children.push_back(maker.flatVector<int32_t>(
      size, [startRow](vector_size_t row) {
        return static_cast<int32_t>(splitMix64(startRow + row + 17));
      }));
  children.push_back(maker.flatVector<double>(
      size, [startRow](vector_size_t row) {
        return static_cast<double>(splitMix64(startRow + row + 31) % 1000000) /
            7.0;
      }));
  if (options.dataset == DatasetKind::kVariable) {
    std::vector<std::string> strings;
    strings.reserve(size);
    for (vector_size_t row = 0; row < size; ++row) {
      strings.push_back(randomString(startRow + row, options.stringLength));
    }
    children.push_back(maker.flatVector<std::string>(strings, VARCHAR()));
  }
  return maker.rowVector(std::move(children));
}

void storeInputBatchOld(
    RowContainer& container,
    const RowVectorPtr& batch,
    std::vector<char*>* rows) {
  std::vector<DecodedVector> decoded;
  decodeBatch(batch, decoded);
  for (vector_size_t row = 0; row < batch->size(); ++row) {
    auto* target = container.newRow();
    if (rows != nullptr) {
      rows->push_back(target);
    }
    for (auto column = 0; column < batch->childrenSize(); ++column) {
      container.store(decoded[column], row, target, column);
    }
  }
}

void storeInputBatchBm(
    BmRowContainer& container,
    const RowVectorPtr& batch,
    std::vector<char*>* rows) {
  std::vector<DecodedVector> decoded;
  decodeBatch(batch, decoded);
  for (vector_size_t row = 0; row < batch->size(); ++row) {
    auto* target = container.newRow();
    if (rows != nullptr) {
      rows->push_back(target);
    }
    for (auto column = 0; column < batch->childrenSize(); ++column) {
      container.store(decoded[column], row, target, column);
    }
  }
}

std::unique_ptr<RowContainer> makeOldRowContainer(
    DatasetKind dataset,
    memory::MemoryPool* pool) {
  return std::make_unique<RowContainer>(columnTypes(dataset), pool);
}

std::unique_ptr<BmRowContainer> makeBmRowContainer(
    DatasetKind dataset,
    const std::shared_ptr<memory::bm::BufferManager>& bufferManager) {
  return std::make_unique<BmRowContainer>(
      columnTypes(dataset), bufferManager, memory::bm::MemoryTag::kHashBuild);
}

OldStoredRows storeOldRows(
    BenchmarkContext& context,
    const BenchmarkOptions& options,
    bool keepRows) {
  OldStoredRows stored;
  stored.container = makeOldRowContainer(options.dataset, context.pool.get());
  storeOldRowsOnly(
      *stored.container, context.pool.get(), options,
      keepRows ? &stored.rows : nullptr);
  return stored;
}

BmStoredRows storeBmRows(
    BenchmarkContext& context,
    const BenchmarkOptions& options,
    bool keepRows) {
  BmStoredRows stored;
  stored.container = makeBmRowContainer(options.dataset, context.bufferManager);
  storeBmRowsOnly(
      *stored.container, context.pool.get(), options,
      keepRows ? &stored.rows : nullptr);
  return stored;
}

void storeOldRowsOnly(
    RowContainer& container,
    memory::MemoryPool* pool,
    const BenchmarkOptions& options,
    std::vector<char*>* rows) {
  const auto totalRows = rowCount(options);
  if (rows != nullptr) {
    rows->reserve(totalRows);
  }
  for (uint64_t offset = 0; offset < totalRows;) {
    const auto batchSize = static_cast<vector_size_t>(
        std::min<uint64_t>(options.batchRows, totalRows - offset));
    auto batch = makeInputBatch(pool, options, offset, batchSize);
    storeInputBatchOld(container, batch, rows);
    offset += batchSize;
  }
}

void storeBmRowsOnly(
    BmRowContainer& container,
    memory::MemoryPool* pool,
    const BenchmarkOptions& options,
    std::vector<char*>* rows) {
  const auto totalRows = rowCount(options);
  if (rows != nullptr) {
    rows->reserve(totalRows);
  }
  for (uint64_t offset = 0; offset < totalRows;) {
    const auto batchSize = static_cast<vector_size_t>(
        std::min<uint64_t>(options.batchRows, totalRows - offset));
    auto batch = makeInputBatch(pool, options, offset, batchSize);
    storeInputBatchBm(container, batch, rows);
    offset += batchSize;
  }
}

void extractOldRows(
    RowContainer& container,
    const std::vector<char*>& rows,
    const BenchmarkOptions& options,
    memory::MemoryPool* pool) {
  const auto types = columnTypes(options.dataset);
  for (size_t offset = 0; offset < rows.size();) {
    const auto batchSize = static_cast<vector_size_t>(
        std::min<size_t>(options.batchRows, rows.size() - offset));
    for (auto column = 0; column < types.size(); ++column) {
      auto result = makeResultVector(types[column], batchSize, pool);
      container.extractColumn(
          rows.data() + offset, batchSize, column, 0, result);
      folly::doNotOptimizeAway(result);
    }
    offset += batchSize;
  }
}

void extractBmRowsResident(
    BmRowContainer& container,
    const std::vector<char*>& inputRows,
    const BenchmarkOptions& options,
    memory::MemoryPool* pool) {
  const auto types = columnTypes(options.dataset);
  std::vector<const char*> rows;
  rows.reserve(options.batchRows);
  for (size_t offset = 0; offset < inputRows.size();) {
    const auto batchSize = static_cast<vector_size_t>(
        std::min<size_t>(options.batchRows, inputRows.size() - offset));
    rows.clear();
    for (vector_size_t i = 0; i < batchSize; ++i) {
      rows.push_back(inputRows[offset + i]);
    }
    for (auto column = 0; column < types.size(); ++column) {
      auto result = makeResultVector(types[column], batchSize, pool);
      container.extractColumnResident(
          rows.data(), batchSize, column, result);
      folly::doNotOptimizeAway(result);
    }
    offset += batchSize;
  }
}

void readBmSpill(
    BmRowContainer& container,
    SegmentId segment,
    const BenchmarkOptions& options) {
  auto session = container.beginBulkReadSegments({&segment, 1});
  std::vector<char*> rows;
  std::vector<RowId> rowIds;
  auto result = session.tryLoadAll(rows, rowIds);
  if (result == LoadAllResult::kLoadedPointers) {
    folly::doNotOptimizeAway(rows.data());
    folly::doNotOptimizeAway(rows.size());
    return;
  }

  for (size_t offset = 0; offset < rowIds.size();) {
    const auto batchSize = static_cast<vector_size_t>(
        std::min<size_t>(options.batchRows, rowIds.size() - offset));
    auto window = session.loadRows(
        {rowIds.data() + offset, static_cast<size_t>(batchSize)});
    folly::doNotOptimizeAway(window.rows.data());
    folly::doNotOptimizeAway(window.rows.size());
    offset += batchSize;
  }
}

OldSpillData spillOldRows(
    BenchmarkContext& context,
    RowContainer& container,
    DatasetKind dataset) {
  auto config = makeOldSpillConfig(context.spillDir);
  Spiller spiller(
      Spiller::Type::kHashJoinBuild,
      &container,
      rowType(dataset),
      HashBitRange{},
      &config,
      /*targetFileSize=*/0,
      /*supportSkewPartition=*/false);
  spiller.spill();
  auto partition = spiller.finishSpill();
  auto rowFormat = std::make_unique<RowFormatInfo>(&container, true);
  return {std::move(rowFormat), std::move(partition)};
}

BmSpillData spillBmRows(
    BenchmarkContext& context,
    const BenchmarkOptions& options) {
  auto stored = storeBmRows(context, options, false);
  BmSpillData spill;
  spill.container = std::move(stored.container);
  spill.segment = spill.container->flushActiveSegment();
  return spill;
}

std::unique_ptr<RowContainer> readOldSpillIntoNewRowContainer(
    BenchmarkContext& context,
    OldSpillData& spillData,
    DatasetKind dataset) {
  auto target = makeOldRowContainer(dataset, context.pool.get());
  auto reader = spillData.partition.createUnorderedReader(
      context.pool.get(), /*spillUringEnabled=*/false, /*isRowBased=*/true);
  std::vector<char*> rows;
  while (reader->nextBatch(rows) != 0) {
    for (auto* row : rows) {
      target->copySerializedRow(row, spillData.rowFormat.get());
    }
  }
  return target;
}

} // namespace bytedance::bolt::exec::bm::benchmarks
