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
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <mutex>
#include <random>
#include <unordered_set>

DEFINE_uint64(
    bm_row_container_data_bytes,
    25ULL << 30,
    "Logical input bytes processed by BM row container benchmarks.");
DEFINE_uint32(
    bm_row_container_batch_rows,
    4096,
    "Input rows per generated batch.");
DEFINE_uint64(
    bm_row_container_reusable_input_bytes,
    128ULL << 20,
    "Logical input bytes to materialize once and repeatedly feed into BM row "
    "container benchmarks. Set to 0 to materialize the full logical input.");
DEFINE_uint64(
    bm_row_container_warmup_data_bytes,
    0,
    "Logical input bytes processed by a same-process per-case warm-up before "
    "the measured BM row container benchmark. Set to 0 to disable warm-up.");
DEFINE_uint32(
    bm_row_container_variable_max_string_length,
    64,
    "Maximum string length for regular variable_small BM row container benchmarks. "
    "Rows use deterministic lengths in [1, max].");
DEFINE_uint32(
    bm_row_container_large_string_length,
    1024,
    "Fixed string length for variable_large BM row container benchmarks.");
DEFINE_uint64(
    bm_row_container_spill_write_buffer_bytes,
    4ULL << 20,
    "Write buffer size used by old RowContainer Spiller benchmarks.");
DEFINE_uint32(
    bm_row_container_memory_multiplier,
    6,
    "Root memory pool capacity as a multiplier of logical input bytes. "
    "Use a larger value for old row-based spill read because it holds source, "
    "reader buffers and restored rows at the same time.");
DEFINE_bool(
    bm_row_container_spill_metrics,
    true,
    "Print per-call spill read/write phase metrics for BM row container "
    "benchmarks to stderr so metric lines can be redirected separately.");
DEFINE_uint32(
    bm_row_container_profile_ready_sleep_seconds,
    0,
    "If non-zero, selected profile-aware BM row container benchmarks print a "
    "Ready marker after input materialization and sleep for this many seconds "
    "before entering the measured path. This lets perf attach after benchmark "
    "setup is complete.");

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
  return bytes + estimatedStringBytesPerRow(
                     options.dataset, options.stringProfiles);
}

common::CompressionKind oldCompressionKind(SpillCompressionKind compression) {
  switch (compression) {
    case SpillCompressionKind::kRaw:
      return common::CompressionKind_NONE;
    case SpillCompressionKind::kLz4:
      return common::CompressionKind_LZ4;
    case SpillCompressionKind::kZstd:
      return common::CompressionKind_ZSTD;
  }
  BOLT_UNREACHABLE();
}

common::RowBasedSpillMode oldRowBasedSpillMode(
    SpillCompressionKind compression) {
  return compression == SpillCompressionKind::kRaw
      ? common::RowBasedSpillMode::RAW
      : common::RowBasedSpillMode::COMPRESSION;
}

memory::bm::compress::CompressionKind bmCompressionKind(
    SpillCompressionKind compression) {
  switch (compression) {
    case SpillCompressionKind::kRaw:
      return memory::bm::compress::CompressionKind::kNone;
    case SpillCompressionKind::kLz4:
      return memory::bm::compress::CompressionKind::kLz4Block;
    case SpillCompressionKind::kZstd:
      return memory::bm::compress::CompressionKind::kZstdFrame;
  }
  BOLT_UNREACHABLE();
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

common::SpillConfig makeOldSpillConfig(
    const std::string& spillDir,
    SpillCompressionKind compression) {
  common::SpillConfig config;
  config.getSpillDirPathCb = [&spillDir]() -> const std::string& {
    return spillDir;
  };
  config.updateAndCheckSpillLimitCb = [](uint64_t) {};
  config.fileNamePrefix = "bm-row-container-old";
  config.maxFileSize = 0;
  config.spillUringEnabled = false;
  config.writeBufferSize = FLAGS_bm_row_container_spill_write_buffer_bytes;
  config.compressionKind = oldCompressionKind(compression);
  config.rowBasedSpillMode = oldRowBasedSpillMode(compression);
  config.maxSpillRunRows = 0;
  return config;
}

VectorPtr makeResultVector(
    const TypePtr& type,
    vector_size_t size,
    memory::MemoryPool* pool) {
  return BaseVector::create(type, size, pool);
}

RowVectorPtr prefixRows(const RowVectorPtr& batch, vector_size_t size) {
  if (size == batch->size()) {
    return batch;
  }
  return std::dynamic_pointer_cast<RowVector>(batch->slice(0, size));
}

template <typename Store>
void storeReusableInputBatches(
    const ReusableInputBatches& input,
    uint64_t totalRows,
    Store store) {
  BOLT_CHECK(!input.batches.empty());
  size_t nextBatch = 0;
  uint64_t remaining = totalRows;
  while (remaining > 0) {
    const auto& batch = input.batches[nextBatch];
    const auto batchRows = static_cast<vector_size_t>(
        std::min<uint64_t>(batch->size(), remaining));
    store(prefixRows(batch, batchRows));
    remaining -= batchRows;
    nextBatch = (nextBatch + 1) % input.batches.size();
  }
}

} // namespace

uint64_t benchmarkNowNs() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

double nsToMs(uint64_t ns) {
  return static_cast<double>(ns) / 1'000'000.0;
}

uint64_t counterDelta(uint64_t before, uint64_t after) {
  return after >= before ? after - before : 0;
}

const char* spillCompressionName(SpillCompressionKind compression) {
  switch (compression) {
    case SpillCompressionKind::kRaw:
      return "raw";
    case SpillCompressionKind::kLz4:
      return "lz4";
    case SpillCompressionKind::kZstd:
      return "zstd";
  }
  BOLT_UNREACHABLE();
}

bool shouldPrintSpillMetrics(
    const char* benchmark,
    DatasetKind dataset,
    SpillCompressionKind compression) {
  if (!FLAGS_bm_row_container_spill_metrics) {
    return false;
  }
  static std::mutex mutex;
  static std::unordered_set<std::string> printed;
  const auto key = fmt::format(
      "{}:{}:{}",
      benchmark,
      datasetName(dataset),
      spillCompressionName(compression));
  std::lock_guard<std::mutex> l(mutex);
  return printed.insert(key).second;
}

namespace {

bool shouldWarmupBenchmarks() {
  return FLAGS_bm_row_container_warmup_data_bytes != 0;
}

BenchmarkOptions makeWarmupOptions(const BenchmarkOptions& options) {
  return benchmarks::options(
      options.dataset,
      FLAGS_bm_row_container_warmup_data_bytes,
      options.compression);
}

} // namespace

BenchmarkContext::BenchmarkContext(
    const std::string& name,
    uint64_t dataBytes,
    uint32_t memoryMultiplier,
    SpillCompressionKind compression)
    : compression(compression) {
  const auto id = benchmarkId.fetch_add(1);
  const auto poolName = fmt::format("bm-row-container-benchmark-{}-{}", name, id);
  const auto multiplier =
      memoryMultiplier == 0 ? FLAGS_bm_row_container_memory_multiplier
                            : memoryMultiplier;
  rootPool = memory::memoryManager()->addRootPool(
      poolName,
      std::max<uint64_t>(dataBytes * multiplier, 1ULL << 30),
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
  config.spillStoreConfig.compressionConfig.kind = bmCompressionKind(compression);
  config.spillStoreConfig.compressionConfig.minCompressBytes = 1;
  config.spillStoreConfig.compressionConfig.lz4.strategy =
      memory::bm::compress::Lz4Strategy::kPooledContext;
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

BenchmarkOptions options(
    DatasetKind dataset,
    uint64_t dataBytes,
    SpillCompressionKind compression) {
  BOLT_CHECK_GT(
      FLAGS_bm_row_container_variable_max_string_length,
      0,
      "--bm_row_container_variable_max_string_length must be greater than 0.");
  BOLT_CHECK_GT(
      FLAGS_bm_row_container_large_string_length,
      0,
      "--bm_row_container_large_string_length must be greater than 0.");
  return BenchmarkOptions{
      .dataset = dataset,
      .dataBytes = dataBytes,
      .batchRows = static_cast<vector_size_t>(
          FLAGS_bm_row_container_batch_rows),
      .stringProfiles =
          StringProfileOptions{
              .variableMaxStringLength =
                  FLAGS_bm_row_container_variable_max_string_length,
              .largeStringLength = FLAGS_bm_row_container_large_string_length},
      .compression = compression};
}

uint64_t rowCount(const BenchmarkOptions& options) {
  return std::max<uint64_t>(
      1, (options.dataBytes + logicalRowBytes(options) - 1) /
          logicalRowBytes(options));
}

void checkOldRowBasedSpillBenchmarkSupported(
    const BenchmarkOptions& options) {
  constexpr uint64_t kMaxOldRowBasedSpillRows =
      static_cast<uint64_t>(std::numeric_limits<int32_t>::max());
  const auto rows = rowCount(options);
  if (rows <= kMaxOldRowBasedSpillRows) {
    return;
  }

  fmt::print(
      stderr,
      "[bm-row-container-error] old row-based Spiller benchmark is not "
      "supported for this input size: dataset={} logical_bytes={} "
      "logical_row_bytes={} estimated_rows={} max_supported_rows={}. "
      "The old Spiller records rowsWritten for one row-based spill run as "
      "int32_t, and this benchmark does not split old spill runs. Reduce "
      "--bm_row_container_data_bytes or run a BM-only benchmark.\n",
      datasetName(options.dataset),
      options.dataBytes,
      logicalRowBytes(options),
      rows,
      kMaxOldRowBasedSpillRows);
  std::exit(1);
}

std::vector<TypePtr> columnTypes(DatasetKind dataset) {
  std::vector<TypePtr> types{BIGINT(), INTEGER(), DOUBLE()};
  if (hasVariableColumn(dataset)) {
    types.push_back(VARCHAR());
  }
  return types;
}

RowTypePtr rowType(DatasetKind dataset) {
  std::vector<std::string> names{"c0", "c1", "c2"};
  if (hasVariableColumn(dataset)) {
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
  if (hasVariableColumn(options.dataset)) {
    std::vector<std::string> strings;
    strings.reserve(size);
    for (vector_size_t row = 0; row < size; ++row) {
      const auto logicalRow = startRow + row;
      strings.push_back(randomString(
          logicalRow,
          stringLengthForRow(
              options.dataset, logicalRow, options.stringProfiles)));
    }
    children.push_back(maker.flatVector<std::string>(strings, VARCHAR()));
  }
  return maker.rowVector(std::move(children));
}

ReusableInputBatches makeReusableInputBatches(
    memory::MemoryPool* pool,
    const BenchmarkOptions& options) {
  const auto totalRows = rowCount(options);
  const auto rowBytes = logicalRowBytes(options);
  const auto cacheBytes = FLAGS_bm_row_container_reusable_input_bytes == 0
      ? options.dataBytes
      : std::min<uint64_t>(
            options.dataBytes, FLAGS_bm_row_container_reusable_input_bytes);
  const auto reusableRows = std::min<uint64_t>(
      totalRows, std::max<uint64_t>(1, (cacheBytes + rowBytes - 1) / rowBytes));

  ReusableInputBatches input;
  input.rows = reusableRows;
  input.batches.reserve((reusableRows + options.batchRows - 1) /
      options.batchRows);
  for (uint64_t offset = 0; offset < reusableRows;) {
    const auto batchSize = static_cast<vector_size_t>(
        std::min<uint64_t>(options.batchRows, reusableRows - offset));
    input.batches.push_back(makeInputBatch(pool, options, offset, batchSize));
    offset += batchSize;
  }
  return input;
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
    auto context = container.appendRow(kDefaultPartition);
    if (rows != nullptr) {
      rows->push_back(context.row());
    }
    for (auto column = 0; column < batch->childrenSize(); ++column) {
      container.store(context, decoded[column], row, column);
    }
  }
}

void storeInputBatchOldBatch(
    RowContainer& container,
    const RowVectorPtr& batch) {
  container.store(batch);
}

void storeInputBatchBmBatch(
    BmRowContainer& container,
    const RowVectorPtr& batch,
    std::vector<char*>* rows,
    BmBatchStringStoreMode stringStoreMode) {
  container.appendBatch(batch, kDefaultPartition, rows, stringStoreMode);
}

void storeReusableInputBatchesOld(
    RowContainer& container,
    const ReusableInputBatches& input,
    const BenchmarkOptions& options,
    std::vector<char*>* rows) {
  if (rows != nullptr) {
    rows->reserve(rowCount(options));
  }
  storeReusableInputBatches(input, rowCount(options), [&](const auto& batch) {
    storeInputBatchOld(container, batch, rows);
  });
}

void storeReusableInputBatchesBm(
    BmRowContainer& container,
    const ReusableInputBatches& input,
    const BenchmarkOptions& options,
    std::vector<char*>* rows) {
  if (rows != nullptr) {
    rows->reserve(rowCount(options));
  }
  storeReusableInputBatches(input, rowCount(options), [&](const auto& batch) {
    storeInputBatchBm(container, batch, rows);
  });
}

void storeReusableInputBatchesOldBatch(
    RowContainer& container,
    const ReusableInputBatches& input,
    const BenchmarkOptions& options) {
  storeReusableInputBatches(input, rowCount(options), [&](const auto& batch) {
    storeInputBatchOldBatch(container, batch);
  });
}

void storeReusableInputBatchesBmBatch(
    BmRowContainer& container,
    const ReusableInputBatches& input,
    const BenchmarkOptions& options,
    std::vector<char*>* rows,
    BmBatchStringStoreMode stringStoreMode) {
  if (rows != nullptr) {
    rows->reserve(rowCount(options));
  }
  storeReusableInputBatches(input, rowCount(options), [&](const auto& batch) {
    storeInputBatchBmBatch(container, batch, rows, stringStoreMode);
  });
}

std::unique_ptr<RowContainer> makeOldRowContainer(
    DatasetKind dataset,
    memory::MemoryPool* pool) {
  return std::make_unique<RowContainer>(columnTypes(dataset), pool);
}

std::unique_ptr<BmRowContainer> makeBmRowContainer(
    DatasetKind dataset,
    const std::shared_ptr<memory::bm::BufferManager>& bufferManager) {
  auto types = columnTypes(dataset);
  std::vector<bool> nullable(types.size(), false);
  return std::make_unique<BmRowContainer>(
      std::move(types),
      std::move(nullable),
      bufferManager,
      memory::bm::MemoryTag::kHashBuild);
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
  auto input = makeReusableInputBatches(pool, options);
  storeReusableInputBatchesOld(container, input, options, rows);
}

void storeBmRowsOnly(
    BmRowContainer& container,
    memory::MemoryPool* pool,
    const BenchmarkOptions& options,
    std::vector<char*>* rows) {
  auto input = makeReusableInputBatches(pool, options);
  storeReusableInputBatchesBm(container, input, options, rows);
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

namespace {

void extractBmRowsResidentImpl(
    BmRowContainer& container,
    const char* const* inputRows,
    size_t inputRowCount,
    const BenchmarkOptions& options,
    memory::MemoryPool* pool) {
  const auto types = columnTypes(options.dataset);
  for (size_t offset = 0; offset < inputRowCount;) {
    const auto batchSize = static_cast<vector_size_t>(
        std::min<size_t>(options.batchRows, inputRowCount - offset));
    for (auto column = 0; column < types.size(); ++column) {
      auto result = makeResultVector(types[column], batchSize, pool);
      container.extractColumnResident(
          inputRows + offset, batchSize, column, result);
      folly::doNotOptimizeAway(result);
    }
    offset += batchSize;
  }
}

} // namespace

void extractBmRowsResident(
    BmRowContainer& container,
    const std::vector<char*>& inputRows,
    const BenchmarkOptions& options,
    memory::MemoryPool* pool) {
  extractBmRowsResidentImpl(
      container,
      inputRows.data(),
      inputRows.size(),
      options,
      pool);
}

void extractBmRowsResident(
    BmRowContainer& container,
    const std::vector<const char*>& inputRows,
    const BenchmarkOptions& options,
    memory::MemoryPool* pool) {
  extractBmRowsResidentImpl(
      container,
      inputRows.data(),
      inputRows.size(),
      options,
      pool);
}

void readBmSpill(
    BmRowContainer& container,
    SegmentId segment,
    const BenchmarkOptions& options,
    BmSpillReadMetrics* metrics) {
  const auto beginStart = benchmarkNowNs();
  container.canBulkRead({&segment, 1});
  if (metrics != nullptr) {
    metrics->beginNs += benchmarkNowNs() - beginStart;
  }

  const auto listRowsStart = benchmarkNowNs();
  const auto totalRows = rowCount(options);
  auto session = container.beginBulkReadSegments({&segment, 1});
  auto rows = session.loadRows();
  if (metrics != nullptr) {
    metrics->listRowsNs += benchmarkNowNs() - listRowsStart;
    metrics->resultPointers = true;
    metrics->rows += rows.size();
  }
  BOLT_CHECK_EQ(totalRows, rows.size());
  folly::doNotOptimizeAway(rows.data());
  folly::doNotOptimizeAway(rows.size());
}

OldSpillData spillOldRows(
    BenchmarkContext& context,
    RowContainer& container,
    DatasetKind dataset,
    OldSpillWriteMetrics* metrics) {
  const auto spillStart = benchmarkNowNs();
  auto config = makeOldSpillConfig(context.spillDir, context.compression);
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
  if (metrics != nullptr) {
    metrics->spillNs += benchmarkNowNs() - spillStart;
    metrics->rows += partition.rowCount();
    metrics->spillBytes += partition.size();
    metrics->files += partition.numFiles();
  }
  auto rowFormat = std::make_unique<RowFormatInfo>(
      &container, context.compression != SpillCompressionKind::kRaw);
  return {std::move(rowFormat), std::move(partition)};
}

BmSpillData spillBmRows(
    BenchmarkContext& context,
    const BenchmarkOptions& options) {
  auto stored = storeBmRows(context, options, false);
  BmSpillData spill;
  spill.container = std::move(stored.container);
  spill.segment = spill.container->spillActiveSegment();
  return spill;
}

std::unique_ptr<RowContainer> readOldSpillIntoNewRowContainer(
    BenchmarkContext& context,
    OldSpillData& spillData,
    DatasetKind dataset,
    OldSpillReadMetrics* metrics,
    std::vector<char*>* restoredRows) {
  auto target = makeOldRowContainer(dataset, context.pool.get());
  if (restoredRows != nullptr) {
    restoredRows->clear();
    restoredRows->reserve(spillData.partition.rowCount());
  }
  const auto createReaderStart = benchmarkNowNs();
  auto reader = spillData.partition.createUnorderedReader(
      context.pool.get(), /*spillUringEnabled=*/false, /*isRowBased=*/true);
  if (metrics != nullptr) {
    metrics->createReaderNs += benchmarkNowNs() - createReaderStart;
  }

  std::vector<char*> rows;
  for (;;) {
    const auto nextBatchStart = benchmarkNowNs();
    const auto batchBytes = reader->nextBatch(rows);
    if (metrics != nullptr) {
      metrics->nextBatchNs += benchmarkNowNs() - nextBatchStart;
    }
    if (batchBytes == 0) {
      break;
    }
    if (metrics != nullptr) {
      ++metrics->batches;
      metrics->rows += rows.size();
      metrics->serializedBytes += batchBytes;
    }
    const auto copyRowsStart = benchmarkNowNs();
    for (auto* row : rows) {
      target->copySerializedRow(row, spillData.rowFormat.get());
    }
    if (metrics != nullptr) {
      metrics->copyRowsNs += benchmarkNowNs() - copyRowsStart;
    }
  }
  if (restoredRows != nullptr) {
    const auto listRowsStart = benchmarkNowNs();
    RowContainerIterator iter;
    const auto numRows = static_cast<int32_t>(target->numRows());
    restoredRows->resize(numRows);
    auto* output = restoredRows->data();
    auto remaining = numRows;
    while (remaining > 0) {
      const auto listed = target->listRows(&iter, remaining, output);
      if (listed == 0) {
        break;
      }
      output += listed;
      remaining -= listed;
    }
    restoredRows->resize(output - restoredRows->data());
    if (metrics != nullptr) {
      metrics->listRowsNs += benchmarkNowNs() - listRowsStart;
    }
  }
  return target;
}

void warmupStoreOld(const BenchmarkOptions& options) {
  if (!shouldWarmupBenchmarks()) {
    return;
  }
  auto warmup = makeWarmupOptions(options);
  BenchmarkContext context(
      "warmup-store-old", warmup.dataBytes, 0, warmup.compression);
  auto container = makeOldRowContainer(warmup.dataset, context.pool.get());
  storeOldRowsOnly(*container, context.pool.get(), warmup);
  folly::doNotOptimizeAway(container->numRows());
}

void warmupStoreBm(const BenchmarkOptions& options) {
  if (!shouldWarmupBenchmarks()) {
    return;
  }
  auto warmup = makeWarmupOptions(options);
  BenchmarkContext context(
      "warmup-store-bm", warmup.dataBytes, 0, warmup.compression);
  auto container = makeBmRowContainer(warmup.dataset, context.bufferManager);
  storeBmRowsOnly(*container, context.pool.get(), warmup);
  folly::doNotOptimizeAway(container->numRows());
}

void warmupStoreBatchOld(const BenchmarkOptions& options) {
  if (!shouldWarmupBenchmarks()) {
    return;
  }
  auto warmup = makeWarmupOptions(options);
  BenchmarkContext context(
      "warmup-store-batch-old", warmup.dataBytes, 0, warmup.compression);
  auto container = makeOldRowContainer(warmup.dataset, context.pool.get());
  auto input = makeReusableInputBatches(context.pool.get(), warmup);
  storeReusableInputBatchesOldBatch(*container, input, warmup);
  folly::doNotOptimizeAway(container->numRows());
}

void warmupStoreBatchBm(const BenchmarkOptions& options) {
  if (!shouldWarmupBenchmarks()) {
    return;
  }
  auto warmup = makeWarmupOptions(options);
  BenchmarkContext context(
      "warmup-store-batch-bm", warmup.dataBytes, 0, warmup.compression);
  auto container = makeBmRowContainer(warmup.dataset, context.bufferManager);
  auto input = makeReusableInputBatches(context.pool.get(), warmup);
  storeReusableInputBatchesBmBatch(*container, input, warmup);
  folly::doNotOptimizeAway(container->numRows());
}

void warmupReadOld(const BenchmarkOptions& options) {
  if (!shouldWarmupBenchmarks()) {
    return;
  }
  auto warmup = makeWarmupOptions(options);
  BenchmarkContext context(
      "warmup-read-old", warmup.dataBytes, 0, warmup.compression);
  auto stored = storeOldRows(context, warmup, true);
  extractOldRows(*stored.container, stored.rows, warmup, context.pool.get());
}

void warmupReadBm(const BenchmarkOptions& options) {
  if (!shouldWarmupBenchmarks()) {
    return;
  }
  auto warmup = makeWarmupOptions(options);
  BenchmarkContext context(
      "warmup-read-bm", warmup.dataBytes, 0, warmup.compression);
  auto stored = storeBmRows(context, warmup, true);
  extractBmRowsResident(
      *stored.container, stored.rows, warmup, context.pool.get());
}

void warmupSpillWriteOld(const BenchmarkOptions& options) {
  if (!shouldWarmupBenchmarks()) {
    return;
  }
  auto warmup = makeWarmupOptions(options);
  checkOldRowBasedSpillBenchmarkSupported(warmup);
  BenchmarkContext context(
      "warmup-spill-write-old", warmup.dataBytes, 0, warmup.compression);
  auto stored = storeOldRows(context, warmup, false);
  auto spill = spillOldRows(context, *stored.container, warmup.dataset);
  folly::doNotOptimizeAway(spill.partition.rowCount());
}

void warmupSpillWriteBm(const BenchmarkOptions& options) {
  if (!shouldWarmupBenchmarks()) {
    return;
  }
  auto warmup = makeWarmupOptions(options);
  BenchmarkContext context(
      "warmup-spill-write-bm", warmup.dataBytes, 0, warmup.compression);
  auto stored = storeBmRows(context, warmup, false);
  const auto segment = stored.container->spillActiveSegment();
  folly::doNotOptimizeAway(segment);
}

void warmupSpillReadOld(const BenchmarkOptions& options) {
  if (!shouldWarmupBenchmarks()) {
    return;
  }
  auto warmup = makeWarmupOptions(options);
  checkOldRowBasedSpillBenchmarkSupported(warmup);
  BenchmarkContext context(
      "warmup-spill-read-old", warmup.dataBytes, 8, warmup.compression);
  auto stored = storeOldRows(context, warmup, false);
  auto spill = spillOldRows(context, *stored.container, warmup.dataset);
  stored.container.reset();
  std::vector<char*> restoredRows;
  restoredRows.reserve(rowCount(warmup));
  auto restored = readOldSpillIntoNewRowContainer(
      context, spill, warmup.dataset, nullptr, &restoredRows);
  folly::doNotOptimizeAway(restored->numRows());
  folly::doNotOptimizeAway(restoredRows.data());
  folly::doNotOptimizeAway(restoredRows.size());
}

void warmupSpillReadBm(const BenchmarkOptions& options) {
  if (!shouldWarmupBenchmarks()) {
    return;
  }
  auto warmup = makeWarmupOptions(options);
  BenchmarkContext context(
      "warmup-spill-read-bm", warmup.dataBytes, 0, warmup.compression);
  auto spill = spillBmRows(context, warmup);
  readBmSpill(*spill.container, spill.segment, warmup);
}

} // namespace bytedance::bolt::exec::bm::benchmarks
