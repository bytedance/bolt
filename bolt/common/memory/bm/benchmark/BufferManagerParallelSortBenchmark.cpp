#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/benchmark/SparkListenableArbitratorContext.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_string(
    bm_parallel_sort_spill_dir,
    "/tmp/bolt-bm-parallel-sort-benchmark",
    "Directory used by parallel BufferManager sort benchmark spill files.");
DEFINE_uint64(
    bm_parallel_sort_data_gb_per_thread,
    10,
    "Logical data size each worker sorts, in GiB.");
DEFINE_uint64(
    bm_parallel_sort_memory_gb,
    8,
    "Shared ExecutionMemoryPool limit, in GiB.");
DEFINE_uint64(
    bm_parallel_sort_threads,
    8,
    "Number of parallel sort workers.");
DEFINE_string(
    bm_parallel_sort_allocate_size,
    "large",
    "BM allocation size class: small, medium, or large.");
DEFINE_uint64(
    bm_parallel_sort_seed,
    88172645463393265ULL,
    "Base seed for deterministic generated uint64 input.");
DEFINE_bool(
    bm_parallel_sort_keep_spill_files,
    false,
    "Keep spill files after the benchmark exits.");
DEFINE_bool(
    bm_parallel_sort_verify,
    true,
    "Verify each worker's sorted output with a k-way merge.");

namespace bytedance::bolt::memory::bm {
namespace {

using Clock = std::chrono::steady_clock;

constexpr uint64_t kGiB = 1024ULL * 1024ULL * 1024ULL;

struct ScopedTimer {
  explicit ScopedTimer(double& elapsedMs)
      : elapsedMs_(elapsedMs), start_(Clock::now()) {}

  ~ScopedTimer() {
    elapsedMs_ =
        std::chrono::duration<double, std::milli>(Clock::now() - start_)
            .count();
  }

  double& elapsedMs_;
  Clock::time_point start_;
};

double gibPerSecond(uint64_t bytes, double elapsedMs) {
  if (elapsedMs <= 0) {
    return 0;
  }
  return (static_cast<double>(bytes) / static_cast<double>(kGiB)) /
      (elapsedMs / 1'000.0);
}

AllocateSize parseAllocateSize(const std::string& value) {
  if (value == "small") {
    return AllocateSize::kSmall;
  }
  if (value == "medium") {
    return AllocateSize::kMedium;
  }
  if (value == "large") {
    return AllocateSize::kLarge;
  }
  throw std::invalid_argument(
      "bm_parallel_sort_allocate_size must be small, medium, or large");
}

uint64_t splitMix64(uint64_t& state) {
  uint64_t z = (state += 0x9e3779b97f4a7c15ULL);
  z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
  z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
  return z ^ (z >> 31);
}

class UInt64DataGenerator {
 public:
  UInt64DataGenerator(uint64_t totalValues, uint64_t seed)
      : totalValues_(totalValues), state_(seed) {}

  bool empty() const {
    return generated_ >= totalValues_;
  }

  uint64_t generated() const {
    return generated_;
  }

  size_t pull(uint64_t* output, size_t maxValues) {
    const auto remaining = totalValues_ - generated_;
    const auto count =
        static_cast<size_t>(std::min<uint64_t>(remaining, maxValues));
    for (size_t i = 0; i < count; ++i) {
      output[i] = splitMix64(state_);
    }
    generated_ += count;
    return count;
  }

 private:
  uint64_t totalValues_{0};
  uint64_t generated_{0};
  uint64_t state_{0};
};

FileBlockAllocatorConfig fileAllocatorConfig(const std::string& directory) {
  FileBlockAllocatorConfig config;
  config.directory = directory;
  config.bucket_sizes = {
      static_cast<int64_t>(allocateSizeBytes(AllocateSize::kSmall)),
      static_cast<int64_t>(allocateSizeBytes(AllocateSize::kMedium)),
      static_cast<int64_t>(allocateSizeBytes(AllocateSize::kLarge))};
  config.file_size_limit_bytes = 1024LL * 1024LL * 1024LL;
  config.max_open_files_per_bucket = 64;
  return config;
}

struct RunBlock {
  std::shared_ptr<BlockHandle> block;
  size_t values{0};
};

struct SortedRun {
  std::vector<RunBlock> blocks;
  uint64_t values{0};
};

struct PinnedRunBlock {
  BufferHandle handle;
  uint64_t* values{nullptr};
  size_t count{0};
  size_t index{0};
};

struct RunCursor {
  SortedRun* run{nullptr};
  BufferManager* manager{nullptr};
  size_t workerIndex{0};
  size_t runIndex{0};
  size_t blockIndex{0};
  uint64_t emitted{0};
  PinnedRunBlock pinned;

  RunCursor(
      SortedRun& sortedRun,
      BufferManager& bufferManager,
      size_t worker,
      size_t index)
      : run(&sortedRun),
        manager(&bufferManager),
        workerIndex(worker),
        runIndex(index) {}

  bool pinNextBlock() {
    pinned = {};
    if (blockIndex >= run->blocks.size()) {
      return false;
    }
    const auto currentBlockIndex = blockIndex++;
    auto& block = run->blocks[currentBlockIndex];
    BOLT_CHECK_NOT_NULL(block.block);
    auto blockHandle = std::move(block.block);
    VLOG(1) << "BM parallel sort verify pin begin"
            << " worker=" << workerIndex
            << " run_index=" << runIndex
            << " block_index=" << currentBlockIndex
            << " run_blocks=" << run->blocks.size();
    pinned.handle = manager->Pin(blockHandle);
    pinned.values = reinterpret_cast<uint64_t*>(pinned.handle.Ptr());
    pinned.count = block.values;
    pinned.index = 0;
    return true;
  }

  uint64_t value() const {
    return pinned.values[pinned.index];
  }

  bool advance() {
    ++pinned.index;
    ++emitted;
    if (pinned.index < pinned.count) {
      return true;
    }
    return pinNextBlock();
  }
};

class BlockedUInt64Array {
 public:
  struct Span {
    uint64_t* values{nullptr};
    size_t count{0};
  };

  explicit BlockedUInt64Array(std::vector<Span> spans)
      : spans_(std::move(spans)) {
    if (!spans_.empty()) {
      valuesPerBlock_ = spans_.front().count;
    }
    for (const auto& span : spans_) {
      total_ += span.count;
    }
  }

  class Reference {
   public:
    Reference(BlockedUInt64Array* array, size_t index)
        : array_(array), index_(index) {}

    operator uint64_t() const {
      return array_->get(index_);
    }

    Reference& operator=(uint64_t value) {
      array_->set(index_, value);
      return *this;
    }

    Reference& operator=(const Reference& other) {
      return *this = static_cast<uint64_t>(other);
    }

    friend void swap(Reference lhs, Reference rhs) {
      const auto tmp = static_cast<uint64_t>(lhs);
      lhs = static_cast<uint64_t>(rhs);
      rhs = tmp;
    }

   private:
    BlockedUInt64Array* array_;
    size_t index_;
  };

  class Iterator {
   public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = uint64_t;
    using difference_type = std::ptrdiff_t;
    using reference = Reference;
    using pointer = void;

    Iterator() = default;
    Iterator(BlockedUInt64Array* array, size_t index)
        : array_(array), index_(index) {}

    reference operator*() const {
      return Reference{array_, index_};
    }

    Iterator& operator++() {
      ++index_;
      return *this;
    }

    Iterator operator++(int) {
      auto copy = *this;
      ++*this;
      return copy;
    }

    Iterator& operator--() {
      --index_;
      return *this;
    }

    Iterator operator--(int) {
      auto copy = *this;
      --*this;
      return copy;
    }

    Iterator& operator+=(difference_type delta) {
      index_ += delta;
      return *this;
    }

    Iterator& operator-=(difference_type delta) {
      index_ -= delta;
      return *this;
    }

    Iterator operator+(difference_type delta) const {
      auto copy = *this;
      copy += delta;
      return copy;
    }

    Iterator operator-(difference_type delta) const {
      auto copy = *this;
      copy -= delta;
      return copy;
    }

    difference_type operator-(const Iterator& other) const {
      return static_cast<difference_type>(index_) -
          static_cast<difference_type>(other.index_);
    }

    reference operator[](difference_type delta) const {
      return *(*this + delta);
    }

    bool operator==(const Iterator& other) const {
      return index_ == other.index_;
    }

    bool operator!=(const Iterator& other) const {
      return !(*this == other);
    }

    bool operator<(const Iterator& other) const {
      return index_ < other.index_;
    }

    bool operator>(const Iterator& other) const {
      return other < *this;
    }

    bool operator<=(const Iterator& other) const {
      return !(other < *this);
    }

    bool operator>=(const Iterator& other) const {
      return !(*this < other);
    }

   private:
    BlockedUInt64Array* array_{nullptr};
    size_t index_{0};
  };

  Iterator begin() {
    return Iterator{this, 0};
  }

  Iterator end() {
    return Iterator{this, total_};
  }

 private:
  uint64_t get(size_t index) const {
    const auto block = index / valuesPerBlock_;
    const auto offset = index % valuesPerBlock_;
    return spans_[block].values[offset];
  }

  void set(size_t index, uint64_t value) {
    const auto block = index / valuesPerBlock_;
    const auto offset = index % valuesPerBlock_;
    spans_[block].values[offset] = value;
  }

  std::vector<Span> spans_;
  size_t valuesPerBlock_{0};
  size_t total_{0};
};

struct ActiveRun {
  SortedRun metadata;
  std::vector<BufferHandle> handles;

  bool empty() const {
    return metadata.values == 0;
  }

  void clear() {
    metadata = {};
    handles.clear();
  }
};

struct WorkerResult {
  size_t workerIndex{0};
  bool ok{false};
  bool verified{false};
  std::string error;
  uint64_t totalBytes{0};
  uint64_t totalValues{0};
  size_t runCount{0};
  size_t blockCount{0};
  double generateAndRunSortMs{0};
  double verifyMs{0};
  BufferManagerStats bmStats;
  std::string bmDebug;
};

void sortActiveRun(ActiveRun& run) {
  std::vector<BlockedUInt64Array::Span> spans;
  spans.reserve(run.handles.size());
  for (size_t i = 0; i < run.handles.size(); ++i) {
    spans.push_back(
        {reinterpret_cast<uint64_t*>(run.handles[i].Ptr()),
         run.metadata.blocks[i].values});
  }
  BlockedUInt64Array array{std::move(spans)};
  std::sort(array.begin(), array.end());
}

size_t countRunBlocks(const std::vector<SortedRun>& runs) {
  size_t blocks = 0;
  for (const auto& run : runs) {
    blocks += run.blocks.size();
  }
  return blocks;
}

bool verifySortedRuns(
    size_t workerIndex,
    BufferManager& manager,
    std::vector<SortedRun>& runs,
    uint64_t expectedValues) {
  struct HeapEntry {
    uint64_t value{0};
    size_t cursor{0};

    bool operator>(const HeapEntry& other) const {
      return value > other.value;
    }
  };

  std::vector<RunCursor> cursors;
  cursors.reserve(runs.size());
  std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<>> heap;

  for (size_t i = 0; i < runs.size(); ++i) {
    cursors.emplace_back(runs[i], manager, workerIndex, i);
    if (cursors.back().pinNextBlock()) {
      heap.push(HeapEntry{cursors.back().value(), i});
    }
  }

  uint64_t emitted = 0;
  uint64_t previous = 0;
  bool first = true;
  while (!heap.empty()) {
    auto entry = heap.top();
    heap.pop();
    if (!first && entry.value < previous) {
      std::cerr << "worker " << workerIndex
                << " sort verification failed at row " << emitted << ": "
                << entry.value << " < " << previous << "\n";
      return false;
    }
    first = false;
    previous = entry.value;
    ++emitted;

    auto& cursor = cursors[entry.cursor];
    if (cursor.advance()) {
      heap.push(HeapEntry{cursor.value(), entry.cursor});
    }
  }
  if (emitted != expectedValues) {
    std::cerr << "worker " << workerIndex << " sort verification emitted "
              << emitted << " values, expected " << expectedValues << "\n";
    return false;
  }
  return true;
}

WorkerResult runWorker(
    size_t workerIndex,
    MemoryPool& root,
    AllocateSize allocateSize,
    uint64_t dataBytes,
    uint64_t seed,
    const std::filesystem::path& spillRoot) {
  WorkerResult result;
  result.workerIndex = workerIndex;
  result.totalBytes = dataBytes;
  result.totalValues = dataBytes / sizeof(uint64_t);
  if (result.totalValues == 0) {
    throw std::invalid_argument("worker data size must be positive");
  }

  const auto spillDir = spillRoot / ("worker_" + std::to_string(workerIndex));
  std::filesystem::create_directories(spillDir);

  BufferManagerConfig config;
  config.poolName = "bm-parallel-sort-" + std::to_string(workerIndex);
  config.spillStoreConfig.fileAllocatorConfig =
      fileAllocatorConfig(spillDir.string());
  auto manager = BufferManager::Create(root, std::move(config));

  const auto blockBytes = allocateSizeBytes(allocateSize);
  const auto valuesPerBlock = blockBytes / sizeof(uint64_t);
  std::vector<SortedRun> runs;
  ActiveRun active;
  UInt64DataGenerator generator{
      result.totalValues, seed + workerIndex * 0x9e3779b97f4a7c15ULL};

  {
    ScopedTimer timer{result.generateAndRunSortMs};
    while (!generator.empty()) {
      if (!manager->MaybeReserve(blockBytes) && !active.empty()) {
        sortActiveRun(active);
        runs.push_back(std::move(active.metadata));
        active.clear();
        manager->ReleaseUnusedReservation();
        continue;
      }

      auto handle = manager->Allocate(blockBytes, MemoryTag::kUnknown);
      auto block = handle.block();
      auto* values = reinterpret_cast<uint64_t*>(handle.Ptr());
      const auto blockValues = generator.pull(values, valuesPerBlock);
      active.metadata.blocks.push_back(
          RunBlock{std::move(block), blockValues});
      active.metadata.values += blockValues;
      active.handles.push_back(std::move(handle));
    }

    if (!active.empty()) {
      sortActiveRun(active);
      runs.push_back(std::move(active.metadata));
      active.clear();
      manager->ReleaseUnusedReservation();
    }
  }

  result.runCount = runs.size();
  result.blockCount = countRunBlocks(runs);
  result.verified = true;
  if (FLAGS_bm_parallel_sort_verify) {
    ScopedTimer timer{result.verifyMs};
    result.verified =
        verifySortedRuns(workerIndex, *manager, runs, result.totalValues);
  }

  result.bmStats = manager->stats();
  result.bmDebug = manager->debugString();
  result.ok = result.verified;
  if (!FLAGS_bm_parallel_sort_keep_spill_files) {
    std::filesystem::remove_all(spillDir);
  }
  return result;
}

void addStats(
    SparkListenableArbitratorContextStats& aggregate,
    const SparkListenableArbitratorContextStats& stats) {
  aggregate.automaticSpillTriggers += stats.automaticSpillTriggers;
  aggregate.automaticSpillRequestedBytes +=
      stats.automaticSpillRequestedBytes;
  aggregate.automaticSpillShrunkenBytes += stats.automaticSpillShrunkenBytes;
  aggregate.automaticSpillReclaimedBytes += stats.automaticSpillReclaimedBytes;
  aggregate.automaticSpillReturnedBytes += stats.automaticSpillReturnedBytes;
  aggregate.automaticSpillTimeUs += stats.automaticSpillTimeUs;
}

void addStats(BufferManagerStats& aggregate, const BufferManagerStats& stats) {
  aggregate.allocatedBlocks += stats.allocatedBlocks;
  aggregate.liveBlocks += stats.liveBlocks;
  aggregate.pinnedResidentBytes += stats.pinnedResidentBytes;
  aggregate.unpinnedResidentBytes += stats.unpinnedResidentBytes;
  aggregate.spilledBytes += stats.spilledBytes;
  aggregate.prefetchingBytes += stats.prefetchingBytes;
  aggregate.spillingBytes += stats.spillingBytes;
  aggregate.reclaimedBytes += stats.reclaimedBytes;
  aggregate.pinCount += stats.pinCount;
  aggregate.pinInMemoryCount += stats.pinInMemoryCount;
  aggregate.pinReadCount += stats.pinReadCount;
  aggregate.batchPinCount += stats.batchPinCount;
  aggregate.prefetchCount += stats.prefetchCount;
  aggregate.reclaimCount += stats.reclaimCount;
  aggregate.reclaimAttemptedBlocks += stats.reclaimAttemptedBlocks;
  aggregate.reclaimSkippedBlocks += stats.reclaimSkippedBlocks;
  aggregate.spillWriteCount += stats.spillWriteCount;
  aggregate.spillReadCount += stats.spillReadCount;
  aggregate.spillWriteBytes += stats.spillWriteBytes;
  aggregate.spillReadBytes += stats.spillReadBytes;
  aggregate.fileAllocateFailures += stats.fileAllocateFailures;
  aggregate.fileFreeFailures += stats.fileFreeFailures;
  aggregate.readIoFailures += stats.readIoFailures;
  aggregate.writeIoFailures += stats.writeIoFailures;
  aggregate.prefetchSubmitFailures += stats.prefetchSubmitFailures;
  aggregate.prefetchIoFailures += stats.prefetchIoFailures;
  aggregate.evictionQueueSize += stats.evictionQueueSize;
  aggregate.evictionQueueStaleEntries += stats.evictionQueueStaleEntries;
}

int runBenchmark() {
  const auto threadCount =
      static_cast<size_t>(FLAGS_bm_parallel_sort_threads);
  const auto allocateSize =
      parseAllocateSize(FLAGS_bm_parallel_sort_allocate_size);
  const auto blockBytes = allocateSizeBytes(allocateSize);
  const auto dataBytes = FLAGS_bm_parallel_sort_data_gb_per_thread * kGiB;
  const auto memoryLimitBytes = FLAGS_bm_parallel_sort_memory_gb * kGiB;
  if (threadCount == 0 || dataBytes == 0 || memoryLimitBytes == 0) {
    throw std::invalid_argument(
        "threads, data_gb_per_thread, and memory_gb must be positive");
  }

  const std::filesystem::path spillRoot{FLAGS_bm_parallel_sort_spill_dir};
  std::filesystem::remove_all(spillRoot);
  std::filesystem::create_directories(spillRoot);

  SparkListenableArbitratorContext::initializeExecutionMemoryPool(
      static_cast<int64_t>(memoryLimitBytes),
      10'000,
      static_cast<int32_t>(threadCount));

  std::vector<std::unique_ptr<SparkListenableArbitratorContext>> contexts;
  std::vector<std::shared_ptr<MemoryPool>> roots;
  contexts.reserve(threadCount);
  roots.reserve(threadCount);
  for (size_t i = 0; i < threadCount; ++i) {
    contexts.push_back(std::make_unique<SparkListenableArbitratorContext>(
        SparkListenableArbitratorContextOptions{
            .name = "bm-parallel-sort-benchmark-" + std::to_string(i),
            .memoryLimitBytes = static_cast<int64_t>(memoryLimitBytes),
            .sessionConf = {},
            .maxTaskNumber = static_cast<int32_t>(threadCount),
            .initializeExecutionMemoryPool = false}));
    contexts.back()->installAutomaticReclaimSpill();
    roots.push_back(contexts.back()->rootPool());
  }

  std::vector<WorkerResult> results(threadCount);
  std::vector<std::exception_ptr> errors(threadCount);
  std::vector<std::thread> threads;
  threads.reserve(threadCount);
  std::atomic<bool> start{false};
  double totalMs = 0;
  {
    ScopedTimer timer{totalMs};
    for (size_t i = 0; i < threadCount; ++i) {
      threads.emplace_back([&, i] {
        while (!start.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        try {
          results[i] = runWorker(
              i,
              *roots[i],
              allocateSize,
              dataBytes,
              FLAGS_bm_parallel_sort_seed,
              spillRoot);
        } catch (...) {
          errors[i] = std::current_exception();
        }
      });
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) {
      thread.join();
    }
  }

  bool ok = true;
  uint64_t aggregateBytes = 0;
  uint64_t aggregateValues = 0;
  size_t aggregateRuns = 0;
  size_t aggregateBlocks = 0;
  double aggregateGenerateMs = 0;
  double aggregateVerifyMs = 0;
  BufferManagerStats aggregateStats;
  SparkListenableArbitratorContextStats aggregateContextStats;

  for (size_t i = 0; i < threadCount; ++i) {
    if (errors[i]) {
      ok = false;
      try {
        std::rethrow_exception(errors[i]);
      } catch (const std::exception& e) {
        results[i].error = e.what();
      } catch (...) {
        results[i].error = "unknown exception";
      }
    } else if (!results[i].ok) {
      ok = false;
    }

    aggregateBytes += results[i].totalBytes;
    aggregateValues += results[i].totalValues;
    aggregateRuns += results[i].runCount;
    aggregateBlocks += results[i].blockCount;
    aggregateGenerateMs += results[i].generateAndRunSortMs;
    aggregateVerifyMs += results[i].verifyMs;
    addStats(aggregateStats, results[i].bmStats);
    addStats(aggregateContextStats, contexts[i]->stats());

    std::cout << "bm_parallel_sort_worker"
              << " worker=" << i
              << " status=" << (results[i].ok ? "ok" : "failed")
              << " error=\"" << results[i].error << "\""
              << " runs=" << results[i].runCount
              << " blocks=" << results[i].blockCount
              << " values=" << results[i].totalValues
              << " generate_sort_ms=" << results[i].generateAndRunSortMs
              << " verify_ms=" << results[i].verifyMs
              << " verified=" << (results[i].verified ? "true" : "false")
              << " spill_write_bytes=" << results[i].bmStats.spillWriteBytes
              << " spill_read_bytes=" << results[i].bmStats.spillReadBytes
              << " reclaim_count=" << results[i].bmStats.reclaimCount
              << " automatic_spill_triggers="
              << contexts[i]->stats().automaticSpillTriggers
              << " automatic_spill_returned_bytes="
              << contexts[i]->stats().automaticSpillReturnedBytes
              << "\n";
  }

  std::cout << "bm_parallel_sort_benchmark\n"
            << "status value=" << (ok ? "ok" : "failed") << "\n"
            << "config"
            << " threads=" << threadCount
            << " data_gb_per_thread="
            << FLAGS_bm_parallel_sort_data_gb_per_thread
            << " memory_gb=" << FLAGS_bm_parallel_sort_memory_gb
            << " allocate_size=" << toString(allocateSize)
            << " block_bytes=" << blockBytes << "\n"
            << "runs"
            << " count=" << aggregateRuns
            << " blocks=" << aggregateBlocks
            << " values=" << aggregateValues << "\n"
            << "timing"
            << " wall_ms=" << totalMs
            << " worker_generate_sort_ms_sum=" << aggregateGenerateMs
            << " worker_verify_ms_sum=" << aggregateVerifyMs
            << " wall_gib_per_s=" << gibPerSecond(aggregateBytes, totalMs)
            << "\n"
            << "automatic_spill"
            << " triggers=" << aggregateContextStats.automaticSpillTriggers
            << " requested_bytes="
            << aggregateContextStats.automaticSpillRequestedBytes
            << " shrunken_bytes="
            << aggregateContextStats.automaticSpillShrunkenBytes
            << " reclaimed_bytes="
            << aggregateContextStats.automaticSpillReclaimedBytes
            << " returned_bytes="
            << aggregateContextStats.automaticSpillReturnedBytes
            << " time_us=" << aggregateContextStats.automaticSpillTimeUs
            << "\n"
            << "bm_summary"
            << " reclaimed_bytes=" << aggregateStats.reclaimedBytes
            << " spill_write_bytes=" << aggregateStats.spillWriteBytes
            << " spill_read_bytes=" << aggregateStats.spillReadBytes
            << " spill_write_count=" << aggregateStats.spillWriteCount
            << " spill_read_count=" << aggregateStats.spillReadCount
            << " reclaim_count=" << aggregateStats.reclaimCount << "\n"
            << "execution_pool "
            << sparksql::ExecutionMemoryPool::instance()->toString() << "\n";

  if (!FLAGS_bm_parallel_sort_keep_spill_files) {
    std::filesystem::remove_all(spillRoot);
  }
  return ok ? 0 : 1;
}

} // namespace
} // namespace bytedance::bolt::memory::bm

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  try {
    return bytedance::bolt::memory::bm::runBenchmark();
  } catch (const std::exception& e) {
    std::cerr << "BM parallel sort benchmark failed: " << e.what() << "\n";
    return 1;
  }
}
