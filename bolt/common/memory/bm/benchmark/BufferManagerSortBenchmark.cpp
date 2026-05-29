#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/benchmark/SparkListenableArbitratorContext.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <queue>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_string(
    bm_sort_spill_dir,
    "/tmp/bolt-bm-sort-benchmark",
    "Directory used by BufferManager spill files.");
DEFINE_uint64(bm_sort_data_gb, 10, "Logical data size to sort, in GiB.");
DEFINE_uint64(bm_sort_memory_gb, 1, "Root memory pool limit, in GiB.");
DEFINE_string(
    bm_sort_allocate_size,
    "large",
    "BM allocation size class: small, medium, or large.");
DEFINE_uint64(
    bm_sort_seed,
    88172645463393265ULL,
    "Seed for deterministic generated uint64 input.");
DEFINE_bool(
    bm_sort_keep_spill_files,
    false,
    "Keep spill files after the benchmark exits.");
DEFINE_bool(
    bm_sort_verify,
    true,
    "Verify sorted output with a k-way merge after run generation.");

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
      "bm_sort_allocate_size must be small, medium, or large");
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
  size_t runIndex{0};
  size_t blockIndex{0};
  uint64_t emitted{0};
  PinnedRunBlock pinned;

  RunCursor(
      SortedRun& sortedRun,
      BufferManager& bufferManager,
      size_t index)
      : run(&sortedRun), manager(&bufferManager), runIndex(index) {}

  bool pinNextBlock() {
    pinned = {};
    if (blockIndex >= run->blocks.size()) {
      return false;
    }
    const auto currentBlockIndex = blockIndex++;
    auto& block = run->blocks[currentBlockIndex];
    BOLT_CHECK_NOT_NULL(block.block);
    auto blockHandle = std::move(block.block);
    auto before = manager->stats();
    VLOG(1) << "BM sort verify pin begin"
            << " run_index=" << runIndex
            << " block_index=" << currentBlockIndex
            << " run_blocks=" << run->blocks.size()
            << " block_values=" << block.values
            << " pinned_resident_bytes=" << before.pinnedResidentBytes
            << " unpinned_resident_bytes=" << before.unpinnedResidentBytes
            << " spilled_bytes=" << before.spilledBytes
            << " prefetching_bytes=" << before.prefetchingBytes
            << " reclaim_count=" << before.reclaimCount
            << " reclaimed_bytes=" << before.reclaimedBytes;
    pinned.handle = manager->Pin(blockHandle);
    auto after = manager->stats();
    VLOG(1) << "BM sort verify pin end"
            << " run_index=" << runIndex
            << " block_index=" << currentBlockIndex
            << " pinned_resident_bytes=" << after.pinnedResidentBytes
            << " unpinned_resident_bytes=" << after.unpinnedResidentBytes
            << " spilled_bytes=" << after.spilledBytes
            << " prefetching_bytes=" << after.prefetchingBytes
            << " reclaim_count=" << after.reclaimCount
            << " reclaimed_bytes=" << after.reclaimedBytes;
    pinned.values = reinterpret_cast<uint64_t*>(pinned.handle.Ptr());
    pinned.count = block.values;
    pinned.index = 0;
    return true;
  }

  bool valid() const {
    return pinned.values != nullptr && pinned.index < pinned.count;
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

void finalizeActiveRun(ActiveRun& run) {
  sortActiveRun(run);
}

bool verifySortedRuns(
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

  VLOG(1) << "BM sort verify begin"
          << " runs=" << runs.size()
          << " expected_values=" << expectedValues
          << " bm=" << manager.debugString();
  for (size_t i = 0; i < runs.size(); ++i) {
    cursors.emplace_back(runs[i], manager, i);
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
      std::cerr << "sort verification failed at row " << emitted << ": "
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
    std::cerr << "sort verification emitted " << emitted << " values, expected "
              << expectedValues << "\n";
    return false;
  }
  VLOG(1) << "BM sort verify end"
          << " emitted=" << emitted
          << " bm=" << manager.debugString();
  return true;
}

size_t countRunBlocks(const std::vector<SortedRun>& runs) {
  size_t blocks = 0;
  for (const auto& run : runs) {
    blocks += run.blocks.size();
  }
  return blocks;
}

void printBenchmarkSummary(
    const BufferManager& manager,
    const SparkListenableArbitratorContext& memoryContext,
    const MemoryPool& root,
    AllocateSize allocateSize,
    uint64_t blockBytes,
    uint64_t totalBytes,
    uint64_t totalValues,
    const std::vector<SortedRun>& runs,
    double generateAndRunSortMs,
    double verifyMs,
    bool verified,
    const char* status) {
  const auto stats = manager.stats();
  const auto contextStats = memoryContext.stats();
  const auto runBlocks = countRunBlocks(runs);
  const auto totalMs = generateAndRunSortMs + verifyMs;
  std::cout << "bm_sort_benchmark\n"
            << "status value=" << status << "\n"
            << "config"
            << " data_gb=" << FLAGS_bm_sort_data_gb
            << " memory_gb=" << FLAGS_bm_sort_memory_gb
            << " allocate_size=" << toString(allocateSize)
            << " block_bytes=" << blockBytes << "\n"
            << "runs"
            << " count=" << runs.size()
            << " blocks=" << runBlocks
            << " values=" << totalValues << "\n"
            << "timing"
            << " generate_sort_ms=" << generateAndRunSortMs
            << " verify_ms=" << verifyMs
            << " total_ms=" << totalMs
            << " generate_sort_gib_per_s="
            << gibPerSecond(totalBytes, generateAndRunSortMs)
            << " verify_gib_per_s="
            << gibPerSecond(totalBytes, verifyMs)
            << " verified=" << (verified ? "true" : "false") << "\n"
            << "automatic_spill"
            << " triggers=" << contextStats.automaticSpillTriggers
            << " requested_bytes="
            << contextStats.automaticSpillRequestedBytes
            << " shrunken_bytes=" << contextStats.automaticSpillShrunkenBytes
            << " reclaimed_bytes=" << contextStats.automaticSpillReclaimedBytes
            << " returned_bytes=" << contextStats.automaticSpillReturnedBytes
            << " time_us=" << contextStats.automaticSpillTimeUs << "\n"
            << "bm_summary"
            << " reclaimable_bytes=" << manager.reclaimableBytes()
            << " reclaimed_bytes=" << stats.reclaimedBytes
            << " spill_write_bytes=" << stats.spillWriteBytes
            << " spill_read_bytes=" << stats.spillReadBytes
            << " spill_write_count=" << stats.spillWriteCount
            << " spill_read_count=" << stats.spillReadCount << "\n"
            << "bm_debug " << manager.debugString() << "\n"
            << "root_pool " << root.toString(true) << "\n";
}

int runBenchmark() {
  const auto allocateSize = parseAllocateSize(FLAGS_bm_sort_allocate_size);
  const auto blockBytes = allocateSizeBytes(allocateSize);
  const auto valuesPerBlock = blockBytes / sizeof(uint64_t);
  const auto totalBytes = FLAGS_bm_sort_data_gb * kGiB;
  const auto totalValues = totalBytes / sizeof(uint64_t);
  const auto memoryLimitBytes = FLAGS_bm_sort_memory_gb * kGiB;
  if (totalValues == 0 || memoryLimitBytes == 0) {
    throw std::invalid_argument("data_gb and memory_gb must be positive");
  }

  std::filesystem::remove_all(FLAGS_bm_sort_spill_dir);
  std::filesystem::create_directories(FLAGS_bm_sort_spill_dir);

  SparkListenableArbitratorContext memoryContext{
      SparkListenableArbitratorContextOptions{
          .name = "bm-sort-benchmark",
          .memoryLimitBytes = static_cast<int64_t>(memoryLimitBytes),
          .sessionConf = {}}};
  auto root = memoryContext.rootPool();

  BufferManagerConfig config;
  config.poolName = "bm-sort-benchmark";
  config.spillStoreConfig.fileAllocatorConfig =
      fileAllocatorConfig(FLAGS_bm_sort_spill_dir);
  auto manager = BufferManager::Create(*root, std::move(config));
  memoryContext.installAutomaticReclaimSpill();

  std::vector<SortedRun> runs;
  ActiveRun active;
  UInt64DataGenerator generator{totalValues, FLAGS_bm_sort_seed};
  double generateAndRunSortMs = 0;
  double verifyMs = 0;

  {
    ScopedTimer timer{generateAndRunSortMs};
    while (!generator.empty()) {
      VLOG(1) << "BM sort MaybeReserve begin"
              << " generated_values=" << generator.generated()
              << " active_blocks=" << active.metadata.blocks.size()
              << " active_values=" << active.metadata.values
              << " run_count=" << runs.size()
              << " block_bytes=" << blockBytes
              << " bm=" << manager->debugString()
              << " root_pool=" << root->toString(true);
      const auto reserveOk = manager->MaybeReserve(blockBytes);
      VLOG(1) << "BM sort MaybeReserve end"
              << " ok=" << reserveOk
              << " generated_values=" << generator.generated()
              << " active_blocks=" << active.metadata.blocks.size()
              << " active_values=" << active.metadata.values
              << " run_count=" << runs.size()
              << " block_bytes=" << blockBytes
              << " bm=" << manager->debugString()
              << " root_pool=" << root->toString(true);
      if (!reserveOk && !active.empty()) {
        VLOG(1) << "BM sort finalize active run after MaybeReserve failure"
                << " generated_values=" << generator.generated()
                << " active_blocks=" << active.metadata.blocks.size()
                << " active_values=" << active.metadata.values
                << " bm=" << manager->debugString()
                << " root_pool=" << root->toString(true);
        finalizeActiveRun(active);
        runs.push_back(std::move(active.metadata));
        active.clear();
        manager->ReleaseUnusedReservation();
        VLOG(1) << "BM sort finalized run"
                << " runs=" << runs.size()
                << " bm=" << manager->debugString()
                << " root_pool=" << root->toString(true);
        continue;
      }

      std::shared_ptr<BlockHandle> block;
      VLOG(1) << "BM sort Allocate begin"
              << " generated_values=" << generator.generated()
              << " active_blocks=" << active.metadata.blocks.size()
              << " active_values=" << active.metadata.values
              << " run_count=" << runs.size()
              << " block_bytes=" << blockBytes
              << " bm=" << manager->debugString()
              << " root_pool=" << root->toString(true);
      auto handle = manager->Allocate(blockBytes, MemoryTag::kUnknown, &block);
      auto* values = reinterpret_cast<uint64_t*>(handle.Ptr());
      const auto blockValues = generator.pull(values, valuesPerBlock);
      VLOG(1) << "BM sort Allocate end"
              << " generated_values=" << generator.generated()
              << " block_id=" << block->id()
              << " block_values=" << blockValues
              << " active_blocks_before_push=" << active.metadata.blocks.size()
              << " active_values_before_push=" << active.metadata.values
              << " run_count=" << runs.size()
              << " bm=" << manager->debugString()
              << " root_pool=" << root->toString(true);

      active.metadata.blocks.push_back(
          RunBlock{std::move(block), blockValues});
      active.metadata.values += blockValues;
      active.handles.push_back(std::move(handle));
      VLOG(1) << "BM sort active run appended"
              << " generated_values=" << generator.generated()
              << " active_blocks=" << active.metadata.blocks.size()
              << " active_values=" << active.metadata.values
              << " run_count=" << runs.size()
              << " bm=" << manager->debugString()
              << " root_pool=" << root->toString(true);
    }

    if (!active.empty()) {
      VLOG(1) << "BM sort finalize last active run"
              << " generated_values=" << generator.generated()
              << " active_blocks=" << active.metadata.blocks.size()
              << " active_values=" << active.metadata.values
              << " bm=" << manager->debugString()
              << " root_pool=" << root->toString(true);
      finalizeActiveRun(active);
      runs.push_back(std::move(active.metadata));
      active.clear();
      manager->ReleaseUnusedReservation();
      VLOG(1) << "BM sort finalized last run"
              << " runs=" << runs.size()
              << " bm=" << manager->debugString()
              << " root_pool=" << root->toString(true);
    }
  }

  bool verified = true;
  try {
    if (FLAGS_bm_sort_verify) {
      ScopedTimer timer{verifyMs};
      verified = verifySortedRuns(*manager, runs, totalValues);
    }
  } catch (...) {
    printBenchmarkSummary(
        *manager,
        memoryContext,
        *root,
        allocateSize,
        blockBytes,
        totalBytes,
        totalValues,
        runs,
        generateAndRunSortMs,
        verifyMs,
        false,
        "exception");
    throw;
  }

  printBenchmarkSummary(
      *manager,
      memoryContext,
      *root,
      allocateSize,
      blockBytes,
      totalBytes,
      totalValues,
      runs,
      generateAndRunSortMs,
      verifyMs,
      verified,
      "ok");

  if (!FLAGS_bm_sort_keep_spill_files) {
    std::filesystem::remove_all(FLAGS_bm_sort_spill_dir);
  }
  return verified ? 0 : 1;
}

} // namespace
} // namespace bytedance::bolt::memory::bm

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  try {
    return bytedance::bolt::memory::bm::runBenchmark();
  } catch (const std::exception& e) {
    std::cerr << "BM sort benchmark failed: " << e.what() << "\n";
    return 1;
  }
}
