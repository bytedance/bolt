#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/file/FileSegmentAllocatorConfig.h"
#include "bolt/common/memory/bm/io/IoBuffer.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <liburing.h>
#include <zstd.h>

// Benchmark design:
//
// This benchmark compares a direct third-party io_uring implementation with
// BufferManager's complete spill path. It intentionally does not reuse
// bm/io/DiskIoScheduler on the direct side, because the target comparison is
// "external liburing + plain memory blocks" versus "BufferManager end-to-end".
// Any difference in io_uring wrapping, file placement, spill record format,
// compression, futures, accounting, state transitions, or reclaim windows is
// part of the measured delta.
//
// Matrix:
//   block size: 256KiB, 1MiB, 4MiB
//   codec:      none, zstd
//   direction:  write all, read batch
//
// Timing:
//   direct_write_all: fill/compress each block, submit writes with an inflight
//     io_uring window, and harvest until all blocks complete.
//   bm_write_all: allocate/fill/unpin blocks before timing; time only
//     BufferManager::Reclaim(totalBytes), which performs spill record build,
//     file allocation, write submission, harvest, and state/accounting updates.
//   direct_read_batch: prepare files before timing; time windowed io_uring
//   reads
//     plus optional zstd decompression and verification.
//   bm_read_batch: prepare spilled blocks before timing; time BatchPin()
//   batches
//     plus optional verification.

DEFINE_string(
    bm_spill_io_dir,
    "/tmp/bolt-bm-spill-io-benchmark",
    "Directory used by the spill IO benchmark.");
DEFINE_uint64(
    bm_spill_io_total_gb,
    2,
    "Logical bytes per benchmark case, in GiB.");
DEFINE_uint64(
    bm_spill_io_inflight,
    128,
    "Inflight write/read window for direct io_uring and BM batching.");
DEFINE_bool(
    bm_spill_io_keep_files,
    false,
    "Keep benchmark files after the benchmark exits.");
DEFINE_bool(
    bm_spill_io_verify,
    true,
    "Verify read payloads after direct and BM read cases.");
DEFINE_string(bm_spill_io_codec, "all", "Codec filter: all, none, or zstd.");
DEFINE_string(
    bm_spill_io_direction,
    "all",
    "Direction filter: all, write, or read.");

namespace bytedance::bolt::memory::bm {
namespace {

using Clock = std::chrono::steady_clock;

constexpr uint64_t kKiB = 1024ULL;
constexpr uint64_t kMiB = 1024ULL * kKiB;
constexpr uint64_t kGiB = 1024ULL * kMiB;

enum class CodecKind {
  kNone,
  kZstd,
};

struct TimedResult {
  double elapsedMs{0};
  uint64_t logicalBytes{0};
  uint64_t physicalBytes{0};
};

struct StoredBlock {
  uint64_t offset{0};
  uint32_t logicalSize{0};
  uint32_t physicalSize{0};
};

struct ScopedFd {
  ScopedFd() = default;
  explicit ScopedFd(int fd) : fd_(fd) {}
  ~ScopedFd() {
    reset();
  }
  ScopedFd(const ScopedFd&) = delete;
  ScopedFd& operator=(const ScopedFd&) = delete;

  int get() const {
    return fd_;
  }

  void reset(int fd = -1) {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    fd_ = fd;
  }

 private:
  int fd_{-1};
};

struct ScopedRing {
  explicit ScopedRing(uint32_t entries) {
    const int ret = io_uring_queue_init(entries, &ring, 0);
    BOLT_CHECK_GE(
        ret, 0, "io_uring_queue_init failed: {}", std::strerror(-ret));
  }

  ~ScopedRing() {
    io_uring_queue_exit(&ring);
  }

  ScopedRing(const ScopedRing&) = delete;
  ScopedRing& operator=(const ScopedRing&) = delete;

  io_uring ring;
};

class Timer {
 public:
  Timer() : start_(Clock::now()) {}

  double elapsedMs() const {
    return std::chrono::duration<double, std::milli>(Clock::now() - start_)
        .count();
  }

 private:
  Clock::time_point start_;
};

std::string codecName(CodecKind codec) {
  switch (codec) {
    case CodecKind::kNone:
      return "none";
    case CodecKind::kZstd:
      return "zstd";
  }
  return "unknown";
}

std::string blockSizeName(uint64_t blockSize) {
  if (blockSize % kMiB == 0) {
    return std::to_string(blockSize / kMiB) + "MiB";
  }
  return std::to_string(blockSize / kKiB) + "KiB";
}

double gibPerSecond(uint64_t bytes, double elapsedMs) {
  if (elapsedMs <= 0) {
    return 0;
  }
  return (static_cast<double>(bytes) / static_cast<double>(kGiB)) /
      (elapsedMs / 1'000.0);
}

bool includeCodec(CodecKind codec) {
  if (FLAGS_bm_spill_io_codec == "all") {
    return true;
  }
  return FLAGS_bm_spill_io_codec == codecName(codec);
}

bool includeDirection(const std::string& direction) {
  return FLAGS_bm_spill_io_direction == "all" ||
      FLAGS_bm_spill_io_direction == direction;
}

uint64_t caseTotalBytes(uint64_t blockSize) {
  const auto requested = FLAGS_bm_spill_io_total_gb * kGiB;
  return std::max<uint64_t>(blockSize, requested / blockSize * blockSize);
}

void fillPattern(char* data, size_t size, uint64_t blockIndex) {
  uint64_t state = blockIndex * 0x9e3779b97f4a7c15ULL + 0xd1b54a32d192ed03ULL;
  size_t offset = 0;
  while (offset + sizeof(uint64_t) <= size) {
    state ^= state >> 30;
    state *= 0xbf58476d1ce4e5b9ULL;
    std::memcpy(data + offset, &state, sizeof(state));
    offset += sizeof(state);
  }
  if (offset < size) {
    std::memcpy(data + offset, &state, size - offset);
  }
}

void verifyPattern(const char* data, size_t size, uint64_t blockIndex) {
  auto expected = IoBuffer::allocateFromMalloc(size);
  fillPattern(expected.data(), size, blockIndex);
  BOLT_CHECK_EQ(
      std::memcmp(data, expected.data(), size),
      0,
      "payload verification failed for block {}",
      blockIndex);
}

IoBuffer makeRecord(uint64_t blockIndex, uint64_t blockSize, CodecKind codec) {
  auto raw = IoBuffer::allocateFromMalloc(blockSize);
  fillPattern(raw.data(), blockSize, blockIndex);
  if (codec == CodecKind::kNone) {
    return raw;
  }

  const auto bound = ZSTD_compressBound(blockSize);
  auto compressed = IoBuffer::allocateFromMalloc(bound);
  const auto size = ZSTD_compress(
      compressed.data(), compressed.size(), raw.data(), raw.length(), 3);
  BOLT_CHECK(!ZSTD_isError(size), "{}", ZSTD_getErrorName(size));
  compressed.setLength(size);
  return compressed;
}

IoBuffer
decodeRecord(const IoBuffer& record, uint64_t rawSize, CodecKind codec) {
  if (codec == CodecKind::kNone) {
    auto decoded = IoBuffer::allocateFromMalloc(rawSize);
    BOLT_CHECK_EQ(record.length(), rawSize);
    std::memcpy(decoded.data(), record.data(), rawSize);
    return decoded;
  }

  auto decoded = IoBuffer::allocateFromMalloc(rawSize);
  const auto size = ZSTD_decompress(
      decoded.data(), decoded.size(), record.data(), record.length());
  BOLT_CHECK(!ZSTD_isError(size), "{}", ZSTD_getErrorName(size));
  BOLT_CHECK_EQ(size, rawSize);
  return decoded;
}

ScopedFd openBenchmarkFile(const std::filesystem::path& path) {
  const int fd =
      ::open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_CLOEXEC, 0600);
  BOLT_CHECK_GE(
      fd,
      0,
      "open failed path={} errno={} {}",
      path.string(),
      errno,
      std::strerror(errno));
  return ScopedFd{fd};
}

void writeFull(int fd, const char* data, size_t size, uint64_t offset) {
  size_t written = 0;
  while (written < size) {
    const auto ret = ::pwrite(
        fd,
        data + written,
        size - written,
        static_cast<off_t>(offset + written));
    BOLT_CHECK_GT(
        ret, 0, "pwrite failed errno={} {}", errno, std::strerror(errno));
    written += static_cast<size_t>(ret);
  }
}

struct DirectRequest {
  IoBuffer buffer;
  uint64_t blockIndex{0};
  uint64_t rawSize{0};
  uint64_t physicalSize{0};
};

void submitWrite(
    ScopedRing& ring,
    int fd,
    uint64_t offset,
    DirectRequest* request) {
  auto* sqe = io_uring_get_sqe(&ring.ring);
  BOLT_CHECK_NOT_NULL(sqe);
  io_uring_prep_write(
      sqe,
      fd,
      request->buffer.data(),
      request->buffer.length(),
      static_cast<off_t>(offset));
  io_uring_sqe_set_data(sqe, request);
  const int ret = io_uring_submit(&ring.ring);
  BOLT_CHECK_GE(
      ret, 0, "io_uring_submit write failed: {}", std::strerror(-ret));
}

void submitRead(
    ScopedRing& ring,
    int fd,
    const StoredBlock& block,
    DirectRequest* request) {
  auto* sqe = io_uring_get_sqe(&ring.ring);
  BOLT_CHECK_NOT_NULL(sqe);
  io_uring_prep_read(
      sqe,
      fd,
      request->buffer.data(),
      request->buffer.length(),
      static_cast<off_t>(block.offset));
  io_uring_sqe_set_data(sqe, request);
  const int ret = io_uring_submit(&ring.ring);
  BOLT_CHECK_GE(ret, 0, "io_uring_submit read failed: {}", std::strerror(-ret));
}

TimedResult runDirectWrite(
    const std::filesystem::path& path,
    uint64_t blockSize,
    CodecKind codec,
    uint64_t totalBytes,
    uint32_t inflight) {
  auto fd = openBenchmarkFile(path);
  ScopedRing ring{inflight};
  const uint64_t blockCount = totalBytes / blockSize;
  uint64_t nextBlock = 0;
  uint64_t nextOffset = 0;
  uint64_t pending = 0;
  uint64_t physicalBytes = 0;
  Timer timer;

  auto submitMore = [&]() {
    while (pending < inflight && nextBlock < blockCount) {
      auto* request = new DirectRequest;
      request->blockIndex = nextBlock;
      request->rawSize = blockSize;
      request->buffer = makeRecord(nextBlock, blockSize, codec);
      request->physicalSize = request->buffer.length();
      submitWrite(ring, fd.get(), nextOffset, request);
      nextOffset += request->physicalSize;
      ++nextBlock;
      ++pending;
    }
  };

  submitMore();
  while (pending > 0) {
    io_uring_cqe* cqe = nullptr;
    const int ret = io_uring_wait_cqe(&ring.ring, &cqe);
    BOLT_CHECK_GE(
        ret, 0, "io_uring_wait_cqe write failed: {}", std::strerror(-ret));
    auto* request = static_cast<DirectRequest*>(io_uring_cqe_get_data(cqe));
    BOLT_CHECK_EQ(cqe->res, static_cast<int>(request->physicalSize));
    physicalBytes += request->physicalSize;
    delete request;
    io_uring_cqe_seen(&ring.ring, cqe);
    --pending;
    submitMore();
  }

  return TimedResult{timer.elapsedMs(), totalBytes, physicalBytes};
}

std::vector<StoredBlock> prepareDirectReadFile(
    const std::filesystem::path& path,
    uint64_t blockSize,
    CodecKind codec,
    uint64_t totalBytes) {
  auto fd = openBenchmarkFile(path);
  const uint64_t blockCount = totalBytes / blockSize;
  std::vector<StoredBlock> blocks;
  blocks.reserve(blockCount);
  uint64_t offset = 0;
  for (uint64_t i = 0; i < blockCount; ++i) {
    auto record = makeRecord(i, blockSize, codec);
    writeFull(fd.get(), record.data(), record.length(), offset);
    blocks.push_back(StoredBlock{
        offset,
        static_cast<uint32_t>(blockSize),
        static_cast<uint32_t>(record.length())});
    offset += record.length();
  }
  return blocks;
}

TimedResult runDirectRead(
    const std::filesystem::path& path,
    std::span<const StoredBlock> blocks,
    CodecKind codec,
    uint32_t inflight) {
  const int fdRaw = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  BOLT_CHECK_GE(
      fdRaw,
      0,
      "open read failed path={} errno={} {}",
      path.string(),
      errno,
      std::strerror(errno));
  ScopedFd fd{fdRaw};
  ScopedRing ring{inflight};
  uint64_t nextBlock = 0;
  uint64_t pending = 0;
  uint64_t logicalBytes = 0;
  uint64_t physicalBytes = 0;
  Timer timer;

  auto submitMore = [&]() {
    while (pending < inflight && nextBlock < blocks.size()) {
      const auto& block = blocks[nextBlock];
      auto* request = new DirectRequest;
      request->blockIndex = nextBlock;
      request->rawSize = block.logicalSize;
      request->physicalSize = block.physicalSize;
      request->buffer = IoBuffer::allocateFromMalloc(block.physicalSize);
      submitRead(ring, fd.get(), block, request);
      ++nextBlock;
      ++pending;
    }
  };

  submitMore();
  while (pending > 0) {
    io_uring_cqe* cqe = nullptr;
    const int ret = io_uring_wait_cqe(&ring.ring, &cqe);
    BOLT_CHECK_GE(
        ret, 0, "io_uring_wait_cqe read failed: {}", std::strerror(-ret));
    auto* request = static_cast<DirectRequest*>(io_uring_cqe_get_data(cqe));
    BOLT_CHECK_EQ(cqe->res, static_cast<int>(request->physicalSize));
    request->buffer.setLength(request->physicalSize);
    if (FLAGS_bm_spill_io_verify) {
      auto decoded = decodeRecord(request->buffer, request->rawSize, codec);
      verifyPattern(decoded.data(), decoded.length(), request->blockIndex);
    }
    logicalBytes += request->rawSize;
    physicalBytes += request->physicalSize;
    delete request;
    io_uring_cqe_seen(&ring.ring, cqe);
    --pending;
    submitMore();
  }

  return TimedResult{timer.elapsedMs(), logicalBytes, physicalBytes};
}

FileSegmentAllocatorConfig fileAllocatorConfig(const std::string& directory) {
  FileSegmentAllocatorConfig config;
  config.directory = directory;
  config.bucket_sizes = {
      static_cast<int64_t>(512 * kKiB),
      static_cast<int64_t>(2 * kMiB),
      static_cast<int64_t>(8 * kMiB)};
  config.file_size_limit_bytes = 1024LL * 1024LL * 1024LL;
  config.max_open_files_per_bucket = 128;
  return config;
}

BufferManagerConfig bufferManagerConfig(
    const std::string& name,
    const std::string& directory,
    CodecKind codec,
    uint32_t inflight) {
  BufferManagerConfig config;
  config.poolName = name;
  config.maxReclaimWriteInflight = inflight;
  config.spillStoreConfig.fileAllocatorConfig = fileAllocatorConfig(directory);
  config.spillStoreConfig.compressionConfig.minCompressBytes = 0;
  if (codec == CodecKind::kNone) {
    config.spillStoreConfig.compressionConfig.kind =
        compress::CompressionKind::kNone;
  } else {
    config.spillStoreConfig.compressionConfig.kind =
        compress::CompressionKind::kZstdFrame;
    config.spillStoreConfig.compressionConfig.zstd.compressionLevel = 3;
  }
  return config;
}

struct BmPreparedBlocks {
  std::unique_ptr<MemoryManager> memoryManager;
  std::shared_ptr<MemoryPool> root;
  std::shared_ptr<BufferManager> manager;
  std::vector<std::shared_ptr<BlockHandle>> blocks;
};

BmPreparedBlocks prepareBmBlocks(
    const std::string& name,
    const std::string& directory,
    uint64_t blockSize,
    CodecKind codec,
    uint64_t totalBytes,
    uint32_t inflight) {
  auto memoryManager = std::make_unique<MemoryManager>();
  auto root = memoryManager->addRootPool(
      name, static_cast<int64_t>(totalBytes + blockSize * inflight * 2));
  auto manager = BufferManager::Create(
      *root, bufferManagerConfig(name, directory, codec, inflight));
  const uint64_t blockCount = totalBytes / blockSize;
  std::vector<std::shared_ptr<BlockHandle>> blocks;
  blocks.reserve(blockCount);
  for (uint64_t i = 0; i < blockCount; ++i) {
    auto handle = manager->Allocate(blockSize, MemoryTag::kTesting);
    fillPattern(handle.Ptr(), blockSize, i);
    blocks.push_back(handle.block());
    handle.Destroy();
  }
  return BmPreparedBlocks{
      std::move(memoryManager),
      std::move(root),
      std::move(manager),
      std::move(blocks)};
}

TimedResult runBmWrite(BmPreparedBlocks& prepared, uint64_t totalBytes) {
  Timer timer;
  const auto reclaimed = prepared.manager->Reclaim(totalBytes);
  const auto elapsedMs = timer.elapsedMs();
  BOLT_CHECK_EQ(reclaimed, totalBytes);
  const auto stats = prepared.manager->stats();
  return TimedResult{
      elapsedMs, stats.spillWriteBytes, stats.spillPhysicalWriteBytes};
}

TimedResult
runBmRead(BmPreparedBlocks& prepared, uint64_t blockSize, uint32_t inflight) {
  std::vector<std::shared_ptr<BlockHandle>> batch;
  batch.reserve(inflight);
  uint64_t logicalBytes = 0;
  Timer timer;
  for (size_t offset = 0; offset < prepared.blocks.size(); offset += inflight) {
    batch.clear();
    const auto end =
        std::min<size_t>(prepared.blocks.size(), offset + inflight);
    for (size_t i = offset; i < end; ++i) {
      batch.push_back(prepared.blocks[i]);
    }
    auto handles = prepared.manager->BatchPin(batch);
    if (FLAGS_bm_spill_io_verify) {
      for (size_t i = 0; i < handles.size(); ++i) {
        verifyPattern(handles[i].Ptr(), blockSize, offset + i);
      }
    }
    logicalBytes += handles.size() * blockSize;
  }
  const auto elapsedMs = timer.elapsedMs();
  const auto stats = prepared.manager->stats();
  return TimedResult{elapsedMs, logicalBytes, stats.spillPhysicalReadBytes};
}

void printResult(
    const std::string& mode,
    CodecKind codec,
    uint64_t blockSize,
    uint64_t inflight,
    const TimedResult& result) {
  std::cout << mode << "," << codecName(codec) << ","
            << blockSizeName(blockSize) << "," << result.logicalBytes << ","
            << result.physicalBytes << "," << inflight << ","
            << result.elapsedMs << ","
            << gibPerSecond(result.logicalBytes, result.elapsedMs) << ","
            << (result.elapsedMs > 0
                    ? (static_cast<double>(result.logicalBytes) / blockSize) /
                        (result.elapsedMs / 1'000.0)
                    : 0)
            << "\n";
}

void runCase(uint64_t blockSize, CodecKind codec) {
  const auto totalBytes = caseTotalBytes(blockSize);
  const auto inflight = static_cast<uint32_t>(FLAGS_bm_spill_io_inflight);
  BOLT_CHECK_GT(inflight, 0);

  const auto caseName = blockSizeName(blockSize) + "-" + codecName(codec);
  const auto caseDir = std::filesystem::path(FLAGS_bm_spill_io_dir) / caseName;
  std::filesystem::remove_all(caseDir);
  std::filesystem::create_directories(caseDir);

  if (includeDirection("write")) {
    auto direct = runDirectWrite(
        caseDir / "direct-write.bin", blockSize, codec, totalBytes, inflight);
    printResult("direct_write_all", codec, blockSize, inflight, direct);

    auto bmPrepared = prepareBmBlocks(
        "bm-spill-write-" + caseName,
        (caseDir / "bm-write").string(),
        blockSize,
        codec,
        totalBytes,
        inflight);
    auto bm = runBmWrite(bmPrepared, totalBytes);
    printResult("bm_write_all", codec, blockSize, inflight, bm);
  }

  if (includeDirection("read")) {
    const auto directBlocks = prepareDirectReadFile(
        caseDir / "direct-read.bin", blockSize, codec, totalBytes);
    auto direct = runDirectRead(
        caseDir / "direct-read.bin", directBlocks, codec, inflight);
    printResult("direct_read_batch", codec, blockSize, inflight, direct);

    auto bmPrepared = prepareBmBlocks(
        "bm-spill-read-" + caseName,
        (caseDir / "bm-read").string(),
        blockSize,
        codec,
        totalBytes,
        inflight);
    (void)runBmWrite(bmPrepared, totalBytes);
    auto bm = runBmRead(bmPrepared, blockSize, inflight);
    printResult("bm_read_batch", codec, blockSize, inflight, bm);
  }

  if (!FLAGS_bm_spill_io_keep_files) {
    std::filesystem::remove_all(caseDir);
  }
}

int runBenchmark() {
  if (FLAGS_bm_spill_io_codec != "all" && FLAGS_bm_spill_io_codec != "none" &&
      FLAGS_bm_spill_io_codec != "zstd") {
    throw std::invalid_argument("bm_spill_io_codec must be all, none, or zstd");
  }
  if (FLAGS_bm_spill_io_direction != "all" &&
      FLAGS_bm_spill_io_direction != "write" &&
      FLAGS_bm_spill_io_direction != "read") {
    throw std::invalid_argument(
        "bm_spill_io_direction must be all, write, or read");
  }

  std::filesystem::create_directories(FLAGS_bm_spill_io_dir);
  std::cout << "mode,codec,block_size,logical_bytes,physical_bytes,inflight,"
               "elapsed_ms,gib_s,ops_s\n";
  for (auto codec : {CodecKind::kNone, CodecKind::kZstd}) {
    if (!includeCodec(codec)) {
      continue;
    }
    for (auto blockSize : {256 * kKiB, kMiB, 4 * kMiB}) {
      runCase(blockSize, codec);
    }
  }
  if (!FLAGS_bm_spill_io_keep_files) {
    std::filesystem::remove_all(FLAGS_bm_spill_io_dir);
  }
  return 0;
}

} // namespace
} // namespace bytedance::bolt::memory::bm

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return bytedance::bolt::memory::bm::runBenchmark();
}
