#include "bolt/common/memory/bm/io/DiskIoScheduler.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerConfig.h"
#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoResult.h"

#include "bolt/common/base/Exceptions.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <future>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_string(
    bm_io_benchmark_dir,
    "/tmp",
    "Directory where benchmark files are created.");
DEFINE_uint64(
    bm_io_total_bytes,
    4 * 1024ULL * 1024ULL * 1024ULL,
    "Bytes written and then read by each benchmark case.");
DEFINE_uint32(
    bm_io_fixed_depth,
    64,
    "Concurrency depth for raw buffered IO and fixed-depth scheduler cases.");
DEFINE_uint32(
    bm_io_adaptive_max_depth,
    64,
    "Maximum depth for adaptive scheduler cases.");
DEFINE_uint32(
    bm_io_adaptive_initial_depth,
    1,
    "Initial depth for adaptive scheduler cases.");
DEFINE_uint32(
    bm_io_adaptive_increase_step,
    4,
    "Depth increase step for adaptive scheduler cases.");
DEFINE_int32(
    bm_io_adaptive_control_interval_ms,
    50,
    "Adaptive depth control interval in milliseconds.");
DEFINE_bool(
    bm_io_keep_files,
    false,
    "Keep benchmark files after the benchmark finishes.");

namespace bytedance::bolt::memory::bm {
namespace {

using Clock = std::chrono::steady_clock;

constexpr size_t k4K = 4 * 1024;
constexpr size_t k256K = 256 * 1024;

enum class Phase {
  Write,
  Read,
};

struct PhaseMetrics {
  uint64_t ops{0};
  uint64_t bytes{0};
  uint64_t errors{0};
  double elapsedMs{0};
};

struct CaseMetrics {
  PhaseMetrics write;
  PhaseMetrics read;
  double fsyncMs{0};
  std::string writeStats;
  std::string readStats;
};

std::string phaseName(Phase phase) {
  return phase == Phase::Write ? "write" : "read";
}

std::string firstLine(const std::string& message) {
  const auto newline = message.find('\n');
  return newline == std::string::npos ? message : message.substr(0, newline);
}

std::string conciseReason(const std::string& message) {
  constexpr std::string_view kReasonPrefix{"Reason: "};
  const auto reason = message.find(kReasonPrefix);
  if (reason == std::string::npos) {
    return firstLine(message);
  }
  const auto start = reason + kReasonPrefix.size();
  const auto end = message.find('\n', start);
  return message.substr(
      start, end == std::string::npos ? std::string::npos : end - start);
}

double elapsedMs(Clock::time_point start, Clock::time_point end) {
  return std::chrono::duration<double, std::milli>(end - start).count();
}

double iops(const PhaseMetrics& metrics) {
  return metrics.elapsedMs > 0 ? metrics.ops * 1000.0 / metrics.elapsedMs : 0;
}

double mibPerSecond(const PhaseMetrics& metrics) {
  return metrics.elapsedMs > 0
      ? (static_cast<double>(metrics.bytes) / (1024.0 * 1024.0)) * 1000.0 /
          metrics.elapsedMs
      : 0;
}

uint64_t roundOps(uint64_t totalBytes, size_t blockSize) {
  return std::max<uint64_t>(1, totalBytes / blockSize);
}

std::string sanitizedName(std::string name) {
  for (auto& c : name) {
    const bool keep =
        (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9');
    if (!keep) {
      c = '_';
    }
  }
  return name;
}

std::string benchmarkPath(const std::string& caseName) {
  std::ostringstream out;
  out << FLAGS_bm_io_benchmark_dir << "/bolt_bm_io_"
      << static_cast<uint64_t>(::getpid()) << "_"
      << sanitizedName(caseName) << ".dat";
  return out.str();
}

int openForWrite(const std::string& path) {
  const int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0600);
  BOLT_CHECK_GE(fd, 0, "open for write failed: {}", std::strerror(errno));
  return fd;
}

int openForRead(const std::string& path) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  BOLT_CHECK_GE(fd, 0, "open for read failed: {}", std::strerror(errno));
  return fd;
}

void closeFd(int fd) {
  BOLT_CHECK_EQ(::close(fd), 0, "close failed: {}", std::strerror(errno));
}

double fsyncAndClose(int fd) {
  const auto start = Clock::now();
  BOLT_CHECK_EQ(::fsync(fd), 0, "fsync failed: {}", std::strerror(errno));
  const auto end = Clock::now();
  closeFd(fd);
  return elapsedMs(start, end);
}

void removeFile(const std::string& path) {
  if (!FLAGS_bm_io_keep_files) {
    if (::unlink(path.c_str()) != 0 && errno != ENOENT) {
      BOLT_FAIL("unlink failed: {}", path);
    }
  }
}

void fillBuffer(char* data, size_t size, uint64_t op) {
  std::memset(data, static_cast<int>(op & 0xff), size);
}

PhaseMetrics runRawPhase(
    int fd,
    Phase phase,
    size_t blockSize,
    uint64_t ops,
    uint32_t depth) {
  std::atomic<uint64_t> nextOp{0};
  std::atomic<uint64_t> completedOps{0};
  std::atomic<uint64_t> completedBytes{0};
  std::atomic<uint64_t> errors{0};
  const auto workerCount = std::max<uint32_t>(1, depth);

  const auto start = Clock::now();
  std::vector<std::thread> workers;
  workers.reserve(workerCount);
  for (uint32_t worker = 0; worker < workerCount; ++worker) {
    workers.emplace_back([&, worker] {
      std::vector<char> buffer(blockSize);
      while (true) {
        const auto op = nextOp.fetch_add(1);
        if (op >= ops) {
          break;
        }
        if (phase == Phase::Write) {
          fillBuffer(buffer.data(), buffer.size(), op + worker);
        }
        const auto offset = static_cast<off_t>(op * blockSize);
        const auto rc =
            phase == Phase::Write
            ? ::pwrite(fd, buffer.data(), blockSize, offset)
            : ::pread(fd, buffer.data(), blockSize, offset);
        if (rc == static_cast<ssize_t>(blockSize)) {
          completedOps.fetch_add(1);
          completedBytes.fetch_add(blockSize);
        } else {
          errors.fetch_add(1);
        }
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }
  const auto end = Clock::now();

  return PhaseMetrics{
      completedOps.load(),
      completedBytes.load(),
      errors.load(),
      elapsedMs(start, end)};
}

DiskIoSchedulerConfig schedulerConfig(
    uint32_t depth,
    bool adaptive) {
  DiskIoSchedulerConfig config;
  config.ringDepth = std::max<uint32_t>(1, depth);
  config.adaptiveDepth.enabled = adaptive;
  config.adaptiveDepth.minDepth = 1;
  config.adaptiveDepth.initialDepth =
      adaptive ? FLAGS_bm_io_adaptive_initial_depth : depth;
  config.adaptiveDepth.maxDepth = std::max<uint32_t>(
      config.adaptiveDepth.initialDepth,
      adaptive ? FLAGS_bm_io_adaptive_max_depth : depth);
  config.ringDepth = std::max(config.ringDepth, config.adaptiveDepth.maxDepth);
  config.adaptiveDepth.increaseStep =
      std::max<uint32_t>(1, FLAGS_bm_io_adaptive_increase_step);
  config.adaptiveDepth.controlInterval =
      std::chrono::milliseconds(FLAGS_bm_io_adaptive_control_interval_ms);
  return config;
}

std::unique_ptr<char[]> makeBuffer(size_t blockSize, uint64_t op) {
  auto buffer = std::make_unique<char[]>(blockSize);
  fillBuffer(buffer.get(), blockSize, op);
  return buffer;
}

IoRequest makeRequest(
    Phase phase,
    int fd,
    size_t blockSize,
    uint64_t op,
    std::unique_ptr<char[]> buffer) {
  IoRequest request;
  request.opcode = phase == Phase::Write ? IoOpcode::Write : IoOpcode::Read;
  request.priority = IoPriority::Medium;
  request.fd = fd;
  request.fileOffset = op * blockSize;
  request.buffer = IoBuffer{std::move(buffer), blockSize, 0, blockSize};
  return request;
}

PhaseMetrics runSchedulerPhase(
    DiskIoScheduler& scheduler,
    int fd,
    Phase phase,
    size_t blockSize,
    uint64_t ops,
    uint32_t maxDepth) {
  const auto batchSize = std::max<uint32_t>(1, maxDepth);
  uint64_t completedOps = 0;
  uint64_t completedBytes = 0;
  uint64_t errors = 0;
  uint64_t nextOp = 0;
  std::vector<std::unique_ptr<char[]>> reusableBuffers;
  reusableBuffers.reserve(batchSize);
  std::vector<std::future<IoResult>> futures;
  futures.reserve(batchSize);

  const auto start = Clock::now();
  while (nextOp < ops) {
    futures.clear();
    const auto batchEnd =
        std::min<uint64_t>(ops, nextOp + static_cast<uint64_t>(batchSize));
    for (; nextOp < batchEnd; ++nextOp) {
      std::unique_ptr<char[]> buffer;
      if (!reusableBuffers.empty()) {
        buffer = std::move(reusableBuffers.back());
        reusableBuffers.pop_back();
        if (phase == Phase::Write) {
          fillBuffer(buffer.get(), blockSize, nextOp);
        }
      } else {
        buffer = makeBuffer(blockSize, nextOp);
      }
      futures.push_back(scheduler.submit(
          makeRequest(phase, fd, blockSize, nextOp, std::move(buffer))));
    }

    for (auto& future : futures) {
      auto result = future.get();
      if (result.ok() && result.bytes == blockSize) {
        ++completedOps;
        completedBytes += result.bytes;
      } else {
        ++errors;
      }
      reusableBuffers.push_back(std::move(result.buffer.data));
    }
  }
  const auto end = Clock::now();

  return PhaseMetrics{completedOps, completedBytes, errors, elapsedMs(start, end)};
}

CaseMetrics runRawCase(
    const std::string& path,
    size_t blockSize,
    uint64_t ops,
    uint32_t depth) {
  int fd = openForWrite(path);
  auto write = runRawPhase(fd, Phase::Write, blockSize, ops, depth);
  const auto fsyncMs = fsyncAndClose(fd);

  fd = openForRead(path);
  auto read = runRawPhase(fd, Phase::Read, blockSize, ops, depth);
  closeFd(fd);

  return CaseMetrics{write, read, fsyncMs, "", ""};
}

CaseMetrics runSchedulerCase(
    const std::string& path,
    size_t blockSize,
    uint64_t ops,
    uint32_t depth,
    bool adaptive) {
  int fd = openForWrite(path);
  const auto writeConfig = schedulerConfig(depth, adaptive);
  PhaseMetrics write;
  std::string writeStats;
  {
    DiskIoScheduler scheduler(writeConfig);
    write = runSchedulerPhase(
        scheduler, fd, Phase::Write, blockSize, ops, writeConfig.ringDepth);
    writeStats = scheduler.stats().toString();
  }
  const auto fsyncMs = fsyncAndClose(fd);

  fd = openForRead(path);
  const auto readConfig = schedulerConfig(depth, adaptive);
  PhaseMetrics read;
  std::string readStats;
  {
    DiskIoScheduler scheduler(readConfig);
    read = runSchedulerPhase(
        scheduler, fd, Phase::Read, blockSize, ops, readConfig.ringDepth);
    readStats = scheduler.stats().toString();
  }
  closeFd(fd);

  return CaseMetrics{write, read, fsyncMs, writeStats, readStats};
}

void printPhaseSummary(
    const std::string& caseName,
    Phase phase,
    size_t blockSize,
    uint32_t depth,
    bool adaptive,
    const PhaseMetrics& metrics,
    double fsyncMs = 0) {
  std::cout << "bm_io_benchmark"
            << " case=" << caseName
            << " phase=" << phaseName(phase)
            << " block_size=" << blockSize
            << " depth=" << depth
            << " adaptive=" << adaptive
            << " buffered_io=true"
            << " preallocate=false"
            << " drop_cache=false"
            << " write_read_barrier=fsync_close_reopen"
            << " ops=" << metrics.ops
            << " bytes=" << metrics.bytes
            << " errors=" << metrics.errors
            << " elapsed_ms=" << std::fixed << std::setprecision(3)
            << metrics.elapsedMs
            << " iops=" << std::fixed << std::setprecision(2)
            << iops(metrics)
            << " mib_per_second=" << std::fixed << std::setprecision(2)
            << mibPerSecond(metrics);
  if (phase == Phase::Write) {
    std::cout << " fsync_ms=" << std::fixed << std::setprecision(3)
              << fsyncMs;
  }
  std::cout << "\n";
}

void printCaseSummary(
    const std::string& caseName,
    size_t blockSize,
    uint32_t depth,
    bool adaptive,
    const CaseMetrics& metrics) {
  printPhaseSummary(
      caseName, Phase::Write, blockSize, depth, adaptive, metrics.write,
      metrics.fsyncMs);
  printPhaseSummary(
      caseName, Phase::Read, blockSize, depth, adaptive, metrics.read);
  if (!metrics.writeStats.empty()) {
    std::cout << "bm_io_benchmark case=" << caseName
              << " phase=write scheduler_stats=\"" << metrics.writeStats
              << "\"\n";
  }
  if (!metrics.readStats.empty()) {
    std::cout << "bm_io_benchmark case=" << caseName
              << " phase=read scheduler_stats=\"" << metrics.readStats
              << "\"\n";
  }
}

void runCase(
    const std::string& caseName,
    size_t blockSize,
    uint32_t depth,
    bool adaptive,
    bool scheduler,
    const std::string& schedulerSkipReason) {
  const auto ops = roundOps(FLAGS_bm_io_total_bytes, blockSize);
  const auto path = benchmarkPath(caseName);
  if (scheduler && !schedulerSkipReason.empty()) {
    std::cout << "bm_io_benchmark"
              << " case=" << caseName
              << " skipped=true"
              << " reason=\"" << schedulerSkipReason << "\"\n";
    return;
  }
  try {
    const auto metrics = scheduler
        ? runSchedulerCase(path, blockSize, ops, depth, adaptive)
        : runRawCase(path, blockSize, ops, depth);
    printCaseSummary(caseName, blockSize, depth, adaptive, metrics);
  } catch (const std::exception& ex) {
    std::cout << "bm_io_benchmark"
              << " case=" << caseName
              << " skipped=true"
              << " reason=\"" << conciseReason(ex.what()) << "\"\n";
  }
  removeFile(path);
}

} // namespace
} // namespace bytedance::bolt::memory::bm

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  using namespace bytedance::bolt::memory::bm;

  BOLT_CHECK_GT(FLAGS_bm_io_total_bytes, 0, "total bytes must be positive");
  BOLT_CHECK_GT(FLAGS_bm_io_fixed_depth, 0, "fixed depth must be positive");
  BOLT_CHECK_GT(
      FLAGS_bm_io_adaptive_max_depth, 0, "adaptive max depth must be positive");
  BOLT_CHECK_GT(
      FLAGS_bm_io_adaptive_initial_depth,
      0,
      "adaptive initial depth must be positive");
  BOLT_CHECK_LE(
      FLAGS_bm_io_adaptive_initial_depth,
      FLAGS_bm_io_adaptive_max_depth,
      "adaptive initial depth must be <= max depth");
  BOLT_CHECK_GT(
      FLAGS_bm_io_adaptive_control_interval_ms,
      0,
      "adaptive control interval must be positive");

  std::string schedulerSkipReason;
  try {
    DiskIoScheduler scheduler(schedulerConfig(FLAGS_bm_io_fixed_depth, false));
  } catch (const std::exception& ex) {
    schedulerSkipReason = conciseReason(ex.what());
  }

  runCase(
      "RawBufferedIo_4K_FixedDepth",
      k4K,
      FLAGS_bm_io_fixed_depth,
      false,
      false,
      schedulerSkipReason);
  runCase(
      "DiskIoScheduler_4K_FixedDepth",
      k4K,
      FLAGS_bm_io_fixed_depth,
      false,
      true,
      schedulerSkipReason);
  runCase(
      "DiskIoScheduler_4K_AdaptiveDepth",
      k4K,
      FLAGS_bm_io_adaptive_max_depth,
      true,
      true,
      schedulerSkipReason);

  runCase(
      "RawBufferedIo_256K_FixedDepth",
      k256K,
      FLAGS_bm_io_fixed_depth,
      false,
      false,
      schedulerSkipReason);
  runCase(
      "DiskIoScheduler_256K_FixedDepth",
      k256K,
      FLAGS_bm_io_fixed_depth,
      false,
      true,
      schedulerSkipReason);
  runCase(
      "DiskIoScheduler_256K_AdaptiveDepth",
      k256K,
      FLAGS_bm_io_adaptive_max_depth,
      true,
      true,
      schedulerSkipReason);

  return 0;
}
