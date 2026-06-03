#include "bolt/common/memory/bm/io/DiskIoScheduler.h"
#include "bolt/common/memory/bm/io/IoRequest.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <deque>
#include <fstream>
#include <future>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <folly/init/Init.h>
#include <gflags/gflags.h>

DEFINE_string(
    bm_io_benchmark_path,
    "/tmp/bolt-bm-io-benchmark.dat",
    "Benchmark file path.");
DEFINE_uint64(
    bm_io_benchmark_file_size_mb,
    4096,
    "Benchmark file size in MiB.");
DEFINE_uint64(
    bm_io_benchmark_block_size_kb,
    256,
    "IO block size in KiB.");
DEFINE_uint64(
    bm_io_benchmark_queue_depth,
    128,
    "Maximum scheduler requests kept in flight.");
DEFINE_uint64(
    bm_io_benchmark_jobs,
    1,
    "Logical job count. Effective in-flight limit is queue_depth * jobs.");
DEFINE_uint64(
    bm_io_benchmark_runtime_sec,
    30,
    "Measured runtime in seconds.");
DEFINE_string(
    bm_io_benchmark_scenario,
    "bandwidth_read",
    "One of bandwidth_read, bandwidth_write, iops_read, iops_write.");
DEFINE_string(
    bm_io_benchmark_output_json,
    "",
    "Optional path for JSON output. Prints JSON to stdout when empty.");

namespace bytedance::bolt::memory::bm {
namespace {

using Clock = std::chrono::steady_clock;

struct Scenario {
  IoOpcode opcode;
  bool randomOffsets;
};

struct InflightRequest {
  std::future<IoResult> future;
  Clock::time_point submitTime;
};

struct Result {
  std::string scenario;
  uint64_t blockSize{0};
  uint64_t queueDepth{0};
  uint64_t jobs{0};
  uint64_t runtimeSec{0};
  uint64_t completedRequests{0};
  uint64_t completedBytes{0};
  uint64_t errors{0};
  double elapsedSec{0};
  double iops{0};
  double bwMiBps{0};
  double p50Us{0};
  double p99Us{0};
  std::string schedulerStats;
};

Scenario parseScenario(std::string_view scenario) {
  if (scenario == "bandwidth_read") {
    return {IoOpcode::Read, false};
  }
  if (scenario == "bandwidth_write") {
    return {IoOpcode::Write, false};
  }
  if (scenario == "iops_read") {
    return {IoOpcode::Read, true};
  }
  if (scenario == "iops_write") {
    return {IoOpcode::Write, true};
  }
  throw std::invalid_argument(
      "unsupported bm_io_benchmark_scenario: " + std::string(scenario));
}

uint64_t percentile(std::vector<uint64_t>& values, double pct) {
  if (values.empty()) {
    return 0;
  }
  std::sort(values.begin(), values.end());
  const auto index = std::min<uint64_t>(
      values.size() - 1,
      static_cast<uint64_t>((pct / 100.0) * static_cast<double>(values.size())));
  return values[index];
}

uint64_t completeOldest(
    std::deque<InflightRequest>& inflight,
    std::vector<IoBuffer>& freeBuffers,
    std::vector<uint64_t>& latenciesUs,
    uint64_t& completedBytes,
    uint64_t& errors) {
  auto current = std::move(inflight.front());
  inflight.pop_front();
  auto result = current.future.get();
  const auto done = Clock::now();
  latenciesUs.push_back(
      std::chrono::duration_cast<std::chrono::microseconds>(
          done - current.submitTime)
          .count());
  completedBytes += result.bytes;
  if (!result.ok()) {
    ++errors;
  }
  freeBuffers.push_back(std::move(result.buffer));
  return 1;
}

void ensureFileSize(const std::string& path, uint64_t fileSize) {
  const int fd = ::open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
  if (fd < 0) {
    throw std::runtime_error("open failed for " + path);
  }
  if (::ftruncate(fd, static_cast<off_t>(fileSize)) != 0) {
    const auto savedErrno = errno;
    ::close(fd);
    throw std::runtime_error(
        "ftruncate failed, errno=" + std::to_string(savedErrno));
  }
  ::close(fd);
}

std::vector<uint64_t> makeOffsets(
    uint64_t fileSize,
    uint64_t blockSize,
    bool randomOffsets) {
  if (fileSize < blockSize) {
    throw std::invalid_argument("file size must be at least block size");
  }
  const uint64_t blockCount = fileSize / blockSize;
  std::vector<uint64_t> offsets(blockCount);
  for (uint64_t i = 0; i < blockCount; ++i) {
    offsets[i] = i * blockSize;
  }
  if (randomOffsets) {
    std::mt19937_64 rng{0xB01710ULL};
    std::shuffle(offsets.begin(), offsets.end(), rng);
  }
  return offsets;
}

IoRequest makeRequest(
    int fd,
    IoOpcode opcode,
    uint64_t offset,
    IoBuffer buffer) {
  IoRequest request;
  request.opcode = opcode;
  request.priority = IoPriority::Medium;
  request.fd = fd;
  request.fileOffset = offset;
  request.buffer = std::move(buffer);
  return request;
}

std::vector<IoBuffer> makeBufferPool(
    uint64_t bufferCount,
    uint64_t blockSize,
    IoOpcode opcode) {
  std::vector<IoBuffer> buffers;
  buffers.reserve(bufferCount);
  for (uint64_t i = 0; i < bufferCount; ++i) {
    auto buffer = IoBuffer::allocateFromMalloc(blockSize);
    if (opcode == IoOpcode::Write) {
      std::fill(
          buffer.data(),
          buffer.data() + buffer.length(),
          static_cast<char>(0x5a));
    }
    buffers.push_back(std::move(buffer));
  }
  return buffers;
}

std::string jsonEscape(std::string_view value) {
  std::string out;
  out.reserve(value.size());
  for (const char c : value) {
    if (c == '\\' || c == '"') {
      out.push_back('\\');
    }
    if (c == '\n') {
      out += "\\n";
    } else {
      out.push_back(c);
    }
  }
  return out;
}

std::string toJson(const Result& result) {
  std::ostringstream out;
  out << "{\n"
      << "  \"backend\": \"scheduler\",\n"
      << "  \"scenario\": \"" << jsonEscape(result.scenario) << "\",\n"
      << "  \"block_size\": " << result.blockSize << ",\n"
      << "  \"queue_depth\": " << result.queueDepth << ",\n"
      << "  \"jobs\": " << result.jobs << ",\n"
      << "  \"runtime_sec\": " << result.runtimeSec << ",\n"
      << "  \"elapsed_sec\": " << result.elapsedSec << ",\n"
      << "  \"completed_requests\": " << result.completedRequests << ",\n"
      << "  \"completed_bytes\": " << result.completedBytes << ",\n"
      << "  \"errors\": " << result.errors << ",\n"
      << "  \"iops\": " << result.iops << ",\n"
      << "  \"bw_mibps\": " << result.bwMiBps << ",\n"
      << "  \"p50_us\": " << result.p50Us << ",\n"
      << "  \"p99_us\": " << result.p99Us << ",\n"
      << "  \"scheduler_stats\": \"" << jsonEscape(result.schedulerStats)
      << "\"\n"
      << "}\n";
  return out.str();
}

Result runSchedulerBenchmark() {
  const auto scenario = parseScenario(FLAGS_bm_io_benchmark_scenario);
  const uint64_t fileSize = FLAGS_bm_io_benchmark_file_size_mb * 1024 * 1024;
  const uint64_t blockSize = FLAGS_bm_io_benchmark_block_size_kb * 1024;
  const uint64_t inflightLimit =
      FLAGS_bm_io_benchmark_queue_depth * FLAGS_bm_io_benchmark_jobs;
  if (blockSize == 0 || inflightLimit == 0 ||
      FLAGS_bm_io_benchmark_runtime_sec == 0) {
    throw std::invalid_argument(
        "block size, inflight limit, and runtime must be positive");
  }

  ensureFileSize(FLAGS_bm_io_benchmark_path, fileSize);
  auto offsets = makeOffsets(fileSize, blockSize, scenario.randomOffsets);
  size_t nextOffset = 0;

  const int fd = ::open(FLAGS_bm_io_benchmark_path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    throw std::runtime_error("open benchmark file failed");
  }

  diskIoScheduler().ensureReady();

  auto freeBuffers = makeBufferPool(inflightLimit, blockSize, scenario.opcode);
  std::deque<InflightRequest> inflight;
  std::vector<uint64_t> latenciesUs;
  uint64_t completedRequests = 0;
  uint64_t completedBytes = 0;
  uint64_t errors = 0;

  const auto started = Clock::now();
  const auto deadline =
      started + std::chrono::seconds(FLAGS_bm_io_benchmark_runtime_sec);

  auto submitOne = [&]() {
    BOLT_CHECK(!freeBuffers.empty(), "benchmark buffer pool exhausted");
    const auto offset = offsets[nextOffset++ % offsets.size()];
    auto buffer = std::move(freeBuffers.back());
    freeBuffers.pop_back();
    auto request = makeRequest(fd, scenario.opcode, offset, std::move(buffer));
    inflight.push_back(
        {diskIoScheduler().submit(std::move(request)), Clock::now()});
  };

  while (Clock::now() < deadline) {
    while (inflight.size() < inflightLimit && Clock::now() < deadline) {
      submitOne();
    }
    completedRequests += completeOldest(
        inflight, freeBuffers, latenciesUs, completedBytes, errors);
  }

  while (!inflight.empty()) {
    completedRequests += completeOldest(
        inflight, freeBuffers, latenciesUs, completedBytes, errors);
  }

  const auto finished = Clock::now();
  ::close(fd);

  const double elapsed =
      std::chrono::duration<double>(finished - started).count();
  auto p50Input = latenciesUs;
  auto p99Input = latenciesUs;
  Result result;
  result.scenario = FLAGS_bm_io_benchmark_scenario;
  result.blockSize = blockSize;
  result.queueDepth = FLAGS_bm_io_benchmark_queue_depth;
  result.jobs = FLAGS_bm_io_benchmark_jobs;
  result.runtimeSec = FLAGS_bm_io_benchmark_runtime_sec;
  result.completedRequests = completedRequests;
  result.completedBytes = completedBytes;
  result.errors = errors;
  result.elapsedSec = elapsed;
  result.iops =
      elapsed > 0 ? static_cast<double>(completedRequests) / elapsed : 0;
  result.bwMiBps =
      elapsed > 0 ? static_cast<double>(completedBytes) / 1024 / 1024 / elapsed
                  : 0;
  result.p50Us = percentile(p50Input, 50.0);
  result.p99Us = percentile(p99Input, 99.0);
  result.schedulerStats = diskIoScheduler().stats().toString();
  return result;
}

} // namespace
} // namespace bytedance::bolt::memory::bm

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  auto result = bytedance::bolt::memory::bm::runSchedulerBenchmark();
  auto json = bytedance::bolt::memory::bm::toJson(result);
  if (FLAGS_bm_io_benchmark_output_json.empty()) {
    std::cout << json;
    return 0;
  }
  std::ofstream out(FLAGS_bm_io_benchmark_output_json);
  if (!out) {
    std::cerr << "failed to open output json: "
              << FLAGS_bm_io_benchmark_output_json << "\n";
    return 1;
  }
  out << json;
  return 0;
}
