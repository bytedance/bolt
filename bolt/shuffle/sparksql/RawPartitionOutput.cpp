/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "bolt/shuffle/sparksql/RawPartitionOutput.h"

#include <folly/ScopeGuard.h>
#include <unistd.h>
#include <algorithm>
#include <random>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/time/Timer.h"
#include "bolt/shuffle/sparksql/Utils.h"

namespace bytedance::bolt::shuffle::sparksql {

std::unique_ptr<RawPartitionOutput> RawPartitionOutput::create(
    PartitionWriterOptions options) {
  if (options.partitionWriterType == PartitionWriterType::kCeleborn) {
    return std::make_unique<RssRawOutput>(std::move(options));
  }
  return std::make_unique<LocalRawOutput>(std::move(options));
}

LocalRawOutput::LocalRawOutput(PartitionWriterOptions options)
    : options_(std::move(options)) {}

LocalRawOutput::~LocalRawOutput() {
  if (spillFile_ != nullptr) {
    ::fclose(spillFile_);
    ::unlink(spillPath_.c_str());
  }
}

void LocalRawOutput::ensureSpillFile() {
  if (spillFile_ != nullptr) {
    return;
  }
  if (!options_.configuredDirs.empty()) {
    // One spill file per task, in a random directory as the local partition
    // writer does, so concurrent tasks spread their spill I/O over the disks.
    const auto& dirs = options_.configuredDirs;
    std::default_random_engine engine(std::random_device{}());
    const auto dir = getSpilledShuffleFileDir(
        dirs[std::uniform_int_distribution<size_t>(0, dirs.size() - 1)(engine)],
        std::uniform_int_distribution<int32_t>(
            0, std::max<int32_t>(options_.numSubDirs, 1) - 1)(engine));
    auto maybePath = createTempShuffleFile(dir);
    BOLT_CHECK(
        maybePath.ok(),
        "Failed to create shuffle spill file: {}",
        maybePath.status().ToString());
    spillPath_ = *maybePath;
  } else {
    // Test convenience: sit next to the data file.
    spillPath_ = options_.dataFile + ".rawspill";
  }
  spillFile_ = ::fopen(spillPath_.c_str(), "wb+");
  BOLT_CHECK_NOT_NULL(spillFile_, "Failed to open {}", spillPath_);
  spillFd_ = ::fileno(spillFile_);
}

void LocalRawOutput::spillWrite(const void* data, size_t bytes) {
  {
    NanosecondTimer timer(&evictTimeNs_);
    BOLT_CHECK_EQ(
        ::fwrite(data, 1, bytes, spillFile_),
        bytes,
        "shuffle spill write failed");
  }
  spillOffset_ += bytes;
  bytesEvicted_ += static_cast<int64_t>(bytes);
}

void LocalRawOutput::flushSpill() {
  NanosecondTimer timer(&evictTimeNs_);
  BOLT_CHECK_EQ(::fflush(spillFile_), 0, "shuffle spill flush failed");
}

void LocalRawOutput::readSpill(uint64_t offset, void* out, size_t bytes) const {
  size_t done = 0;
  auto* dst = reinterpret_cast<char*>(out);
  while (done < bytes) {
    const auto n = ::pread(spillFd_, dst + done, bytes - done, offset + done);
    BOLT_CHECK_GT(n, 0, "shuffle spill read failed");
    done += n;
  }
}

void LocalRawOutput::copySpill(std::FILE* out, uint64_t begin, uint64_t bytes) {
  char copyBuffer[64 << 10];
  while (bytes > 0) {
    const size_t chunk =
        bytes < sizeof(copyBuffer) ? bytes : sizeof(copyBuffer);
    readSpill(begin, copyBuffer, chunk);
    writeOut(out, copyBuffer, chunk);
    begin += chunk;
    bytes -= chunk;
  }
}

void LocalRawOutput::writeOut(std::FILE* out, const void* data, size_t bytes) {
  NanosecondTimer timer(&writeTimeNs_);
  BOLT_CHECK_EQ(
      ::fwrite(data, 1, bytes, out), bytes, "shuffle data file write failed");
  finalBytes_ += bytes;
}

void LocalRawOutput::spillRaw(const RawBlockSource& next) {
  ensureSpillFile();
  auto& ends = rawRuns_.emplace_back(options_.numPartitions + 1);
  ends[0] = spillOffset_;
  BufferPtr block;
  for (uint32_t pid = 0; pid < options_.numPartitions; ++pid) {
    while (next(pid, block)) {
      spillWrite(block->as<char>(), block->size());
    }
    ends[pid + 1] = spillOffset_;
  }
  flushSpill();
}

void LocalRawOutput::finish(ShuffleWriterMetrics& metrics) {
  writeDataFile(nullptr, metrics);
}

void LocalRawOutput::writeDataFile(
    const std::function<void(std::FILE* out, uint32_t pid)>& writeOwn,
    ShuffleWriterMetrics& metrics) {
  std::FILE* out = ::fopen(options_.dataFile.c_str(), "wb");
  BOLT_CHECK_NOT_NULL(
      out, "Failed to open shuffle data file {}", options_.dataFile);
  {
    // A failed write must not leak the descriptor; the success path closes
    // it below, where a failing close is reported.
    SCOPE_FAIL {
      ::fclose(out);
    };
    finalBytes_ = 0;
    metrics.partitionLengths.assign(options_.numPartitions, 0);
    for (uint32_t pid = 0; pid < options_.numPartitions; ++pid) {
      const uint64_t partitionStart = finalBytes_;
      if (writeOwn) {
        writeOwn(out, pid);
      }
      for (const auto& ends : rawRuns_) {
        copySpill(out, ends[pid], ends[pid + 1] - ends[pid]);
      }
      metrics.partitionLengths[pid] =
          static_cast<int64_t>(finalBytes_ - partitionStart);
    }
    NanosecondTimer timer(&writeTimeNs_);
    BOLT_CHECK_EQ(::fflush(out), 0, "shuffle data file flush failed");
  }
  BOLT_CHECK_EQ(::fclose(out), 0, "shuffle data file close failed");

  if (spillFile_ != nullptr) {
    ::fclose(spillFile_);
    ::unlink(spillPath_.c_str());
    spillFile_ = nullptr;
    spillFd_ = -1;
  }
  metrics.totalBytesWritten = static_cast<int64_t>(finalBytes_);
  metrics.totalBytesEvicted = bytesEvicted_;
  metrics.totalWriteTime = static_cast<int64_t>(writeTimeNs_);
  metrics.totalEvictTime = static_cast<int64_t>(evictTimeNs_);
}

RssRawOutput::RssRawOutput(PartitionWriterOptions options)
    : options_(std::move(options)),
      client_(options_.rssClient),
      partitionLengths_(options_.numPartitions, 0) {
  BOLT_CHECK_NOT_NULL(
      client_, "a remote shuffle output requires an RSS client");
}

void RssRawOutput::spillRaw(const RawBlockSource& next) {
  BufferPtr block;
  for (uint32_t pid = 0; pid < options_.numPartitions; ++pid) {
    while (next(pid, block)) {
      push(pid, block);
    }
  }
}

void RssRawOutput::push(uint32_t pid, BufferPtr& bytes) {
  int32_t pushed;
  {
    NanosecondTimer timer(&pushTimeNs_);
    pushed = client_->pushPartitionData(
        static_cast<int32_t>(pid),
        bytes->asMutable<char>(),
        static_cast<int64_t>(bytes->size()));
  }
  partitionLengths_[pid] += pushed;
  pushedBytes_ += pushed;
  ++numPushes_;
}

void RssRawOutput::finish(ShuffleWriterMetrics& metrics) {
  client_->stop();
  metrics.partitionLengths = partitionLengths_;
  metrics.totalBytesWritten = pushedBytes_;
  metrics.totalBytesEvicted = pushedBytes_;
  metrics.totalWriteTime = 0;
  metrics.totalEvictTime = static_cast<int64_t>(pushTimeNs_);
}

} // namespace bytedance::bolt::shuffle::sparksql
