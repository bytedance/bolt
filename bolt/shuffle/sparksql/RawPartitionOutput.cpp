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

#include <unistd.h>
#include <algorithm>
#include <random>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/time/Timer.h"
#include "bolt/shuffle/sparksql/Utils.h"

namespace bytedance::bolt::shuffle::sparksql {

ShuffleSpillFile::ShuffleSpillFile(
    PartitionWriterOptions options,
    std::string name)
    : options_(std::move(options)), name_(std::move(name)) {}

ShuffleSpillFile::~ShuffleSpillFile() {
  remove();
}

uint64_t ShuffleSpillFile::append(const void* data, size_t bytes) {
  BOLT_CHECK(!sealed_, "shuffle spill file appended after it was sealed");
  if (file_ == nullptr) {
    if (!options_.configuredDirs.empty()) {
      // In a random directory as the local partition writer does, so
      // concurrent tasks spread their spill I/O over the disks.
      const auto& dirs = options_.configuredDirs;
      std::default_random_engine engine(std::random_device{}());
      const auto dir = getSpilledShuffleFileDir(
          dirs[std::uniform_int_distribution<size_t>(
              0, dirs.size() - 1)(engine)],
          std::uniform_int_distribution<int32_t>(
              0, std::max<int32_t>(options_.numSubDirs, 1) - 1)(engine));
      auto maybePath = createTempShuffleFile(dir);
      BOLT_CHECK(
          maybePath.ok(),
          "Failed to create shuffle spill file: {}",
          maybePath.status().ToString());
      path_ = *maybePath;
    } else {
      // Test convenience: sit next to the data file.
      path_ = options_.dataFile + "." + name_ + ".spill";
    }
    file_ = ::fopen(path_.c_str(), "wb+");
    BOLT_CHECK_NOT_NULL(file_, "Failed to open {}", path_);
    fd_ = ::fileno(file_);
  }
  const uint64_t offset = end_;
  // The I/O calls stay out of the check macros: those evaluate an operand
  // again to format the failure message.
  size_t written;
  {
    NanosecondTimer timer(&writeTimeNs_);
    written = ::fwrite(data, 1, bytes, file_);
  }
  BOLT_CHECK_EQ(written, bytes, "shuffle spill write failed");
  end_ += bytes;
  return offset;
}

void ShuffleSpillFile::flush() {
  if (file_ == nullptr) {
    return;
  }
  int status;
  {
    NanosecondTimer timer(&writeTimeNs_);
    status = ::fflush(file_);
  }
  BOLT_CHECK_EQ(status, 0, "shuffle spill flush failed");
}

void ShuffleSpillFile::read(uint64_t offset, void* out, size_t bytes) const {
  BOLT_CHECK(
      offset + bytes <= end_, "shuffle spill read beyond what was written");
  size_t done = 0;
  auto* dst = reinterpret_cast<char*>(out);
  while (done < bytes) {
    const auto n = ::pread(fd_, dst + done, bytes - done, offset + done);
    BOLT_CHECK_GT(n, 0, "shuffle spill read failed");
    done += n;
  }
}

void ShuffleSpillFile::remove() {
  if (file_ != nullptr) {
    ::fclose(file_);
    ::unlink(path_.c_str());
    file_ = nullptr;
    fd_ = -1;
  }
}

DataFileWriter::DataFileWriter(const std::string& path, uint32_t numPartitions)
    : file_(::fopen(path.c_str(), "wb")), lengths_(numPartitions, 0) {
  BOLT_CHECK_NOT_NULL(file_, "Failed to open shuffle data file {}", path);
}

DataFileWriter::~DataFileWriter() {
  if (file_ != nullptr) {
    ::fclose(file_);
  }
}

void DataFileWriter::beginPartition(uint32_t pid) {
  BOLT_CHECK_LT(pid, lengths_.size());
  pid_ = pid;
}

void DataFileWriter::write(const void* data, size_t bytes) {
  size_t written;
  {
    NanosecondTimer timer(&writeTimeNs_);
    written = ::fwrite(data, 1, bytes, file_);
  }
  BOLT_CHECK_EQ(written, bytes, "shuffle data file write failed");
  lengths_[pid_] += static_cast<int64_t>(bytes);
  bytes_ += static_cast<int64_t>(bytes);
}

void DataFileWriter::copyFrom(
    const ShuffleSpillFile& spill,
    uint64_t begin,
    uint64_t bytes) {
  char buffer[64 << 10];
  while (bytes > 0) {
    const size_t chunk = bytes < sizeof(buffer) ? bytes : sizeof(buffer);
    spill.read(begin, buffer, chunk);
    write(buffer, chunk);
    begin += chunk;
    bytes -= chunk;
  }
}

void DataFileWriter::close(ShuffleWriterMetrics& metrics) {
  int status;
  {
    NanosecondTimer timer(&writeTimeNs_);
    status = ::fflush(file_);
  }
  BOLT_CHECK_EQ(status, 0, "shuffle data file flush failed");
  std::FILE* file = file_;
  file_ = nullptr;
  status = ::fclose(file);
  BOLT_CHECK_EQ(status, 0, "shuffle data file close failed");
  metrics.partitionLengths = lengths_;
  metrics.totalBytesWritten = bytes_;
  metrics.totalWriteTime = static_cast<int64_t>(writeTimeNs_);
}

std::unique_ptr<RawPartitionOutput> RawPartitionOutput::create(
    PartitionWriterOptions options) {
  if (options.partitionWriterType == PartitionWriterType::kCeleborn) {
    return std::make_unique<RssRawOutput>(std::move(options));
  }
  return std::make_unique<LocalRawOutput>(std::move(options));
}

void PartialSpiller::mergeToDataFile(
    const PartitionWriterOptions& options,
    std::initializer_list<PartialSpiller*> spillers,
    ShuffleWriterMetrics& metrics) {
  // Nothing may be appended behind the merge: it would break the
  // partition-major order or be missed by partitions already written.
  for (auto* spiller : spillers) {
    BOLT_CHECK(!spiller->spill_.sealed(), "partial spiller merged twice");
    spiller->spill_.flush();
    spiller->spill_.seal();
  }
  DataFileWriter out(options.dataFile, options.numPartitions);
  for (uint32_t pid = 0; pid < options.numPartitions; ++pid) {
    out.beginPartition(pid);
    for (auto* spiller : spillers) {
      spiller->mergeToFinal(pid, out);
    }
  }
  out.close(metrics);
  metrics.totalBytesEvicted = 0;
  metrics.totalEvictTime = 0;
  for (auto* spiller : spillers) {
    spiller->spill_.remove();
    metrics.totalBytesEvicted += spiller->spill_.bytesWritten();
    metrics.totalEvictTime +=
        static_cast<int64_t>(spiller->spill_.writeTimeNs());
  }
}

LocalRawOutput::LocalRawOutput(PartitionWriterOptions options)
    : PartialSpiller(options, "raw"), options_(std::move(options)) {}

void LocalRawOutput::spillRaw(const RawBlockSource& next) {
  BOLT_CHECK(!spill_.sealed(), "spillRaw after the merge");
  auto& ends = rawRuns_.emplace_back(options_.numPartitions + 1);
  ends[0] = spill_.end();
  BufferPtr block;
  for (uint32_t pid = 0; pid < options_.numPartitions; ++pid) {
    while (next(pid, block)) {
      spill_.append(block->as<char>(), block->size());
    }
    ends[pid + 1] = spill_.end();
  }
  spill_.flush();
}

void LocalRawOutput::finish(ShuffleWriterMetrics& metrics) {
  mergeToDataFile(options_, {this}, metrics);
}

void LocalRawOutput::mergeToFinal(uint32_t pid, DataFileWriter& out) {
  for (const auto& ends : rawRuns_) {
    out.copyFrom(spill_, ends[pid], ends[pid + 1] - ends[pid]);
  }
}

RssRawOutput::RssRawOutput(PartitionWriterOptions options)
    : options_(std::move(options)),
      client_(options_.rssClient),
      partitionLengths_(options_.numPartitions, 0) {
  BOLT_CHECK_NOT_NULL(
      client_, "a remote shuffle output requires an RSS client");
}

void RssRawOutput::spillRaw(const RawBlockSource& next) {
  BOLT_CHECK(!finished_, "spillRaw after finish");
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
  BOLT_CHECK(!finished_, "finish called twice");
  finished_ = true;
  client_->stop();
  LOG(INFO) << "Shuffle RSS output: pushes=" << numPushes_
            << ", pushed bytes=" << pushedBytes_;
  metrics.partitionLengths = partitionLengths_;
  metrics.totalBytesWritten = pushedBytes_;
  metrics.totalBytesEvicted = pushedBytes_;
  metrics.totalWriteTime = 0;
  metrics.totalEvictTime = static_cast<int64_t>(pushTimeNs_);
}

} // namespace bytedance::bolt::shuffle::sparksql
