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

#include "bolt/shuffle/sparksql/cell/CompositeShuffleWriter.h"

#include <folly/ScopeGuard.h>
#include <cstring>
#include <limits>
#include <numeric>

#include "bolt/common/base/CheckedArithmetic.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/shuffle/sparksql/cell/CellPayload.h"
#include "bolt/shuffle/sparksql/compression/Compression.h"

namespace bytedance::bolt::shuffle::sparksql::cell {

namespace {

/// Raw body bytes per CompositeRows block: bounds the block workspaces and
/// gives the reader batch-sized pieces.
constexpr uint64_t kCompositeBlockBytes = 4 << 20;

/// Assembles one CompositeRows block (CellFormat.h) of `rows` into `block`.
/// The body is gathered in place, so a stored block needs no second copy
/// and a compressed one is copied back over it; when the compression
/// workspace cannot be funded the block is stored.
void buildBlock(
    const uint8_t* const* rows,
    size_t numRows,
    uint64_t rawBytes,
    Codec* codec,
    BufferPtr& block,
    BufferPtr& scratch,
    memory::MemoryPool* pool,
    uint64_t& compressTimeNs) {
  BOLT_CHECK_LE(
      numRows,
      std::numeric_limits<uint32_t>::max(),
      "too many rows for one composite block");
  resetBuffer(
      block, checkedPlus<size_t>(kCompositeBlockHeaderBytes, rawBytes), pool);
  char* body = block->asMutable<char>() + kCompositeBlockHeaderBytes;
  uint64_t offset = 0;
  for (size_t i = 0; i < numRows; ++i) {
    const uint64_t bytes = compositeRowBytes(rows[i]);
    ::memcpy(body + offset, rows[i], bytes);
    offset += bytes;
  }

  auto layout = RunLayout::kCombinedUncompressed;
  uint64_t stored = rawBytes;
  try {
    const char* compressed = nullptr;
    layout = maybeCompressRun(
        codec,
        body,
        rawBytes,
        scratch,
        pool,
        compressed,
        stored,
        compressTimeNs);
    if (layout == RunLayout::kCombined) {
      ::memcpy(body, compressed, stored);
    }
  } catch (const std::exception&) {
    scratch.reset();
    layout = RunLayout::kCombinedUncompressed;
    stored = rawBytes;
  }

  char* header = block->asMutable<char>();
  header[0] = static_cast<char>(CellBlockKind::kCompositeRows);
  const auto rowCount = static_cast<uint32_t>(numRows);
  ::memcpy(header + 1, &rowCount, 4);
  ::memcpy(header + 5, &rawBytes, 8);
  ::memcpy(header + 13, &stored, 8);
  header[21] = static_cast<char>(layout);
  block->setSize(kCompositeBlockHeaderBytes + stored);
}

} // namespace

CompositeShuffleWriter::CompositeShuffleWriter(
    ShuffleWriterOptions options,
    memory::MemoryPool* boltPool,
    arrow::MemoryPool* arrowPool)
    : ShuffleWriter(
          options.partitionWriterOptions.numPartitions,
          /*partitionWriter=*/nullptr, // output goes through output()
          options, // copied: the count above is read from it
          arrowPool),
      boltPool_(boltPool),
      converter_(boltPool),
      heldRows_(numPartitions_),
      partition2RowCount_(numPartitions_),
      rawPartitionBytes_(numPartitions_, 0) {
  auto maybePartitioner = Partitioner::make(
      options_.partitioning, numPartitions_, options_.startPartitionId);
  BOLT_CHECK(
      maybePartitioner.ok(),
      "Failed to create partitioner: {}",
      maybePartitioner.status().ToString());
  partitioner_ = *maybePartitioner;
  BOLT_CHECK(
      partitioner_->hasPid(),
      "CellShuffleWriter requires hash or range partitioning");
  const auto& writerOptions = options_.partitionWriterOptions;
  if (writerOptions.compressionType != arrow::Compression::UNCOMPRESSED) {
    codec_ = createCodec(
        writerOptions.compressionType,
        CodecOptions{
            getCodecBackend(writerOptions.codecBackend),
            writerOptions.compressionLevel,
            writerOptions.checksumEnabled});
  }
}

CompositeShuffleWriter::~CompositeShuffleWriter() {
  // A failed split or stop may leave composite rows held.
  converter_.reset();
  boltPool_->release();
}

arrow::Status CompositeShuffleWriter::split(RowVectorPtr rv, int64_t memLimit) {
  BOLT_CHECK(!finished_, "split after stop");
  if (!RowVector::isComposite(rv)) {
    if (compositeActive_) {
      flushComposite();
      compositeActive_ = false;
    }
    return splitOrdinary(std::move(rv), memLimit);
  }
  if (!compositeActive_) {
    RETURN_NOT_OK(flushOrdinary());
    compositeActive_ = true;
  }
  splitComposite(std::static_pointer_cast<CompositeRowVector>(rv));
  return arrow::Status::OK();
}

arrow::Status CompositeShuffleWriter::reclaimFixedSize(
    int64_t size,
    int64_t* actual) {
  if (!compositeActive_) {
    return reclaimOrdinary(size, actual);
  }
  const int64_t before = boltPool_->reservedBytes();
  flushComposite();
  *actual = std::max<int64_t>(0, before - boltPool_->reservedBytes());
  return arrow::Status::OK();
}

arrow::Status CompositeShuffleWriter::stop() {
  BOLT_CHECK(!finished_, "stop called twice");
  finished_ = true;
  // Order matters: stopOrdinary() ends output() - it merges the composite
  // spill file into the data file, or ends the Celeborn attempt - so every
  // held composite row must be in output() before it runs.
  flushComposite();
  RETURN_NOT_OK(stopOrdinary());
  // stopOrdinary() reported the ordinary bytes only; the composite blocks'
  // raw bytes are added here, partition by partition.
  metrics_.rawPartitionLengths.resize(numPartitions_, 0);
  for (int32_t pid = 0; pid < numPartitions_; ++pid) {
    metrics_.rawPartitionLengths[pid] += rawPartitionBytes_[pid];
  }
  metrics_.dataSize = std::accumulate(
      metrics_.rawPartitionLengths.begin(),
      metrics_.rawPartitionLengths.end(),
      int64_t{0});
  metrics_.totalCompressTime += static_cast<int64_t>(compressTimeNs_);
  metrics_.convertTime += static_cast<int64_t>(convertTimeNs_);
  return arrow::Status::OK();
}

void CompositeShuffleWriter::splitComposite(const CompositeRowVectorPtr& rv) {
  {
    compositeInProgress_ = true;
    SCOPE_EXIT {
      compositeInProgress_ = false;
    };
    const vector_size_t numRows = rv->size();
    if (numRows == 0) {
      return;
    }
    // Composite rows are copied in whole batches: make room first.
    if (heldRowBytes_ > 0 && !boltPool_->maybeReserve(rv->totalRowSize())) {
      writeComposite();
    }
    pidDecoded_.decode(*rv->childAt(0));
    batchPids_.resize(numRows);
    for (vector_size_t row = 0; row < numRows; ++row) {
      batchPids_[row] = pidDecoded_.valueAt<int32_t>(row);
    }
    const auto status = partitioner_->compute(
        batchPids_.data(), numRows, row2Partition_, partition2RowCount_);
    BOLT_CHECK(status.ok(), "{}", status.ToString());
    const uint64_t start = currentTimeNs();
    converter_.convert(rv, row2Partition_, heldRows_);
    convertTimeNs_ += currentTimeNs() - start;
    heldRowBytes_ += rv->totalRowSize();
  }
  if (flushDeferred_) {
    flushComposite();
  }
}

void CompositeShuffleWriter::flushComposite() {
  if (compositeInProgress_) {
    flushDeferred_ = true;
    return;
  }
  compositeInProgress_ = true;
  SCOPE_EXIT {
    compositeInProgress_ = false;
  };
  writeComposite();
}

void CompositeShuffleWriter::writeComposite() {
  SCOPE_EXIT {
    flushDeferred_ = false;
  };
  if (heldRowBytes_ == 0) {
    return;
  }
  nextBlockRow_ = 0;
  output().spillRaw(
      [this](uint32_t pid, BufferPtr& block) { return nextBlock(pid, block); });
  converter_.reset();
  heldRowBytes_ = 0;
  compressScratch_.reset();
  boltPool_->release();
}

bool CompositeShuffleWriter::nextBlock(uint32_t pid, BufferPtr& block) {
  auto& rows = heldRows_[pid];
  if (nextBlockRow_ >= rows.size()) {
    rows.clear();
    nextBlockRow_ = 0;
    return false;
  }
  const size_t begin = nextBlockRow_;
  uint64_t raw = 0;
  while (nextBlockRow_ < rows.size()) {
    const uint64_t bytes = compositeRowBytes(rows[nextBlockRow_]);
    if (nextBlockRow_ > begin && raw + bytes > kCompositeBlockBytes) {
      break;
    }
    raw += bytes;
    ++nextBlockRow_;
  }
  buildBlock(
      rows.data() + begin,
      nextBlockRow_ - begin,
      raw,
      codec_.get(),
      block,
      compressScratch_,
      boltPool_,
      compressTimeNs_);
  rawPartitionBytes_[pid] +=
      static_cast<int64_t>(kCompositeBlockHeaderBytes + raw);
  return true;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
