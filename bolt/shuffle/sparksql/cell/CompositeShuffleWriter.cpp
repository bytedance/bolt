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

uint64_t compositeRowBytes(const uint8_t* row) {
  int32_t size;
  ::memcpy(&size, row, sizeof(size));
  return sizeof(size) + static_cast<uint64_t>(size);
}

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
  resizeBuffer(block, 0, pool);
  resizeBuffer(
      block, checkedPlus<size_t>(kCompositeBlockHeaderBytes, rawBytes), pool);
  char* body = block->asMutable<char>() + kCompositeBlockHeaderBytes;
  uint64_t offset = 0;
  for (size_t i = 0; i < numRows; ++i) {
    const uint64_t bytes = compositeRowBytes(rows[i]);
    ::memcpy(body + offset, rows[i], bytes);
    offset += bytes;
  }

  auto layout = RunLayout::kCombinedStored;
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
    layout = RunLayout::kCombinedStored;
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
      rows_(numPartitions_),
      partition2RowCount_(numPartitions_),
      rawBytes_(numPartitions_, 0) {
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
  if (options_.cellOptions.compositeInput &&
      writerOptions.compressionType != arrow::Compression::UNCOMPRESSED) {
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
    if (compositeHeld_) {
      flushComposite();
      compositeHeld_ = false;
    }
    return splitOrdinary(std::move(rv), memLimit);
  }
  // Without the framing the reader would take composite blocks for
  // payloads: fail loudly instead of writing unreadable bytes.
  BOLT_CHECK(
      options_.cellOptions.compositeInput,
      "CompositeRowVector input needs HashAggregation composite output "
      "enabled for the query, on both shuffle ends");
  if (!compositeHeld_) {
    RETURN_NOT_OK(flushOrdinary());
    compositeHeld_ = true;
  }
  splitComposite(std::static_pointer_cast<CompositeRowVector>(rv));
  return arrow::Status::OK();
}

arrow::Status CompositeShuffleWriter::reclaimFixedSize(
    int64_t size,
    int64_t* actual) {
  if (!compositeHeld_) {
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
  // The composite rows go out before the output's final merge.
  flushComposite();
  RETURN_NOT_OK(stopOrdinary());
  metrics_.rawPartitionLengths.resize(numPartitions_, 0);
  for (int32_t pid = 0; pid < numPartitions_; ++pid) {
    metrics_.rawPartitionLengths[pid] += rawBytes_[pid];
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
    busy_ = true;
    SCOPE_EXIT {
      busy_ = false;
    };
    const vector_size_t numRows = rv->size();
    if (numRows == 0) {
      return;
    }
    // Composite rows are copied in whole batches: make room first.
    if (heldBytes_ > 0 && !boltPool_->maybeReserve(rv->totalRowSize())) {
      writeComposite();
    }
    pidDecoded_.decode(*rv->childAt(0));
    pids_.resize(numRows);
    for (vector_size_t row = 0; row < numRows; ++row) {
      pids_[row] = pidDecoded_.valueAt<int32_t>(row);
    }
    const auto status = partitioner_->compute(
        pids_.data(), numRows, row2Partition_, partition2RowCount_);
    BOLT_CHECK(status.ok(), "{}", status.ToString());
    const uint64_t start = currentTimeNs();
    converter_.convert(rv, row2Partition_, rows_);
    convertTimeNs_ += currentTimeNs() - start;
    heldBytes_ += rv->totalRowSize();
  }
  if (flushRequested_) {
    flushComposite();
  }
}

void CompositeShuffleWriter::flushComposite() {
  if (busy_) {
    flushRequested_ = true;
    return;
  }
  busy_ = true;
  SCOPE_EXIT {
    busy_ = false;
  };
  writeComposite();
}

void CompositeShuffleWriter::writeComposite() {
  SCOPE_EXIT {
    flushRequested_ = false;
  };
  if (heldBytes_ == 0) {
    return;
  }
  nextRow_ = 0;
  output().spillRaw(
      [this](uint32_t pid, BufferPtr& block) { return nextBlock(pid, block); });
  converter_.reset();
  heldBytes_ = 0;
  scratch_.reset();
  boltPool_->release();
}

bool CompositeShuffleWriter::nextBlock(uint32_t pid, BufferPtr& block) {
  auto& rows = rows_[pid];
  if (nextRow_ >= rows.size()) {
    rows.clear();
    nextRow_ = 0;
    return false;
  }
  const size_t begin = nextRow_;
  uint64_t raw = 0;
  while (nextRow_ < rows.size()) {
    const uint64_t bytes = compositeRowBytes(rows[nextRow_]);
    if (nextRow_ > begin && raw + bytes > kCompositeBlockBytes) {
      break;
    }
    raw += bytes;
    ++nextRow_;
  }
  buildBlock(
      rows.data() + begin,
      nextRow_ - begin,
      raw,
      codec_.get(),
      block,
      scratch_,
      boltPool_,
      compressTimeNs_);
  rawBytes_[pid] += static_cast<int64_t>(kCompositeBlockHeaderBytes + raw);
  return true;
}

} // namespace bytedance::bolt::shuffle::sparksql::cell
