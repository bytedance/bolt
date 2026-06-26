#include "bolt/exec/bm/BmStreamingWindowBuild.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/memory/MemoryArbitrator.h"
#include "bolt/common/memory/bm/MemoryTag.h"

#include <folly/ScopeGuard.h>

#include <algorithm>

namespace bytedance::bolt::exec::window {

namespace {

std::vector<bool> makeNullableColumns(size_t size) {
  return std::vector<bool>(size, true);
}

} // namespace

BmStreamingWindowBuild::BmStreamingWindowBuild(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    memory::MemoryPool* pool,
    const common::SpillConfig* spillConfig,
    tsan_atomic<bool>* nonReclaimableSection,
    uint64_t spillMemoryThreshold,
    bool enableJit,
    std::shared_ptr<memory::bm::BufferManager> bufferManager)
    : WindowBuild(
          windowNode,
          pool,
          spillConfig,
          nonReclaimableSection,
          spillMemoryThreshold,
          enableJit),
      bufferManager_(std::move(bufferManager)),
      bmData_(std::make_unique<exec::bm::BmRowContainer>(
          inputType_->children(),
          makeNullableColumns(inputType_->size()),
          bufferManager_,
          memory::bm::MemoryTag::kWindow)),
      logicalTypes_(windowNode->inputType()->children()) {
  boundaryKeyColumns_.reserve(partitionKeyInfo_.size() + sortKeyInfo_.size());
  auto addBoundaryKeyColumn = [&](int32_t column) {
    if (std::find(
            boundaryKeyColumns_.begin(), boundaryKeyColumns_.end(), column) ==
        boundaryKeyColumns_.end()) {
      boundaryKeyColumns_.push_back(column);
    }
  };
  for (const auto& key : partitionKeyInfo_) {
    addBoundaryKeyColumn(key.first);
  }
  for (const auto& key : sortKeyInfo_) {
    addBoundaryKeyColumn(key.first);
  }
}

bool BmStreamingWindowBuild::needsInput() {
  releasePartitionIfConsumed();
  return !inputFinished_ && readyPartitions_.empty() &&
      returnedPartitionRows_ == 0;
}

bool BmStreamingWindowBuild::hasOutputAll() {
  releasePartitionIfConsumed();
  return inputFinished_ && readyPartitions_.empty() &&
      !openPartition_.has_value() && returnedPartitionRows_ == 0;
}

RowVectorPtr BmStreamingWindowBuild::makeReorderedInput(
    const RowVectorPtr& input) const {
  std::vector<VectorPtr> children;
  children.reserve(inputChannels_.size());
  for (const auto channel : inputChannels_) {
    children.push_back(input->childAt(channel));
  }
  return std::make_shared<RowVector>(
      pool_, inputType_, nullptr, input->size(), std::move(children));
}

void BmStreamingWindowBuild::appendRowsToOpenPartition(
    exec::bm::SegmentId segment,
    exec::bm::RowNumber begin,
    const std::vector<char*>& rows,
    vector_size_t offset,
    vector_size_t size) {
  if (size == 0) {
    return;
  }
  if (!openPartition_.has_value()) {
    openPartition_.emplace();
  }

  auto& descriptor = *openPartition_;
  if (collectPeerBoundaries()) {
    descriptor.peerBoundaryMode = BmPeerBoundaryMode::kPrecomputed;
  }
  const bool keepResidentRows = !descriptor.hasSpilledRows &&
      descriptor.residentRows.size() == descriptor.numRows;
  // Keep a compact physical description of the partition. Adjacent rows from
  // the same BM segment are merged into one range so WindowRead can load by
  // segment-local ranges instead of materializing one RowId per row.
  if (!descriptor.ranges.empty()) {
    auto& last = descriptor.ranges.back();
    if (last.segment == segment && last.begin + last.count == begin) {
      last.count += static_cast<exec::bm::RowNumber>(size);
    } else {
      descriptor.ranges.push_back(
          {segment, begin, static_cast<exec::bm::RowNumber>(size)});
    }
  } else {
    descriptor.ranges.push_back(
        {segment, begin, static_cast<exec::bm::RowNumber>(size)});
  }

  if (keepResidentRows) {
    descriptor.residentRows.insert(
        descriptor.residentRows.end(),
        rows.begin() + offset,
        rows.begin() + offset + size);
  }
  descriptor.numRows += size;
}

BmStreamingWindowBuild::AdjacentRelation
BmStreamingWindowBuild::classifyAdjacentRows(
    const char* left,
    const char* right) const {
  if (!partitionKeyInfo_.empty() && comparePartitionRows(left, right) != 0) {
    return AdjacentRelation::kNewPartition;
  }
  if (!collectPeerBoundaries() || compareSortRows(left, right) == 0) {
    return AdjacentRelation::kSamePeer;
  }
  return AdjacentRelation::kNewPeer;
}

void BmStreamingWindowBuild::markPeerStart(vector_size_t rowOffset) {
  if (!collectPeerBoundaries() || rowOffset == 0) {
    return;
  }
  if (!openPartition_.has_value()) {
    openPartition_.emplace();
  }
  auto& descriptor = *openPartition_;
  descriptor.peerBoundaryMode = BmPeerBoundaryMode::kPrecomputed;
  const auto words = bits::nwords(static_cast<int32_t>(rowOffset + 1));
  if (descriptor.peerStartBits.size() < words) {
    descriptor.peerStartBits.resize(words, 0);
  }
  bits::setBit(descriptor.peerStartBits.data(), rowOffset);
}

void BmStreamingWindowBuild::closeOpenPartition() {
  if (!openPartition_.has_value() || openPartition_->numRows == 0) {
    openPartition_.reset();
    return;
  }
  readyPartitions_.push_back(std::move(*openPartition_));
  openPartition_.reset();
}

void BmStreamingWindowBuild::splitRowsIntoPartitions(
    exec::bm::SegmentId segment,
    exec::bm::RowNumber begin,
    const std::vector<char*>& rows) {
  if (rows.empty()) {
    return;
  }

  // Check the boundary between the previous input batch and this batch. The
  // upstream sort keeps rows ordered by partition and sort keys, so this single
  // adjacent classification can detect both partition and peer boundaries.
  if (previousRow_.row != nullptr) {
    switch (classifyAdjacentRows(previousRow_.row, rows.front())) {
      case AdjacentRelation::kNewPartition:
        closeOpenPartition();
        break;
      case AdjacentRelation::kNewPeer:
        BOLT_CHECK(openPartition_.has_value());
        markPeerStart(openPartition_->numRows);
        break;
      case AdjacentRelation::kSamePeer:
        break;
    }
  }

  // Fast path for a batch that cannot contain a partition boundary. If there is
  // only one row, or the first and last rows have the same partition and sort
  // keys needed by this build, sorted input guarantees every row in between
  // belongs to the same partition and peer group.
  if (rows.size() == 1 ||
      classifyAdjacentRows(rows.front(), rows.back()) ==
          AdjacentRelation::kSamePeer) {
    appendRowsToOpenPartition(segment, begin, rows, 0, rows.size());
    recordResidentPreviousRow(rows.back());
    return;
  }

  vector_size_t runBegin = 0;
  for (vector_size_t i = 1; i < rows.size(); ++i) {
    switch (classifyAdjacentRows(rows[i - 1], rows[i])) {
      case AdjacentRelation::kSamePeer:
        continue;
      case AdjacentRelation::kNewPeer: {
        const auto peerStartOffset = openPartition_.has_value()
            ? openPartition_->numRows + i - runBegin
            : i - runBegin;
        markPeerStart(peerStartOffset);
      }
        continue;
      case AdjacentRelation::kNewPartition:
        break;
    }
    appendRowsToOpenPartition(
        segment,
        static_cast<exec::bm::RowNumber>(begin + runBegin),
        rows,
        runBegin,
        i - runBegin);
    closeOpenPartition();
    runBegin = i;
  }

  // tail process
  appendRowsToOpenPartition(
      segment,
      static_cast<exec::bm::RowNumber>(begin + runBegin),
      rows,
      runBegin,
      rows.size() - runBegin);
  recordResidentPreviousRow(rows.back());
}

void BmStreamingWindowBuild::recordResidentPreviousRow(const char* row) {
  previousRow_.row = row;
  previousRow_.ownedRow.clear();
  previousRow_.ownedVariableData.clear();
}

void BmStreamingWindowBuild::prepareMemoryForAppend(const RowVectorPtr& input) {
  constexpr uint64_t kAppendMemoryAmplification = 2;
  constexpr uint64_t kMinAppendGranularitySlack = 16ULL << 20;
  const auto estimatedBytes = (input->usedSize() * kAppendMemoryAmplification) +
      kMinAppendGranularitySlack;
  // Preflight before entering appendBatch(), which mutates BM segment/chunk
  // metadata under a non-reclaimable section. maybeReserve() can trigger memory
  // arbitration while this Window is still reclaimable, allowing
  // Window::reclaim to spill the current active segment first. The result is
  // only a hint: the real append can still allocate later if memory changes.
  memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
  const auto reserved = bufferManager_->MaybeReserve(estimatedBytes);
  bufferManager_->ReleaseUnusedReservation();
  (void)reserved;
}

void BmStreamingWindowBuild::copyPreviousRowBeforeSpill() {
  if (previousRow_.row == nullptr ||
      previousRow_.row == previousRow_.ownedRow.data() ||
      boundaryKeyColumns_.empty()) {
    return;
  }
  bmData_->copyRowWithDeepColumns(
      previousRow_.row,
      {boundaryKeyColumns_.data(), boundaryKeyColumns_.size()},
      previousRow_.ownedRow,
      previousRow_.ownedVariableData);
  previousRow_.row = previousRow_.ownedRow.data();
}

void BmStreamingWindowBuild::clearAllResidentRowPointers() {
  if (openPartition_.has_value()) {
    openPartition_->residentRows.clear();
  }
  for (auto& descriptor : readyPartitions_) {
    descriptor.residentRows.clear();
  }
}

void BmStreamingWindowBuild::markAllDescriptorsSpilled() {
  if (openPartition_.has_value()) {
    openPartition_->hasSpilledRows = true;
  }
  for (auto& descriptor : readyPartitions_) {
    descriptor.hasSpilledRows = true;
  }
}

bool BmStreamingWindowBuild::descriptorContainsActiveSegment(
    const BmWindowPartitionDescriptor& descriptor) const {
  if (bmData_ == nullptr) {
    return false;
  }
  const auto activeSegment = bmData_->activeSegmentId();
  if (activeSegment == exec::bm::kNoSegment) {
    return false;
  }
  for (const auto& range : descriptor.ranges) {
    if (range.segment == activeSegment) {
      return true;
    }
  }
  return false;
}

vector_size_t BmStreamingWindowBuild::activeRowsInDescriptor(
    const BmWindowPartitionDescriptor& descriptor) const {
  if (bmData_ == nullptr) {
    return 0;
  }
  const auto activeSegment = bmData_->activeSegmentId();
  if (activeSegment == exec::bm::kNoSegment) {
    return 0;
  }

  uint64_t rows = 0;
  for (const auto& range : descriptor.ranges) {
    if (range.segment == activeSegment) {
      rows += range.count;
    }
  }
  return static_cast<vector_size_t>(rows);
}

void BmStreamingWindowBuild::spillActiveRows() {
  if (returnedPartitionRows_ != 0 || activeRows_ == 0) {
    return;
  }

  copyPreviousRowBeforeSpill();
  markAllDescriptorsSpilled();
  clearAllResidentRowPointers();

  const auto statsBefore = bufferManager_->stats();
  const auto spilledRows = activeRows_;
  bmData_->spillActiveSegment();
  const auto statsAfter = bufferManager_->stats();
  const auto spilledBytes =
      statsAfter.spillWriteBytes - statsBefore.spillWriteBytes;
  const auto spillWrites =
      statsAfter.spillWriteCount - statsBefore.spillWriteCount;

  ++spillStats_.spillRuns;
  ++spillStats_.spilledPartitions;
  spillStats_.spilledInputBytes += spilledBytes;
  spillStats_.spilledBytes += spilledBytes;
  spillStats_.spilledRows += spilledRows;
  spillStats_.spilledFiles += spillWrites;
  spillStats_.spillWrites += spillWrites;

  activeRows_ = 0;
}

void BmStreamingWindowBuild::releasePartitionIfConsumed() {
  if (returnedPartitionRows_ == 0 || !returnedPartition_.expired()) {
    return;
  }

  bmData_->popFrontRows(returnedPartitionRows_);
  activeRows_ = returnedActiveRows_ >= activeRows_
      ? 0
      : (activeRows_ - returnedActiveRows_);
  returnedPartitionRows_ = 0;
  returnedActiveRows_ = 0;
}

void BmStreamingWindowBuild::addInput(RowVectorPtr input) {
  releasePartitionIfConsumed();
  BOLT_CHECK(
      readyPartitions_.empty(), "Cannot add input while a partition is ready");
  BOLT_CHECK_EQ(
      returnedPartitionRows_,
      0,
      "Cannot add input while a partition is still being consumed");

  if (input->size() == 0) {
    return;
  }

  auto reordered = makeReorderedInput(input);
  std::vector<char*> rows;
  prepareMemoryForAppend(reordered);
  *nonReclaimableSection_ = true;
  auto reclaimGuard =
      folly::makeGuard([&] { *nonReclaimableSection_ = false; });
  bmData_->appendBatch(reordered, exec::bm::kDefaultPartition, &rows);
  BOLT_CHECK_EQ(rows.size(), input->size());

  const auto segment = bmData_->activeSegmentId();
  const auto end = bmData_->activeSegmentNextRowNumber();
  BOLT_CHECK_GE(end, rows.size());
  const auto begin = static_cast<exec::bm::RowNumber>(end - rows.size());

  splitRowsIntoPartitions(segment, begin, rows);
  activeRows_ += rows.size();
  numRows_ += input->size();
}

void BmStreamingWindowBuild::spill() {
  releasePartitionIfConsumed();
  if (returnedPartitionRows_ != 0) {
    if (auto partition = returnedPartition_.lock()) {
      if (auto bmPartition =
              std::dynamic_pointer_cast<BmWindowPartition>(partition)) {
        bmPartition->reclaimReadChunks();
      }
    }
    return;
  }
  spillActiveRows();
}

std::optional<common::SpillStats> BmStreamingWindowBuild::windowSpilledStats()
    const {
  if (spillStats_.spillRuns == 0) {
    return std::nullopt;
  }
  return spillStats_;
}

void BmStreamingWindowBuild::noMoreInput() {
  inputFinished_ = true;
  closeOpenPartition();
}

bool BmStreamingWindowBuild::hasNextPartition() {
  releasePartitionIfConsumed();
  if (returnedPartitionRows_ != 0) {
    return false;
  }
  if (inputFinished_) {
    closeOpenPartition();
  }
  return !readyPartitions_.empty();
}

std::shared_ptr<exec::WindowPartition> BmStreamingWindowBuild::nextPartition() {
  BOLT_CHECK(hasNextPartition(), "No window partitions available");
  if (readyPartitions_.front().hasSpilledRows &&
      descriptorContainsActiveSegment(readyPartitions_.front())) {
    spillActiveRows();
  }
  auto descriptor = std::move(readyPartitions_.front());
  readyPartitions_.pop_front();

  returnedPartitionRows_ = descriptor.numRows;
  returnedActiveRows_ = activeRowsInDescriptor(descriptor);

  auto partition = std::make_shared<BmWindowPartition>(
      bmData_.get(),
      pool_,
      logicalTypes_,
      std::move(descriptor),
      inversedInputChannels_,
      sortKeyInfo_);
  returnedPartition_ = partition;
  return partition;
}

void BmStreamingWindowBuild::finish() {
  readyPartitions_.clear();
  openPartition_.reset();
  returnedPartition_.reset();
  returnedPartitionRows_ = 0;
  returnedActiveRows_ = 0;
  previousRow_ = PreviousRow{};
  inputFinished_ = false;
  activeRows_ = 0;
  bmData_.reset();
}

} // namespace bytedance::bolt::exec::window
