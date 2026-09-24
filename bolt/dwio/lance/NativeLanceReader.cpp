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

#include "bolt/dwio/lance/NativeLanceReader.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <limits>
#include <mutex>
#include <numeric>
#include <unordered_map>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/dwio/common/ParallelFor.h"
#include "bolt/dwio/lance/NativeLanceFileOpenTask.h"
#include "bolt/type/Filter.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::lance::reader {
namespace {

using RowSet = std::vector<vector_size_t, memory::StlAllocator<vector_size_t>>;

NativeLanceRowSelection selectionForRows(
    const RowSet& rows,
    vector_size_t inputSize) {
  if (rows.size() == static_cast<size_t>(inputSize)) {
    bool identity = true;
    for (vector_size_t row = 0; row < inputSize; ++row) {
      if (rows[row] != row) {
        identity = false;
        break;
      }
    }
    if (identity) {
      return NativeLanceRowSelection::all();
    }
  }
  return NativeLanceRowSelection::rows(rows);
}

NativeLanceColumnRequest columnRequest(
    uint64_t rowStart,
    vector_size_t inputSize,
    const RowSet& rows,
    NativeLanceDecodePurpose purpose) {
  return {
      .rowStart = rowStart,
      .rowCount = static_cast<uint64_t>(inputSize),
      .selection = selectionForRows(rows, inputSize),
      .purpose = purpose};
}

void initializeSelectedRows(
    RowSet& rows,
    vector_size_t inputSize,
    const uint64_t* deletedRows) {
  rows.resize(inputSize);
  if (deletedRows == nullptr) {
    std::iota(rows.begin(), rows.end(), 0);
    return;
  }
  auto output = rows.begin();
  for (vector_size_t row = 0; row < inputSize; ++row) {
    if (!bits::isBitSet(deletedRows, row)) {
      *output++ = row;
    }
  }
  rows.erase(output, rows.end());
}

template <TypeKind kind>
bool testFilterRow(
    const BaseVector& vector,
    common::Filter& filter,
    vector_size_t row) {
  using T = typename TypeTraits<kind>::NativeType;
  const auto* values = vector.asUnchecked<SimpleVector<T>>();
  return common::applyFilter(filter, values->valueAt(row));
}

bool testFilterRow(
    const BaseVector& vector,
    common::Filter& filter,
    vector_size_t row) {
  if (vector.isNullAt(row)) {
    return filter.testNull();
  }
  switch (vector.typeKind()) {
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      BOLT_USER_CHECK(
          filter.kind() == common::FilterKind::kIsNull ||
              filter.kind() == common::FilterKind::kIsNotNull,
          "Complex type can only take a null filter, got {}",
          filter.toString());
      return filter.testNonNull();
    default:
      return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH(
          testFilterRow, vector.typeKind(), vector, filter, row);
  }
}

bool testScanSpecRow(
    const BaseVector& vector,
    const common::ScanSpec& scanSpec,
    vector_size_t row) {
  if (vector.isNullAt(row)) {
    return scanSpec.testNull();
  }
  if (scanSpec.filter() != nullptr &&
      !testFilterRow(vector, *scanSpec.filter(), row)) {
    return false;
  }
  if (vector.typeKind() != TypeKind::ROW) {
    return true;
  }

  const auto* values = vector.wrappedVector()->as<RowVector>();
  BOLT_CHECK_NOT_NULL(values);
  const auto& rowType = vector.type()->asRow();
  const auto nestedRow = vector.wrappedIndex(row);
  for (const auto& child : scanSpec.children()) {
    if (!child->hasFilter()) {
      continue;
    }
    if (child->isConstant()) {
      if (!testScanSpecRow(*child->constantValue(), *child, 0)) {
        return false;
      }
      continue;
    }
    const auto index = rowType.getChildIdx(child->fieldName());
    BOLT_CHECK_GE(
        index,
        0,
        "Nested Lance filter field '{}' is missing",
        child->fieldName());
    if (!testScanSpecRow(*values->childAt(index), *child, nestedRow)) {
      return false;
    }
  }
  return true;
}

bool requiresSubfieldPruning(
    const TypePtr& type,
    const common::ScanSpec& scanSpec) {
  switch (type->kind()) {
    case TypeKind::ROW: {
      const auto& rowType = type->asRow();
      bool hasProjectedChild = false;
      for (const auto& child : scanSpec.children()) {
        if (!child->projectOut()) {
          continue;
        }
        hasProjectedChild = true;
        if (child->isConstant()) {
          return true;
        }
        const auto childIndex = rowType.getChildIdx(child->fieldName());
        if (childIndex >= 0 &&
            requiresSubfieldPruning(rowType.childAt(childIndex), *child)) {
          return true;
        }
      }
      if (!hasProjectedChild) {
        return false;
      }
      for (uint32_t childIndex = 0; childIndex < rowType.size(); ++childIndex) {
        const auto* child = scanSpec.childByName(rowType.nameOf(childIndex));
        if (child == nullptr || !child->projectOut()) {
          return true;
        }
      }
      return false;
    }
    case TypeKind::ARRAY: {
      if (scanSpec.maxArrayElementsCount() !=
          std::numeric_limits<vector_size_t>::max()) {
        return true;
      }
      const auto* elements =
          scanSpec.childByName(common::ScanSpec::kArrayElementsFieldName);
      return elements != nullptr &&
          requiresSubfieldPruning(type->childAt(0), *elements);
    }
    case TypeKind::MAP: {
      if (scanSpec.maxArrayElementsCount() !=
          std::numeric_limits<vector_size_t>::max()) {
        return true;
      }
      const auto* keys =
          scanSpec.childByName(common::ScanSpec::kMapKeysFieldName);
      if (keys != nullptr &&
          (keys->hasFilter() ||
           requiresSubfieldPruning(type->childAt(0), *keys))) {
        return true;
      }
      const auto* values =
          scanSpec.childByName(common::ScanSpec::kMapValuesFieldName);
      return values != nullptr &&
          requiresSubfieldPruning(type->childAt(1), *values);
    }
    default:
      return false;
  }
}

BufferPtr copyLogicalNulls(const BaseVector& vector, memory::MemoryPool& pool) {
  BufferPtr nulls;
  for (vector_size_t row = 0; row < vector.size(); ++row) {
    if (!vector.isNullAt(row)) {
      continue;
    }
    if (nulls == nullptr) {
      nulls = allocateNulls(vector.size(), &pool);
    }
    bits::setNull(nulls->asMutable<uint64_t>(), row);
  }
  return nulls;
}

VectorPtr wrapSelected(
    const VectorPtr& source,
    const RowSet& selected,
    memory::MemoryPool& pool) {
  if (selected.empty()) {
    return BaseVector::create(source->type(), 0, &pool);
  }
  auto indices = allocateIndices(selected.size(), &pool);
  std::copy(
      selected.begin(), selected.end(), indices->asMutable<vector_size_t>());
  return BaseVector::wrapInDictionary(
      nullptr, std::move(indices), selected.size(), source);
}

VectorPtr applySubfieldPruning(
    VectorPtr vector,
    const common::ScanSpec& scanSpec,
    memory::MemoryPool& pool) {
  if (!requiresSubfieldPruning(vector->type(), scanSpec)) {
    return vector;
  }

  switch (vector->typeKind()) {
    case TypeKind::ROW: {
      const auto* source = vector->wrappedVector()->as<RowVector>();
      BOLT_CHECK_NOT_NULL(source);
      const auto& rowType = vector->type()->asRow();
      RowSet parentRows(
          vector->size(), memory::StlAllocator<vector_size_t>(&pool));
      for (vector_size_t row = 0; row < vector->size(); ++row) {
        parentRows[row] = vector->wrappedIndex(row);
      }
      std::vector<VectorPtr> children;
      children.reserve(rowType.size());
      for (uint32_t childIndex = 0; childIndex < rowType.size(); ++childIndex) {
        const auto* childSpec =
            scanSpec.childByName(rowType.nameOf(childIndex));
        if (childSpec == nullptr || !childSpec->projectOut()) {
          children.push_back(BaseVector::createNullConstant(
              rowType.childAt(childIndex), vector->size(), &pool));
          continue;
        }
        if (childSpec->isConstant()) {
          children.push_back(BaseVector::wrapInConstant(
              vector->size(), 0, childSpec->constantValue()));
          continue;
        }
        auto child =
            wrapSelected(source->childAt(childIndex), parentRows, pool);
        children.push_back(
            applySubfieldPruning(std::move(child), *childSpec, pool));
      }
      return std::make_shared<RowVector>(
          &pool,
          vector->type(),
          copyLogicalNulls(*vector, pool),
          vector->size(),
          std::move(children));
    }
    case TypeKind::ARRAY: {
      const auto* source = vector->wrappedVector()->as<ArrayVector>();
      BOLT_CHECK_NOT_NULL(source);
      auto offsets = allocateOffsets(vector->size(), &pool);
      auto sizes = allocateSizes(vector->size(), &pool);
      auto* rawOffsets = offsets->asMutable<vector_size_t>();
      auto* rawSizes = sizes->asMutable<vector_size_t>();
      RowSet elementRows{memory::StlAllocator<vector_size_t>(&pool)};
      for (vector_size_t row = 0; row < vector->size(); ++row) {
        rawOffsets[row] = elementRows.size();
        if (vector->isNullAt(row)) {
          rawSizes[row] = 0;
          continue;
        }
        const auto sourceRow = vector->wrappedIndex(row);
        const auto size = std::min(
            source->sizeAt(sourceRow), scanSpec.maxArrayElementsCount());
        rawSizes[row] = size;
        const auto offset = source->offsetAt(sourceRow);
        for (vector_size_t element = 0; element < size; ++element) {
          elementRows.push_back(offset + element);
        }
      }
      auto elements = wrapSelected(source->elements(), elementRows, pool);
      if (const auto* elementSpec =
              scanSpec.childByName(common::ScanSpec::kArrayElementsFieldName)) {
        elements =
            applySubfieldPruning(std::move(elements), *elementSpec, pool);
      }
      return std::make_shared<ArrayVector>(
          &pool,
          vector->type(),
          copyLogicalNulls(*vector, pool),
          vector->size(),
          std::move(offsets),
          std::move(sizes),
          std::move(elements));
    }
    case TypeKind::MAP: {
      const auto* source = vector->wrappedVector()->as<MapVector>();
      BOLT_CHECK_NOT_NULL(source);
      const auto* keySpec =
          scanSpec.childByName(common::ScanSpec::kMapKeysFieldName);
      auto offsets = allocateOffsets(vector->size(), &pool);
      auto sizes = allocateSizes(vector->size(), &pool);
      auto* rawOffsets = offsets->asMutable<vector_size_t>();
      auto* rawSizes = sizes->asMutable<vector_size_t>();
      RowSet entryRows{memory::StlAllocator<vector_size_t>(&pool)};
      for (vector_size_t row = 0; row < vector->size(); ++row) {
        rawOffsets[row] = entryRows.size();
        if (vector->isNullAt(row)) {
          rawSizes[row] = 0;
          continue;
        }
        const auto sourceRow = vector->wrappedIndex(row);
        const auto size = std::min(
            source->sizeAt(sourceRow), scanSpec.maxArrayElementsCount());
        const auto offset = source->offsetAt(sourceRow);
        for (vector_size_t entry = 0; entry < size; ++entry) {
          const auto sourceEntry = offset + entry;
          if (keySpec == nullptr || !keySpec->hasFilter() ||
              testScanSpecRow(*source->mapKeys(), *keySpec, sourceEntry)) {
            entryRows.push_back(sourceEntry);
          }
        }
        rawSizes[row] = entryRows.size() - rawOffsets[row];
      }
      auto keys = wrapSelected(source->mapKeys(), entryRows, pool);
      if (keySpec != nullptr) {
        keys = applySubfieldPruning(std::move(keys), *keySpec, pool);
      }
      auto values = wrapSelected(source->mapValues(), entryRows, pool);
      if (const auto* valueSpec =
              scanSpec.childByName(common::ScanSpec::kMapValuesFieldName)) {
        values = applySubfieldPruning(std::move(values), *valueSpec, pool);
      }
      return std::make_shared<MapVector>(
          &pool,
          vector->type(),
          copyLogicalNulls(*vector, pool),
          vector->size(),
          std::move(offsets),
          std::move(sizes),
          std::move(keys),
          std::move(values),
          std::nullopt,
          source->hasSortedKeys());
    }
    default:
      return vector;
  }
}

VectorPtr applyScanSpecProjection(
    VectorPtr result,
    const common::ScanSpec& scanSpec,
    memory::MemoryPool& pool) {
  const auto* rows = result->as<RowVector>();
  BOLT_CHECK_NOT_NULL(rows);
  std::vector<VectorPtr> children = rows->children();
  bool changed = false;
  for (const auto& childSpec : scanSpec.children()) {
    if (!childSpec->projectOut() || childSpec->isConstant()) {
      continue;
    }
    BOLT_CHECK_NE(childSpec->channel(), common::ScanSpec::kNoChannel);
    auto& child = children.at(childSpec->channel());
    if (requiresSubfieldPruning(child->type(), *childSpec)) {
      child = applySubfieldPruning(std::move(child), *childSpec, pool);
      changed = true;
    }
  }
  if (!changed) {
    return result;
  }
  return std::make_shared<RowVector>(
      &pool,
      result->type(),
      result->nulls(),
      result->size(),
      std::move(children));
}

struct NativeLanceFilterResult {
  RowSet selectedRows;
  std::unordered_map<uint32_t, VectorPtr> predecodedColumns;
};

NativeLanceFilterResult decodeFilters(
    NativeLanceColumnSource& source,
    const NativeLanceScanPlan& scanPlan,
    const RowTypePtr& fileType,
    const common::ScanSpec& scanSpec,
    uint64_t batchRowStart,
    vector_size_t batchSize,
    memory::MemoryPool& pool,
    const uint64_t* deletedRows) {
  RowSet selectedRows{memory::StlAllocator<vector_size_t>(&pool)};
  initializeSelectedRows(selectedRows, batchSize, deletedRows);

  struct DecodedFilterColumn {
    DecodedFilterColumn(RowSet inputRows, VectorPtr inputValues)
        : rows(std::move(inputRows)), values(std::move(inputValues)) {}

    RowSet rows;
    VectorPtr values;
  };
  std::unordered_map<uint32_t, DecodedFilterColumn> decodedFilterColumns;

  // Filter columns are decoded first. Every later filter sees only rows that
  // passed the earlier filters, allowing its I/O to shrink to matching ranges.
  for (const auto& child : scanSpec.children()) {
    if (!child->hasFilter()) {
      continue;
    }
    const auto columnIndex = child->isConstant()
        ? std::optional<uint32_t>{}
        : std::make_optional<uint32_t>(
              fileType->getChildIdx(child->fieldName()));
    const auto request = columnRequest(
        batchRowStart,
        batchSize,
        selectedRows,
        NativeLanceDecodePurpose::kFilter);
    VectorPtr values;
    if (child->isConstant()) {
      values = BaseVector::wrapInConstant(
          request.outputSize(), 0, child->constantValue());
    } else {
      const auto& reader = scanPlan.filterColumnReader(*columnIndex);
      reader.plan(source, request);
      values = reader.read(source, request, pool, true);
    }

    auto inputRows = selectedRows;
    RowSet passingRows{memory::StlAllocator<vector_size_t>(&pool)};
    passingRows.reserve(selectedRows.size());
    for (vector_size_t row = 0; row < values->size(); ++row) {
      if (testScanSpecRow(*values, *child, row)) {
        passingRows.push_back(selectedRows[row]);
      }
    }
    selectedRows = std::move(passingRows);
    if (child->projectOut() && columnIndex.has_value()) {
      decodedFilterColumns.emplace(
          *columnIndex,
          DecodedFilterColumn(std::move(inputRows), std::move(values)));
    }
    if (selectedRows.empty()) {
      break;
    }
  }

  std::unordered_map<uint32_t, VectorPtr> predecodedColumns;
  predecodedColumns.reserve(decodedFilterColumns.size());
  for (auto& [columnIndex, decoded] : decodedFilterColumns) {
    if (decoded.rows == selectedRows) {
      predecodedColumns.emplace(columnIndex, std::move(decoded.values));
      continue;
    }
    auto indices = allocateIndices(selectedRows.size(), &pool);
    auto* rawIndices = indices->asMutable<vector_size_t>();
    size_t sourceIndex = 0;
    for (size_t i = 0; i < selectedRows.size(); ++i) {
      while (sourceIndex < decoded.rows.size() &&
             decoded.rows[sourceIndex] < selectedRows[i]) {
        ++sourceIndex;
      }
      BOLT_CHECK_LT(sourceIndex, decoded.rows.size());
      BOLT_CHECK_EQ(decoded.rows[sourceIndex], selectedRows[i]);
      rawIndices[i] = static_cast<vector_size_t>(sourceIndex);
    }
    predecodedColumns.emplace(
        columnIndex,
        BaseVector::wrapInDictionary(
            nullptr,
            std::move(indices),
            selectedRows.size(),
            std::move(decoded.values)));
  }

  return {std::move(selectedRows), std::move(predecodedColumns)};
}

} // namespace

NativeLanceScanCoordinator::NativeLanceScanCoordinator(
    std::shared_ptr<const NativeLanceFileContext> fileContext,
    dwio::common::RowReaderOptions options)
    : fileContext_(std::move(fileContext)),
      options_(std::move(options)),
      input_(fileContext_->newInput()),
      decoder_(
          *input_,
          fileContext_->metadata(),
          fileContext_->pool(),
          fileContext_->blobResolver(),
          fileContext_->readSchedulerOptions()),
      scanPlan_(NativeLanceScanPlan::build(*fileContext_, options_)) {
  maxBatchBytes_ = options_.getMaxBatchBytes();
  estimatedBytesPerRow_ = scanPlan_->estimatedBytesPerRow();
  currentRow_ =
      scanPlan_->rowRanges().empty() ? 0 : scanPlan_->rowRanges().front().first;
  initializePrefetchRanges();
}

NativeLanceScanCoordinator::~NativeLanceScanCoordinator() {
  cancel();
}

void NativeLanceScanCoordinator::cancel() {
  {
    std::lock_guard<std::mutex> lock(stateMutex_);
    if (state_ != NativeLanceScanState::kFinished &&
        state_ != NativeLanceScanState::kFailed) {
      state_ = NativeLanceScanState::kCancelled;
    }
    if (window_.has_value()) {
      window_->cancel();
    }
  }
  decoder_.cancel();
}

NativeLanceScanState NativeLanceScanCoordinator::state() const {
  std::lock_guard<std::mutex> lock(stateMutex_);
  return state_;
}

void NativeLanceScanCoordinator::transition(
    NativeLanceScanState expected,
    NativeLanceScanState desired) {
  std::lock_guard<std::mutex> lock(stateMutex_);
  BOLT_CHECK(state_ == expected, "Invalid native Lance scan state transition");
  state_ = desired;
}

void NativeLanceScanCoordinator::fail(std::exception_ptr error) {
  std::lock_guard<std::mutex> lock(stateMutex_);
  if (state_ == NativeLanceScanState::kCancelled) {
    return;
  }
  failure_ = std::move(error);
  if (window_.has_value()) {
    window_->fail(failure_);
  }
  state_ = NativeLanceScanState::kFailed;
}

void NativeLanceScanCoordinator::rethrowFailure() const {
  BOLT_CHECK_NOT_NULL(failure_);
  std::rethrow_exception(failure_);
}

void NativeLanceScanCoordinator::advancePastFinishedRange() {
  while (currentRange_ < rowRanges().size() &&
         currentRow_ >= rowRanges()[currentRange_].second) {
    ++currentRange_;
    if (currentRange_ < rowRanges().size()) {
      currentRow_ = rowRanges()[currentRange_].first;
    }
  }
}

int64_t NativeLanceScanCoordinator::nextRowNumber() {
  advancePastFinishedRange();
  return currentRange_ >= rowRanges().size()
      ? kAtEnd
      : static_cast<int64_t>(currentRow_);
}

int64_t NativeLanceScanCoordinator::nextReadSize(uint64_t size) {
  BOLT_CHECK_GT(size, 0);
  advancePastFinishedRange();
  if (currentRange_ >= rowRanges().size()) {
    return kAtEnd;
  }
  return static_cast<int64_t>(capReadSize(
      std::min(size, rowRanges()[currentRange_].second - currentRow_)));
}

uint64_t NativeLanceScanCoordinator::capReadSize(uint64_t size) const {
  if (maxBatchBytes_ <= 0 || estimatedBytesPerRow_ == 0) {
    return size;
  }
  return std::min<uint64_t>(
      size, std::max<int64_t>(1, maxBatchBytes_ / estimatedBytesPerRow_));
}

void NativeLanceScanCoordinator::initializePrefetchRanges() {
  prefetchRanges_.clear();
  for (const auto& [begin, end] : rowRanges()) {
    auto next = begin;
    while (next < end) {
      const auto rows = capReadSize(end - next);
      BOLT_CHECK_GT(rows, 0);
      prefetchRanges_.push_back({.begin = next, .end = next + rows});
      next += rows;
    }
  }
  prefetchStatuses_.assign(prefetchRanges_.size(), FetchStatus::kNotStarted);
  prefetchBatons_.clear();
  prefetchBatons_.reserve(prefetchRanges_.size());
  for (size_t i = 0; i < prefetchRanges_.size(); ++i) {
    prefetchBatons_.push_back(std::make_shared<folly::Baton<>>());
  }
}

std::optional<std::vector<dwio::common::RowReader::PrefetchUnit>>
NativeLanceScanCoordinator::prefetchUnits() {
  std::vector<PrefetchUnit> units;
  units.reserve(prefetchRanges_.size());
  for (size_t rangeIndex = 0; rangeIndex < prefetchRanges_.size();
       ++rangeIndex) {
    const auto& range = prefetchRanges_[rangeIndex];
    units.push_back(
        {.rowCount = range.end - range.begin,
         .prefetch = std::bind(
             &NativeLanceScanCoordinator::prefetchRange, this, rangeIndex)});
  }
  return units;
}

bool NativeLanceScanCoordinator::allPrefetchIssued() const {
  // TableScan uses this hook to decide whether it may preload the next split.
  // Lance row prefetch units are optional and do not gate correctness of the
  // current split, so keep split-level preloading enabled like Parquet.
  return true;
}

dwio::common::RowReader::FetchResult NativeLanceScanCoordinator::prefetchRange(
    size_t rangeIndex) {
  BOLT_CHECK_LT(rangeIndex, prefetchRanges_.size());
  {
    std::lock_guard<std::mutex> lock(prefetchMutex_);
    auto& status = prefetchStatuses_[rangeIndex];
    if (status == FetchStatus::kFinished) {
      return FetchResult::kAlreadyFetched;
    }
    if (status == FetchStatus::kInProgress) {
      return FetchResult::kInProgress;
    }
    status = FetchStatus::kInProgress;
    prefetchBatons_[rangeIndex] = std::make_shared<folly::Baton<>>();
  }

  const auto range = prefetchRanges_[rangeIndex];
  try {
    // Prefetch advances the coordinator-owned scheduler. It must not fork an
    // independent decoder because page cursors and codec sessions are
    // scan-local state.
    std::lock_guard<std::mutex> decoderLock(decoderMutex_);
    const NativeLanceColumnRequest request{
        .rowStart = range.begin,
        .rowCount = range.end - range.begin,
        .purpose = NativeLanceDecodePurpose::kPrefetch};
    scanPlan_->rootColumnReader().planRead(decoder_, request);
  } catch (...) {
    std::shared_ptr<folly::Baton<>> baton;
    {
      std::lock_guard<std::mutex> lock(prefetchMutex_);
      prefetchStatuses_[rangeIndex] = FetchStatus::kNotStarted;
      baton = prefetchBatons_[rangeIndex];
    }
    baton->post();
    throw;
  }
  std::shared_ptr<folly::Baton<>> baton;
  {
    std::lock_guard<std::mutex> lock(prefetchMutex_);
    prefetchStatuses_[rangeIndex] = FetchStatus::kFinished;
    baton = prefetchBatons_[rangeIndex];
  }
  baton->post();
  return FetchResult::kFetched;
}

void NativeLanceScanCoordinator::markPrefetchRangesFinished(
    uint64_t begin,
    uint64_t end) {
  if (begin >= end) {
    return;
  }
  std::lock_guard<std::mutex> lock(prefetchMutex_);
  for (size_t i = 0; i < prefetchRanges_.size(); ++i) {
    const auto& range = prefetchRanges_[i];
    if (range.end > begin && range.end <= end) {
      if (prefetchStatuses_[i] == FetchStatus::kInProgress) {
        continue;
      }
      prefetchStatuses_[i] = FetchStatus::kFinished;
      prefetchBatons_[i]->post();
    }
  }
}

void NativeLanceScanCoordinator::prepareNextBatchPipeline(
    uint64_t readEnd,
    uint64_t requestedRows) {
  if (input_->supportSyncLoad()) {
    return;
  }
  uint64_t nextBegin = 0;
  uint64_t nextEnd = 0;
  if (readEnd < rowRanges()[currentRange_].second) {
    nextBegin = readEnd;
    nextEnd = rowRanges()[currentRange_].second;
  } else if (currentRange_ + 1 < rowRanges().size()) {
    nextBegin = rowRanges()[currentRange_ + 1].first;
    nextEnd = rowRanges()[currentRange_ + 1].second;
  } else {
    return;
  }
  const auto rows = capReadSize(std::min(requestedRows, nextEnd - nextBegin));
  if (rows == 0) {
    return;
  }
  const auto rangeIndex = prefetchRangeIndex(nextBegin, nextBegin + rows);
  if (!rangeIndex.has_value()) {
    return;
  }
  std::shared_ptr<folly::Baton<>> baton;
  {
    std::lock_guard<std::mutex> lock(prefetchMutex_);
    if (prefetchStatuses_[*rangeIndex] != FetchStatus::kNotStarted) {
      return;
    }
    prefetchStatuses_[*rangeIndex] = FetchStatus::kInProgress;
    baton = prefetchBatons_[*rangeIndex] = std::make_shared<folly::Baton<>>();
  }
  try {
    const NativeLanceColumnRequest request{
        .rowStart = nextBegin,
        .rowCount = rows,
        .purpose = NativeLanceDecodePurpose::kPrefetch};
    scanPlan_->rootColumnReader().planRead(decoder_, request);
    {
      std::lock_guard<std::mutex> lock(prefetchMutex_);
      prefetchStatuses_[*rangeIndex] = FetchStatus::kFinished;
    }
  } catch (...) {
    std::lock_guard<std::mutex> lock(prefetchMutex_);
    prefetchStatuses_[*rangeIndex] = FetchStatus::kNotStarted;
  }
  baton->post();
}

std::optional<size_t> NativeLanceScanCoordinator::prefetchRangeIndex(
    uint64_t begin,
    uint64_t end) const {
  for (size_t i = 0; i < prefetchRanges_.size(); ++i) {
    const auto& range = prefetchRanges_[i];
    if (range.begin <= begin && end <= range.end) {
      return i;
    }
  }
  return std::nullopt;
}

bool NativeLanceScanCoordinator::waitForPrefetchedRange(
    uint64_t begin,
    uint64_t end) {
  const auto index = prefetchRangeIndex(begin, end);
  if (!index.has_value()) {
    return false;
  }
  std::shared_ptr<folly::Baton<>> baton;
  {
    std::lock_guard<std::mutex> lock(prefetchMutex_);
    if (prefetchStatuses_[*index] == FetchStatus::kNotStarted) {
      return false;
    }
    baton = prefetchBatons_[*index];
  }
  baton->wait();
  std::lock_guard<std::mutex> lock(prefetchMutex_);
  return prefetchStatuses_[*index] == FetchStatus::kFinished;
}

void NativeLanceScanCoordinator::readFiltered(
    uint64_t readBegin,
    uint64_t readEnd,
    uint64_t requestedRows,
    const common::ScanSpec& scanSpec,
    const dwio::common::Mutation* mutation,
    VectorPtr& result) {
  const auto rowsToRead = readEnd - readBegin;
  auto filtered = decodeFilters(
      decoder_,
      *scanPlan_,
      fileContext_->metadata().rowType(),
      scanSpec,
      readBegin,
      static_cast<vector_size_t>(rowsToRead),
      fileContext_->pool(),
      mutation == nullptr ? nullptr : mutation->deletedRows);
  transition(
      NativeLanceScanState::kDecodingFilters,
      NativeLanceScanState::kDecodingValues);
  window_->filtersReady();
  const auto request = columnRequest(
      readBegin,
      static_cast<vector_size_t>(rowsToRead),
      filtered.selectedRows,
      NativeLanceDecodePurpose::kProjection);
  result = scanPlan_->rootColumnReader().read(
      decoder_,
      request,
      fileContext_->pool(),
      false,
      &filtered.predecodedColumns);
  result = applyScanSpecProjection(
      std::move(result), scanSpec, fileContext_->pool());
  prepareNextBatchPipeline(readEnd, requestedRows);
}

uint64_t NativeLanceScanCoordinator::next(
    uint64_t size,
    VectorPtr& result,
    const dwio::common::Mutation* mutation) {
  if (size == 0) {
    return 0;
  }
  advancePastFinishedRange();
  {
    std::lock_guard<std::mutex> lock(stateMutex_);
    if (state_ == NativeLanceScanState::kFinished ||
        currentRange_ >= rowRanges().size()) {
      state_ = NativeLanceScanState::kFinished;
      return 0;
    }
    if (state_ == NativeLanceScanState::kFailed) {
      rethrowFailure();
    }
    BOLT_CHECK(
        state_ != NativeLanceScanState::kCancelled,
        "Cannot read from a cancelled native Lance scan");
    BOLT_CHECK(
        state_ == NativeLanceScanState::kIdle,
        "A native Lance scan already has an active operation");
    state_ = NativeLanceScanState::kPlanningWindow;
    window_.emplace(++generation_, currentRow_, size);
  }

  try {
    const auto& scanSpec = options_.getScanSpec();
    const auto decodeState = scanSpec && scanSpec->hasFilter()
        ? NativeLanceScanState::kDecodingFilters
        : NativeLanceScanState::kDecodingValues;
    transition(NativeLanceScanState::kPlanningWindow, decodeState);
    window_->beginDecode(scanSpec && scanSpec->hasFilter());
    VectorPtr decoded;
    const auto rows = nextImpl(size, decoded, mutation);
    if (rows == 0) {
      std::lock_guard<std::mutex> lock(stateMutex_);
      window_->cancel();
      window_.reset();
      state_ = NativeLanceScanState::kFinished;
      return 0;
    }
    transition(
        NativeLanceScanState::kDecodingValues,
        NativeLanceScanState::kAssemblingBatch);
    window_->beginAssembly();
    window_->publish(rows, std::move(decoded));
    transition(
        NativeLanceScanState::kAssemblingBatch,
        NativeLanceScanState::kOutputReady);
    transition(
        NativeLanceScanState::kOutputReady,
        NativeLanceScanState::kDrainingOutput);
    const auto drainedRows = window_->drain(result);
    BOLT_CHECK_EQ(drainedRows, rows);
    window_.reset();
    const auto finished = nextRowNumber() == kAtEnd;
    {
      std::lock_guard<std::mutex> lock(stateMutex_);
      BOLT_CHECK(state_ == NativeLanceScanState::kDrainingOutput);
      state_ = finished ? NativeLanceScanState::kFinished
                        : NativeLanceScanState::kIdle;
    }
    return rows;
  } catch (...) {
    fail(std::current_exception());
    throw;
  }
}

uint64_t NativeLanceScanCoordinator::nextImpl(
    uint64_t size,
    VectorPtr& result,
    const dwio::common::Mutation* mutation) {
  advancePastFinishedRange();
  if (size == 0 || currentRange_ >= rowRanges().size()) {
    return 0;
  }

  const auto rowsToRead = capReadSize(
      std::min(size, rowRanges()[currentRange_].second - currentRow_));
  const auto decodeStart = std::chrono::steady_clock::now();
  BOLT_CHECK_LE(
      rowsToRead,
      static_cast<uint64_t>(std::numeric_limits<vector_size_t>::max()));
  const auto readBegin = currentRow_;
  const auto readEnd = readBegin + rowsToRead;
  const auto primaryRangesPlanned = waitForPrefetchedRange(readBegin, readEnd);
  std::lock_guard<std::mutex> decoderLock(decoderMutex_);
  const auto& scanSpec = options_.getScanSpec();
  if (FOLLY_UNLIKELY(scanSpec && scanSpec->hasFilter())) {
    readFiltered(readBegin, readEnd, size, *scanSpec, mutation, result);
    currentRow_ += rowsToRead;
    markPrefetchRangesFinished(readBegin, currentRow_);
    advancePastFinishedRange();
    ++batchesRead_;
    const auto decodeTimeNs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - decodeStart)
            .count();
    decodeTimeNs_ += decodeTimeNs;
    if (const auto& report = options_.getDecodingTimeMsCallback()) {
      report(decodeTimeNs / 1'000'000);
    }
    if (rowsToRead > 0 && result != nullptr) {
      estimatedBytesPerRow_ = std::max<uint64_t>(
          estimatedBytesPerRow_,
          (result->retainedSize() + rowsToRead - 1) / rowsToRead);
    }
    return rowsToRead;
  }
  RowSet selectedRows{
      memory::StlAllocator<vector_size_t>(&fileContext_->pool())};
  NativeLanceColumnRequest request{
      .rowStart = currentRow_,
      .rowCount = rowsToRead,
      .purpose = NativeLanceDecodePurpose::kProjection};
  if (mutation != nullptr) {
    initializeSelectedRows(
        selectedRows,
        static_cast<vector_size_t>(rowsToRead),
        mutation->deletedRows);
    request.selection =
        selectionForRows(selectedRows, static_cast<vector_size_t>(rowsToRead));
  }
  NativeLanceColumnReadTask columnTask(
      scanPlan_->rootColumnReader(), request, primaryRangesPlanned);
  if (!primaryRangesPlanned) {
    columnTask.plan(decoder_);
  }
  columnTask.decode(decoder_, fileContext_->pool());
  result = columnTask.consume();
  prepareNextBatchPipeline(readEnd, size);
  if (scanSpec) {
    result = applyScanSpecProjection(
        std::move(result), *scanSpec, fileContext_->pool());
  }
  currentRow_ += rowsToRead;
  markPrefetchRangesFinished(readBegin, currentRow_);
  advancePastFinishedRange();
  ++batchesRead_;
  const auto decodeTimeNs =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - decodeStart)
          .count();
  decodeTimeNs_ += decodeTimeNs;
  if (const auto& report = options_.getDecodingTimeMsCallback()) {
    report(decodeTimeNs / 1'000'000);
  }
  if (rowsToRead > 0 && result != nullptr) {
    estimatedBytesPerRow_ = std::max<uint64_t>(
        estimatedBytesPerRow_,
        (result->retainedSize() + rowsToRead - 1) / rowsToRead);
  }
  return rowsToRead;
}

uint64_t NativeLanceScanCoordinator::skip(uint64_t skipSize) {
  {
    std::lock_guard<std::mutex> lock(stateMutex_);
    if (state_ == NativeLanceScanState::kFinished) {
      return 0;
    }
    if (state_ == NativeLanceScanState::kFailed) {
      rethrowFailure();
    }
    BOLT_CHECK(
        state_ != NativeLanceScanState::kCancelled,
        "Cannot skip a cancelled native Lance scan");
    BOLT_CHECK(
        state_ == NativeLanceScanState::kIdle,
        "A native Lance scan already has an active operation");
    state_ = NativeLanceScanState::kSkipping;
  }

  try {
    uint64_t skipped = 0;
    while (skipSize > 0) {
      advancePastFinishedRange();
      if (currentRange_ >= rowRanges().size()) {
        break;
      }
      const auto inRange =
          std::min(skipSize, rowRanges()[currentRange_].second - currentRow_);
      const auto skipBegin = currentRow_;
      currentRow_ += inRange;
      markPrefetchRangesFinished(skipBegin, currentRow_);
      skipped += inRange;
      skipSize -= inRange;
    }
    advancePastFinishedRange();
    {
      std::lock_guard<std::mutex> lock(stateMutex_);
      BOLT_CHECK(state_ == NativeLanceScanState::kSkipping);
      state_ = currentRange_ >= rowRanges().size()
          ? NativeLanceScanState::kFinished
          : NativeLanceScanState::kIdle;
    }
    return skipped;
  } catch (...) {
    fail(std::current_exception());
    throw;
  }
}

void NativeLanceScanCoordinator::updateRuntimeStats(
    dwio::common::RuntimeStatistics& stats) const {
  stats.processedStrides += batchesRead_;
  stats.decodeTimeNs += decodeTimeNs_;
}

void NativeLanceScanCoordinator::resetFilterCaches() {}

std::optional<size_t> NativeLanceScanCoordinator::estimatedRowSize() const {
  return estimatedBytesPerRow_;
}

NativeLanceRowReader::NativeLanceRowReader(
    std::shared_ptr<const NativeLanceFileContext> fileContext,
    dwio::common::RowReaderOptions options)
    : coordinator_(std::make_unique<NativeLanceScanCoordinator>(
          std::move(fileContext),
          std::move(options))) {}

NativeLanceRowReader::~NativeLanceRowReader() = default;

int64_t NativeLanceRowReader::nextRowNumber() {
  return coordinator_->nextRowNumber();
}

int64_t NativeLanceRowReader::nextReadSize(uint64_t size) {
  return coordinator_->nextReadSize(size);
}

uint64_t NativeLanceRowReader::next(
    uint64_t size,
    VectorPtr& result,
    const dwio::common::Mutation* mutation) {
  return coordinator_->next(size, result, mutation);
}

uint64_t NativeLanceRowReader::skip(uint64_t skipSize) {
  return coordinator_->skip(skipSize);
}

void NativeLanceRowReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& stats) const {
  coordinator_->updateRuntimeStats(stats);
}

void NativeLanceRowReader::resetFilterCaches() {
  coordinator_->resetFilterCaches();
}

std::optional<size_t> NativeLanceRowReader::estimatedRowSize() const {
  return coordinator_->estimatedRowSize();
}

bool NativeLanceRowReader::allPrefetchIssued() const {
  return coordinator_->allPrefetchIssued();
}

std::optional<std::vector<dwio::common::RowReader::PrefetchUnit>>
NativeLanceRowReader::prefetchUnits() {
  return coordinator_->prefetchUnits();
}

NativeLanceReader::NativeLanceReader(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options,
    std::shared_ptr<const NativeLanceBlobResolver> blobResolver)
    : NativeLanceReader(
          std::move(input),
          options,
          std::move(blobResolver),
          defaultNativeLanceTypeAdapter()) {}

NativeLanceReader::NativeLanceReader(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options,
    std::shared_ptr<const NativeLanceBlobResolver> blobResolver,
    std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter)
    : fileContext_(openNativeLanceFile(
          std::move(input),
          options,
          std::move(blobResolver),
          std::move(typeAdapter))) {}

std::optional<uint64_t> NativeLanceReader::numberOfRows() const {
  return fileContext_->metadata().numRows();
}

const RowTypePtr& NativeLanceReader::rowType() const {
  return fileContext_->metadata().rowType();
}

const std::shared_ptr<const dwio::common::TypeWithId>&
NativeLanceReader::typeWithId() const {
  return fileContext_->typeWithId();
}

size_t NativeLanceReader::loadedColumnMetadataCount() const {
  return fileContext_->metadata().loadedColumnMetadataCount();
}

std::unique_ptr<dwio::common::RowReader> NativeLanceReader::createRowReader(
    const dwio::common::RowReaderOptions& options) const {
  return std::make_unique<NativeLanceRowReader>(fileContext_, options);
}

std::unique_ptr<dwio::common::ColumnStatistics>
NativeLanceReader::columnStatistics(uint32_t index) const {
  const auto& typeWithId = fileContext_->typeWithId();
  const auto& metadata = fileContext_->metadata();
  uint32_t firstPhysical = 0;
  uint32_t physicalCount = metadata.numPhysicalColumns();
  if (index != typeWithId->id()) {
    const auto child = std::find_if(
        typeWithId->getChildren().begin(),
        typeWithId->getChildren().end(),
        [index](const auto& candidate) { return candidate->id() == index; });
    if (child == typeWithId->getChildren().end()) {
      return nullptr;
    }
    const auto logicalColumn = (*child)->column();
    firstPhysical = metadata.physicalColumnIndex(logicalColumn);
    physicalCount = metadata.usesStructuralEncoding()
        ? metadata.structuralField(logicalColumn).physicalColumnCount
        : metadata.physicalColumnSpan(firstPhysical);
  }

  uint64_t storageBytes = 0;
  for (uint32_t physical = firstPhysical;
       physical < firstPhysical + physicalCount;
       ++physical) {
    const auto& column = metadata.column(physical);
    for (const auto& page : column.pages()) {
      for (const auto bytes : page.buffer_sizes()) {
        BOLT_CHECK_LE(
            storageBytes, std::numeric_limits<uint64_t>::max() - bytes);
        storageBytes += bytes;
      }
    }
  }
  return std::make_unique<dwio::common::ColumnStatistics>(
      std::nullopt, std::nullopt, std::nullopt, storageBytes);
}

std::unique_ptr<dwio::common::Reader> NativeLanceReaderFactory::createReader(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options) {
  return std::make_unique<NativeLanceReader>(
      std::move(input), options, blobResolver_, typeAdapter_);
}

} // namespace bytedance::bolt::lance::reader
