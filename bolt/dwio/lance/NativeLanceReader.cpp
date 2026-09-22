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
#include "bolt/type/Filter.h"
#include "bolt/vector/ComplexVector.h"

namespace bytedance::bolt::lance::reader {
namespace {

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

NativeLanceReadPlan::Options makeReadPlanOptions(
    const dwio::common::ReaderOptions& options) {
  BOLT_CHECK_GT(options.loadQuantum(), 0);
  BOLT_CHECK_GT(options.maxCoalesceBytes(), 0);
  return {
      .maxReadBytes = static_cast<uint64_t>(options.loadQuantum()),
      .maxInFlightBytes = static_cast<uint64_t>(options.maxCoalesceBytes())};
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

uint64_t estimateTypeBytesPerRow(const TypePtr& type) {
  constexpr uint64_t kNullOverhead = 1;
  switch (type->kind()) {
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return sizeof(StringView) + 16 + kNullOverhead;
    case TypeKind::ARRAY:
      return 2 * sizeof(vector_size_t) +
          estimateTypeBytesPerRow(type->childAt(0)) + kNullOverhead;
    case TypeKind::MAP:
      return 2 * sizeof(vector_size_t) +
          estimateTypeBytesPerRow(type->childAt(0)) +
          estimateTypeBytesPerRow(type->childAt(1)) + kNullOverhead;
    case TypeKind::ROW: {
      uint64_t size = kNullOverhead;
      for (uint32_t i = 0; i < type->size(); ++i) {
        size += estimateTypeBytesPerRow(type->childAt(i));
      }
      return size;
    }
    case TypeKind::UNKNOWN:
      return kNullOverhead;
    default:
      return type->cppSizeInBytes() + kNullOverhead;
  }
}

uint64_t estimateReadBytesPerRow(
    const RowTypePtr& fileType,
    const dwio::common::RowReaderOptions& options) {
  uint64_t estimate = 0;
  if (const auto& scanSpec = options.getScanSpec()) {
    for (const auto& child : scanSpec->children()) {
      if (!child->isConstant()) {
        estimate +=
            estimateTypeBytesPerRow(fileType->findChild(child->fieldName()));
      }
    }
  } else if (const auto& selector = options.getSelector()) {
    const auto selectedType = selector->buildSelectedReordered();
    for (const auto& type : selectedType->children()) {
      estimate += estimateTypeBytesPerRow(type);
    }
  } else {
    estimate = estimateTypeBytesPerRow(fileType);
  }
  return std::max<uint64_t>(estimate, 1);
}

VectorPtr readSelective(
    NativeLanceDecoder& decoder,
    const RowTypePtr& fileType,
    const common::ScanSpec& scanSpec,
    uint64_t batchRowStart,
    vector_size_t batchSize,
    memory::MemoryPool& pool,
    const uint64_t* deletedRows = nullptr) {
  using RowSet =
      std::vector<vector_size_t, memory::StlAllocator<vector_size_t>>;
  RowSet selectedRows(batchSize, memory::StlAllocator<vector_size_t>(&pool));
  if (deletedRows == nullptr) {
    std::iota(selectedRows.begin(), selectedRows.end(), 0);
  } else {
    auto output = selectedRows.begin();
    for (vector_size_t row = 0; row < batchSize; ++row) {
      if (!bits::isBitSet(deletedRows, row)) {
        *output++ = row;
      }
    }
    selectedRows.erase(output, selectedRows.end());
  }
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
    auto values = child->isConstant()
        ? BaseVector::wrapInConstant(
              selectedRows.size(), 0, child->constantValue())
        : decoder.decodeSelectedRows(*columnIndex, batchRowStart, selectedRows);
    const auto inputRows = selectedRows;
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
          *columnIndex, DecodedFilterColumn(inputRows, std::move(values)));
    }
    if (selectedRows.empty()) {
      break;
    }
  }

  column_index_t numOutputColumns = 0;
  for (const auto& child : scanSpec.children()) {
    if (child->projectOut()) {
      BOLT_CHECK_NE(child->channel(), common::ScanSpec::kNoChannel);
      numOutputColumns = std::max(numOutputColumns, child->channel() + 1);
    }
  }
  std::vector<std::string> names(numOutputColumns);
  std::vector<TypePtr> types(numOutputColumns);
  std::vector<VectorPtr> children(numOutputColumns);
  for (const auto& child : scanSpec.children()) {
    if (!child->projectOut()) {
      continue;
    }
    const auto channel = child->channel();
    names[channel] = child->fieldName();
    if (child->isConstant()) {
      types[channel] = child->constantValue()->type();
      children[channel] = BaseVector::wrapInConstant(
          selectedRows.size(), 0, child->constantValue());
    } else {
      const auto columnIndex = fileType->getChildIdx(child->fieldName());
      types[channel] = fileType->childAt(columnIndex);
      const auto decoded = decodedFilterColumns.find(columnIndex);
      if (decoded == decodedFilterColumns.end()) {
        children[channel] = decoder.decodeSelectedRows(
            columnIndex, batchRowStart, selectedRows);
      } else {
        auto indices = allocateIndices(selectedRows.size(), &pool);
        auto* rawIndices = indices->asMutable<vector_size_t>();
        size_t sourceIndex = 0;
        for (size_t i = 0; i < selectedRows.size(); ++i) {
          while (sourceIndex < decoded->second.rows.size() &&
                 decoded->second.rows[sourceIndex] < selectedRows[i]) {
            ++sourceIndex;
          }
          BOLT_CHECK_LT(sourceIndex, decoded->second.rows.size());
          BOLT_CHECK_EQ(decoded->second.rows[sourceIndex], selectedRows[i]);
          rawIndices[i] = static_cast<vector_size_t>(sourceIndex);
        }
        children[channel] = BaseVector::wrapInDictionary(
            nullptr,
            std::move(indices),
            selectedRows.size(),
            decoded->second.values);
      }
    }
  }
  return std::make_shared<RowVector>(
      &pool,
      ROW(std::move(names), std::move(types)),
      nullptr,
      static_cast<vector_size_t>(selectedRows.size()),
      std::move(children));
}

VectorPtr applyDeletedRows(
    VectorPtr result,
    const uint64_t* deletedRows,
    vector_size_t batchSize,
    memory::MemoryPool& pool) {
  if (deletedRows == nullptr) {
    return result;
  }
  vector_size_t outputSize = 0;
  for (vector_size_t row = 0; row < batchSize; ++row) {
    outputSize += !bits::isBitSet(deletedRows, row);
  }
  if (outputSize == batchSize) {
    return result;
  }
  auto indices = allocateIndices(outputSize, &pool);
  auto* rawIndices = indices->asMutable<vector_size_t>();
  vector_size_t output = 0;
  for (vector_size_t row = 0; row < batchSize; ++row) {
    if (!bits::isBitSet(deletedRows, row)) {
      rawIndices[output++] = row;
    }
  }
  const auto* rows = result->as<RowVector>();
  BOLT_CHECK_NOT_NULL(rows);
  std::vector<VectorPtr> children;
  children.reserve(rows->childrenSize());
  for (const auto& child : rows->children()) {
    children.push_back(
        BaseVector::wrapInDictionary(nullptr, indices, outputSize, child));
  }
  return std::make_shared<RowVector>(
      &pool, result->type(), nullptr, outputSize, std::move(children));
}

} // namespace

NativeLanceReaderBase::NativeLanceReaderBase(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options,
    std::shared_ptr<const NativeLanceBlobResolver> blobResolver)
    : NativeLanceReaderBase(
          std::move(input),
          options,
          std::move(blobResolver),
          defaultNativeLanceTypeAdapter()) {}

NativeLanceReaderBase::NativeLanceReaderBase(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options,
    std::shared_ptr<const NativeLanceBlobResolver> blobResolver,
    std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter)
    : pool_(options.getMemoryPool()),
      input_(std::move(input)),
      metadata_(*input_, pool_, std::move(typeAdapter)),
      typeWithId_(dwio::common::TypeWithId::create(metadata_.rowType())),
      blobResolver_(
          blobResolver == nullptr
              ? nullptr
              : std::make_shared<CachingNativeLanceBlobResolver>(
                    std::move(blobResolver))),
      readPlanOptions_(makeReadPlanOptions(options)),
      decodedPageCache_(std::make_shared<NativeLanceDecodedPageCache>(
          NativeLanceDecodedPageCache::kDefaultMaxBytes)) {
  BOLT_CHECK(
      !options.isFileColumnNamesReadAsLowerCase(),
      "The Lance format does not support reading column names as lowercase");
}

NativeLanceRowReader::NativeLanceRowReader(
    std::shared_ptr<NativeLanceReaderBase> readerBase,
    dwio::common::RowReaderOptions options)
    : readerBase_(std::move(readerBase)),
      options_(std::move(options)),
      decoder_(
          readerBase_->input(),
          readerBase_->metadata(),
          readerBase_->pool(),
          false,
          readerBase_->blobResolver(),
          readerBase_->readPlanOptions(),
          readerBase_->decodedPageCache()) {
  maxBatchBytes_ = options_.getMaxBatchBytes();
  estimatedBytesPerRow_ =
      estimateReadBytesPerRow(readerBase_->metadata().rowType(), options_);
  rootColumnReader_ = NativeLanceStructColumnReader::buildRoot(
      readerBase_->metadata().rowType(), options_);
  auto requiredColumns = rootColumnReader_->fileColumnIndices();
  if (const auto& scanSpec = options_.getScanSpec()) {
    for (const auto& child : scanSpec->children()) {
      if (!child->isConstant() && child->hasFilter()) {
        requiredColumns.push_back(
            readerBase_->metadata().rowType()->getChildIdx(child->fieldName()));
      }
    }
  }
  std::sort(requiredColumns.begin(), requiredColumns.end());
  requiredColumns.erase(
      std::unique(requiredColumns.begin(), requiredColumns.end()),
      requiredColumns.end());
  readerBase_->metadata().loadLogicalColumns(requiredColumns);
  rowRanges_ = readerBase_->metadata().rowRangesForFileRange(
      options_.getOffset(), options_.getLimit());
  currentRow_ = rowRanges_.empty() ? 0 : rowRanges_.front().first;
  initializePrefetchRanges();
}

NativeLanceRowReader::~NativeLanceRowReader() {
  decoder_.cancelReadPlan();
  std::lock_guard<std::mutex> lock(prefetchMutex_);
  if (pipeline_.has_value()) {
    pipeline_->decoder->cancelReadPlan();
    pipeline_.reset();
  }
  for (auto& decoder : prefetchDecoders_) {
    if (decoder != nullptr) {
      decoder->cancelReadPlan();
    }
  }
}

void NativeLanceRowReader::advancePastFinishedRange() {
  while (currentRange_ < rowRanges_.size() &&
         currentRow_ >= rowRanges_[currentRange_].second) {
    ++currentRange_;
    if (currentRange_ < rowRanges_.size()) {
      currentRow_ = rowRanges_[currentRange_].first;
    }
  }
}

int64_t NativeLanceRowReader::nextRowNumber() {
  advancePastFinishedRange();
  return currentRange_ >= rowRanges_.size() ? kAtEnd
                                            : static_cast<int64_t>(currentRow_);
}

int64_t NativeLanceRowReader::nextReadSize(uint64_t size) {
  BOLT_CHECK_GT(size, 0);
  advancePastFinishedRange();
  if (currentRange_ >= rowRanges_.size()) {
    return kAtEnd;
  }
  return static_cast<int64_t>(capReadSize(
      std::min(size, rowRanges_[currentRange_].second - currentRow_)));
}

uint64_t NativeLanceRowReader::capReadSize(uint64_t size) const {
  if (maxBatchBytes_ <= 0 || estimatedBytesPerRow_ == 0) {
    return size;
  }
  return std::min<uint64_t>(
      size, std::max<int64_t>(1, maxBatchBytes_ / estimatedBytesPerRow_));
}

void NativeLanceRowReader::initializePrefetchRanges() {
  prefetchRanges_.clear();
  for (const auto& [begin, end] : rowRanges_) {
    auto next = begin;
    while (next < end) {
      const auto rows = capReadSize(end - next);
      BOLT_CHECK_GT(rows, 0);
      prefetchRanges_.push_back({.begin = next, .end = next + rows});
      next += rows;
    }
  }
  prefetchStatuses_.assign(prefetchRanges_.size(), FetchStatus::kNotStarted);
  prefetchInputs_.resize(prefetchRanges_.size());
  prefetchDecoders_.resize(prefetchRanges_.size());
  prefetchBatons_.clear();
  prefetchBatons_.reserve(prefetchRanges_.size());
  for (size_t i = 0; i < prefetchRanges_.size(); ++i) {
    prefetchBatons_.push_back(std::make_shared<folly::Baton<>>());
  }
}

std::optional<std::vector<dwio::common::RowReader::PrefetchUnit>>
NativeLanceRowReader::prefetchUnits() {
  std::vector<PrefetchUnit> units;
  units.reserve(prefetchRanges_.size());
  for (size_t rangeIndex = 0; rangeIndex < prefetchRanges_.size();
       ++rangeIndex) {
    const auto& range = prefetchRanges_[rangeIndex];
    units.push_back(
        {.rowCount = range.end - range.begin,
         .prefetch = std::bind(
             &NativeLanceRowReader::prefetchRange, this, rangeIndex)});
  }
  return units;
}

bool NativeLanceRowReader::allPrefetchIssued() const {
  // TableScan uses this hook to decide whether it may preload the next split.
  // Lance row prefetch units are optional and do not gate correctness of the
  // current split, so keep split-level preloading enabled like Parquet.
  return true;
}

dwio::common::RowReader::FetchResult NativeLanceRowReader::prefetchRange(
    size_t rangeIndex) {
  BOLT_CHECK_LT(rangeIndex, prefetchRanges_.size());
  {
    std::lock_guard<std::mutex> lock(decoderMutex_);
    const auto& range = prefetchRanges_[rangeIndex];
    if (pipeline_.has_value() && pipeline_->begin == range.begin &&
        pipeline_->end == range.end) {
      return FetchResult::kAlreadyFetched;
    }
  }
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
  {
    std::lock_guard<std::mutex> prefetchLock(prefetchMutex_);
    if (prefetchDecoders_[rangeIndex] == nullptr) {
      prefetchInputs_[rangeIndex] = readerBase_->input().clone();
      prefetchDecoders_[rangeIndex] = std::make_unique<NativeLanceDecoder>(
          *prefetchInputs_[rangeIndex],
          readerBase_->metadata(),
          readerBase_->pool(),
          true,
          readerBase_->blobResolver(),
          readerBase_->readPlanOptions(),
          readerBase_->decodedPageCache());
    }
  }

  try {
    rootColumnReader_->planRead(
        *prefetchDecoders_[rangeIndex], range.begin, range.end - range.begin);
  } catch (...) {
    std::shared_ptr<folly::Baton<>> baton;
    {
      std::lock_guard<std::mutex> lock(prefetchMutex_);
      prefetchStatuses_[rangeIndex] = FetchStatus::kNotStarted;
      prefetchDecoders_[rangeIndex]->cancelReadPlan();
      prefetchDecoders_[rangeIndex].reset();
      prefetchInputs_[rangeIndex].reset();
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

void NativeLanceRowReader::markPrefetchRangesFinished(
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
      if (prefetchDecoders_[i] != nullptr) {
        prefetchDecoders_[i]->cancelReadPlan();
      }
      prefetchDecoders_[i].reset();
      prefetchInputs_[i].reset();
      prefetchBatons_[i]->post();
    }
  }
}

void NativeLanceRowReader::prepareNextBatchPipeline(
    uint64_t readEnd,
    uint64_t requestedRows) {
  if (readerBase_->input().supportSyncLoad()) {
    return;
  }
  uint64_t nextBegin = 0;
  uint64_t nextEnd = 0;
  if (readEnd < rowRanges_[currentRange_].second) {
    nextBegin = readEnd;
    nextEnd = rowRanges_[currentRange_].second;
  } else if (currentRange_ + 1 < rowRanges_.size()) {
    nextBegin = rowRanges_[currentRange_ + 1].first;
    nextEnd = rowRanges_[currentRange_ + 1].second;
  } else {
    return;
  }
  const auto rows = capReadSize(std::min(requestedRows, nextEnd - nextBegin));
  if (rows == 0) {
    return;
  }
  if (const auto existing = prefetchRangeIndex(nextBegin, nextBegin + rows);
      existing.has_value()) {
    std::lock_guard<std::mutex> lock(prefetchMutex_);
    if (prefetchStatuses_[*existing] != FetchStatus::kNotStarted) {
      return;
    }
  }
  auto input = readerBase_->input().clone();
  auto decoder = std::make_unique<NativeLanceDecoder>(
      *input,
      readerBase_->metadata(),
      readerBase_->pool(),
      true,
      readerBase_->blobResolver(),
      readerBase_->readPlanOptions(),
      readerBase_->decodedPageCache());
  try {
    rootColumnReader_->planRead(*decoder, nextBegin, rows);
    pipeline_ = PipelineState{
        .begin = nextBegin,
        .end = nextBegin + rows,
        .input = std::move(input),
        .decoder = std::move(decoder)};
  } catch (...) {
    decoder->cancelReadPlan();
  }
}

std::optional<NativeLanceRowReader::PipelineState>
NativeLanceRowReader::takePipeline(uint64_t readBegin, uint64_t readEnd) {
  if (!pipeline_.has_value()) {
    return std::nullopt;
  }
  if (pipeline_->begin == readBegin && pipeline_->end == readEnd) {
    auto result = std::move(pipeline_);
    pipeline_.reset();
    return result;
  }
  pipeline_->decoder->cancelReadPlan();
  pipeline_.reset();
  return std::nullopt;
}

std::optional<size_t> NativeLanceRowReader::prefetchRangeIndex(
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

NativeLanceDecoder* NativeLanceRowReader::prefetchedDecoderForRange(
    uint64_t begin,
    uint64_t end) {
  const auto index = prefetchRangeIndex(begin, end);
  if (!index.has_value()) {
    return nullptr;
  }
  std::shared_ptr<folly::Baton<>> baton;
  {
    std::lock_guard<std::mutex> lock(prefetchMutex_);
    if (prefetchStatuses_[*index] == FetchStatus::kNotStarted) {
      return nullptr;
    }
    baton = prefetchBatons_[*index];
  }
  baton->wait();
  std::lock_guard<std::mutex> lock(prefetchMutex_);
  if (prefetchStatuses_[*index] == FetchStatus::kFinished &&
      prefetchDecoders_[*index] != nullptr) {
    return prefetchDecoders_[*index].get();
  }
  return nullptr;
}

uint64_t NativeLanceRowReader::next(
    uint64_t size,
    VectorPtr& result,
    const dwio::common::Mutation* mutation) {
  advancePastFinishedRange();
  if (size == 0 || currentRange_ >= rowRanges_.size()) {
    return 0;
  }

  const auto rowsToRead = capReadSize(
      std::min(size, rowRanges_[currentRange_].second - currentRow_));
  const auto decodeStart = std::chrono::steady_clock::now();
  BOLT_CHECK_LE(
      rowsToRead,
      static_cast<uint64_t>(std::numeric_limits<vector_size_t>::max()));
  const auto readBegin = currentRow_;
  const auto readEnd = readBegin + rowsToRead;
  const auto& fileType = readerBase_->metadata().rowType();
  std::lock_guard<std::mutex> decoderLock(decoderMutex_);
  auto pipeline = takePipeline(readBegin, readEnd);
  NativeLanceDecoder* const decoder = pipeline.has_value()
      ? pipeline->decoder.get()
      : prefetchedDecoderForRange(readBegin, readEnd);
  prepareNextBatchPipeline(readEnd, size);
  auto& readDecoder = decoder == nullptr ? decoder_ : *decoder;
  if (const auto& scanSpec = options_.getScanSpec();
      scanSpec && scanSpec->hasFilter()) {
    result = readSelective(
        readDecoder,
        fileType,
        *scanSpec,
        currentRow_,
        static_cast<vector_size_t>(rowsToRead),
        readerBase_->pool(),
        mutation == nullptr ? nullptr : mutation->deletedRows);
  } else {
    result = rootColumnReader_->read(
        readDecoder, currentRow_, rowsToRead, readerBase_->pool());
    result = applyDeletedRows(
        std::move(result),
        mutation == nullptr ? nullptr : mutation->deletedRows,
        static_cast<vector_size_t>(rowsToRead),
        readerBase_->pool());
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

uint64_t NativeLanceRowReader::skip(uint64_t skipSize) {
  uint64_t skipped = 0;
  while (skipSize > 0) {
    advancePastFinishedRange();
    if (currentRange_ >= rowRanges_.size()) {
      break;
    }
    const auto inRange =
        std::min(skipSize, rowRanges_[currentRange_].second - currentRow_);
    const auto skipBegin = currentRow_;
    currentRow_ += inRange;
    markPrefetchRangesFinished(skipBegin, currentRow_);
    skipped += inRange;
    skipSize -= inRange;
  }
  advancePastFinishedRange();
  return skipped;
}

void NativeLanceRowReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& stats) const {
  stats.processedStrides += batchesRead_;
  stats.decodeTimeNs += decodeTimeNs_;
}

void NativeLanceRowReader::resetFilterCaches() {}

std::optional<size_t> NativeLanceRowReader::estimatedRowSize() const {
  return estimatedBytesPerRow_;
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
    : readerBase_(std::make_shared<NativeLanceReaderBase>(
          std::move(input),
          options,
          std::move(blobResolver),
          std::move(typeAdapter))) {}

std::optional<uint64_t> NativeLanceReader::numberOfRows() const {
  return readerBase_->metadata().numRows();
}

const RowTypePtr& NativeLanceReader::rowType() const {
  return readerBase_->metadata().rowType();
}

const std::shared_ptr<const dwio::common::TypeWithId>&
NativeLanceReader::typeWithId() const {
  return readerBase_->typeWithId();
}

NativeLanceMetadata::DebugStats NativeLanceReader::debugStats() const {
  return readerBase_->metadata().debugStats();
}

size_t NativeLanceReader::loadedColumnMetadataCount() const {
  return readerBase_->metadata().loadedColumnMetadataCount();
}

std::unique_ptr<dwio::common::RowReader> NativeLanceReader::createRowReader(
    const dwio::common::RowReaderOptions& options) const {
  return std::make_unique<NativeLanceRowReader>(readerBase_, options);
}

std::unique_ptr<dwio::common::ColumnStatistics>
NativeLanceReader::columnStatistics(uint32_t index) const {
  const auto& typeWithId = readerBase_->typeWithId();
  const auto& metadata = readerBase_->metadata();
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
