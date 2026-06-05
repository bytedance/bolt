#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/AllocateSize.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/VectorTypeUtils.h"

#include <algorithm>
#include <cstring>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <string_view>

namespace bytedance::bolt::exec {
namespace {

template <TypeKind Kind>
int32_t kindSize() {
  return sizeof(typename KindToFlatVector<Kind>::HashRowType);
}

int32_t typeKindSize(TypeKind kind) {
  if (kind == TypeKind::UNKNOWN) {
    return sizeof(UnknownValue);
  }
  if (kind == TypeKind::VARCHAR || kind == TypeKind::VARBINARY) {
    return sizeof(VarData);
  }
  return BOLT_DYNAMIC_TYPE_DISPATCH(kindSize, kind);
}

void clearBit(char* bits, uint32_t idx) {
  auto bitsAs8Bit = reinterpret_cast<uint8_t*>(bits);
  bitsAs8Bit[idx / 8] &= ~(1 << (idx % 8));
}

bool isNullAt(const char* row, int32_t nullByte, uint8_t nullMask) {
  return (row[nullByte] & nullMask) != 0;
}

template <typename T>
int32_t comparePrimitiveAsc(const T& left, const T& right) {
  if constexpr (std::is_floating_point_v<T>) {
    const bool leftNan = std::isnan(left);
    const bool rightNan = std::isnan(right);
    if (leftNan) {
      return rightNan ? 0 : 1;
    }
    if (rightNan) {
      return -1;
    }
  }
  return left < right ? -1 : left == right ? 0 : 1;
}

int32_t compareStringAsc(StringView left, StringView right) {
  const auto result = std::string_view(left.data(), left.size())
                          .compare(std::string_view(right.data(), right.size()));
  return result < 0 ? -1 : result > 0 ? 1 : 0;
}

class PinnedRows {
 public:
  std::vector<memory::bm::BufferHandle>& handles() {
    return handles_;
  }

 private:
  std::vector<memory::bm::BufferHandle> handles_;
};

} // namespace

BmRowContainer::BmRowContainer(
    std::vector<TypePtr> keyTypes,
    std::vector<TypePtr> dependentTypes,
    std::shared_ptr<memory::bm::BufferManager> bufferManager,
    memory::bm::MemoryTag tag,
    uint32_t rowBlockSize,
    uint32_t heapBlockSize)
    : keyTypes_(std::move(keyTypes)),
      dependentTypes_(std::move(dependentTypes)),
      bufferManager_(std::move(bufferManager)),
      tag_(tag),
      rowBlockSize_(rowBlockSize == 0
              ? memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)
              : rowBlockSize),
      heapBlockSize_(heapBlockSize == 0
              ? memory::bm::allocateSizeBytes(memory::bm::AllocateSize::kLarge)
              : heapBlockSize) {
  if (!bufferManager_) {
    throw std::invalid_argument("BmRowContainer requires BufferManager");
  }
  computeLayout();
  BOLT_CHECK_GT(fixedRowSize_, 0);
  BOLT_CHECK_LE(fixedRowSize_, rowBlockSize_);
}

BmRowContainer::~BmRowContainer() {
  rowBlocks_.clear();
  if (bufferManager_) {
    bufferManager_->ReleaseUnusedReservation();
  }
}

void BmRowContainer::computeLayout() {
  int32_t offset = 0;
  int32_t nullOffset = 0;
  bool isVariableWidth = false;
  for (auto& type : keyTypes_) {
    types_.push_back(type);
    typeKinds_.push_back(type->kind());
    offsets_.push_back(offset);
    offset += typeKindSize(type->kind());
    nullOffsets_.push_back(nullOffset++);
    isVariableWidth |= !type->isFixedWidth();
  }

  offset = std::max<int32_t>(offset, sizeof(void*));
  const int32_t firstNullByteOffset = offset;

  for (auto& type : dependentTypes_) {
    types_.push_back(type);
    typeKinds_.push_back(type->kind());
    nullOffsets_.push_back(nullOffset++);
    isVariableWidth |= !type->isFixedWidth();
  }

  nullOffsets_.push_back(nullOffset);
  freeFlagOffset_ = nullOffset + firstNullByteOffset * 8;
  ++nullOffset;

  for (auto& null : nullOffsets_) {
    null += firstNullByteOffset * 8;
  }

  const int32_t nullBytes = bits::nbytes(nullOffsets_.size());
  offset += nullBytes;

  for (auto& type : dependentTypes_) {
    offsets_.push_back(offset);
    offset += typeKindSize(type->kind());
  }

  if (isVariableWidth) {
    rowSizeOffset_ = offset;
    offset += sizeof(uint32_t);
  }

  fixedRowSize_ = bits::roundUp(offset, alignment_);
  if (!nullOffsets_.empty()) {
    initialNulls_.resize(nullBytes, 0x0);
  }

  for (auto i = 0; i < offsets_.size(); ++i) {
    rowColumns_.emplace_back(offsets_[i], nullOffsets_[i]);
  }
}

RowId BmRowContainer::newRow() {
  auto& block = ensureWritableRowBlock();
  const auto rowOffset = bits::roundUp(block.usedBytes, alignment_);
  BOLT_CHECK_LE(rowOffset + fixedRowSize_, rowBlockSize_);
  auto* row = block.pinnedHandle->Ptr() + rowOffset;
  initializeRow(row);
  block.usedBytes = rowOffset + fixedRowSize_;
  ++block.liveRows;
  ++numRows_;
  return RowId{activeRowBlockId_, static_cast<uint32_t>(rowOffset)};
}

void BmRowContainer::store(
    const DecodedVector& decoded,
    vector_size_t index,
    RowId row,
    int32_t column) {
  BOLT_CHECK_GE(column, 0);
  BOLT_CHECK_LT(column, typeKinds_.size());
  auto* rowPtr = mutableRow(row);
  const auto rowColumn = rowColumns_[column];
  storeDispatch(typeKinds_[column], decoded, index, rowPtr, rowColumn);
}

std::vector<RowId> BmRowContainer::store(const RowVectorPtr& input) {
  BOLT_CHECK_NOT_NULL(input);
  BOLT_CHECK_EQ(input->childrenSize(), types_.size());

  std::vector<DecodedVector> decoded;
  decoded.reserve(types_.size());
  for (auto i = 0; i < types_.size(); ++i) {
    BOLT_CHECK(input->childAt(i)->type()->equivalent(*types_[i]));
    decoded.emplace_back(*input->childAt(i));
  }

  std::vector<RowId> rows;
  rows.reserve(input->size());
  for (auto rowIndex = 0; rowIndex < input->size(); ++rowIndex) {
    auto row = newRow();
    for (auto column = 0; column < decoded.size(); ++column) {
      store(decoded[column], rowIndex, row, column);
    }
    rows.push_back(row);
  }
  return rows;
}

void BmRowContainer::extractColumn(
    const RowId* rows,
    int32_t numRows,
    int32_t column,
    const VectorPtr& result,
    bool exactSize) {
  extractColumn(
      folly::Range<const RowId*>(rows, numRows),
      column,
      0,
      result,
      exactSize);
}

void BmRowContainer::extractColumn(
    folly::Range<const RowId*> rows,
    int32_t column,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) {
  BOLT_CHECK_GE(column, 0);
  BOLT_CHECK_LT(column, rowColumns_.size());
  BOLT_CHECK_LE(resultOffset + rows.size(), result->size());

  PinnedRows pinnedRows;
  std::vector<const char*> rowPtrs;
  rowPtrs.reserve(rows.size());
  for (auto row : rows) {
    rowPtrs.push_back(pinRow(row, pinnedRows.handles()));
  }
  extractDispatch(
      typeKinds_[column],
      rowPtrs.data(),
      rows.size(),
      rowColumns_[column],
      result,
      resultOffset,
      exactSize);
}

uint64_t BmRowContainer::allocatedBytes() const {
  return static_cast<uint64_t>(rowBlocks_.size()) * rowBlockSize_ +
      static_cast<uint64_t>(heapBlocks_.size()) * heapBlockSize_;
}

uint64_t BmRowContainer::usedBytes() const {
  uint64_t bytes = 0;
  for (const auto& block : rowBlocks_) {
    bytes += block.usedBytes;
  }
  for (const auto& block : heapBlocks_) {
    bytes += block.usedBytes;
  }
  return bytes;
}

uint64_t BmRowContainer::heapAllocatedBytes() const {
  return static_cast<uint64_t>(heapBlocks_.size()) * heapBlockSize_;
}

std::optional<int64_t> BmRowContainer::estimateRowSize() const {
  if (numRows_ == 0) {
    return std::nullopt;
  }
  return static_cast<int64_t>(usedBytes() / numRows_);
}

void BmRowContainer::clear() {
  rowBlocks_.clear();
  heapBlocks_.clear();
  activeRowBlockId_ = 0;
  activeHeapBlockId_ = 0;
  numRows_ = 0;
  if (bufferManager_) {
    bufferManager_->ReleaseUnusedReservation();
  }
}

void BmRowContainer::spillAllBlocksForBenchmark() {
  releaseColdPins();
  auto blocks = coldBlocks();
  if (!blocks.empty()) {
    bufferManager_->SpillBlocks(blocks);
  }
}

void BmRowContainer::extractNulls(
    const RowId* rows,
    int32_t numRows,
    int32_t column,
    const BufferPtr& result) {
  extractNulls(folly::Range<const RowId*>(rows, numRows), column, result);
}

void BmRowContainer::extractNulls(
    folly::Range<const RowId*> rows,
    int32_t column,
    const BufferPtr& result) {
  BOLT_CHECK_GE(column, 0);
  BOLT_CHECK_LT(column, rowColumns_.size());
  BOLT_CHECK_GE(result->size(), bits::nbytes(rows.size()));

  auto* rawResult = result->asMutable<uint64_t>();
  bits::fillBits(rawResult, 0, rows.size(), false);
  const auto rowColumn = rowColumns_[column];

  PinnedRows pinnedRows;
  for (auto i = 0; i < rows.size(); ++i) {
    const auto* row = pinRow(rows[i], pinnedRows.handles());
    if (isNullAt(row, rowColumn.nullByte(), rowColumn.nullMask())) {
      bits::setBit(rawResult, i, true);
    }
  }
}

int32_t BmRowContainer::compare(
    RowId left,
    RowId right,
    int32_t column,
    CompareFlags flags) {
  BOLT_CHECK_GE(column, 0);
  BOLT_CHECK_LT(column, rowColumns_.size());

  std::vector<memory::bm::BufferHandle> pins;
  const auto* leftRow = pinRow(left, pins);
  const auto* rightRow = pinRow(right, pins);
  return compareDispatch(
      typeKinds_[column],
      leftRow,
      rightRow,
      rowColumns_[column],
      pins,
      flags);
}

int32_t BmRowContainer::compareRows(
    RowId left,
    RowId right,
    const std::vector<CompareFlags>& flags) {
  BOLT_CHECK(flags.empty() || flags.size() == keyTypes_.size());
  for (auto i = 0; i < keyTypes_.size(); ++i) {
    const auto result =
        compare(left, right, i, flags.empty() ? CompareFlags() : flags[i]);
    if (result != 0) {
      return result;
    }
  }
  return 0;
}

BmRowContainer::BmBlockState& BmRowContainer::ensureWritableRowBlock() {
  if (rowBlocks_.empty() || !hasRowCapacity(rowBlocks_[activeRowBlockId_])) {
    reserveNewBlock(StorageBlockKind::kRow);
  }
  return rowBlocks_[activeRowBlockId_];
}

bool BmRowContainer::hasRowCapacity(const BmBlockState& block) const {
  const auto rowOffset = bits::roundUp(block.usedBytes, alignment_);
  return rowOffset + fixedRowSize_ <= rowBlockSize_;
}

char* BmRowContainer::mutableRow(RowId row) {
  BOLT_CHECK_LT(row.rowBlockId, rowBlocks_.size());
  auto& block = rowBlocks_[row.rowBlockId];
  BOLT_CHECK(block.pinnedHandle.has_value());
  BOLT_CHECK_LE(row.rowOffset + fixedRowSize_, block.usedBytes);
  return block.pinnedHandle->Ptr() + row.rowOffset;
}

const char* BmRowContainer::pinRow(
    RowId row,
    std::vector<memory::bm::BufferHandle>& pins) {
  BOLT_CHECK_LT(row.rowBlockId, rowBlocks_.size());
  auto& block = rowBlocks_[row.rowBlockId];
  BOLT_CHECK_LE(row.rowOffset + fixedRowSize_, block.usedBytes);
  if (block.pinnedHandle.has_value()) {
    return block.pinnedHandle->Ptr() + row.rowOffset;
  }
  auto handle = bufferManager_->Pin(block.block);
  const auto* ptr = handle.Ptr() + row.rowOffset;
  pins.push_back(std::move(handle));
  return ptr;
}

StringView BmRowContainer::stringView(
    const char* row,
    BmRowColumn column,
    std::vector<memory::bm::BufferHandle>& pins) {
  const auto ref = *reinterpret_cast<const VarData*>(row + column.offset());
  if (ref.size == 0) {
    return StringView("", 0);
  }
  BOLT_CHECK_LT(ref.heapBlockId, heapBlocks_.size());
  auto& block = heapBlocks_[ref.heapBlockId];
  BOLT_CHECK_LE(ref.heapOffset + ref.size, block.usedBytes);
  if (block.pinnedHandle.has_value()) {
    return StringView(block.pinnedHandle->Ptr() + ref.heapOffset, ref.size);
  }

  auto handle = bufferManager_->Pin(block.block);
  const auto* ptr = handle.Ptr() + ref.heapOffset;
  pins.push_back(std::move(handle));
  return StringView(ptr, ref.size);
}

char* BmRowContainer::initializeRow(char* row) {
  std::memset(row, 0, fixedRowSize_);
  if (!initialNulls_.empty()) {
    std::memcpy(
        row + BmRowColumn::nullByte(nullOffsets_[0]),
        initialNulls_.data(),
        initialNulls_.size());
  }
  if (rowSizeOffset_) {
    *reinterpret_cast<uint32_t*>(row + rowSizeOffset_) = 0;
  }
  clearBit(row, freeFlagOffset_);
  return row;
}

void BmRowContainer::reserveNewBlock(StorageBlockKind kind) {
  auto& blocks =
      kind == StorageBlockKind::kRow ? rowBlocks_ : heapBlocks_;
  auto& activeBlockId =
      kind == StorageBlockKind::kRow ? activeRowBlockId_ : activeHeapBlockId_;
  const auto blockSize =
      kind == StorageBlockKind::kRow ? rowBlockSize_ : heapBlockSize_;
  const auto* failureMessage = kind == StorageBlockKind::kRow
      ? "BmRowContainer cannot reserve a new row block"
      : "BmRowContainer cannot reserve a new heap block";

  if (!bufferManager_->MaybeReserve(blockSize)) {
    releaseColdPins();
    auto blocksToSpill = coldBlocks();
    if (!blocksToSpill.empty()) {
      bufferManager_->SpillBlocks(blocksToSpill);
    }
    BOLT_CHECK(bufferManager_->MaybeReserve(blockSize), failureMessage);
  }

  auto handle = bufferManager_->Allocate(blockSize, tag_);
  bufferManager_->ReleaseUnusedReservation();
  BmBlockState state;
  state.block = handle.block();
  state.pinnedHandle.emplace(std::move(handle));
  blocks.push_back(std::move(state));
  activeBlockId = blocks.size() - 1;
}

VarData BmRowContainer::appendVariableWidth(StringView value) {
  BOLT_CHECK_LE(value.size(), heapBlockSize_);
  if (value.size() == 0) {
    return VarData{};
  }
  if (heapBlocks_.empty() ||
      heapBlocks_[activeHeapBlockId_].usedBytes + value.size() > heapBlockSize_) {
    reserveNewBlock(StorageBlockKind::kHeap);
  }

  auto& block = heapBlocks_[activeHeapBlockId_];
  BOLT_CHECK(block.pinnedHandle.has_value());
  const auto offset = block.usedBytes;
  std::memcpy(block.pinnedHandle->Ptr() + offset, value.data(), value.size());
  block.usedBytes += value.size();
  return VarData{
      activeHeapBlockId_, static_cast<uint32_t>(offset), static_cast<uint32_t>(value.size())};
}

void BmRowContainer::releaseColdPins() {
  if (rowBlocks_.empty()) {
    return;
  }
  for (uint32_t i = 0; i < rowBlocks_.size(); ++i) {
    if (i == activeRowBlockId_) {
      continue;
    }
    rowBlocks_[i].pinnedHandle.reset();
  }
  for (uint32_t i = 0; i < heapBlocks_.size(); ++i) {
    if (i == activeHeapBlockId_) {
      continue;
    }
    heapBlocks_[i].pinnedHandle.reset();
  }
}

std::vector<std::shared_ptr<memory::bm::BlockHandle>>
BmRowContainer::coldBlocks() const {
  std::vector<std::shared_ptr<memory::bm::BlockHandle>> blocks;
  blocks.reserve(rowBlocks_.size());
  for (uint32_t i = 0; i < rowBlocks_.size(); ++i) {
    const auto& block = rowBlocks_[i];
    if (i != activeRowBlockId_ && !block.pinnedHandle.has_value()) {
      blocks.push_back(block.block);
    }
  }
  for (uint32_t i = 0; i < heapBlocks_.size(); ++i) {
    const auto& block = heapBlocks_[i];
    if (i != activeHeapBlockId_ && !block.pinnedHandle.has_value()) {
      blocks.push_back(block.block);
    }
  }
  return blocks;
}

void BmRowContainer::storeDispatch(
    TypeKind kind,
    const DecodedVector& decoded,
    vector_size_t index,
    char* row,
    BmRowColumn column) {
  switch (kind) {
    case TypeKind::BOOLEAN:
      return storeWithNulls<TypeKind::BOOLEAN>(
          decoded, index, row, column.offset(), column.nullByte(), column.nullMask());
    case TypeKind::TINYINT:
      return storeWithNulls<TypeKind::TINYINT>(
          decoded, index, row, column.offset(), column.nullByte(), column.nullMask());
    case TypeKind::SMALLINT:
      return storeWithNulls<TypeKind::SMALLINT>(
          decoded, index, row, column.offset(), column.nullByte(), column.nullMask());
    case TypeKind::INTEGER:
      return storeWithNulls<TypeKind::INTEGER>(
          decoded, index, row, column.offset(), column.nullByte(), column.nullMask());
    case TypeKind::BIGINT:
      return storeWithNulls<TypeKind::BIGINT>(
          decoded, index, row, column.offset(), column.nullByte(), column.nullMask());
    case TypeKind::HUGEINT:
      return storeWithNulls<TypeKind::HUGEINT>(
          decoded, index, row, column.offset(), column.nullByte(), column.nullMask());
    case TypeKind::REAL:
      return storeWithNulls<TypeKind::REAL>(
          decoded, index, row, column.offset(), column.nullByte(), column.nullMask());
    case TypeKind::DOUBLE:
      return storeWithNulls<TypeKind::DOUBLE>(
          decoded, index, row, column.offset(), column.nullByte(), column.nullMask());
    case TypeKind::TIMESTAMP:
      return storeWithNulls<TypeKind::TIMESTAMP>(
          decoded, index, row, column.offset(), column.nullByte(), column.nullMask());
    case TypeKind::VARCHAR:
      return storeWithNulls<TypeKind::VARCHAR>(
          decoded, index, row, column.offset(), column.nullByte(), column.nullMask());
    case TypeKind::VARBINARY:
      return storeWithNulls<TypeKind::VARBINARY>(
          decoded, index, row, column.offset(), column.nullByte(), column.nullMask());
    case TypeKind::VARIANT:
      return storeWithNulls<TypeKind::VARIANT>(
          decoded, index, row, column.offset(), column.nullByte(), column.nullMask());
    default:
      BOLT_NYI(
          "BmRowContainer store does not support type {} yet",
          mapTypeKindToName(kind));
  }
}

void BmRowContainer::extractDispatch(
    TypeKind kind,
    const char* const* rows,
    int32_t numRows,
      BmRowColumn column,
      const VectorPtr& result,
      vector_size_t resultOffset,
      bool exactSize) {
  switch (kind) {
    case TypeKind::BOOLEAN:
      return extractColumnTyped<TypeKind::BOOLEAN>(
          rows, numRows, column, result, resultOffset, exactSize);
    case TypeKind::TINYINT:
      return extractColumnTyped<TypeKind::TINYINT>(
          rows, numRows, column, result, resultOffset, exactSize);
    case TypeKind::SMALLINT:
      return extractColumnTyped<TypeKind::SMALLINT>(
          rows, numRows, column, result, resultOffset, exactSize);
    case TypeKind::INTEGER:
      return extractColumnTyped<TypeKind::INTEGER>(
          rows, numRows, column, result, resultOffset, exactSize);
    case TypeKind::BIGINT:
      return extractColumnTyped<TypeKind::BIGINT>(
          rows, numRows, column, result, resultOffset, exactSize);
    case TypeKind::HUGEINT:
      return extractColumnTyped<TypeKind::HUGEINT>(
          rows, numRows, column, result, resultOffset, exactSize);
    case TypeKind::REAL:
      return extractColumnTyped<TypeKind::REAL>(
          rows, numRows, column, result, resultOffset, exactSize);
    case TypeKind::DOUBLE:
      return extractColumnTyped<TypeKind::DOUBLE>(
          rows, numRows, column, result, resultOffset, exactSize);
    case TypeKind::TIMESTAMP:
      return extractColumnTyped<TypeKind::TIMESTAMP>(
          rows, numRows, column, result, resultOffset, exactSize);
    case TypeKind::VARCHAR:
      return extractColumnTyped<TypeKind::VARCHAR>(
          rows, numRows, column, result, resultOffset, exactSize);
    case TypeKind::VARBINARY:
      return extractColumnTyped<TypeKind::VARBINARY>(
          rows, numRows, column, result, resultOffset, exactSize);
    case TypeKind::VARIANT:
      return extractColumnTyped<TypeKind::VARIANT>(
          rows, numRows, column, result, resultOffset, exactSize);
    default:
      BOLT_NYI(
          "BmRowContainer extract does not support type {} yet",
          mapTypeKindToName(kind));
  }
}

int32_t BmRowContainer::compareDispatch(
    TypeKind kind,
    const char* left,
    const char* right,
    BmRowColumn column,
    std::vector<memory::bm::BufferHandle>& pins,
    CompareFlags flags) {
  switch (kind) {
    case TypeKind::BOOLEAN:
      return compareTyped<TypeKind::BOOLEAN>(left, right, column, pins, flags);
    case TypeKind::TINYINT:
      return compareTyped<TypeKind::TINYINT>(left, right, column, pins, flags);
    case TypeKind::SMALLINT:
      return compareTyped<TypeKind::SMALLINT>(left, right, column, pins, flags);
    case TypeKind::INTEGER:
      return compareTyped<TypeKind::INTEGER>(left, right, column, pins, flags);
    case TypeKind::BIGINT:
      return compareTyped<TypeKind::BIGINT>(left, right, column, pins, flags);
    case TypeKind::HUGEINT:
      return compareTyped<TypeKind::HUGEINT>(left, right, column, pins, flags);
    case TypeKind::REAL:
      return compareTyped<TypeKind::REAL>(left, right, column, pins, flags);
    case TypeKind::DOUBLE:
      return compareTyped<TypeKind::DOUBLE>(left, right, column, pins, flags);
    case TypeKind::TIMESTAMP:
      return compareTyped<TypeKind::TIMESTAMP>(left, right, column, pins, flags);
    case TypeKind::VARCHAR:
      return compareTyped<TypeKind::VARCHAR>(left, right, column, pins, flags);
    case TypeKind::VARBINARY:
      return compareTyped<TypeKind::VARBINARY>(left, right, column, pins, flags);
    default:
      BOLT_NYI(
          "BmRowContainer compare does not support type {} yet",
          mapTypeKindToName(kind));
  }
}

template <TypeKind Kind>
void BmRowContainer::storeWithNulls(
    const DecodedVector& decoded,
    vector_size_t index,
    char* row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  using T = typename TypeTraits<Kind>::NativeType;
  if (decoded.isNullAt(index)) {
    row[nullByte] |= nullMask;
    if constexpr (std::is_arithmetic_v<T>) {
      *reinterpret_cast<T*>(row + offset) = std::numeric_limits<T>::max();
    } else if constexpr (std::is_same_v<T, StringView>) {
      *reinterpret_cast<VarData*>(row + offset) = VarData{};
    } else {
      *reinterpret_cast<T*>(row + offset) = T();
    }
    return;
  }

  row[nullByte] &= ~nullMask;
  if constexpr (std::is_same_v<T, StringView>) {
    *reinterpret_cast<VarData*>(row + offset) =
        appendVariableWidth(decoded.valueAt<StringView>(index));
  } else {
    *reinterpret_cast<T*>(row + offset) = decoded.valueAt<T>(index);
  }
}

template <TypeKind Kind>
void BmRowContainer::extractColumnTyped(
    const char* const* rows,
    int32_t numRows,
    BmRowColumn column,
    const VectorPtr& result,
    vector_size_t resultOffset,
    bool exactSize) {
  using T = typename TypeTraits<Kind>::NativeType;
  auto* flatResult = result->asFlatVector<T>();
  BOLT_CHECK_NOT_NULL(flatResult);
  if constexpr (std::is_same_v<T, StringView>) {
    PinnedRows pinnedRows;
    for (int32_t i = 0; i < numRows; ++i) {
      const auto* row = rows[i];
      const auto isNull =
          row == nullptr || isNullAt(row, column.nullByte(), column.nullMask());
      flatResult->setNull(resultOffset + i, isNull);
      if (isNull) {
        continue;
      }
      flatResult->setStringViewValue(
          resultOffset + i,
          stringView(row, column, pinnedRows.handles()),
          exactSize);
    }
  } else {
    for (int32_t i = 0; i < numRows; ++i) {
      const auto* row = rows[i];
      const auto isNull =
          row == nullptr || isNullAt(row, column.nullByte(), column.nullMask());
      flatResult->setNull(resultOffset + i, isNull);
      if (isNull) {
        continue;
      }
      flatResult->set(
          resultOffset + i, *reinterpret_cast<const T*>(row + column.offset()));
    }
  }
}

template <TypeKind Kind>
int32_t BmRowContainer::compareTyped(
    const char* left,
    const char* right,
    BmRowColumn column,
    std::vector<memory::bm::BufferHandle>& pins,
    CompareFlags flags) {
  using T = typename TypeTraits<Kind>::NativeType;
  const bool leftNull = isNullAt(left, column.nullByte(), column.nullMask());
  const bool rightNull = isNullAt(right, column.nullByte(), column.nullMask());
  if (leftNull) {
    return rightNull ? 0 : flags.nullsFirst ? -1 : 1;
  }
  if (rightNull) {
    return flags.nullsFirst ? 1 : -1;
  }

  int32_t result;
  if constexpr (std::is_same_v<T, StringView>) {
    result = compareStringAsc(
        stringView(left, column, pins), stringView(right, column, pins));
  } else {
    result = comparePrimitiveAsc(
        *reinterpret_cast<const T*>(left + column.offset()),
        *reinterpret_cast<const T*>(right + column.offset()));
  }
  return flags.ascending ? result : -result;
}

} // namespace bytedance::bolt::exec
