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
      blocks_(bufferManager_, tag),
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
  blocks_.clear();
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
  auto* row = blocks_.activeData(activeRowBlockId_) + rowOffset;
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

bool BmRowContainer::tryStore(const RowVectorPtr& input) {
  BOLT_CHECK_NOT_NULL(input);
  BOLT_CHECK_EQ(input->childrenSize(), types_.size());
  const auto variableBytes = rowSizeOffset_ == 0 ? 0 : input->estimateFlatSize();

  const auto rowsPerBlock = rowBlockSize_ / fixedRowSize_;
  BOLT_CHECK_GT(rowsPerBlock, 0);
  uint64_t availableRows = 0;
  if (activeRowBlockId_ != std::numeric_limits<uint32_t>::max()) {
    const auto& block = blocks_.block(activeRowBlockId_);
    const auto rowOffset = bits::roundUp(block.usedBytes, alignment_);
    if (rowOffset < rowBlockSize_) {
      availableRows = (rowBlockSize_ - rowOffset) / fixedRowSize_;
    }
  }

  const auto remainingRows =
      input->size() > availableRows ? input->size() - availableRows : 0;
  const auto rowBlocks =
      (remainingRows + rowsPerBlock - 1) / rowsPerBlock;

  uint64_t heapBlocks = 0;
  uint64_t remainingHeapBytes = 0;
  if (activeHeapBlockId_ != std::numeric_limits<uint32_t>::max()) {
    const auto& block = blocks_.block(activeHeapBlockId_);
    BOLT_CHECK_LE(block.usedBytes, heapBlockSize_);
    remainingHeapBytes = heapBlockSize_ - block.usedBytes;
  }
  if (variableBytes > remainingHeapBytes) {
    const auto newHeapBytes = variableBytes - remainingHeapBytes;
    heapBlocks = (newHeapBytes + heapBlockSize_ - 1) / heapBlockSize_;
  }

  const auto bytesToReserve =
      rowBlocks * rowBlockSize_ + heapBlocks * heapBlockSize_;
  if (bytesToReserve == 0) {
    return true;
  }
  const auto canReserve = bufferManager_->MaybeReserve(bytesToReserve);
  bufferManager_->ReleaseUnusedReservation();
  return canReserve;
}

void BmRowContainer::preload(std::vector<BlockId>& blockIds) {
  blocks_.pinBlocks(
      blockIds,
      [this](uint32_t candidateBlockId) {
        return canReclaimBlock(candidateBlockId);
      });
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

  switch (typeKinds_[column]) {
    // Complex types still use the generic path, which currently reports NYI.
    // The fast path below is only valid for flat fixed-width and string types.
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
    case TypeKind::VARIANT:
      break;
    default:
      extractColumnFast(
          typeKinds_[column],
          rows,
          rowColumns_[column],
          resultOffset,
          result,
          exactSize);
      return;
  }

  std::vector<const char*> rowPtrs;
  rowPtrs.reserve(rows.size());
  for (auto row : rows) {
    rowPtrs.push_back(pinRow(row));
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
  return blocks_.allocatedBytes();
}

uint64_t BmRowContainer::usedBytes() const {
  return blocks_.usedBytes();
}

uint64_t BmRowContainer::heapAllocatedBytes() const {
  uint64_t bytes = 0;
  for (auto blockId : heapBlockIds_) {
    bytes += blocks_.block(blockId).capacity;
  }
  return bytes;
}

std::optional<int64_t> BmRowContainer::estimateRowSize() const {
  if (numRows_ == 0) {
    return std::nullopt;
  }
  return static_cast<int64_t>(usedBytes() / numRows_);
}

void BmRowContainer::clear() {
  blocks_.clear();
  heapBlockIds_.clear();
  activeRowBlockId_ = std::numeric_limits<uint32_t>::max();
  activeHeapBlockId_ = std::numeric_limits<uint32_t>::max();
  numRows_ = 0;
}

void BmRowContainer::spillAllBlocksForBenchmark() {
  blocks_.spillReclaimableBlocks(
      0, [this](uint32_t blockId) { return canReclaimBlock(blockId); });
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

  for (auto i = 0; i < rows.size(); ++i) {
    const auto* row = pinRow(rows[i]);
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

  const auto* leftRow = pinRow(left);
  const auto* rightRow = pinRow(right);
  return compareDispatch(
      typeKinds_[column],
      leftRow,
      rightRow,
      rowColumns_[column],
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

BmBlockState& BmRowContainer::ensureWritableRowBlock() {
  if (activeRowBlockId_ == std::numeric_limits<uint32_t>::max() ||
      !hasRowCapacity(blocks_.block(activeRowBlockId_))) {
    activeRowBlockId_ = allocateBlockAfterPressure(
        rowBlockSize_, "BmRowContainer cannot allocate a new row block");
  }
  return blocks_.block(activeRowBlockId_);
}

bool BmRowContainer::hasRowCapacity(const BmBlockState& block) const {
  const auto rowOffset = bits::roundUp(block.usedBytes, alignment_);
  return rowOffset + fixedRowSize_ <= rowBlockSize_;
}

char* BmRowContainer::mutableRow(RowId row) {
  auto& block = blocks_.block(row.blockId);
  BOLT_CHECK_LE(row.rowOffset + fixedRowSize_, block.usedBytes);
  return blocks_.activeData(row.blockId) + row.rowOffset;
}

const char* BmRowContainer::pinRow(RowId row) {
  auto& block = blocks_.block(row.blockId);
  BOLT_CHECK_LE(row.rowOffset + fixedRowSize_, block.usedBytes);
  return pinnedBlockDataAfterPressure(
             row.blockId, "BmRowContainer cannot pin a row block") +
      row.rowOffset;
}

StringView BmRowContainer::stringView(const char* row, BmRowColumn column) {
  const auto ref = *reinterpret_cast<const VarData*>(row + column.offset());
  if (ref.size == 0) {
    return StringView("", 0);
  }
  auto& block = blocks_.block(ref.blockId);
  BOLT_CHECK_LE(ref.offset + ref.size, block.usedBytes);
  return StringView(
      pinnedBlockDataAfterPressure(
          ref.blockId, "BmRowContainer cannot pin a heap block") +
          ref.offset,
      ref.size);
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

VarData BmRowContainer::appendVariableWidth(StringView value) {
  BOLT_CHECK_LE(value.size(), heapBlockSize_);
  if (value.size() == 0) {
    return VarData{};
  }
  if (activeHeapBlockId_ == std::numeric_limits<uint32_t>::max() ||
      blocks_.block(activeHeapBlockId_).usedBytes + value.size() >
          heapBlockSize_) {
    activeHeapBlockId_ = allocateBlockAfterPressure(
        heapBlockSize_, "BmRowContainer cannot allocate a new heap block");
    heapBlockIds_.push_back(activeHeapBlockId_);
  }

  auto& block = blocks_.block(activeHeapBlockId_);
  const auto offset = block.usedBytes;
  std::memcpy(
      blocks_.activeData(activeHeapBlockId_) + offset,
      value.data(),
      value.size());
  block.usedBytes += value.size();
  return VarData{
      activeHeapBlockId_,
      static_cast<uint32_t>(offset),
      static_cast<uint32_t>(value.size())};
}

uint32_t BmRowContainer::allocateBlockAfterPressure(
    uint32_t capacity,
    const char* failureMessage) {
  auto blockId = blocks_.tryAllocateBlock(capacity);
  if (blockId.has_value()) {
    return blockId.value();
  }

  blocks_.spillReclaimableBlocks(
      0, [this](uint32_t candidateBlockId) {
        return canReclaimBlock(candidateBlockId);
      });
  blockId = blocks_.tryAllocateBlock(capacity);
  BOLT_CHECK(blockId.has_value(), failureMessage);
  return blockId.value();
}

const char* BmRowContainer::pinnedBlockDataAfterPressure(
    uint32_t blockId,
    const char* failureMessage) {
  if (const auto* data = blocks_.tryPinnedData(blockId)) {
    return data;
  }

  blocks_.spillReclaimableBlocks(
      0, [this, blockId](uint32_t candidateBlockId) {
        return candidateBlockId != blockId && canReclaimBlock(candidateBlockId);
      });
  const auto* data = blocks_.tryPinnedData(blockId);
  BOLT_CHECK_NOT_NULL(data, failureMessage);
  return data;
}

bool BmRowContainer::canReclaimBlock(uint32_t blockId) const {
  return blockId != activeRowBlockId_ && blockId != activeHeapBlockId_;
}

void BmRowContainer::storeDispatch(
    TypeKind kind,
    const DecodedVector& decoded,
    vector_size_t index,
    char* row,
    BmRowColumn column) {
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      storeWithNulls,
      kind,
      decoded,
      index,
      row,
      column.offset(),
      column.nullByte(),
      column.nullMask());
}

void BmRowContainer::extractDispatch(
    TypeKind kind,
    const char* const* rows,
    int32_t numRows,
      BmRowColumn column,
      const VectorPtr& result,
      vector_size_t resultOffset,
      bool exactSize) {
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnTyped,
      kind,
      rows,
      numRows,
      column,
      result,
      resultOffset,
      exactSize);
}

void BmRowContainer::extractColumnFast(
    TypeKind kind,
    folly::Range<const RowId*> rows,
    BmRowColumn column,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) {
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      extractColumnFastTyped,
      kind,
      rows,
      column,
      resultOffset,
      result,
      exactSize);
}

int32_t BmRowContainer::compareDispatch(
    TypeKind kind,
    const char* left,
    const char* right,
    BmRowColumn column,
    CompareFlags flags) {
  return BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      compareTyped, kind, left, right, column, flags);
}

template <TypeKind Kind>
void BmRowContainer::storeWithNulls(
    const DecodedVector& decoded,
    vector_size_t index,
    char* row,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  if constexpr (
      Kind == TypeKind::UNKNOWN || Kind == TypeKind::OPAQUE ||
      Kind == TypeKind::ARRAY || Kind == TypeKind::MAP ||
      Kind == TypeKind::ROW) {
    BOLT_NYI(
        "BmRowContainer store does not support type {} yet",
        mapTypeKindToName(Kind));
  } else {
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
}

template <TypeKind Kind>
void BmRowContainer::extractColumnTyped(
    const char* const* rows,
    int32_t numRows,
    BmRowColumn column,
    const VectorPtr& result,
    vector_size_t resultOffset,
    bool exactSize) {
  if constexpr (
      Kind == TypeKind::UNKNOWN || Kind == TypeKind::OPAQUE ||
      Kind == TypeKind::ARRAY || Kind == TypeKind::MAP ||
      Kind == TypeKind::ROW) {
    BOLT_NYI(
        "BmRowContainer extract does not support type {} yet",
        mapTypeKindToName(Kind));
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    auto* flatResult = result->asFlatVector<T>();
    BOLT_CHECK_NOT_NULL(flatResult);
    if constexpr (std::is_same_v<T, StringView>) {
      for (int32_t i = 0; i < numRows; ++i) {
        const auto* row = rows[i];
        const auto isNull = row == nullptr ||
            isNullAt(row, column.nullByte(), column.nullMask());
        flatResult->setNull(resultOffset + i, isNull);
        if (isNull) {
          continue;
        }
        flatResult->setStringViewValue(
            resultOffset + i,
            stringView(row, column),
            exactSize);
      }
    } else {
      for (int32_t i = 0; i < numRows; ++i) {
        const auto* row = rows[i];
        const auto isNull = row == nullptr ||
            isNullAt(row, column.nullByte(), column.nullMask());
        flatResult->setNull(resultOffset + i, isNull);
        if (isNull) {
          continue;
        }
        flatResult->set(
            resultOffset + i,
            *reinterpret_cast<const T*>(row + column.offset()));
      }
    }
  }
}

template <TypeKind Kind>
void BmRowContainer::extractColumnFastTyped(
    folly::Range<const RowId*> rows,
    BmRowColumn column,
    vector_size_t resultOffset,
    const VectorPtr& result,
    bool exactSize) {
  if constexpr (
      Kind == TypeKind::UNKNOWN || Kind == TypeKind::OPAQUE ||
      Kind == TypeKind::ARRAY || Kind == TypeKind::MAP ||
      Kind == TypeKind::ROW || Kind == TypeKind::VARIANT) {
    BOLT_NYI(
        "BmRowContainer fast extract does not support type {} yet",
        mapTypeKindToName(Kind));
  } else {
    using T = typename TypeTraits<Kind>::NativeType;
    auto* flatResult = result->asFlatVector<T>();
    BOLT_CHECK_NOT_NULL(flatResult);
    if constexpr (std::is_same_v<T, StringView>) {
      constexpr auto kInvalidBlockId = std::numeric_limits<uint32_t>::max();
      uint32_t rowBlockId = kInvalidBlockId;
      const char* rowBlockData = nullptr;
      uint32_t heapBlockId = kInvalidBlockId;
      const char* heapBlockData = nullptr;

      for (auto i = 0; i < rows.size(); ++i) {
        const auto row = rows[i];
        if (row.blockId != rowBlockId) {
          auto& block = blocks_.block(row.blockId);
          BOLT_CHECK_LE(row.rowOffset + fixedRowSize_, block.usedBytes);
          rowBlockData = pinnedBlockDataAfterPressure(
              row.blockId,
              "BmRowContainer cannot pin a row block for fast extract");
          rowBlockId = row.blockId;
        } else {
          auto& block = blocks_.block(row.blockId);
          BOLT_CHECK_LE(row.rowOffset + fixedRowSize_, block.usedBytes);
        }

        const auto* rowPtr = rowBlockData + row.rowOffset;
        const auto output = resultOffset + i;
        const auto isNull =
            isNullAt(rowPtr, column.nullByte(), column.nullMask());
        flatResult->setNull(output, isNull);
        if (isNull) {
          continue;
        }

        const auto ref =
            *reinterpret_cast<const VarData*>(rowPtr + column.offset());
        if (ref.size == 0) {
          flatResult->setStringViewValue(output, StringView("", 0), exactSize);
          continue;
        }

        if (ref.blockId != heapBlockId) {
          auto& block = blocks_.block(ref.blockId);
          BOLT_CHECK_LE(ref.offset + ref.size, block.usedBytes);
          heapBlockData = pinnedBlockDataAfterPressure(
              ref.blockId,
              "BmRowContainer cannot pin a heap block for fast extract");
          heapBlockId = ref.blockId;
        } else {
          auto& block = blocks_.block(ref.blockId);
          BOLT_CHECK_LE(ref.offset + ref.size, block.usedBytes);
        }

        flatResult->setStringViewValue(
            output,
            StringView(heapBlockData + ref.offset, ref.size),
            exactSize);
      }
    } else {
      uint32_t rowBlockId = std::numeric_limits<uint32_t>::max();
      const char* rowBlockData = nullptr;
      for (auto i = 0; i < rows.size(); ++i) {
        const auto row = rows[i];
        if (row.blockId != rowBlockId) {
          auto& block = blocks_.block(row.blockId);
          BOLT_CHECK_LE(row.rowOffset + fixedRowSize_, block.usedBytes);
          rowBlockData = pinnedBlockDataAfterPressure(
              row.blockId,
              "BmRowContainer cannot pin a row block for fast extract");
          rowBlockId = row.blockId;
        } else {
          auto& block = blocks_.block(row.blockId);
          BOLT_CHECK_LE(row.rowOffset + fixedRowSize_, block.usedBytes);
        }

        const auto* rowPtr = rowBlockData + row.rowOffset;
        const auto output = resultOffset + i;
        const auto isNull =
            isNullAt(rowPtr, column.nullByte(), column.nullMask());
        flatResult->setNull(output, isNull);
        if (!isNull) {
          flatResult->set(
              output,
              *reinterpret_cast<const T*>(rowPtr + column.offset()));
        }
      }
    }
  }
}

template <TypeKind Kind>
int32_t BmRowContainer::compareTyped(
    const char* left,
    const char* right,
    BmRowColumn column,
    CompareFlags flags) {
  if constexpr (
      Kind == TypeKind::UNKNOWN || Kind == TypeKind::OPAQUE ||
      Kind == TypeKind::ARRAY || Kind == TypeKind::MAP ||
      Kind == TypeKind::ROW || Kind == TypeKind::VARIANT) {
    BOLT_NYI(
        "BmRowContainer compare does not support type {} yet",
        mapTypeKindToName(Kind));
  } else {
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
      result = compareStringAsc(stringView(left, column), stringView(right, column));
    } else {
      result = comparePrimitiveAsc(
          *reinterpret_cast<const T*>(left + column.offset()),
          *reinterpret_cast<const T*>(right + column.offset()));
    }
    return flags.ascending ? result : -result;
  }
}

} // namespace bytedance::bolt::exec
