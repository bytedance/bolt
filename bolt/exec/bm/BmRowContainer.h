#pragma once

#include "bolt/common/memory/bm/BufferHandle.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/base/CompareFlags.h"
#include "bolt/buffer/Buffer.h"
#include "bolt/exec/bm/BmPressureAwareBlockArena.h"
#include "bolt/exec/bm/BmRowTypes.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <vector>

#include <folly/Range.h>

namespace bytedance::bolt::exec {

class BmRowContainer {
 public:
  BmRowContainer(
      std::vector<TypePtr> keyTypes,
      std::vector<TypePtr> dependentTypes,
      std::shared_ptr<memory::bm::BufferManager> bufferManager,
      memory::bm::MemoryTag tag = memory::bm::MemoryTag::kWindow,
      uint32_t rowBlockSize = 0,
      uint32_t heapBlockSize = 0);
  ~BmRowContainer();

  RowId newRow();

  void store(
      const DecodedVector& decoded,
      vector_size_t index,
      RowId row,
      int32_t column);

  std::vector<RowId> store(const RowVectorPtr& input);

  void extractColumn(
      const RowId* rows,
      int32_t numRows,
      int32_t column,
      const VectorPtr& result,
      bool exactSize = false);

  void extractColumn(
      folly::Range<const RowId*> rows,
      int32_t column,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize = false);

  void extractNulls(
      const RowId* rows,
      int32_t numRows,
      int32_t column,
      const BufferPtr& result);

  void extractNulls(
      folly::Range<const RowId*> rows,
      int32_t column,
      const BufferPtr& result);

  int32_t compare(
      RowId left,
      RowId right,
      int32_t column,
      CompareFlags flags = CompareFlags());

  int32_t compareRows(
      RowId left,
      RowId right,
      const std::vector<CompareFlags>& flags = {});

  int64_t numRows() const {
    return numRows_;
  }

  int32_t fixedRowSize() const {
    return fixedRowSize_;
  }

  uint64_t allocatedBytes() const;
  uint64_t usedBytes() const;
  uint64_t heapAllocatedBytes() const;
  std::optional<int64_t> estimateRowSize() const;

  const BmRowColumn& columnAt(int32_t column) const {
    return rowColumns_.at(column);
  }

  const std::vector<BmRowColumn>& columns() const {
    return rowColumns_;
  }

  const std::vector<TypePtr>& columnTypes() const {
    return types_;
  }

  const std::vector<TypePtr>& keyTypes() const {
    return keyTypes_;
  }

  void clear();
  void spillAllBlocksForBenchmark();

 private:
  void computeLayout();
  BmBlockState& ensureWritableRowBlock();
  bool hasRowCapacity(const BmBlockState& block) const;
  char* mutableRow(RowId row);
  const char* pinRow(RowId row);
  StringView stringView(const char* row, BmRowColumn column);
  char* initializeRow(char* row);
  VarData appendVariableWidth(StringView value);
  uint32_t allocateBlockAfterPressure(uint32_t capacity, const char* failureMessage);
  const char* pinnedBlockDataAfterPressure(
      uint32_t blockId,
      const char* failureMessage);
  bool canReclaimBlock(uint32_t blockId) const;
  void storeDispatch(
      TypeKind kind,
      const DecodedVector& decoded,
      vector_size_t index,
      char* row,
      BmRowColumn column);
  void extractDispatch(
      TypeKind kind,
      const char* const* rows,
      int32_t numRows,
      BmRowColumn column,
      const VectorPtr& result,
      vector_size_t resultOffset,
      bool exactSize);
  void extractColumnFast(
      TypeKind kind,
      folly::Range<const RowId*> rows,
      BmRowColumn column,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize);

  int32_t compareDispatch(
      TypeKind kind,
      const char* left,
      const char* right,
      BmRowColumn column,
      CompareFlags flags);

  template <TypeKind Kind>
  void storeWithNulls(
      const DecodedVector& decoded,
      vector_size_t index,
      char* row,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask);

  template <TypeKind Kind>
  void extractColumnTyped(
      const char* const* rows,
      int32_t numRows,
      BmRowColumn column,
      const VectorPtr& result,
      vector_size_t resultOffset,
      bool exactSize);

  template <TypeKind Kind>
  void extractColumnFastTyped(
      folly::Range<const RowId*> rows,
      BmRowColumn column,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize);

  template <TypeKind Kind>
  int32_t compareTyped(
      const char* left,
      const char* right,
      BmRowColumn column,
      CompareFlags flags);

  std::vector<TypePtr> keyTypes_;
  std::vector<TypePtr> dependentTypes_;
  std::vector<TypePtr> types_;
  std::vector<TypeKind> typeKinds_;
  std::vector<int32_t> offsets_;
  std::vector<int32_t> nullOffsets_;
  std::vector<BmRowColumn> rowColumns_;
  std::vector<char> initialNulls_;

  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
  memory::bm::MemoryTag tag_;
  BmPressureAwareBlockArena blocks_;
  uint32_t rowBlockSize_;
  uint32_t heapBlockSize_;
  int32_t fixedRowSize_{0};
  int32_t alignment_{8};
  int32_t rowSizeOffset_{0};
  int32_t freeFlagOffset_{0};
  int64_t numRows_{0};

  std::vector<uint32_t> heapBlockIds_;
  uint32_t activeRowBlockId_{std::numeric_limits<uint32_t>::max()};
  uint32_t activeHeapBlockId_{std::numeric_limits<uint32_t>::max()};
};

} // namespace bytedance::bolt::exec
