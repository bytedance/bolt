#pragma once

#include "bolt/common/memory/bm/BufferHandle.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/base/CompareFlags.h"
#include "bolt/buffer/Buffer.h"
#include "bolt/exec/bm/BmRowLayout.h"
#include "bolt/exec/bm/BmRowTypes.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include <folly/Range.h>

namespace bytedance::bolt::exec {

struct BmBlockState;
class BmPressureAwareBlockArena;

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
      RowId& row,
      int32_t column);

  std::vector<RowId> store(const RowVectorPtr& input);

  // 尝试把行所在的row block和主heap block加载进内存，不保证全部成功。
  void preloadRows(folly::Range<const RowId*> rows);

  // 对Row Container的访问，要么就零散地走Extract Column，要么就走Batch Preload。
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
    return layout_.fixedRowSize();
  }

  uint64_t allocatedBytes() const;
  uint64_t usedBytes() const;
  uint64_t heapAllocatedBytes() const;
  std::optional<int64_t> estimateRowSize() const;

  const BmRowColumn& columnAt(int32_t column) const {
    return layout_.columnAt(column);
  }

  const std::vector<BmRowColumn>& columns() const {
    return layout_.columns();
  }

  const std::vector<TypePtr>& columnTypes() const {
    return layout_.columnTypes();
  }

  const std::vector<TypePtr>& keyTypes() const {
    return layout_.keyTypes();
  }

  void discardAllRows();
  void spillAllBlocks();

 private:
  BmBlockState& ensureWritableRowBlock();
  bool hasRowCapacity(const BmBlockState& block) const;
  char* mutableRow(RowId row);
  const char* pinRow(RowId row);
  StringView stringView(const char* row, BmRowColumn column);
  char* initializeRow(char* row);
  VarData appendVariableWidth(StringView value);
  const char* pinBlockForRead(
      uint32_t blockId,
      const char* failureMessage);
  std::vector<BlockId> protectedBlocksForRead(
      std::span<const BlockId> blockIds) const;
  template <TypeKind Kind>
  void storeWithNulls(
      const DecodedVector& decoded,
      vector_size_t index,
      RowId& rowId,
      char* row,
      int32_t offset,
      int32_t nullByte,
      uint8_t nullMask);

  template <TypeKind Kind>
  void extractColumnFastTyped(
      folly::Range<const RowId*> rows,
      BmRowColumn column,
      vector_size_t resultOffset,
      const VectorPtr& result,
      bool exactSize);

  template <TypeKind Kind>
  int32_t compareTyped(
      RowId left,
      RowId right,
      BmRowColumn column,
      CompareFlags flags);

  BmRowLayout layout_;
  memory::bm::MemoryTag tag_;
  std::unique_ptr<BmPressureAwareBlockArena> blocks_;
  uint32_t rowBlockSize_;
  uint32_t heapBlockSize_;
  int64_t numRows_{0};

  std::vector<uint32_t> heapBlockIds_;
  uint32_t activeRowBlockId_{std::numeric_limits<uint32_t>::max()};
  uint32_t activeHeapBlockId_{std::numeric_limits<uint32_t>::max()};
};

} // namespace bytedance::bolt::exec
