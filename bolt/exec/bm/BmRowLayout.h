#pragma once

#include "bolt/common/base/Exceptions.h"
#include "bolt/type/Type.h"
#include "bolt/type/StringView.h"
#include "bolt/vector/TypeAliases.h"

#include <folly/Portability.h>

#include <cstdint>
#include <cstring>
#include <vector>

namespace bytedance::bolt {
class DecodedVector;

namespace exec::bm {
class BmRowContainer;
class RowWriteContext;
}
} // namespace bytedance::bolt

namespace bytedance::bolt::exec::bm {

struct ColumnLayout {
  // Logical type of the column.
  TypePtr type;
  // Offset of the fixed-width cell inside one row.
  uint32_t offset{0};
  // Fixed-width cell size. VARCHAR stores StringView here; payload bytes live in
  // heap blocks.
  uint32_t width{0};
  // Whether this column owns a null bit.
  bool nullable{false};
  // Byte offset of the null bit inside one row.
  uint32_t nullByte{0};
  // Bit mask inside nullByte. Zero means non-nullable.
  uint8_t nullMask{0};
};

struct StringColumnLayout {
  // Offset of StringView inside one row.
  uint32_t offset{0};
  // Null metadata duplicated here so string rebasing can skip null values
  // without looking up the full ColumnLayout.
  bool nullable{false};
  uint32_t nullByte{0};
  uint8_t nullMask{0};
};

struct ColumnStorePlan {
  using StoreValueFn = void (BmRowContainer::*)(
      const DecodedVector& decoded,
      vector_size_t sourceIndex,
      RowWriteContext& context,
      const ColumnStorePlan& plan);

  TypePtr type;
  TypeKind kind{TypeKind::UNKNOWN};
  uint32_t offset{0};
  uint32_t width{0};
  bool nullable{false};
  uint32_t nullByte{0};
  uint8_t nullMask{0};
  bool stringKind{false};
  StoreValueFn storeFn{nullptr};
};

// Computes the fixed row layout used by BmRowContainer. This class owns no row
// memory; it only describes offsets, widths, and null-bit placement.
class BmRowLayout {
 public:
  BmRowLayout() = default;

  BmRowLayout(
      const std::vector<TypePtr>& types,
      const std::vector<bool>& nullable,
      uint32_t rowBlockSize);

  FOLLY_ALWAYS_INLINE const ColumnLayout& column(int32_t column) const {
    return columns_[column];
  }

  FOLLY_ALWAYS_INLINE const std::vector<ColumnLayout>& columns() const {
    return columns_;
  }

  FOLLY_ALWAYS_INLINE const std::vector<StringColumnLayout>& stringColumns()
      const {
    return stringColumns_;
  }

  FOLLY_ALWAYS_INLINE const ColumnStorePlan& storePlan(int32_t column) const {
    return storePlans_[column];
  }

  FOLLY_ALWAYS_INLINE uint32_t rowSize() const {
    return fixedRowSize_;
  }

  // BM owns variable-width payloads at chunk/block granularity, so nullable
  // null cells leave their fixed payload bytes undefined. Readers must check
  // null bits before reading payload cells.
  FOLLY_ALWAYS_INLINE void initializeNulls(char* row) const {
    if (FOLLY_LIKELY(nullBytes_ == 0)) {
      return;
    }
    if (FOLLY_LIKELY(nullBytes_ == 1)) {
      *row = 0;
      return;
    }
    std::memset(row, 0, nullBytes_);
  }

  FOLLY_ALWAYS_INLINE bool isNull(const char* row, int32_t column) const {
    const auto& layout = columns_[column];
    return layout.nullMask != 0 && (row[layout.nullByte] & layout.nullMask);
  }

  FOLLY_ALWAYS_INLINE bool isNull(
      const char* row,
      const StringColumnLayout& column) const {
    return column.nullable && (row[column.nullByte] & column.nullMask);
  }

  FOLLY_ALWAYS_INLINE void setNull(
      char* row,
      int32_t column,
      bool null) const {
    const auto& layout = columns_[column];
    if (FOLLY_UNLIKELY(layout.nullMask == 0)) {
      BOLT_DCHECK(!null, "Column {} is not nullable", column);
      return;
    }
    auto& byte = row[layout.nullByte];
    const auto mask = static_cast<char>(layout.nullMask);
    if (FOLLY_UNLIKELY(null)) {
      byte |= mask;
    } else {
      byte &= ~mask;
    }
  }

  FOLLY_ALWAYS_INLINE char* valueAddress(char* row, int32_t column) const {
    return row + columns_[column].offset;
  }

  FOLLY_ALWAYS_INLINE const char* valueAddress(
      const char* row,
      int32_t column) const {
    return row + columns_[column].offset;
  }

 private:
  std::vector<ColumnLayout> columns_;
  std::vector<StringColumnLayout> stringColumns_;
  std::vector<ColumnStorePlan> storePlans_;
  uint32_t nullBytes_{0};
  uint32_t fixedRowSize_{0};
};

} // namespace bytedance::bolt::exec::bm
