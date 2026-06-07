#pragma once

#include "bolt/common/base/Exceptions.h"

#include <cstdint>
#include <optional>

namespace bytedance::bolt::exec {

using BlockId = uint32_t;
using RowOffset = uint32_t;

struct RowId {
  static constexpr uint32_t kOffsetAlignment = 8;
  static constexpr uint32_t kOffsetBits = 20;
  static constexpr uint32_t kBlockBits = 22;
  static constexpr uint64_t kOffsetMask = (1ULL << kOffsetBits) - 1;
  static constexpr uint64_t kBlockMask = (1ULL << kBlockBits) - 1;
  static constexpr BlockId kNoHeapBlock = static_cast<BlockId>(kBlockMask);
  static constexpr BlockId kMaxBlockId = kNoHeapBlock - 1;

  static RowId make(BlockId rowBlockId, RowOffset rowOffset) {
    return make(rowBlockId, rowOffset, kNoHeapBlock);
  }

  static RowId make(
      BlockId rowBlockId,
      RowOffset rowOffset,
      BlockId primaryHeapBlockId) {
    validateBlockId(rowBlockId);
    validateRowOffset(rowOffset);
    if (primaryHeapBlockId != kNoHeapBlock) {
      validateBlockId(primaryHeapBlockId);
    }

    const auto offsetUnits = rowOffset / kOffsetAlignment;
    return RowId{
        static_cast<uint64_t>(rowBlockId) |
        (static_cast<uint64_t>(offsetUnits) << kBlockBits) |
        (static_cast<uint64_t>(primaryHeapBlockId) <<
         (kBlockBits + kOffsetBits))};
  }

  BlockId rowBlockId() const {
    return static_cast<BlockId>(bits & kBlockMask);
  }

  RowOffset rowOffset() const {
    return static_cast<RowOffset>(
        ((bits >> kBlockBits) & kOffsetMask) * kOffsetAlignment);
  }

  std::optional<BlockId> primaryHeapBlockId() const {
    const auto blockId = static_cast<BlockId>(
        (bits >> (kBlockBits + kOffsetBits)) & kBlockMask);
    if (blockId == kNoHeapBlock) {
      return std::nullopt;
    }
    return blockId;
  }

  void setPrimaryHeapBlockId(BlockId blockId) {
    validateBlockId(blockId);
    bits &= ~(kBlockMask << (kBlockBits + kOffsetBits));
    bits |= static_cast<uint64_t>(blockId) << (kBlockBits + kOffsetBits);
  }

  uint64_t bits{0};

 private:
  static void validateBlockId(BlockId blockId) {
    BOLT_CHECK_LE(blockId, kMaxBlockId);
  }

  static void validateRowOffset(RowOffset rowOffset) {
    BOLT_CHECK_EQ(rowOffset % kOffsetAlignment, 0);
    BOLT_CHECK_LE(rowOffset / kOffsetAlignment, kOffsetMask);
  }
};

static_assert(sizeof(RowId) == sizeof(uint64_t));

struct VarData {
  BlockId blockId{0};
  uint32_t offset{0};
  uint32_t size{0};
};

class BmRowColumn {
 public:
  BmRowColumn(int32_t offset, int32_t nullOffset)
      : packedOffsets_(packOffsets(offset, nullOffset)) {}

  int32_t offset() const {
    return packedOffsets_ >> 32;
  }

  int32_t nullByte() const {
    return static_cast<uint32_t>(packedOffsets_) >> 8;
  }

  uint8_t nullMask() const {
    return packedOffsets_ & 0xff;
  }

  static int32_t nullByte(int32_t nullOffset) {
    return nullOffset / 8;
  }

 private:
  static uint64_t packOffsets(int32_t offset, int32_t nullOffset) {
    return (1UL << (nullOffset & 7)) | ((nullOffset & ~7UL) << 5) |
        static_cast<uint64_t>(offset) << 32;
  }

  uint64_t packedOffsets_;
};

} // namespace bytedance::bolt::exec
