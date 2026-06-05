#pragma once

#include <cstdint>

namespace bytedance::bolt::exec {

struct RowId {
  uint32_t rowBlockId{0};
  uint32_t rowOffset{0};
};

struct VarData {
  uint32_t heapBlockId{0};
  uint32_t heapOffset{0};
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
