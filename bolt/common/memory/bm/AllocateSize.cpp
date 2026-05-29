#include "bolt/common/memory/bm/AllocateSize.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::memory::bm {

size_t allocateSizeBytes(AllocateSize size) {
  const auto index = static_cast<size_t>(size);
  BOLT_CHECK_LT(index, kAllocateSizeBytes.size());
  return kAllocateSizeBytes[index];
}

const char* toString(AllocateSize size) {
  switch (size) {
    case AllocateSize::kSmall:
      return "small";
    case AllocateSize::kMedium:
      return "medium";
    case AllocateSize::kLarge:
      return "large";
  }
  BOLT_FAIL("unknown BM allocate size {}", static_cast<int>(size));
}

} // namespace bytedance::bolt::memory::bm
