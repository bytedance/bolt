#include "bolt/common/memory/bm/MemoryTag.h"

namespace bytedance::bolt::memory::bm {

const char* toString(MemoryTag tag) {
  switch (tag) {
    case MemoryTag::kUnknown:
      return "Unknown";
    case MemoryTag::kHashBuild:
      return "HashBuild";
    case MemoryTag::kAggregation:
      return "Aggregation";
    case MemoryTag::kSort:
      return "Sort";
    case MemoryTag::kWindow:
      return "Window";
    case MemoryTag::kExchange:
      return "Exchange";
    case MemoryTag::kTesting:
      return "Testing";
  }
  return "Unknown";
}

} // namespace bytedance::bolt::memory::bm
