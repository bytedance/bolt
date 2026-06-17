#pragma once

#include "bolt/exec/bm/BmRowContainerMetrics.h"
#include "bolt/vector/TypeAliases.h"

#include <cstdint>

namespace bytedance::bolt::exec::bm {

struct ChunkData;

// Batch-only append range. A range maps a contiguous slice of source rows to a
// contiguous row-block slice inside one BM chunk.
struct BatchAppendRange {
  ChunkData* chunk{nullptr};
  char* rowBegin{nullptr};
  vector_size_t sourceBegin{0};
  vector_size_t rowCount{0};
};

enum class BmBatchStringStoreMode {
  kCopy,
  // Benchmark-only mode. Stored StringViews reference the input vectors, so the
  // resulting container must not outlive those vectors and must not spill.
  kReferenceInputStringForBenchmark,
};

} // namespace bytedance::bolt::exec::bm
