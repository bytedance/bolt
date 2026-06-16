#pragma once

#include "bolt/exec/bm/BmRowContainerTypes.h"
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

struct BmBatchAppendMetrics {
  uint64_t batches{0};
  uint64_t rows{0};
  uint64_t fixedColumns{0};
  uint64_t stringColumns{0};
  uint64_t stringRows{0};
  uint64_t stringInlineRows{0};
  uint64_t stringCopiedBytes{0};
  uint64_t stringReferencedBytes{0};
  uint64_t stringHeapAllocCalls{0};
  uint64_t stringFastAllocHits{0};
  uint64_t stringSlowAllocHits{0};
  uint64_t stringHeapBlockSwitches{0};
  uint64_t stringRecordHeapCalls{0};
  uint64_t totalNs{0};
  uint64_t reserveRowsNs{0};
  uint64_t decodeNs{0};
  uint64_t fixedStoreNs{0};
  uint64_t stringStoreNs{0};
};

} // namespace bytedance::bolt::exec::bm
