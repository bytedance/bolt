#pragma once

#include "bolt/common/memory/bm/SpillCandidateProvider.h"
#include "bolt/common/memory/bm/SpillStore.h"
#include "bolt/common/memory/bm/io/IoPriority.h"

#include <cstdint>
#include <functional>

namespace bytedance::bolt::memory::bm {

class BufferManagerStatsCollector;

class SpillWriteDriver {
 public:
  using SubmitWrite =
      std::function<SpillWriteFuture(IoBuffer&, size_t, IoPriority)>;

  SpillWriteDriver(
      uint32_t maxInflight,
      IoPriority priority,
      SubmitWrite submitWrite,
      BufferManagerStatsCollector& accounting);

  uint64_t Spill(uint64_t targetBytes, SpillCandidateProvider nextCandidate);

 private:
  uint32_t maxInflight_{0};
  IoPriority priority_{IoPriority::Medium};
  SubmitWrite submitWrite_;
  BufferManagerStatsCollector& accounting_;
};

} // namespace bytedance::bolt::memory::bm
