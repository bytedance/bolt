#include "bolt/common/memory/bm/SpillWriteDriver.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/bm/BlockMemory.h"
#include "bolt/common/memory/bm/BufferManagerStats.h"
#include "bolt/common/memory/bm/ReclaimWriteWindow.h"

#include <utility>

namespace bytedance::bolt::memory::bm {

SpillWriteDriver::SpillWriteDriver(
    uint32_t maxInflight,
    IoPriority priority,
    SubmitWrite submitWrite,
    BufferManagerStatsCollector& accounting)
    : maxInflight_(maxInflight),
      priority_(priority),
      submitWrite_(std::move(submitWrite)),
      accounting_(accounting) {}

uint64_t SpillWriteDriver::Spill(
    uint64_t targetBytes,
    SpillCandidateProvider nextCandidate) {
  BOLT_CHECK_GT(maxInflight_, 0);
  BOLT_CHECK(static_cast<bool>(nextCandidate));

  ReclaimWriteWindow writeWindow{
      maxInflight_, priority_, std::move(submitWrite_), accounting_};
  uint64_t submitted = 0;
  uint64_t reclaimed = 0;
  bool noMoreCandidates = false;

  auto submitMore = [&]() {
    while (writeWindow.canSubmit() &&
           (targetBytes == 0 || submitted < targetBytes) &&
           !noMoreCandidates) {
      auto memory = nextCandidate();
      if (!memory) {
        noMoreCandidates = true;
        break;
      }

      accounting_.RecordReclaimAttemptedBlock();
      const auto blockSize = memory->size;
      writeWindow.Submit(std::move(memory));
      submitted += blockSize;
    }
  };

  submitMore();
  while (writeWindow.hasPending()) {
    auto result = writeWindow.HarvestNext();
    if (!result.ok()) {
      BOLT_FAIL(
          "BM spill write failed, block_id={}, io_error={}, native_error={}, bytes={}",
          result.memory->id,
          static_cast<int>(result.io.error),
          result.io.nativeErrorCode,
          result.io.bytes);
    }

    reclaimed += result.reclaimedBytes;
    if (targetBytes == 0 || reclaimed < targetBytes) {
      submitMore();
    }
  }

  accounting_.RecordReclaimedBytes(reclaimed);
  return reclaimed;
}

} // namespace bytedance::bolt::memory::bm
