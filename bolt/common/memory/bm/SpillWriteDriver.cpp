/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
           (targetBytes == 0 || submitted < targetBytes) && !noMoreCandidates) {
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
