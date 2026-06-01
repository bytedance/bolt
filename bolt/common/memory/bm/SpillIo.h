#pragma once

#include "bolt/common/memory/bm/ManagedFileSegment.h"
#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/bm/io/IoResult.h"

#include <cstddef>
#include <future>

namespace bytedance::bolt::memory::bm {

class SpillIo {
 public:
  std::future<IoResult> SubmitReadRaw(
      const ManagedFileSegment& segment,
      size_t size,
      IoPriority priority);

  std::future<IoResult> SubmitWriteRaw(
      const FileSegment& segment,
      IoBuffer& payload,
      IoPriority priority);

 private:
  void EnsureWriteSchedulerReady();

  bool writeSchedulerReady_{false};
};

} // namespace bytedance::bolt::memory::bm
