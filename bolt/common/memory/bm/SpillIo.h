#pragma once

#include "bolt/common/memory/bm/OwnedFileExtent.h"
#include "bolt/common/memory/bm/io/IoPriority.h"
#include "bolt/common/memory/bm/io/IoResult.h"

#include <cstddef>
#include <future>

namespace bytedance::bolt::memory::bm {

class SpillIo {
 public:
  std::future<IoResult> SubmitReadRaw(
      const OwnedFileExtent& extent,
      size_t size,
      IoPriority priority);

  IoResult WriteRaw(
      const FileExtent& extent,
      IoBuffer& payload,
      IoPriority priority);

 private:
  void EnsureWriteSchedulerReady();

  bool writeSchedulerReady_{false};
};

} // namespace bytedance::bolt::memory::bm
