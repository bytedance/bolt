#pragma once

#include <future>

#include "bolt/common/memory/bm/io/DiskIoSchedulerStats.h"
#include "bolt/common/memory/bm/io/IoRequest.h"
#include "bolt/common/memory/bm/io/IoResult.h"

namespace bytedance::bolt::memory::bm {

class DiskIoScheduler {
 public:
  DiskIoScheduler(const DiskIoScheduler&) = delete;
  DiskIoScheduler& operator=(const DiskIoScheduler&) = delete;

  std::future<IoResult> submit(IoRequest request) const;
  DiskIoSchedulerStats stats() const;

 private:
  DiskIoScheduler() = default;

  friend DiskIoScheduler& diskIoScheduler();
};

DiskIoScheduler& diskIoScheduler();

} // namespace bytedance::bolt::memory::bm
