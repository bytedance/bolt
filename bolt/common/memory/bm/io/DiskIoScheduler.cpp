#include "bolt/common/memory/bm/io/DiskIoScheduler.h"

#include "bolt/common/memory/bm/io/DiskIoSchedulerConfig.h"
#include "bolt/common/memory/bm/io/DiskIoSchedulerImpl.h"

#include <utility>

namespace bytedance::bolt::memory::bm {
namespace {

DiskIoSchedulerImpl& globalDiskIoSchedulerImpl() {
  static auto* scheduler = new DiskIoSchedulerImpl(DiskIoSchedulerConfig{});
  return *scheduler;
}

} // namespace

std::future<IoResult> DiskIoScheduler::submit(IoRequest request) const {
  return globalDiskIoSchedulerImpl().submit(std::move(request));
}

void DiskIoScheduler::ensureReady() const {
  (void)globalDiskIoSchedulerImpl();
}

DiskIoSchedulerStats DiskIoScheduler::stats() const {
  return globalDiskIoSchedulerImpl().stats();
}

DiskIoScheduler& diskIoScheduler() {
  static auto* scheduler = new DiskIoScheduler();
  return *scheduler;
}

} // namespace bytedance::bolt::memory::bm
