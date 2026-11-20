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
