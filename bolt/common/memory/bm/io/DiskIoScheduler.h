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
  void ensureReady() const;
  DiskIoSchedulerStats stats() const;

 private:
  DiskIoScheduler() = default;

  friend DiskIoScheduler& diskIoScheduler();
};

DiskIoScheduler& diskIoScheduler();

} // namespace bytedance::bolt::memory::bm
