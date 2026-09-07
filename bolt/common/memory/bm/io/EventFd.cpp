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

#include "bolt/common/memory/bm/io/EventFd.h"

#include "bolt/common/base/Exceptions.h"

#include <sys/eventfd.h>
#include <cerrno>
#include <cstdint>
#include <cstring>

namespace bytedance::bolt::memory::bm {

EventFd::EventFd() {
  fd_.reset(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
  BOLT_CHECK_GE(fd_.get(), 0, "eventfd failed: {}", std::strerror(errno));
}

int EventFd::fd() const {
  return fd_.get();
}

void EventFd::notify() const {
  uint64_t value = 1;
  while (true) {
    const auto written = ::write(fd_.get(), &value, sizeof(value));
    if (written == sizeof(value)) {
      return;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    // EAGAIN means the counter is already saturated. The fd remains readable,
    // so the waiting worker will still observe the pending wakeup.
    if (written < 0 && errno == EAGAIN) {
      return;
    }
    BOLT_CHECK(false, "eventfd write failed: {}", std::strerror(errno));
  }
}

void EventFd::drainNonBlocking() const {
  uint64_t value = 0;
  while (true) {
    const auto bytes = ::read(fd_.get(), &value, sizeof(value));
    if (bytes == sizeof(value)) {
      continue;
    }
    if (bytes < 0 && errno == EINTR) {
      continue;
    }
    if (bytes < 0 && errno == EAGAIN) {
      return;
    }
    BOLT_CHECK(false, "eventfd read failed: {}", std::strerror(errno));
  }
}

} // namespace bytedance::bolt::memory::bm
