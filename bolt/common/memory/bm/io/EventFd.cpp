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

#if defined(__APPLE__)
#include <fcntl.h>
#else
#include <sys/eventfd.h>
#endif
#include <cerrno>
#include <cstdint>
#include <cstring>

namespace bytedance::bolt::memory::bm {

EventFd::EventFd() {
#if defined(__APPLE__)
  int pipeFds[2];
  BOLT_CHECK_EQ(::pipe(pipeFds), 0, "pipe failed: {}", std::strerror(errno));
  fd_.reset(pipeFds[0]);
  notifyFd_.reset(pipeFds[1]);
  for (const int pipeFd : pipeFds) {
    BOLT_CHECK_GE(
        ::fcntl(pipeFd, F_SETFL, ::fcntl(pipeFd, F_GETFL) | O_NONBLOCK),
        0,
        "fcntl O_NONBLOCK failed: {}",
        std::strerror(errno));
    BOLT_CHECK_GE(
        ::fcntl(pipeFd, F_SETFD, ::fcntl(pipeFd, F_GETFD) | FD_CLOEXEC),
        0,
        "fcntl FD_CLOEXEC failed: {}",
        std::strerror(errno));
  }
#else
  fd_.reset(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
  BOLT_CHECK_GE(fd_.get(), 0, "eventfd failed: {}", std::strerror(errno));
#endif
}

int EventFd::fd() const {
  return fd_.get();
}

void EventFd::notify() const {
  uint64_t value = 1;
  while (true) {
#if defined(__APPLE__)
    const auto written = ::write(notifyFd_.get(), &value, sizeof(value));
#else
    const auto written = ::write(fd_.get(), &value, sizeof(value));
#endif
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
