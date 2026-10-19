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

#include "bolt/common/memory/bm/base/ScopedFd.h"

#include <fcntl.h>
#include <unistd.h>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

TEST(ScopedFdTest, closesOwnedFdOnReset) {
  int fds[2];
  ASSERT_EQ(0, ::pipe(fds));
  ::close(fds[1]);

  ScopedFd fd(fds[0]);
  const int raw = fd.get();
  fd.reset();

  EXPECT_EQ(-1, ::fcntl(raw, F_GETFD));
}

TEST(ScopedFdTest, moveTransfersOwnership) {
  int fds[2];
  ASSERT_EQ(0, ::pipe(fds));
  ::close(fds[1]);

  ScopedFd fd(fds[0]);
  const int raw = fd.get();
  ScopedFd moved(std::move(fd));

  EXPECT_EQ(-1, fd.get());
  EXPECT_EQ(raw, moved.get());
}

TEST(ScopedFdTest, moveAssignmentClosesPreviousFdAndHandlesSelfAssignment) {
  int first[2];
  int second[2];
  ASSERT_EQ(0, ::pipe(first));
  ASSERT_EQ(0, ::pipe(second));
  ::close(first[1]);
  ::close(second[1]);

  ScopedFd target(first[0]);
  ScopedFd source(second[0]);
  const int previous = target.get();
  const int transferred = source.get();

  target = std::move(source);

  EXPECT_EQ(-1, source.get());
  EXPECT_EQ(transferred, target.get());
  EXPECT_EQ(-1, ::fcntl(previous, F_GETFD));

  target = std::move(target);
  EXPECT_EQ(transferred, target.get());
}
