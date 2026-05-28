#include "bolt/common/memory/bm/io/ScopedFd.h"

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
