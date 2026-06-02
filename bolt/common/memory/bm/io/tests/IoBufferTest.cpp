#include "bolt/common/memory/bm/io/IoBuffer.h"

#include <cstring>
#include <exception>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

TEST(IoBufferTest, AllocateFromMallocOwnsWritableMemory) {
  auto buffer = IoBuffer::allocateFromMalloc(128);

  ASSERT_TRUE(buffer.valid());
  EXPECT_EQ(4096, buffer.size());
  EXPECT_EQ(128, buffer.length());
  std::memset(buffer.data(), 42, buffer.length());
  EXPECT_EQ(42, buffer.data()[127]);
}

TEST(IoBufferTest, AllocateFromMallocRejectsZeroSize) {
  EXPECT_THROW((void)IoBuffer::allocateFromMalloc(0), std::exception);
}
