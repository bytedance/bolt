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

#include "bolt/common/base/MemoryCopy.h"

#include <array>
#include <cstring>

#include <gtest/gtest.h>

#if defined(__unix__)
#include <sys/mman.h>
#include <unistd.h>

#include <folly/ScopeGuard.h>
#endif

namespace bytedance::bolt::memory {
namespace {

#if defined(__AVX2__)
constexpr bool kHasNonTemporalStores = true;
#else
constexpr bool kHasNonTemporalStores = false;
#endif

std::array<char, 512> sourceBytes() {
  std::array<char, 512> source;
  for (size_t i = 0; i < source.size(); ++i) {
    source[i] = static_cast<char>((i * 37 + i / 7) & 255);
  }
  return source;
}

TEST(MemoryCopyTest, under64AllLengthsAndAlignments) {
  alignas(64) const auto source = sourceBytes();
  alignas(64) std::array<char, 512> actual;
  for (size_t bytes = 0; bytes < 64; ++bytes) {
    for (size_t srcOffset = 0; srcOffset < 64; ++srcOffset) {
      for (size_t dstOffset = 0; dstOffset < 64; ++dstOffset) {
        actual.fill('\x5a');
        auto expected = actual;
        const auto* src = source.data() + 64 + srcOffset;
        auto* dst = actual.data() + 64 + dstOffset;
        std::memcpy(expected.data() + 64 + dstOffset, src, bytes);
        copyUnder64(dst, src, bytes);
        ASSERT_EQ(actual, expected)
            << "bytes=" << bytes << " srcOffset=" << srcOffset
            << " dstOffset=" << dstOffset;
      }
    }
  }
}

TEST(MemoryCopyTest, nonTemporalAligned) {
  alignas(64) const auto source = sourceBytes();
  alignas(64) std::array<char, 512> actual;
  for (const size_t bytes : {64, 128, 192, 256}) {
    for (size_t srcOffset = 0; srcOffset < 64; ++srcOffset) {
      actual.fill('\x5a');
      auto expected = actual;
      const auto* src = source.data() + srcOffset;
      std::memcpy(expected.data() + 64, src, bytes);
      const bool streamed =
          copyNonTemporalAligned(actual.data() + 64, src, bytes);
      nonTemporalStoreFence();
      ASSERT_EQ(streamed, kHasNonTemporalStores);
      ASSERT_EQ(actual, expected)
          << "bytes=" << bytes << " srcOffset=" << srcOffset;
      if (bytes == 64) {
        actual.fill('\x5a');
        const bool streamed64 = copyNonTemporalAligned(actual.data() + 64, src);
        nonTemporalStoreFence();
        ASSERT_EQ(streamed64, kHasNonTemporalStores);
        ASSERT_EQ(actual, expected) << "srcOffset=" << srcOffset;
      }
    }
  }
}

TEST(MemoryCopyTest, nonTemporalPrefixesAndTails) {
  alignas(64) const auto source = sourceBytes();
  alignas(64) std::array<char, 512> actual;
  for (size_t bytes = 0; bytes <= 257; ++bytes) {
    for (const size_t srcOffset : {0, 1, 7, 31, 32, 63}) {
      for (size_t dstOffset = 0; dstOffset < 64; ++dstOffset) {
        actual.fill('\x5a');
        auto expected = actual;
        const auto* src = source.data() + srcOffset;
        auto* dst = actual.data() + 64 + dstOffset;
        std::memcpy(expected.data() + 64 + dstOffset, src, bytes);
        const bool streamed = copyNonTemporal(dst, src, bytes);
        nonTemporalStoreFence();
        const size_t prefix = (64 - dstOffset) % 64;
        ASSERT_EQ(streamed, kHasNonTemporalStores && bytes >= prefix + 64);
        ASSERT_EQ(actual, expected)
            << "bytes=" << bytes << " srcOffset=" << srcOffset
            << " dstOffset=" << dstOffset;
      }
    }
  }
}

#if defined(__unix__)
TEST(MemoryCopyTest, guardedBoundaries) {
  const auto systemPageSize = sysconf(_SC_PAGESIZE);
  ASSERT_GT(systemPageSize, 257);
  const auto pageSize = static_cast<size_t>(systemPageSize);
  const size_t mappingBytes = 3 * pageSize;
  auto* sourceMapping = static_cast<char*>(mmap(
      nullptr, mappingBytes, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
  ASSERT_NE(sourceMapping, MAP_FAILED);
  SCOPE_EXIT {
    munmap(sourceMapping, mappingBytes);
  };
  auto* targetMapping = static_cast<char*>(mmap(
      nullptr, mappingBytes, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
  ASSERT_NE(targetMapping, MAP_FAILED);
  SCOPE_EXIT {
    munmap(targetMapping, mappingBytes);
  };
  auto* source = sourceMapping + pageSize;
  auto* target = targetMapping + pageSize;
  ASSERT_EQ(mprotect(source, pageSize, PROT_READ | PROT_WRITE), 0);
  ASSERT_EQ(mprotect(target, pageSize, PROT_READ | PROT_WRITE), 0);
  for (size_t i = 0; i < pageSize; ++i) {
    source[i] = static_cast<char>((i * 37 + i / 7) & 255);
  }

  // Check both ends of accessible pages. The tail cases also vary alignment
  // and fault on any overread or overwrite beyond the requested byte range.
  for (size_t bytes = 0; bytes <= 257; ++bytes) {
    for (const bool atEnd : {false, true}) {
      const size_t offset = atEnd ? pageSize - bytes : 0;
      const auto* src = source + offset;
      auto* dst = target + offset;
      if (bytes < 64) {
        std::memset(target, '\x5a', pageSize);
        copyUnder64(dst, src, bytes);
        ASSERT_EQ(std::memcmp(dst, src, bytes), 0);
      }
      std::memset(target, '\x5a', pageSize);
      copyNonTemporal(dst, src, bytes);
      nonTemporalStoreFence();
      ASSERT_EQ(std::memcmp(dst, src, bytes), 0);
      if (bytes != 0 && bytes % 64 == 0) {
        std::memset(target, '\x5a', pageSize);
        copyNonTemporalAligned(dst, src, bytes);
        nonTemporalStoreFence();
        ASSERT_EQ(std::memcmp(dst, src, bytes), 0);
      }
      if (bytes == 64) {
        std::memset(target, '\x5a', pageSize);
        const bool streamed = copyNonTemporalAligned(dst, src);
        nonTemporalStoreFence();
        ASSERT_EQ(streamed, kHasNonTemporalStores);
        ASSERT_EQ(std::memcmp(dst, src, bytes), 0);
      }
    }
  }
}
#endif

} // namespace
} // namespace bytedance::bolt::memory
