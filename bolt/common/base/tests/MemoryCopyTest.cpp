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
#include <atomic>
#include <bit>
#include <cstring>
#include <limits>
#include <thread>

#include <folly/BenchmarkUtil.h>
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

template <typename T>
void checkLoadAtAllOffsets(T expected) {
  alignas(64) std::array<char, 64 + sizeof(T)> source;
  for (size_t offset = 0; offset < 64; ++offset) {
    source.fill('\x5a');
    std::memcpy(source.data() + offset, &expected, sizeof(T));
    const auto* src = source.data() + offset;
    folly::makeUnpredictable(src);
    const auto actual = loadUnaligned<T>(src);
    folly::doNotOptimizeAway(actual);
    EXPECT_EQ(std::memcmp(&actual, &expected, sizeof(T)), 0)
        << "width=" << sizeof(T) << " offset=" << offset;
  }
}

TEST(MemoryCopyTest, loadUnalignedValuesAndAlignments) {
  const auto checkInteger = []<typename T>() {
    checkLoadAtAllOffsets(T{0});
    checkLoadAtAllOffsets(std::numeric_limits<T>::min());
    checkLoadAtAllOffsets(std::numeric_limits<T>::max());
  };
  checkInteger.template operator()<int8_t>();
  checkInteger.template operator()<uint8_t>();
  checkInteger.template operator()<int16_t>();
  checkInteger.template operator()<uint16_t>();
  checkInteger.template operator()<int32_t>();
  checkInteger.template operator()<uint32_t>();
  checkInteger.template operator()<int64_t>();
  checkInteger.template operator()<uint64_t>();
  checkLoadAtAllOffsets(
      std::array<uint64_t, 2>{0x0123456789abcdefULL, 0xfedcba9876543210ULL});

  // Compare representations so signed zero and NaN payloads are preserved.
  for (const uint32_t bits :
       {0x00000000U,
        0x80000000U,
        0x3f800000U,
        0xbf800000U,
        0x7f800000U,
        0xff800000U,
        0x7fc12345U,
        0xffc54321U}) {
    checkLoadAtAllOffsets(std::bit_cast<float>(bits));
  }
  for (const uint64_t bits :
       {0x0000000000000000ULL,
        0x8000000000000000ULL,
        0x3ff0000000000000ULL,
        0xbff0000000000000ULL,
        0x7ff0000000000000ULL,
        0xfff0000000000000ULL,
        0x7ff8123456789abcULL,
        0xfff8fedcba987654ULL}) {
    checkLoadAtAllOffsets(std::bit_cast<double>(bits));
  }
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
  // Same-thread reads and reuse of ordinary WB memory deliberately do not
  // fence.
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
      ASSERT_EQ(streamed, kHasNonTemporalStores);
      ASSERT_EQ(actual, expected)
          << "bytes=" << bytes << " srcOffset=" << srcOffset;
      if (bytes == 64) {
        actual.fill('\x5a');
        const bool streamed64 = copyNonTemporalAligned(actual.data() + 64, src);
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
        const size_t prefix = (64 - dstOffset) % 64;
        ASSERT_EQ(streamed, kHasNonTemporalStores && bytes >= prefix + 64);
        ASSERT_EQ(actual, expected)
            << "bytes=" << bytes << " srcOffset=" << srcOffset
            << " dstOffset=" << dstOffset;
      }
    }
  }
}

TEST(MemoryCopyTest, nonTemporalPublicationAndReuse) {
  constexpr uint32_t kIterations = 1024;
  constexpr size_t kBufferBytes = 4096 + 128;
  constexpr std::array<size_t, 3> kCopyBytes{64, 4096, 4097};
  constexpr std::array<size_t, 3> kOffsets{64, 64, 65};
  alignas(64) std::array<char, kBufferBytes> actual;
  alignas(64) std::atomic<uint32_t> published{0};
  alignas(64) std::atomic<uint32_t> consumed{0};
  bool storesAsExpected = true;
  const auto sourceFor = [](uint32_t generation) {
    std::array<uint64_t, kBufferBytes / sizeof(uint64_t)> source;
    for (size_t i = 0; i < source.size(); ++i) {
      source[i] = uint64_t{generation} * 0x9e3779b97f4a7c15ULL + i;
    }
    return source;
  };
  std::thread producer([&] {
    for (uint32_t generation = 1; generation <= kIterations; ++generation) {
      // Do not reuse the buffer until the consumer has finished reading it.
      while (consumed.load(std::memory_order_acquire) != generation - 1) {
        std::this_thread::yield();
      }
      actual.fill('\x5a');
      const auto source = sourceFor(generation);
      const auto* src = reinterpret_cast<const char*>(source.data()) + 1;
      const size_t variant = generation % kCopyBytes.size();
      auto* dst = actual.data() + kOffsets[variant];
      const size_t bytes = kCopyBytes[variant];
      const bool streamed = variant == 0 ? copyNonTemporalAligned(dst, src)
          : variant == 1 ? copyNonTemporalAligned(dst, src, bytes)
                         : copyNonTemporal(dst, src, bytes);
      storesAsExpected = storesAsExpected && streamed == kHasNonTemporalStores;
      // Release/acquire publishes the buffer; SFENCE orders the NT stores
      // before that publication. Neither substitutes for the other.
      nonTemporalStoreFence();
      published.store(generation, std::memory_order_release);
    }
  });

  uint32_t firstMismatch = 0;
  for (uint32_t generation = 1; generation <= kIterations; ++generation) {
    while (published.load(std::memory_order_acquire) != generation) {
      std::this_thread::yield();
    }
    const auto source = sourceFor(generation);
    const size_t variant = generation % kCopyBytes.size();
    std::array<char, kBufferBytes> expected;
    expected.fill('\x5a');
    std::memcpy(
        expected.data() + kOffsets[variant],
        reinterpret_cast<const char*>(source.data()) + 1,
        kCopyBytes[variant]);
    if (actual != expected && firstMismatch == 0) {
      firstMismatch = generation;
    }
    // Always acknowledge, even after a mismatch, so the producer can finish.
    consumed.store(generation, std::memory_order_release);
  }
  producer.join();
  EXPECT_TRUE(storesAsExpected);
  EXPECT_EQ(firstMismatch, 0) << "first mismatching publication";
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

  const auto checkLoad = [&]<typename T>() {
    for (size_t offset = 0; offset < 64; ++offset) {
      for (const auto* src :
           {source + offset, source + pageSize - sizeof(T) - offset}) {
        folly::makeUnpredictable(src);
        const auto actual = loadUnaligned<T>(src);
        folly::doNotOptimizeAway(actual);
        EXPECT_EQ(std::memcmp(&actual, src, sizeof(T)), 0)
            << "width=" << sizeof(T) << " offset=" << offset;
      }
    }
  };
  checkLoad.template operator()<uint8_t>();
  checkLoad.template operator()<uint16_t>();
  checkLoad.template operator()<uint32_t>();
  checkLoad.template operator()<uint64_t>();
  checkLoad.template operator()<std::array<uint64_t, 2>>();

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
      ASSERT_EQ(std::memcmp(dst, src, bytes), 0);
      if (bytes != 0 && bytes % 64 == 0) {
        std::memset(target, '\x5a', pageSize);
        copyNonTemporalAligned(dst, src, bytes);
        ASSERT_EQ(std::memcmp(dst, src, bytes), 0);
      }
      if (bytes == 64) {
        std::memset(target, '\x5a', pageSize);
        const bool streamed = copyNonTemporalAligned(dst, src);
        ASSERT_EQ(streamed, kHasNonTemporalStores);
        ASSERT_EQ(std::memcmp(dst, src, bytes), 0);
      }
    }
  }
}
#endif

} // namespace
} // namespace bytedance::bolt::memory
