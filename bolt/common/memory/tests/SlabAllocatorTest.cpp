/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/common/memory/MemoryPool.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "bolt/common/memory/Memory.h"

namespace bytedance::bolt::memory {
namespace {

constexpr std::size_t kNumValues = 1'000'000;
constexpr std::size_t kStressAllocationBytes = (1ULL << 30) + 1;

std::vector<std::size_t> makeRandomAllocationSizes() {
  constexpr std::array<std::size_t, 3> kLargeSizes{32 << 10, 64 << 10, 4 << 20};
  std::mt19937 random{42};
  std::uniform_int_distribution<int32_t> allocationType{0, 9};
  std::uniform_int_distribution<std::size_t> smallSize{1, 1 << 10};
  std::uniform_int_distribution<std::size_t> largeSizeIndex{
      0, kLargeSizes.size() - 1};

  std::vector<std::size_t> sizes;
  std::size_t totalBytes{0};
  while (totalBytes < kStressAllocationBytes) {
    const auto bytes = allocationType(random) < 3
        ? kLargeSizes[largeSizeIndex(random)]
        : smallSize(random);
    sizes.push_back(bytes);
    totalBytes += bytes;
  }
  return sizes;
}

class SlabAllocatorTest : public testing::Test {
 protected:
  void SetUp() override {
    MemoryManager::Options options;
    options.allocatorCapacity = 512 << 20;
    options.arbitratorCapacity = 512 << 20;
    options.trackDefaultUsage = true;
    options.useMmapAllocator = false;
    memoryManager_ = std::make_unique<MemoryManager>(options);
    pool_ = memoryManager_->addLeafPool("SlabAllocatorTest");
    resource_ = std::make_unique<SlabMemoryResource>(pool_.get());
  }

  std::unique_ptr<MemoryManager> memoryManager_;
  std::shared_ptr<MemoryPool> pool_;
  std::unique_ptr<SlabMemoryResource> resource_;
};

template <typename Vector>
void expectVectorContents(
    const Vector& values,
    std::initializer_list<typename Vector::value_type> expected) {
  ASSERT_EQ(values.size(), expected.size());
  auto valueIt = values.begin();
  auto expectedIt = expected.begin();
  for (; valueIt != values.end(); ++valueIt, ++expectedIt) {
    EXPECT_EQ(*valueIt, *expectedIt);
  }
}

TEST_F(SlabAllocatorTest, StdVector) {
  std::vector<int64_t, SlabAllocator<int64_t>> values{
      SlabAllocator<int64_t>(resource_.get())};

  for (std::size_t i = 0; i < kNumValues; ++i) {
    values.push_back(i * 3);
  }

  ASSERT_EQ(values.size(), kNumValues);
  for (std::size_t i = 0; i < kNumValues; ++i) {
    EXPECT_EQ(values[i], i * 3);
  }
}

TEST_F(SlabAllocatorTest, StdMap) {
  std::map<
      int32_t,
      std::string,
      std::less<int32_t>,
      SlabAllocator<std::pair<const int32_t, std::string>>>
      values{SlabAllocator<std::pair<const int32_t, std::string>>(
          resource_.get())};

  for (std::size_t i = 0; i < kNumValues; ++i) {
    values.emplace(i, std::to_string(i * 7));
  }

  ASSERT_EQ(values.size(), kNumValues);
  for (std::size_t i = 0; i < kNumValues; ++i) {
    EXPECT_EQ(values.at(i), std::to_string(i * 7));
  }
}

TEST_F(SlabAllocatorTest, StdList) {
  std::list<int32_t, SlabAllocator<int32_t>> values{
      SlabAllocator<int32_t>(resource_.get())};

  for (std::size_t i = 0; i < kNumValues; ++i) {
    values.push_back(i);
  }

  std::size_t expected = 0;
  for (const auto value : values) {
    EXPECT_EQ(value, expected++);
  }
  EXPECT_EQ(expected, kNumValues);
}

TEST_F(SlabAllocatorTest, StdSet) {
  std::set<int32_t, std::less<int32_t>, SlabAllocator<int32_t>> values{
      SlabAllocator<int32_t>(resource_.get())};

  for (std::size_t i = kNumValues; i > 0; --i) {
    values.insert(i - 1);
  }

  ASSERT_EQ(values.size(), kNumValues);
  std::size_t expected = 0;
  for (const auto value : values) {
    EXPECT_EQ(value, expected++);
  }
  EXPECT_EQ(expected, kNumValues);
}

TEST_F(SlabAllocatorTest, LargeAllocationUsesMemoryPool) {
  constexpr std::size_t kLargeNumValues = 4 * 1024 * 1024;
  std::vector<int64_t, SlabAllocator<int64_t>> values{
      SlabAllocator<int64_t>(resource_.get())};

  values.resize(kLargeNumValues);
  for (std::size_t i = 0; i < values.size(); ++i) {
    values[i] = static_cast<int64_t>(i);
  }

  ASSERT_EQ(values.size(), kLargeNumValues);
  EXPECT_EQ(values.front(), 0);
  EXPECT_EQ(values.back(), static_cast<int64_t>(kLargeNumValues - 1));
  EXPECT_GT(pool_->currentBytes(), 0);
}

TEST_F(SlabAllocatorTest, AlignmentRoundsAllocatedBytes) {
  SlabAllocator<char, 64> allocator{resource_.get()};

  auto* value = allocator.allocate(1);
  EXPECT_EQ(resource_->usedBytes(), 64);

  allocator.deallocate(value, 1);
  EXPECT_EQ(resource_->usedBytes(), 0);
}

TEST_F(SlabAllocatorTest, LargeStdVector) {
  {
    std::vector<double, SlabAllocator<double>> data{
        SlabAllocator<double>{resource_.get()}};
    constexpr int32_t kNumDoubles = 256 * 1024;

    for (auto i = 0; i < kNumDoubles; ++i) {
      data.push_back(i);
    }

    ASSERT_EQ(data.size(), kNumDoubles);
    for (auto i = 0; i < kNumDoubles; ++i) {
      ASSERT_EQ(data[i], i);
    }
    EXPECT_GT(pool_->currentBytes(), 0);
  }
  EXPECT_EQ(pool_->currentBytes(), 0);
}

TEST_F(SlabAllocatorTest, LargeStdVectorWithLargeElementAlignment) {
  {
    std::vector<__int128_t, SlabAllocator<__int128_t>> data{
        SlabAllocator<__int128_t>{resource_.get()}};
    constexpr int32_t kNumInt128 = 256 * 1024;

    for (auto i = 0; i < kNumInt128; ++i) {
      data.push_back(i);
    }

    ASSERT_EQ(data.size(), kNumInt128);
    for (auto i = 0; i < kNumInt128; ++i) {
      ASSERT_EQ(data[i], i);
    }
    EXPECT_GT(pool_->currentBytes(), 0);
  }
  EXPECT_EQ(pool_->currentBytes(), 0);
}

TEST_F(SlabAllocatorTest, Overflow) {
  SlabAllocator<int64_t> allocator{resource_.get()};

  EXPECT_THROW(allocator.allocate(1ULL << 62), BoltException);
  auto* value = allocator.allocate(1);
  EXPECT_THROW(allocator.deallocate(value, 1ULL << 62), BoltException);
  allocator.deallocate(value, 1);
}

TEST_F(SlabAllocatorTest, AlignmentGuarantees) {
  {
    SlabAllocator<int64_t, 4> allocator{resource_.get()};
    auto* value = allocator.allocate(1);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(value) % alignof(int64_t), 0);
    allocator.deallocate(value, 1);
  }

  {
    SlabAllocator<int64_t, 8> allocator{resource_.get()};
    auto* value = allocator.allocate(1);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(value) % alignof(int64_t), 0);
    allocator.deallocate(value, 1);
  }

  {
    SlabAllocator<int64_t, 16> allocator{resource_.get()};
    auto* value = allocator.allocate(1);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(value) % 16, 0);
    allocator.deallocate(value, 1);
  }

  {
    SlabAllocator<int64_t, 32> allocator{resource_.get()};
    auto* value = allocator.allocate(1);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(value) % 32, 0);
    allocator.deallocate(value, 1);
  }

  {
    SlabAllocator<int64_t, 64> allocator{resource_.get()};
    auto* value = allocator.allocate(1);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(value) % 64, 0);
    allocator.deallocate(value, 1);
  }
}

TEST_F(SlabAllocatorTest, StdMapWithCustomAlignment) {
  SlabAllocator<std::pair<const int32_t, double>, 32> allocator{
      resource_.get()};
  std::map<
      int32_t,
      double,
      std::less<int32_t>,
      SlabAllocator<std::pair<const int32_t, double>, 32>>
      values{allocator};

  for (auto i = 0; i < 10'000; ++i) {
    values.try_emplace(i, i + 0.05);
  }
  for (auto i = 0; i < 10'000; ++i) {
    ASSERT_EQ(1, values.count(i));
  }

  values.clear();
  for (auto i = 0; i < 10'000; ++i) {
    ASSERT_EQ(0, values.count(i));
  }

  for (auto i = 10'000; i < 20'000; ++i) {
    values.try_emplace(i, i + 0.15);
  }
  for (auto i = 10'000; i < 20'000; ++i) {
    ASSERT_EQ(1, values.count(i));
  }
}

TEST_F(SlabAllocatorTest, BasicAllocateDeallocate) {
  SlabAllocator<int> allocator{resource_.get()};

  int* values = allocator.allocate(5);
  for (int i = 0; i < 5; ++i) {
    values[i] = i * 10;
  }

  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(values[i], i * 10);
  }

  allocator.deallocate(values, 5);
}

TEST_F(SlabAllocatorTest, DeallocateReleasesReservation) {
  SlabAllocator<int64_t, 64> allocator{resource_.get()};

  auto* value = allocator.allocate(1);
  EXPECT_EQ(resource_->usedBytes(), 64);
  EXPECT_EQ(resource_->reservedBytes(), 1 << 20);
  EXPECT_EQ(pool_->reservedBytes(), 1 << 20);

  allocator.deallocate(value, 1);
  EXPECT_EQ(resource_->usedBytes(), 0);
  EXPECT_EQ(resource_->reservedBytes(), 0);
  EXPECT_EQ(pool_->reservedBytes(), 0);
}

TEST_F(SlabAllocatorTest, TypeAlignment) {
  {
    SlabAllocator<double> allocator{resource_.get()};
    double* value = allocator.allocate(1);

    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(value) % alignof(double), 0);

    allocator.deallocate(value, 1);
  }

  {
    SlabAllocator<__int128_t> allocator{resource_.get()};
    __int128_t* value = allocator.allocate(1);

    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(value) % alignof(__int128_t), 0);

    allocator.deallocate(value, 1);
  }
}

TEST_F(SlabAllocatorTest, MultipleAllocations) {
  SlabAllocator<int> allocator{resource_.get()};

  for (int i = 0; i < 10; ++i) {
    int* values = allocator.allocate(5);
    for (int j = 0; j < 5; ++j) {
      values[j] = i * 10 + j;
    }

    for (int j = 0; j < 5; ++j) {
      EXPECT_EQ(values[j], i * 10 + j);
    }

    allocator.deallocate(values, 5);
  }
}

TEST_F(SlabAllocatorTest, RebindSharesPool) {
  SlabAllocator<int64_t, 64> allocator{resource_.get()};
  using ByteAllocator = typename std::allocator_traits<
      decltype(allocator)>::template rebind_alloc<std::byte>;

  ByteAllocator reboundAllocator{allocator};

  EXPECT_EQ(reboundAllocator, allocator);
  EXPECT_EQ(resource_->usedBytes(), 0);
  EXPECT_EQ(resource_->reservedBytes(), 0);

  auto* bytes = reboundAllocator.allocate(1);
  EXPECT_EQ(resource_->usedBytes(), 64);
  EXPECT_EQ(resource_->reservedBytes(), 1 << 20);

  auto* values = allocator.allocate(1);
  EXPECT_EQ(resource_->usedBytes(), 128);
  EXPECT_EQ(resource_->reservedBytes(), 1 << 20);

  reboundAllocator.deallocate(bytes, 1);
  EXPECT_EQ(resource_->usedBytes(), 64);
  EXPECT_EQ(resource_->reservedBytes(), 1 << 20);

  allocator.deallocate(values, 1);
  EXPECT_EQ(resource_->usedBytes(), 0);
  EXPECT_EQ(resource_->reservedBytes(), 0);
}

TEST_F(SlabAllocatorTest, AllocatorUsesExternalMemoryResource) {
  {
    SlabMemoryResource resource{pool_.get()};
    SlabAllocator<char, 64> allocator{&resource};
    SlabAllocator<int64_t, 64> reboundAllocator{allocator};

    auto* value = allocator.allocate(1);
    EXPECT_EQ(resource.usedBytes(), 64);
    EXPECT_EQ(pool_->reservedBytes(), 1 << 20);

    allocator.deallocate(value, 1);
    EXPECT_EQ(resource.usedBytes(), 0);
    EXPECT_EQ(pool_->reservedBytes(), 0);
  }
  EXPECT_EQ(pool_->reservedBytes(), 0);
}

TEST_F(SlabAllocatorTest, MonotonicMemoryResourceAllocatesFromChunks) {
  MonotonicMemoryResource resource{pool_.get(), 128};

  auto* first = resource.allocate(8, alignof(int64_t));
  auto* second = resource.allocate(1, 64);

  EXPECT_EQ(reinterpret_cast<std::uintptr_t>(first) % alignof(int64_t), 0);
  EXPECT_EQ(second, static_cast<std::byte*>(first) + 8);
  EXPECT_EQ(resource.usedBytes(), 9);
  EXPECT_EQ(resource.reservedBytes(), 64 << 10);
  EXPECT_GT(pool_->currentBytes(), 0);

  resource.deallocate(first, 8, alignof(int64_t));
  resource.deallocate(second, 1, 64);

  EXPECT_EQ(resource.usedBytes(), 9);
  EXPECT_GT(pool_->currentBytes(), 0);
}

TEST_F(SlabAllocatorTest, MonotonicMemoryResourceDefaultsToByteAlignment) {
  MonotonicMemoryResource resource{pool_.get()};

  auto* first = static_cast<std::byte*>(resource.allocate(3));
  auto* second = static_cast<std::byte*>(resource.allocate(5));

  EXPECT_EQ(second, first + 3);
  EXPECT_EQ(resource.usedBytes(), 8);
}

TEST_F(
    SlabAllocatorTest,
    MonotonicMemoryResourceSelectsChunkSizeByRequestSize) {
  auto verifyReservedBytes = [&](std::size_t allocationBytes,
                                 std::size_t expectedReservedBytes) {
    MonotonicMemoryResource resource{pool_.get(), 128};
    auto* data = resource.allocate(allocationBytes, alignof(std::max_align_t));

    EXPECT_NE(data, nullptr);
    EXPECT_EQ(resource.usedBytes(), allocationBytes);
    EXPECT_EQ(resource.reservedBytes(), expectedReservedBytes);
  };

  verifyReservedBytes(16 << 10, 64 << 10);
  verifyReservedBytes(64 << 10, 64 << 10);
  verifyReservedBytes((64 << 10) + 1, (64 << 10) + 1);
}

TEST_F(SlabAllocatorTest, MonotonicMemoryResourceAllocatesLargeChunks) {
  constexpr std::array<std::size_t, 3> kAllocationSizes{
      2 << 20, 4 << 20, 8 << 20};

  for (const auto bytes : kAllocationSizes) {
    {
      MonotonicMemoryResource resource{pool_.get()};
      auto* data = static_cast<std::byte*>(resource.allocate(bytes, 64));

      ASSERT_NE(data, nullptr);
      EXPECT_EQ(reinterpret_cast<std::uintptr_t>(data) % 64, 0);
      data[0] = std::byte{0x12};
      data[bytes - 1] = std::byte{0x34};
      EXPECT_EQ(data[0], std::byte{0x12});
      EXPECT_EQ(data[bytes - 1], std::byte{0x34});
      EXPECT_EQ(resource.usedBytes(), bytes);
      EXPECT_EQ(resource.reservedBytes(), bytes);
    }
    EXPECT_EQ(pool_->currentBytes(), 0);
  }

  constexpr std::array<std::size_t, 3> kSharedResourceAllocationSizes{
      4 << 20, 8 << 20, 8 << 20};
  {
    MonotonicMemoryResource resource{pool_.get()};
    std::array<std::byte*, kSharedResourceAllocationSizes.size()> allocations;
    std::size_t totalBytes{0};
    for (std::size_t i = 0; i < kSharedResourceAllocationSizes.size(); ++i) {
      const auto bytes = kSharedResourceAllocationSizes[i];
      allocations[i] = static_cast<std::byte*>(resource.allocate(bytes, 64));
      ASSERT_NE(allocations[i], nullptr);
      EXPECT_EQ(reinterpret_cast<std::uintptr_t>(allocations[i]) % 64, 0);
      allocations[i][0] = std::byte{0x12};
      allocations[i][bytes - 1] = std::byte{0x34};
      totalBytes += bytes;
    }

    EXPECT_EQ(resource.usedBytes(), totalBytes);
    EXPECT_EQ(resource.reservedBytes(), totalBytes);
    for (std::size_t i = 0; i < kSharedResourceAllocationSizes.size(); ++i) {
      resource.deallocate(
          allocations[i], kSharedResourceAllocationSizes[i], 64);
      EXPECT_EQ(resource.usedBytes(), totalBytes);
      EXPECT_EQ(resource.reservedBytes(), totalBytes);
    }
  }
  EXPECT_EQ(pool_->currentBytes(), 0);
}

TEST_F(SlabAllocatorTest, MonotonicMemoryResourceDoesNotReuseExactFit) {
  MonotonicMemoryResource resource{pool_.get(), 128};

  ASSERT_NE(resource.allocate(32 << 10, alignof(std::max_align_t)), nullptr);
  ASSERT_NE(resource.allocate(32 << 10, alignof(std::max_align_t)), nullptr);

  EXPECT_EQ(resource.usedBytes(), 64 << 10);
  EXPECT_EQ(resource.reservedBytes(), 128 << 10);
}

TEST_F(
    SlabAllocatorTest,
    MonotonicMemoryResourceUsesContiguousChunksAfterThreshold) {
  MonotonicMemoryResource resource{pool_.get(), 128};

  for (auto i = 0; i < 5; ++i) {
    ASSERT_NE(resource.allocate(64 << 10, alignof(std::max_align_t)), nullptr);
  }
  EXPECT_EQ(resource.reservedBytes(), 5 * (64 << 10));

  ASSERT_NE(resource.allocate(1, alignof(std::max_align_t)), nullptr);
  EXPECT_EQ(
      resource.reservedBytes(),
      5 * (64 << 10) + AllocationTraits::kHugePageSize);
}

TEST_F(SlabAllocatorTest, MonotonicMemoryResourceReleasesChunksOnDestroy) {
  {
    MonotonicMemoryResource resource{pool_.get(), 1024};

    auto* first = resource.allocate(900, alignof(int64_t));
    auto* second = resource.allocate(900, alignof(int64_t));

    EXPECT_NE(first, nullptr);
    EXPECT_NE(second, nullptr);
    EXPECT_EQ(resource.usedBytes(), 1800);
    EXPECT_EQ(resource.reservedBytes(), 64 << 10);
    EXPECT_GT(pool_->currentBytes(), 0);
  }

  EXPECT_EQ(pool_->currentBytes(), 0);
}

TEST_F(SlabAllocatorTest, RandomAllocationsReleaseAllPoolMemory) {
  MemoryManager::Options options;
  options.allocatorCapacity = 2LL << 30;
  options.arbitratorCapacity = 2LL << 30;
  options.trackDefaultUsage = true;
  options.useMmapAllocator = false;
  auto memoryManager = std::make_unique<MemoryManager>(options);
  auto pool = memoryManager->addLeafPool("RandomAllocations");
  const auto sizes = makeRandomAllocationSizes();

  {
    SlabMemoryResource resource{pool.get()};
    SlabAllocator<std::byte, 64> allocator{resource};
    std::vector<std::byte*> allocations;
    allocations.reserve(sizes.size());
    for (const auto bytes : sizes) {
      allocations.push_back(allocator.allocate(bytes));
    }
    EXPECT_GT(pool->currentBytes(), 1ULL << 30);

    for (std::size_t i = allocations.size(); i > 0; --i) {
      allocator.deallocate(allocations[i - 1], sizes[i - 1]);
    }
  }
  EXPECT_EQ(pool->currentBytes(), 0);
  EXPECT_EQ(pool->reservedBytes(), 0);

  {
    MonotonicMemoryResource resource{pool.get()};
    MonotonicAllocator<std::byte, 64> allocator{resource};
    std::vector<std::byte*> allocations;
    allocations.reserve(sizes.size());
    for (const auto bytes : sizes) {
      allocations.push_back(allocator.allocate(bytes));
    }
    EXPECT_GT(pool->currentBytes(), 1ULL << 30);

    for (std::size_t i = allocations.size(); i > 0; --i) {
      allocator.deallocate(allocations[i - 1], sizes[i - 1]);
    }
  }
  EXPECT_EQ(pool->currentBytes(), 0);
  EXPECT_EQ(pool->reservedBytes(), 0);
}

TEST_F(SlabAllocatorTest, MonotonicAllocatorStdContainers) {
  MonotonicMemoryResource resource{pool_.get()};
  std::vector<int64_t, MonotonicAllocator<int64_t>> values{
      MonotonicAllocator<int64_t>{resource}};

  for (std::size_t i = 0; i < 10'000; ++i) {
    values.push_back(i * 3);
  }

  ASSERT_EQ(values.size(), 10'000);
  for (std::size_t i = 0; i < values.size(); ++i) {
    EXPECT_EQ(values[i], i * 3);
  }
  EXPECT_GT(resource.usedBytes(), values.size() * sizeof(int64_t));
}

TEST_F(SlabAllocatorTest, MonotonicAllocatorAlignmentAndDeallocate) {
  MonotonicMemoryResource resource{pool_.get()};
  MonotonicAllocator<char, 64> allocator{resource};

  auto* first = allocator.allocate(1);
  auto* second = allocator.allocate(1);
  EXPECT_EQ(reinterpret_cast<std::uintptr_t>(first) % 64, 0);
  EXPECT_EQ(reinterpret_cast<std::uintptr_t>(second) % 64, 0);
  EXPECT_EQ(resource.usedBytes(), 128);

  allocator.deallocate(first, 1);
  allocator.deallocate(second, 1);
  EXPECT_EQ(resource.usedBytes(), 128);
}

TEST_F(SlabAllocatorTest, MonotonicAllocatorRebindAndEquality) {
  MonotonicMemoryResource resource{pool_.get()};
  MonotonicMemoryResource otherResource{pool_.get()};
  MonotonicAllocator<int64_t, 64> allocator{resource};
  using ByteAllocator = typename std::allocator_traits<
      decltype(allocator)>::template rebind_alloc<std::byte>;

  ByteAllocator rebound{allocator};
  MonotonicAllocator<int64_t, 64> other{otherResource};
  EXPECT_EQ(allocator, rebound);
  EXPECT_NE(allocator, other);

  auto* bytes = rebound.allocate(1);
  EXPECT_EQ(reinterpret_cast<std::uintptr_t>(bytes) % 64, 0);
  EXPECT_EQ(resource.usedBytes(), 64);
}

TEST_F(SlabAllocatorTest, MonotonicAllocatorContainerPropagationTraits) {
  using Traits = std::allocator_traits<MonotonicAllocator<int64_t, 64>>;

  static_assert(!Traits::propagate_on_container_copy_assignment::value);
  static_assert(Traits::propagate_on_container_move_assignment::value);
  static_assert(Traits::propagate_on_container_swap::value);
}

TEST_F(SlabAllocatorTest, AllocatorsAreEqualOnlyWhenSharingState) {
  SlabAllocator<int64_t, 64> allocator{resource_.get()};
  SlabMemoryResource otherResource{pool_.get()};
  SlabAllocator<int64_t, 64> other{&otherResource};
  SlabAllocator<int64_t, 64> copy{allocator};
  using ByteAllocator = typename std::allocator_traits<
      decltype(allocator)>::template rebind_alloc<std::byte>;

  ByteAllocator reboundAllocator{allocator};

  EXPECT_EQ(allocator, copy);
  EXPECT_EQ(allocator, reboundAllocator);
  EXPECT_NE(allocator, other);
}

TEST_F(SlabAllocatorTest, CopySharesStateAndCanDeallocate) {
  SlabAllocator<char, 64> allocator{resource_.get()};
  SlabAllocator<char, 64> copy{allocator};

  auto* value = allocator.allocate(1);
  EXPECT_EQ(resource_->usedBytes(), 64);

  copy.deallocate(value, 1);
  EXPECT_EQ(resource_->usedBytes(), 0);
}

TEST_F(SlabAllocatorTest, ContainerPropagationTraits) {
  using Allocator = SlabAllocator<int64_t, 64>;
  using Traits = std::allocator_traits<Allocator>;

  static_assert(!Traits::propagate_on_container_copy_assignment::value);
  static_assert(Traits::propagate_on_container_move_assignment::value);
  static_assert(Traits::propagate_on_container_swap::value);
}

TEST_F(SlabAllocatorTest, ContainerCopyAssignmentDoesNotPropagateAllocator) {
  using Vector = std::vector<int64_t, SlabAllocator<int64_t, 64>>;
  auto otherPool = memoryManager_->addLeafPool("SlabAllocatorTestOther");
  SlabMemoryResource otherResource{otherPool.get()};

  Vector source{SlabAllocator<int64_t, 64>{resource_.get()}};
  source.assign({1, 2, 3, 4});

  Vector target{SlabAllocator<int64_t, 64>{&otherResource}};
  target.assign({9, 8});
  const auto targetAllocator = target.get_allocator();

  target = source;

  EXPECT_EQ(target, source);
  EXPECT_EQ(target.get_allocator(), targetAllocator);
  EXPECT_NE(target.get_allocator(), source.get_allocator());
}

TEST_F(SlabAllocatorTest, ContainerMoveAssignmentPropagatesAllocator) {
  using Vector = std::vector<int64_t, SlabAllocator<int64_t, 64>>;
  auto otherPool = memoryManager_->addLeafPool("SlabAllocatorTestOther");
  SlabMemoryResource otherResource{otherPool.get()};

  Vector source{SlabAllocator<int64_t, 64>{resource_.get()}};
  source.assign({1, 2, 3, 4});
  const auto sourceAllocator = source.get_allocator();

  Vector target{SlabAllocator<int64_t, 64>{&otherResource}};
  target.assign({9, 8});

  target = std::move(source);

  expectVectorContents(target, {1, 2, 3, 4});
  EXPECT_EQ(target.get_allocator(), sourceAllocator);
}

TEST_F(SlabAllocatorTest, ContainerSwapPropagatesAllocator) {
  using Vector = std::vector<int64_t, SlabAllocator<int64_t, 64>>;
  auto otherPool = memoryManager_->addLeafPool("SlabAllocatorTestOther");
  SlabMemoryResource otherResource{otherPool.get()};

  Vector left{SlabAllocator<int64_t, 64>{resource_.get()}};
  left.assign({1, 2, 3});
  const auto leftAllocator = left.get_allocator();

  Vector right{SlabAllocator<int64_t, 64>{&otherResource}};
  right.assign({9, 8});
  const auto rightAllocator = right.get_allocator();

  using std::swap;
  swap(left, right);

  expectVectorContents(left, {9, 8});
  expectVectorContents(right, {1, 2, 3});
  EXPECT_EQ(left.get_allocator(), rightAllocator);
  EXPECT_EQ(right.get_allocator(), leftAllocator);
}

TEST_F(SlabAllocatorTest, MoveAndSwapExchangeState) {
  auto otherPool = memoryManager_->addLeafPool("SlabAllocatorTestOther");
  SlabMemoryResource otherResource{otherPool.get()};
  SlabAllocator<char, 64> left{resource_.get()};
  SlabAllocator<char, 64> right{&otherResource};

  auto* leftValue = left.allocate(1);
  auto* rightValue = right.allocate(65);

  EXPECT_EQ(resource_->usedBytes(), 64);
  EXPECT_EQ(otherResource.usedBytes(), 128);

  using std::swap;
  swap(left, right);

  EXPECT_EQ(resource_->usedBytes(), 64);
  EXPECT_EQ(otherResource.usedBytes(), 128);

  SlabAllocator<char, 64> moved{std::move(left)};
  EXPECT_NE(moved, left);
  EXPECT_EQ(otherResource.usedBytes(), 128);

  moved.deallocate(rightValue, 65);
  right.deallocate(leftValue, 1);
  EXPECT_EQ(resource_->usedBytes(), 0);
  EXPECT_EQ(otherResource.usedBytes(), 0);
}

} // namespace
} // namespace bytedance::bolt::memory
