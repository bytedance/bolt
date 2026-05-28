#include "bolt/common/memory/bm/io/InflightRegistry.h"

#include <chrono>
#include <future>
#include <memory>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

namespace {

IoRequest makeRequest(IoPriority priority) {
  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = priority;
  request.fd = 7;
  request.buffer = IoBuffer{std::make_unique<char[]>(4096), 4096, 0, 4096};
  return request;
}

} // namespace

TEST(InflightRegistryTest, AddsAndTakesRequestById) {
  InflightRegistry registry;
  const auto now = std::chrono::steady_clock::now();
  std::promise<IoResult> promise;

  registry.add(
      10,
      InflightIoRequest{
          makeRequest(IoPriority::High), std::move(promise), now, now});

  EXPECT_EQ(registry.size(), 1);
  EXPECT_FALSE(registry.empty());

  auto request = registry.take(10);

  ASSERT_TRUE(request.has_value());
  EXPECT_EQ(request->request.priority, IoPriority::High);
  EXPECT_TRUE(registry.empty());
}

TEST(InflightRegistryTest, TakeUnknownIdReturnsNullopt) {
  InflightRegistry registry;

  auto request = registry.take(99);

  EXPECT_FALSE(request.has_value());
  EXPECT_TRUE(registry.empty());
}
