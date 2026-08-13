#include "bolt/common/memory/Memory.h"

#include <folly/init/Init.h>
#include <gtest/gtest.h>

int main(int argc, char** argv) {
  bytedance::bolt::memory::MemoryManager::testingSetInstance({});
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
