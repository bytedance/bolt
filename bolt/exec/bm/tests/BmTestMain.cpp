#include "bolt/common/memory/Memory.h"

#include <gtest/gtest.h>

int main(int argc, char** argv) {
  bytedance::bolt::memory::MemoryManager::testingSetInstance({});
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
