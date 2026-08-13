#pragma once

#include "bolt/exec/bm/BmRowContainer.h"

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace bytedance::bolt::exec::bm {

class BmRowContainerTest : public testing::Test,
                           public bytedance::bolt::test::VectorTestBase {
 protected:
  void SetUp() override {
    root_ = memory::memoryManager()->addRootPool(
        fmt::format(
            "bm-row-container-{}",
            testing::UnitTest::GetInstance()->current_test_info()->name()),
        64 << 20,
        memory::MemoryReclaimer::create());
    memory::bm::BufferManagerConfig config;
    config.poolName = root_->name();
    config.spillStoreConfig.fileAllocatorConfig =
        memory::bm::test::ValidConfigWithDirectory(
            memory::bm::test::UniqueTempDir(root_->name()));
    bufferManager_ = memory::bm::BufferManager::Create(*root_, std::move(config));
  }

  RowVectorPtr makeInput() {
    return makeRowVector({
        makeFlatVector<int64_t>({10, 3, 7, 3}),
        makeFlatVector<std::string>({"delta", "alpha", "charlie", "bravo"}),
    });
  }

  std::vector<char*> storeAll(BmRowContainer& container, RowVectorPtr input) {
    SelectivityVector rows(input->size());
    std::vector<DecodedVector> decoded(input->childrenSize());
    for (auto i = 0; i < input->childrenSize(); ++i) {
      decoded[i].decode(*input->childAt(i), rows);
    }

    std::vector<char*> rowsOut;
    rowsOut.reserve(input->size());
    for (auto row = 0; row < input->size(); ++row) {
      auto context = container.appendRow();
      for (auto column = 0; column < input->childrenSize(); ++column) {
        container.store(context, decoded[column], row, column);
      }
      rowsOut.push_back(context.row());
    }
    return rowsOut;
  }

  std::shared_ptr<memory::MemoryPool> root_;
  std::shared_ptr<memory::bm::BufferManager> bufferManager_;
};

} // namespace bytedance::bolt::exec::bm
