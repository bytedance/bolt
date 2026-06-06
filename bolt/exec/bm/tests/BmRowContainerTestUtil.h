#pragma once

#include "bolt/common/memory/Memory.h"
#include "bolt/common/memory/bm/BufferManager.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"
#include "bolt/exec/bm/BmRowContainer.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

#include <cstddef>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

namespace bytedance::bolt::exec {

constexpr std::size_t kLargeBlockBytes = 4 * 1024 * 1024;

inline bool isIoUringUnavailable(const std::exception& e) {
  return std::string(e.what()).find("io_uring_queue_init failed") !=
      std::string::npos;
}

class BmRowContainerTest : public testing::Test {
 protected:
  void SetUp() override {
    root_ = memoryManager_.addRootPool(
        fmt::format(
            "bm-row-container-root-{}",
            testing::UnitTest::GetInstance()->current_test_info()->name()),
        256 << 20,
        memory::MemoryReclaimer::create());
    leaf_ = root_->addLeafChild("bm-row-container-vector");
  }

  std::shared_ptr<memory::bm::BufferManager> makeBufferManager(
      const std::string& name,
      memory::MemoryPool* parent = nullptr) {
    const auto directory =
        memory::bm::test::UniqueTempDir(fmt::format("bm-row-container-{}", name));
    std::filesystem::remove_all(directory);

    memory::bm::BufferManagerConfig config;
    config.poolName = fmt::format("bm-row-container-{}", name);
    config.spillStoreConfig.fileAllocatorConfig =
        memory::bm::test::ValidConfigWithDirectory(directory);
    return memory::bm::BufferManager::Create(
        parent == nullptr ? *root_ : *parent, std::move(config));
  }

  memory::MemoryManager memoryManager_;
  std::shared_ptr<memory::MemoryPool> root_;
  std::shared_ptr<memory::MemoryPool> leaf_;
};

template <typename T>
VectorPtr makeFlatVector(
    memory::MemoryPool* pool,
    const TypePtr& type,
    const std::vector<std::optional<T>>& values) {
  auto vector = BaseVector::create(type, values.size(), pool);
  auto* flat = vector->template asFlatVector<T>();
  auto* rawValues = flat->mutableRawValues();
  for (auto i = 0; i < values.size(); ++i) {
    if (values[i].has_value()) {
      rawValues[i] = values[i].value();
    } else {
      vector->setNull(i, true);
    }
  }
  return vector;
}

inline VectorPtr makeBigintVector(
    memory::MemoryPool* pool,
    std::vector<std::optional<int64_t>> values) {
  return makeFlatVector<int64_t>(pool, BIGINT(), values);
}

inline VectorPtr makeIntegerVector(
    memory::MemoryPool* pool,
    std::vector<std::optional<int32_t>> values) {
  return makeFlatVector<int32_t>(pool, INTEGER(), values);
}

inline VectorPtr makeDoubleVector(
    memory::MemoryPool* pool,
    std::vector<std::optional<double>> values) {
  return makeFlatVector<double>(pool, DOUBLE(), values);
}

inline VectorPtr makeVarcharVector(
    memory::MemoryPool* pool,
    std::vector<std::optional<StringView>> values) {
  return makeFlatVector<StringView>(pool, VARCHAR(), values);
}

inline RowVectorPtr makeRowVector(
    memory::MemoryPool* pool,
    std::vector<std::string> names,
    std::vector<VectorPtr> children) {
  std::vector<TypePtr> types;
  types.reserve(children.size());
  for (const auto& child : children) {
    types.push_back(child->type());
  }
  return std::make_shared<RowVector>(
      pool,
      ROW(std::move(names), std::move(types)),
      nullptr,
      children.front()->size(),
      std::move(children));
}

} // namespace bytedance::bolt::exec
