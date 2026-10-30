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
 * 2026-10-29.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/file/FileSystems.h"
#include "bolt/exec/Driver.h"
#include "bolt/exec/OrderBy.h"
#include "bolt/exec/SpillableWindowBuild.h"
#include "bolt/exec/Window.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/functions/lib/window/tests/WindowTestBase.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "bolt/functions/sparksql/window/WindowFunctionsRegistration.h"

using namespace bytedance::bolt::exec::test;

namespace bytedance::bolt::window::test {

namespace {

class WindowStorageCleanupTest : public WindowTestBase {
 protected:
  RowVectorPtr input(
      vector_size_t rows,
      const std::string& key,
      bool nullKey,
      bool nullValues) {
    return makeRowVector(
        {"partition_key",
         "value",
         "row_id",
         "payload",
         "tag1",
         "tag2",
         "tag3",
         "count1",
         "count2",
         "flag"},
        {makeFlatVector<StringView>(
             rows,
             [&](auto) { return StringView(key); },
             [nullKey](auto) { return nullKey; }),
         makeFlatVector<StringView>(
             rows,
             [](auto row) { return StringView(row % 2 ? "bravo" : "alpha"); },
             [nullValues](auto row) { return nullValues || row % 7 == 0; }),
         makeFlatVector<int64_t>(
             rows, [](auto row) { return row; }, nullEvery(5)),
         makeFlatVector<std::string>(
             rows,
             [](auto row) { return std::string(96, 'a' + row % 26); },
             nullEvery(11)),
         makeFlatVector<StringView>(
             rows, [](auto) { return StringView("tag-a"); }),
         makeFlatVector<StringView>(
             rows, [](auto) { return StringView("tag-b"); }),
         makeFlatVector<StringView>(
             rows, [](auto) { return StringView("type"); }),
         makeFlatVector<int64_t>(rows, [](auto row) { return row % 3; }),
         makeFlatVector<int64_t>(rows, [](auto row) { return row % 7; }),
         makeFlatVector<int32_t>(rows, [](auto row) { return row % 2; })});
  }

  void checkMinimum(const VectorPtr& result, bool nullValues) {
    auto* minimum = result->as<SimpleVector<StringView>>();
    ASSERT_NE(minimum, nullptr);
    for (vector_size_t row = 0; row < result->size(); ++row) {
      ASSERT_EQ(minimum->isNullAt(row), nullValues);
      if (!nullValues) {
        ASSERT_EQ(minimum->valueAt(row), StringView("alpha"));
      }
    }
  }
};

class WindowBuildMemoryProbe : public exec::SpillableWindowBuild<false> {
 public:
  using exec::SpillableWindowBuild<false>::SpillableWindowBuild;

  uint64_t residentRows() const {
    return data_->numRows();
  }
  uint64_t allocatedBytes() const {
    return data_->allocatedBytes();
  }
  column_index_t reorderedChannel(column_index_t channel) const {
    return inversedInputChannels_[channel];
  }

  void spillWithIncomingBatchDecoded(const RowVectorPtr& incoming) {
    for (auto i = 0; i < inputChannels_.size(); ++i) {
      decodedInputVectors_[i].decode(*incoming->childAt(inputChannels_[i]));
    }
    spill();
    // Arbitration can spill after addInput has decoded the next batch.
    // Restoring the copied boundary row must leave every decoder untouched.
    for (const auto& decoded : decodedInputVectors_) {
      ASSERT_EQ(decoded.size(), incoming->size());
    }
    const auto& ids = decodedInputVectors_[reorderedChannel(2)];
    ASSERT_TRUE(ids.isNullAt(0));
    ASSERT_EQ(ids.valueAt<int64_t>(1), 1);
    ASSERT_EQ(ids.valueAt<int64_t>(incoming->size() - 1), incoming->size() - 1);
  }
};

TEST_F(WindowStorageCleanupTest, releaseCopiedBoundaryRowStorage) {
  filesystems::registerLocalFileSystem();
  for (bool nonLeadingKey : {false, true}) {
    for (bool nullKey : {false, true}) {
      for (bool nullValues : {false, true}) {
        SCOPED_TRACE(fmt::format(
            "nonLeadingKey={}, nullKey={}, nullValues={}",
            nonLeadingKey,
            nullKey,
            nullValues));
        auto first = input(131072, "same-partition", nullKey, nullValues);
        auto second = input(8192, "same-partition", nullKey, nullValues);
        const auto key = nonLeadingKey ? "tag1" : "partition_key";
        auto node = std::dynamic_pointer_cast<const core::WindowNode>(
            PlanBuilder()
                .values({first, second})
                .streamingWindow({fmt::format(
                    "min(value) over (partition by {} ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
                    key)})
                .planNode());
        ASSERT_TRUE(node->inputsSorted());
        auto root = memory::memoryManager()->addRootPool();
        auto buildPool = root->addLeafChild("boundary-window");
        auto dir = TempDirectoryPath::create();
        common::SpillConfig config(
            [&]() -> const std::string& { return dir->path; },
            [](uint64_t) {},
            "boundary-window",
            0,
            false,
            0,
            nullptr,
            5,
            10,
            0,
            0,
            0,
            0,
            0,
            0,
            "none",
            {},
            "disabled",
            {},
            false);
        tsan_atomic<bool> nonReclaimable{false};
        WindowBuildMemoryProbe build(
            node,
            buildPool.get(),
            &config,
            &nonReclaimable,
            4096,
            1 << 20,
            0,
            false);
        build.setNumRowsPerOutput(4096);
        build.setIfAggWindowFunc(true);
        HashStringAllocator allocator(memory::spillMemoryPool());
        std::vector<std::unique_ptr<exec::WindowFunction>> functions;
        functions.push_back(exec::WindowFunction::create(
            "min",
            {{VARCHAR(), nullptr, 1}},
            VARCHAR(),
            false,
            memory::spillMemoryPool(),
            &allocator,
            core::QueryConfig({})));
        build.setWindowFunction(std::move(functions));
        build.addInput(first);
        const auto allocatedBefore = build.allocatedBytes();
        build.spillWithIncomingBatchDecoded(second);
        ASSERT_EQ(build.residentRows(), 1);
        ASSERT_LT(build.allocatedBytes(), allocatedBefore / 2);
        build.addInput(second);
        build.noMoreInput();
        auto partition = build.nextPartition();
        ASSERT_TRUE(partition->isSpilled());
        RowVectorPtr output;
        bool isEnd = false;
        vector_size_t totalRows = 0;
        vector_size_t nullIds = 0;
        while (partition->getBatch(output, isEnd)) {
          auto result = BaseVector::create(VARCHAR(), output->size(), pool());
          partition->getWindowFunctionResults(result, output->size(), 0, true);
          checkMinimum(result, nullValues);
          auto* ids = output->childAt(2)->as<SimpleVector<int64_t>>();
          auto* payload = output->childAt(3)->as<SimpleVector<StringView>>();
          for (vector_size_t row = 0; row < output->size(); ++row) {
            ASSERT_EQ(output->childAt(0)->isNullAt(row), nullKey);
            if (ids->isNullAt(row)) {
              ++nullIds;
              continue;
            }
            const auto id = ids->valueAt(row);
            ASSERT_GE(id, 0);
            ASSERT_LT(id, 131072);
            ASSERT_EQ(payload->isNullAt(row), id % 11 == 0);
            if (!payload->isNullAt(row)) {
              ASSERT_EQ(
                  payload->valueAt(row).str(), std::string(96, 'a' + id % 26));
            }
          }
          totalRows += output->size();
          if (isEnd)
            break;
        }
        ASSERT_EQ(totalRows, 139264);
        ASSERT_EQ(nullIds, (131072 + 4) / 5 + (8192 + 4) / 5);
        partition.reset();
        output.reset();
        build.finish();
      }
    }
  }
}

} // namespace
} // namespace bytedance::bolt::window::test
