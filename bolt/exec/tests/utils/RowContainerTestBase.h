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

#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include <random>
#include "bolt/common/file/FileSystems.h"
#include "bolt/dwio/common/tests/utils/BatchMaker.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/VectorHasher.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/serializers/PrestoSerializer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
namespace bytedance::bolt::exec::test {

class RowContainerTestBase : public testing::Test,
                             public bolt::test::VectorTestBase {
 protected:
  void SetUp() override {
    if (!isRegisteredVectorSerde()) {
      bytedance::bolt::serializer::presto::PrestoVectorSerde::
          registerVectorSerde();
    }
    filesystems::registerLocalFileSystem();
  }

  RowVectorPtr makeDataset(
      const TypePtr& rowType,
      const size_t size,
      std::function<void(RowVectorPtr)> customizeData) {
    auto batch = std::static_pointer_cast<RowVector>(
        bolt::test::BatchMaker::createBatch(rowType, size, *pool_));
    if (customizeData) {
      customizeData(batch);
    }
    return batch;
  }

  std::unique_ptr<RowContainer> makeRowContainer(
      const std::vector<TypePtr>& keyTypes,
      const std::vector<TypePtr>& dependentTypes,
      bool isJoinBuild = true,
      bool useListRowIndex = false) {
    auto container = std::make_unique<RowContainer>(
        keyTypes,
        !isJoinBuild,
        std::vector<Accumulator>{},
        dependentTypes,
        isJoinBuild,
        isJoinBuild,
        true,
        true,
        useListRowIndex,
        pool_.get());
    BOLT_CHECK(container->testingMutable());
    return container;
  }
};
} // namespace bytedance::bolt::exec::test
