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
#include "bolt/exec/Exchange.h"
#include "bolt/exec/Split.h"
#include "bolt/exec/tests/utils/ConnectorTestBase.h"
namespace bytedance::bolt::exec {
namespace {

class SplitToStringTest : public test::ConnectorTestBase {};

} // namespace

TEST_F(SplitToStringTest, remoteSplit) {
  Split split{std::make_shared<RemoteConnectorSplit>("test")};
  ASSERT_EQ("Split: [Remote: test] -1", split.toString());

  split.groupId = 7;
  ASSERT_EQ("Split: [Remote: test] 7", split.toString());
}

TEST_F(SplitToStringTest, hiveSplit) {
  Split split{makeConnectorSplit(
      "path/to/file.parquet",
      7,
      100,
      connector::makeOptions(
          {{"fileFormat",
            static_cast<int>(dwio::common::FileFormat::PARQUET)}}))};
  ASSERT_EQ("Split: [Hive: path/to/file.parquet 7 - 100] -1", split.toString());

  split.groupId = 7;
  ASSERT_EQ("Split: [Hive: path/to/file.parquet 7 - 100] 7", split.toString());
}
} // namespace bytedance::bolt::exec
