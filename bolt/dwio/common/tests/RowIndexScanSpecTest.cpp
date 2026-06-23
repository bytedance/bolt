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

#include "bolt/dwio/common/ScanSpec.h"
#include "bolt/type/Type.h"

#include <gtest/gtest.h>

using namespace bytedance::bolt;
using namespace bytedance::bolt::dwio::common;

TEST(RowIndexScanSpecTest, SetAndGetRowIndexAttributes) {
  auto rowType = ROW({{"_row_index", BIGINT()}});
  common::ScanSpec spec("root");
  auto* child = spec.addFieldRecursively("_row_index", *rowType->childAt(0), 0);
  ASSERT_NE(child, nullptr);

  EXPECT_FALSE(child->isRowIndex());
  child->setIsRowIndex(true);
  EXPECT_TRUE(child->isRowIndex());

  EXPECT_EQ(child->getRowIndexBase(), 0);
  child->setRowIndexBase(123);
  EXPECT_EQ(child->getRowIndexBase(), 123);
}
