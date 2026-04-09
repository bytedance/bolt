/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
 * SPDX-License-Identifier: Apache-2.0
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
