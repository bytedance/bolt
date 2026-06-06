#include "bolt/exec/bm/BmRowLayout.h"

#include "bolt/type/Type.h"

#include <cstdint>

#include <gtest/gtest.h>

namespace bytedance::bolt::exec {
namespace {

TEST(BmRowLayoutTest, ComputesFixedAndDependentColumnLayout) {
  BmRowLayout layout({BIGINT(), INTEGER()}, {DOUBLE()});

  ASSERT_EQ(3, layout.numColumns());
  EXPECT_EQ(2, layout.keyTypes().size());
  EXPECT_EQ(3, layout.columnTypes().size());
  EXPECT_EQ(TypeKind::BIGINT, layout.typeKindAt(0));
  EXPECT_EQ(TypeKind::INTEGER, layout.typeKindAt(1));
  EXPECT_EQ(TypeKind::DOUBLE, layout.typeKindAt(2));

  EXPECT_EQ(0, layout.columnAt(0).offset());
  EXPECT_EQ(8, layout.columnAt(1).offset());
  EXPECT_EQ(13, layout.columnAt(2).offset());
  EXPECT_EQ(12, layout.columnAt(0).nullByte());
  EXPECT_EQ(12, layout.columnAt(1).nullByte());
  EXPECT_EQ(12, layout.columnAt(2).nullByte());
  EXPECT_EQ(1, layout.columnAt(0).nullMask());
  EXPECT_EQ(2, layout.columnAt(1).nullMask());
  EXPECT_EQ(4, layout.columnAt(2).nullMask());
  EXPECT_EQ(8, layout.alignment());
  EXPECT_EQ(24, layout.fixedRowSize());
  EXPECT_EQ(0, layout.rowSizeOffset());
  EXPECT_EQ(99, layout.freeFlagOffset());
  EXPECT_EQ(12, layout.firstNullByteOffset());
  EXPECT_FALSE(layout.hasVariableWidth());
  EXPECT_EQ(1, layout.initialNulls().size());
}

TEST(BmRowLayoutTest, TracksVariableWidthRowSizeOffset) {
  BmRowLayout layout({VARCHAR()}, {});

  ASSERT_EQ(1, layout.numColumns());
  EXPECT_EQ(TypeKind::VARCHAR, layout.typeKindAt(0));
  EXPECT_EQ(0, layout.columnAt(0).offset());
  EXPECT_EQ(12, layout.columnAt(0).nullByte());
  EXPECT_EQ(1, layout.columnAt(0).nullMask());
  EXPECT_TRUE(layout.hasVariableWidth());
  EXPECT_EQ(13, layout.rowSizeOffset());
  EXPECT_EQ(24, layout.fixedRowSize());
}

} // namespace
} // namespace bytedance::bolt::exec
