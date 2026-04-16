/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "bolt/type/Type.h"

using namespace bytedance::bolt;

TEST(VariantTypeTest, Basic) {
  auto variantType = VARIANT();
  EXPECT_EQ(variantType->kind(), TypeKind::VARIANT);
  EXPECT_EQ(variantType->toString(), "VARIANT");
  EXPECT_EQ(variantType->name(), "VARIANT");
  EXPECT_EQ(variantType->kindName(), "VARIANT");
  // VARIANT is NOT a primitive type (it has composite storage via
  // VariantVector).
  EXPECT_FALSE(variantType->isPrimitiveType());
  EXPECT_FALSE(variantType->isFixedWidth());
  // VARIANT comparison is byte-level and NOT semantically correct, so
  // isOrderable/isComparable are false to prevent incorrect GROUP BY/JOIN/etc.
  EXPECT_FALSE(variantType->isOrderable());
  EXPECT_FALSE(variantType->isComparable());
  EXPECT_EQ(variantType->size(), 2);
  EXPECT_NO_THROW(variantType->childAt(0));
  EXPECT_EQ(variantType->cppSizeInBytes(), sizeof(VariantValue));
}

TEST(VariantTypeTest, Factory) {
  auto t1 = TypeFactory<TypeKind::VARIANT>::create();
  auto t2 = VARIANT();
  EXPECT_EQ(t1, t2);
  EXPECT_TRUE(t1->equivalent(*t2));
}

TEST(VariantTypeTest, Registration) {
  EXPECT_EQ(mapNameToTypeKind("VARIANT"), TypeKind::VARIANT);
  EXPECT_EQ(mapTypeKindToName(TypeKind::VARIANT), "VARIANT");
  EXPECT_TRUE(hasType("VARIANT"));
}

TEST(VariantTypeTest, Serialization) {
  auto type = VARIANT();
  auto serialized = type->serialize();
  auto deserialized = Type::create(serialized);
  EXPECT_TRUE(type->equivalent(*deserialized));
}
