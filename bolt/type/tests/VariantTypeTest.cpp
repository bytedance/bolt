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
