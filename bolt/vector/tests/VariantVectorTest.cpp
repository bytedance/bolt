/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/vector/VariantVector.h"
#include <gtest/gtest.h>
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance::bolt;

class VariantVectorTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {}
};

TEST_F(VariantVectorTest, Basic) {
  auto type = VARIANT();
  auto vector = VariantVector::create(pool(), type, 10);

  EXPECT_EQ(vector->size(), 10);
  EXPECT_EQ(vector->encoding(), VectorEncoding::Simple::VARIANT);
  EXPECT_EQ(vector->type(), type);

  auto valueVector = vector->valueChildVector();
  auto metadataVector = vector->metadataChildVector();

  EXPECT_EQ(valueVector->size(), 10);
  EXPECT_EQ(metadataVector->size(), 10);
  EXPECT_EQ(valueVector->typeKind(), TypeKind::VARBINARY);
  EXPECT_EQ(metadataVector->typeKind(), TypeKind::VARBINARY);
}

TEST_F(VariantVectorTest, Slicing) {
  auto type = VARIANT();

  std::vector<std::optional<StringView>> valueData;
  std::vector<std::optional<StringView>> metadataData;
  std::vector<std::string> valueStrings;
  std::vector<std::string> metadataStrings;
  for (int i = 0; i < 10; i++) {
    valueStrings.push_back("val" + std::to_string(i));
    metadataStrings.push_back("meta" + std::to_string(i));
    valueData.push_back(StringView(valueStrings.back()));
    metadataData.push_back(StringView(metadataStrings.back()));
  }

  auto valueVector = makeNullableFlatVector<StringView>(valueData, VARBINARY());
  auto metadataVector =
      makeNullableFlatVector<StringView>(metadataData, VARBINARY());

  std::vector<VectorPtr> children;
  children.push_back(valueVector);
  children.push_back(metadataVector);

  auto vector = std::make_shared<VariantVector>(
      pool(),
      (TypePtr)type,
      BufferPtr(nullptr),
      (size_t)10,
      std::move(children),
      std::nullopt);

  auto sliced = std::dynamic_pointer_cast<VariantVector>(vector->slice(2, 5));
  ASSERT_TRUE(sliced != nullptr);
  EXPECT_EQ(sliced->size(), 5);

  auto slicedValues = sliced->valueChildVector()->asFlatVector<StringView>();
  EXPECT_EQ(slicedValues->valueAt(0), StringView("val2"));
}

TEST_F(VariantVectorTest, HashAll) {
  auto type = VARIANT();

  std::vector<std::optional<StringView>> valueData = {
      StringView("v1"), StringView("v2"), StringView("v3")};
  std::vector<std::optional<StringView>> metadataData = {
      StringView("m1"), StringView("m2"), StringView("m3")};

  auto valueVector = makeNullableFlatVector<StringView>(valueData, VARBINARY());
  auto metadataVector =
      makeNullableFlatVector<StringView>(metadataData, VARBINARY());

  std::vector<VectorPtr> children = {valueVector, metadataVector};
  BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(3), pool());
  auto* rawNulls = nulls->asMutable<uint64_t>();
  bits::fillBits(rawNulls, 0, 3, true);
  bits::setNull(rawNulls, 2, true);

  auto vector = std::make_shared<VariantVector>(
      pool(), type, nulls, 3, std::move(children), 1);

  auto hashes = vector->hashAll();
  ASSERT_EQ(hashes->size(), 3);

  EXPECT_EQ(hashes->valueAt(0), vector->hashValueAt(0));
  EXPECT_EQ(hashes->valueAt(1), vector->hashValueAt(1));
  EXPECT_EQ(hashes->valueAt(2), BaseVector::kNullHash);
}
