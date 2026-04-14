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

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>
#include <string_view>
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/FlatVector.h"

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/vector/arrow/Bridge.h"
namespace bytedance::bolt::test {
namespace {

constexpr std::string_view kArrowExtensionNameKey = "ARROW:extension:name";
constexpr std::string_view kSparkVariantExtensionName = "spark.variant";

void appendMetadataInt32(std::string& metadata, int32_t value) {
  metadata.append(reinterpret_cast<const char*>(&value), sizeof(value));
}

std::string makeArrowMetadata(std::string_view key, std::string_view value) {
  std::string metadata;
  appendMetadataInt32(metadata, 1);
  appendMetadataInt32(metadata, static_cast<int32_t>(key.size()));
  metadata.append(key.data(), key.size());
  appendMetadataInt32(metadata, static_cast<int32_t>(value.size()));
  metadata.append(value.data(), value.size());
  return metadata;
}

static void mockRelease(ArrowSchema*) {}

class ArrowBridgeSchemaExportTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void testScalarType(const TypePtr& type, const char* arrowFormat) {
    ArrowSchema arrowSchema;
    exportToArrow(type, arrowSchema);

    verifyScalarType(arrowSchema, arrowFormat);

    arrowSchema.release(&arrowSchema);
    EXPECT_EQ(nullptr, arrowSchema.release);
    EXPECT_EQ(nullptr, arrowSchema.private_data);
  }

  void verifyScalarType(
      const ArrowSchema& arrowSchema,
      const char* arrowFormat,
      const char* name = nullptr) {
    EXPECT_STREQ(arrowFormat, arrowSchema.format);
    if (name == nullptr) {
      EXPECT_EQ(nullptr, arrowSchema.name);
    } else {
      EXPECT_STREQ(name, arrowSchema.name);
    }
    EXPECT_EQ(nullptr, arrowSchema.metadata);
    EXPECT_EQ(arrowSchema.flags | ARROW_FLAG_NULLABLE, ARROW_FLAG_NULLABLE);

    EXPECT_EQ(0, arrowSchema.n_children);
    EXPECT_EQ(nullptr, arrowSchema.children);
    EXPECT_EQ(nullptr, arrowSchema.dictionary);
    EXPECT_NE(nullptr, arrowSchema.release);
  }

  // Doesn't check the actual format string of the scalar leaf types (this is
  // tested by the function above), but tests that the types are nested in the
  // correct way.
  void testNestedType(const TypePtr& type) {
    ArrowSchema arrowSchema;
    exportToArrow(type, arrowSchema);

    verifyNestedType(type, &arrowSchema);

    arrowSchema.release(&arrowSchema);
    EXPECT_EQ(nullptr, arrowSchema.release);
    EXPECT_EQ(nullptr, arrowSchema.private_data);
  }

  void verifyNestedType(const TypePtr& type, ArrowSchema* schema) {
    if (type->kind() == TypeKind::ARRAY) {
      EXPECT_STREQ("+l", schema->format);
    } else if (type->kind() == TypeKind::MAP) {
      EXPECT_STREQ("+m", schema->format);
      ASSERT_EQ(schema->n_children, 1);
      schema = schema->children[0];
      // Map data should be a non-nullable struct type
      ASSERT_EQ(schema->flags & ARROW_FLAG_NULLABLE, 0);
      ASSERT_EQ(schema->n_children, 2);
      // Map data key type should be a non-nullable
      ASSERT_EQ(schema->children[0]->flags & ARROW_FLAG_NULLABLE, 0);
    } else if (type->kind() == TypeKind::ROW) {
      EXPECT_STREQ("+s", schema->format);
    }
    // Scalar type.
    else {
      EXPECT_EQ(nullptr, schema->children);
    }
    ASSERT_EQ(type->size(), schema->n_children);

    // Recurse down the children.
    for (size_t i = 0; i < type->size(); ++i) {
      verifyNestedType(type->childAt(i), schema->children[i]);

      // If this is a rowType, assert that the children returned with the
      // correct name set.
      if (auto rowType = std::dynamic_pointer_cast<const RowType>(type)) {
        EXPECT_EQ(rowType->nameOf(i), std::string(schema->children[i]->name));
      }
    }
  }

  void testConstant(const TypePtr& type, const char* arrowFormat) {
    ArrowSchema arrowSchema;
    const bool isScalar = (type->size() == 0);
    const bool constantSize = 100;

    // If scalar, create the constant vector directly; if complex type, create a
    // complex vector first, then wrap it in a dictionary.
    auto constantVector = isScalar
        ? BaseVector::createConstant(
              type, variant(type->kind()), constantSize, pool_.get())
        : BaseVector::wrapInConstant(
              constantSize,
              3, // index to use for the constant
              BaseVector::create(type, 100, pool_.get()));

    bolt::exportToArrow(constantVector, arrowSchema, options_);

    EXPECT_STREQ("+r", arrowSchema.format);
    EXPECT_EQ(nullptr, arrowSchema.name);

    EXPECT_EQ(2, arrowSchema.n_children);
    EXPECT_NE(nullptr, arrowSchema.children);
    EXPECT_EQ(nullptr, arrowSchema.dictionary);

    // Validate run_ends.
    EXPECT_NE(nullptr, arrowSchema.children[0]);
    const auto& runEnds = *arrowSchema.children[0];

    EXPECT_STREQ("i", runEnds.format);
    EXPECT_STREQ("run_ends", runEnds.name);
    EXPECT_EQ(0, runEnds.n_children);
    EXPECT_EQ(nullptr, runEnds.children);
    EXPECT_EQ(nullptr, runEnds.dictionary);

    // Validate values.
    EXPECT_NE(nullptr, arrowSchema.children[1]);

    if (isScalar) {
      verifyScalarType(*arrowSchema.children[1], arrowFormat, "values");
    } else {
      EXPECT_STREQ(arrowFormat, arrowSchema.children[1]->format);
      verifyNestedType(type, arrowSchema.children[1]);
    }

    arrowSchema.release(&arrowSchema);
    EXPECT_EQ(nullptr, arrowSchema.release);
    EXPECT_EQ(nullptr, arrowSchema.private_data);
  }

  void exportToArrow(const TypePtr& type, ArrowSchema& out) {
    bolt::exportToArrow(
        BaseVector::create(type, 0, pool_.get()), out, options_);
  }

  ArrowSchema makeArrowSchema(const char* format) {
    return ArrowSchema{
        .format = format,
        .name = nullptr,
        .metadata = nullptr,
        .flags = 0,
        .n_children = 0,
        .children = nullptr,
        .dictionary = nullptr,
        .release = mockRelease,
        .private_data = nullptr,
    };
  }

  ArrowOptions options_;
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};

TEST_F(ArrowBridgeSchemaExportTest, scalar) {
  testScalarType(TINYINT(), "c");
  testScalarType(SMALLINT(), "s");
  testScalarType(INTEGER(), "i");
  testScalarType(BIGINT(), "l");

  testScalarType(BOOLEAN(), "b");

  testScalarType(REAL(), "f");
  testScalarType(DOUBLE(), "g");

  testScalarType(VARCHAR(), "u");
  testScalarType(VARBINARY(), "z");

  options_.timestampTimeZone = "+01:0";
  options_.timestampUnit = TimestampUnit::kSecond;
  testScalarType(TIMESTAMP(), "tss:+01:0");
  options_.timestampUnit = TimestampUnit::kMilli;
  testScalarType(TIMESTAMP(), "tsm:+01:0");
  options_.timestampUnit = TimestampUnit::kMicro;
  testScalarType(TIMESTAMP(), "tsu:+01:0");
  options_.timestampUnit = TimestampUnit::kNano;
  testScalarType(TIMESTAMP(), "tsn:+01:0");

  testScalarType(DATE(), "tdD");
  testScalarType(INTERVAL_YEAR_MONTH(), "tiM");

  testScalarType(DECIMAL(10, 4), "d:10,4");
  testScalarType(DECIMAL(20, 15), "d:20,15");

  testScalarType(UNKNOWN(), "n");
}

TEST_F(ArrowBridgeSchemaExportTest, nested) {
  // Array.
  testNestedType(ARRAY(INTEGER()));
  testNestedType(ARRAY(VARCHAR()));
  testNestedType(ARRAY(ARRAY(TINYINT())));
  testNestedType(ARRAY(ARRAY(ARRAY(ARRAY(BOOLEAN())))));

  // Map.
  testNestedType(MAP(INTEGER(), DOUBLE()));
  testNestedType(MAP(VARBINARY(), BOOLEAN()));
  testNestedType(MAP(VARBINARY(), MAP(SMALLINT(), REAL())));
  testNestedType(MAP(VARBINARY(), MAP(SMALLINT(), MAP(INTEGER(), BIGINT()))));

  // Row.
  testNestedType(ROW({}));
  testNestedType(ROW({INTEGER()}));
  testNestedType(ROW({INTEGER(), DOUBLE()}));
  testNestedType(
      ROW({INTEGER(), DOUBLE(), ROW({BIGINT(), REAL(), BOOLEAN()})}));

  // Row with names.
  testNestedType(ROW({"my_col"}, {INTEGER()}));
  testNestedType(ROW({"my_col", "my_other_col"}, {INTEGER(), VARCHAR()}));

  // Mix and match.
  testNestedType(
      ROW({"c1", "c2", "c3"},
          {
              ARRAY(INTEGER()),
              MAP(ROW({VARBINARY(), SMALLINT()}), BOOLEAN()),
              ARRAY(MAP(INTEGER(), VARCHAR())),
          }));
}

TEST_F(ArrowBridgeSchemaExportTest, variant) {
  ArrowSchema arrowSchema;
  exportToArrow(VARIANT(), arrowSchema);

  EXPECT_STREQ("+s", arrowSchema.format);
  ASSERT_NE(nullptr, arrowSchema.metadata);
  ASSERT_EQ(2, arrowSchema.n_children);
  ASSERT_NE(nullptr, arrowSchema.children);

  EXPECT_STREQ("z", arrowSchema.children[0]->format);
  EXPECT_STREQ("value", arrowSchema.children[0]->name);
  EXPECT_STREQ("z", arrowSchema.children[1]->format);
  EXPECT_STREQ("metadata", arrowSchema.children[1]->name);

  auto roundTripped = importFromArrow(arrowSchema);
  EXPECT_EQ(*VARIANT(), *roundTripped);

  arrowSchema.release(&arrowSchema);
  EXPECT_EQ(nullptr, arrowSchema.release);
  EXPECT_EQ(nullptr, arrowSchema.private_data);
}

TEST_F(ArrowBridgeSchemaExportTest, constant) {
  testConstant(TINYINT(), "c");
  testConstant(INTEGER(), "i");
  testConstant(BOOLEAN(), "b");
  testConstant(DOUBLE(), "g");
  testConstant(VARCHAR(), "u");
  testConstant(DATE(), "tdD");
  testConstant(INTERVAL_YEAR_MONTH(), "tiM");
  testConstant(UNKNOWN(), "n");

  testConstant(ARRAY(INTEGER()), "+l");
  testConstant(ARRAY(UNKNOWN()), "+l");
  testConstant(MAP(BOOLEAN(), REAL()), "+m");
  testConstant(MAP(UNKNOWN(), REAL()), "+m");
  testConstant(ROW({TIMESTAMP(), DOUBLE()}), "+s");
  testConstant(ROW({UNKNOWN(), UNKNOWN()}), "+s");
  testConstant(ROW({UNKNOWN(), UNKNOWN()}), "+s");
}

class ArrowBridgeSchemaImportTest : public ArrowBridgeSchemaExportTest {
 protected:
  TypePtr testSchemaImport(const char* format) {
    auto arrowSchema = makeArrowSchema(format);
    auto type = importFromArrow(arrowSchema);
    arrowSchema.release(&arrowSchema);
    return type;
  }

  TypePtr testSchemaDictionaryImport(const char* indexFmt, ArrowSchema schema) {
    auto dictionarySchema = makeArrowSchema(indexFmt);
    dictionarySchema.dictionary = &schema;

    auto type = importFromArrow(dictionarySchema);
    dictionarySchema.release(&dictionarySchema);
    return type;
  }

  TypePtr testSchemaReeImport(const char* valuesFmt) {
    auto reeSchema = makeArrowSchema("+r");
    auto runsSchema = makeArrowSchema("i");
    auto valuesSchema = makeArrowSchema(valuesFmt);

    std::vector<ArrowSchema*> schemas{&runsSchema, &valuesSchema};
    reeSchema.n_children = 2;
    reeSchema.children = schemas.data();

    auto type = importFromArrow(reeSchema);
    reeSchema.release(&reeSchema);
    return type;
  }

  ArrowSchema makeComplexArrowSchema(
      std::vector<ArrowSchema>& schemas,
      std::vector<ArrowSchema*>& schemaPtrs,
      std::vector<ArrowSchema>& mapSchemas,
      std::vector<ArrowSchema*>& mapSchemaPtrs,
      const char* mainFormat,
      const std::vector<const char*>& childrenFormat,
      const std::vector<const char*>& colNames = {}) {
    schemas.clear();
    schemaPtrs.clear();
    mapSchemas.clear();
    mapSchemaPtrs.clear();
    schemas.resize(childrenFormat.size());
    schemaPtrs.resize(childrenFormat.size());

    for (size_t i = 0; i < childrenFormat.size(); ++i) {
      schemas[i] = makeArrowSchema(childrenFormat[i]);
      if (colNames.size() > i) {
        schemas[i].name = colNames[i];
      }
      schemaPtrs[i] = &schemas[i];
    }

    auto mainSchema = makeArrowSchema(mainFormat);
    if (strcmp(mainFormat, "+m") == 0) {
      // Arrow wraps key and value in a struct.
      mapSchemas.resize(1);
      mapSchemaPtrs.resize(1);
      mapSchemas[0] = makeArrowSchema("+s");
      auto* child = &mapSchemas[0];
      mapSchemaPtrs[0] = &mapSchemas[0];
      child->n_children = schemaPtrs.size();
      child->children = schemaPtrs.data();
      mainSchema.n_children = 1;
      mainSchema.children = mapSchemaPtrs.data();
    } else {
      mainSchema.n_children = (int64_t)schemaPtrs.size();
      mainSchema.children = schemaPtrs.data();
    }

    return mainSchema;
  }

  TypePtr testSchemaImportComplex(
      const char* mainFormat,
      const std::vector<const char*>& childrenFormat,
      const std::vector<const char*>& colNames = {}) {
    std::vector<ArrowSchema> schemas;
    std::vector<ArrowSchema> mapSchemas;
    std::vector<ArrowSchema*> schemaPtrs;
    std::vector<ArrowSchema*> mapSchemaPtrs;
    auto type = importFromArrow(makeComplexArrowSchema(
        schemas,
        schemaPtrs,
        mapSchemas,
        mapSchemaPtrs,
        mainFormat,
        childrenFormat,
        colNames));
    return type;
  }
};

TEST_F(ArrowBridgeSchemaImportTest, scalar) {
  EXPECT_EQ(*BOOLEAN(), *testSchemaImport("b"));
  EXPECT_EQ(*TINYINT(), *testSchemaImport("c"));
  EXPECT_EQ(*SMALLINT(), *testSchemaImport("s"));
  EXPECT_EQ(*INTEGER(), *testSchemaImport("i"));
  EXPECT_EQ(*BIGINT(), *testSchemaImport("l"));
  EXPECT_EQ(*REAL(), *testSchemaImport("f"));
  EXPECT_EQ(*DOUBLE(), *testSchemaImport("g"));

  EXPECT_EQ(*VARCHAR(), *testSchemaImport("u"));
  EXPECT_EQ(*VARCHAR(), *testSchemaImport("U"));
  EXPECT_EQ(*VARBINARY(), *testSchemaImport("z"));
  EXPECT_EQ(*VARBINARY(), *testSchemaImport("Z"));

  // Temporal.
  EXPECT_EQ(*TIMESTAMP(), *testSchemaImport("tsu:"));
  EXPECT_EQ(*DATE(), *testSchemaImport("tdD"));
  EXPECT_EQ(*INTERVAL_YEAR_MONTH(), *testSchemaImport("tiM"));

  EXPECT_EQ(*DECIMAL(10, 4), *testSchemaImport("d:10,4"));
  EXPECT_EQ(*DECIMAL(20, 15), *testSchemaImport("d:20,15"));
  BOLT_ASSERT_THROW(
      *testSchemaImport("d2,15"),
      "Unable to convert 'd2,15' ArrowSchema decimal format to Bolt decimal");
  EXPECT_EQ(*DECIMAL(10, 4), *testSchemaImport("d:10,4,128"));
  BOLT_ASSERT_THROW(
      *testSchemaImport("d:10,4,256"),
      "Conversion failed for 'd:10,4,256'. Bolt decimal does not support custom bitwidth.");
  BOLT_ASSERT_THROW(
      *testSchemaImport("d:10,4,"),
      "Unable to convert 'd:10,4,' ArrowSchema decimal format to Bolt decimal");
  BOLT_ASSERT_THROW(
      *testSchemaImport("d:10"),
      "Unable to convert 'd:10' ArrowSchema decimal format to Bolt decimal");
  BOLT_ASSERT_THROW(
      *testSchemaImport("d:"),
      "Unable to convert 'd:' ArrowSchema decimal format to Bolt decimal");
  BOLT_ASSERT_THROW(
      *testSchemaImport("d:10,"),
      "Unable to convert 'd:10,' ArrowSchema decimal format to Bolt decimal");
}

TEST_F(ArrowBridgeSchemaImportTest, complexTypes) {
  // Array.
  EXPECT_EQ(*ARRAY(BIGINT()), *testSchemaImportComplex("+l", {"l"}));
  EXPECT_EQ(*ARRAY(TIMESTAMP()), *testSchemaImportComplex("+l", {"tsn:"}));
  EXPECT_EQ(*ARRAY(DATE()), *testSchemaImportComplex("+l", {"tdD"}));
  EXPECT_EQ(
      *ARRAY(INTERVAL_YEAR_MONTH()), *testSchemaImportComplex("+l", {"tiM"}));
  EXPECT_EQ(*ARRAY(VARCHAR()), *testSchemaImportComplex("+l", {"U"}));

  EXPECT_EQ(*ARRAY(DECIMAL(10, 4)), *testSchemaImportComplex("+l", {"d:10,4"}));
  EXPECT_EQ(
      *ARRAY(DECIMAL(20, 15)), *testSchemaImportComplex("+l", {"d:20,15"}));

  // Map.
  EXPECT_EQ(
      *MAP(VARCHAR(), BOOLEAN()), *testSchemaImportComplex("+m", {"U", "b"}));
  EXPECT_EQ(
      *MAP(SMALLINT(), REAL()), *testSchemaImportComplex("+m", {"s", "f"}));

  // Row/struct.
  EXPECT_EQ(
      *ROW({SMALLINT(), REAL()}), *testSchemaImportComplex("+s", {"s", "f"}));
  EXPECT_EQ(
      *ROW({SMALLINT(), REAL(), VARCHAR(), BOOLEAN()}),
      *testSchemaImportComplex("+s", {"s", "f", "u", "b"}));

  // Named
  EXPECT_EQ(
      *ROW({"col1", "col2"}, {SMALLINT(), REAL()}),
      *testSchemaImportComplex("+s", {"s", "f"}, {"col1", "col2"}));
}

TEST_F(ArrowBridgeSchemaImportTest, variantExtensionMetadata) {
  std::vector<ArrowSchema> schemas;
  std::vector<ArrowSchema> mapSchemas;
  std::vector<ArrowSchema*> schemaPtrs;
  std::vector<ArrowSchema*> mapSchemaPtrs;

  auto rowSchema = makeComplexArrowSchema(
      schemas,
      schemaPtrs,
      mapSchemas,
      mapSchemaPtrs,
      "+s",
      {"z", "z"},
      {"value", "metadata"});
  auto metadata =
      makeArrowMetadata(kArrowExtensionNameKey, kSparkVariantExtensionName);
  rowSchema.metadata = metadata.data();
  EXPECT_EQ(*VARIANT(), *importFromArrow(rowSchema));

  auto plainStruct = makeComplexArrowSchema(
      schemas,
      schemaPtrs,
      mapSchemas,
      mapSchemaPtrs,
      "+s",
      {"z", "z"},
      {"value", "metadata"});
  plainStruct.metadata = nullptr;
  EXPECT_EQ(
      *ROW({"value", "metadata"}, {VARBINARY(), VARBINARY()}),
      *importFromArrow(plainStruct));
}

TEST_F(ArrowBridgeSchemaImportTest, unsupported) {
  EXPECT_THROW(testSchemaImport("C"), BoltUserError);
  EXPECT_THROW(testSchemaImport("S"), BoltUserError);
  EXPECT_THROW(testSchemaImport("I"), BoltUserError);
  EXPECT_THROW(testSchemaImport("L"), BoltUserError);
  EXPECT_THROW(testSchemaImport("e"), BoltUserError);

  EXPECT_THROW(testSchemaImport("w:42"), BoltUserError);

  EXPECT_THROW(testSchemaImport("tdm"), BoltUserError);
  EXPECT_THROW(testSchemaImport("tts"), BoltUserError);
  EXPECT_THROW(testSchemaImport("ttm"), BoltUserError);
  EXPECT_THROW(testSchemaImport("tDs"), BoltUserError);

  EXPECT_THROW(testSchemaImport("+"), BoltUserError);
  EXPECT_THROW(testSchemaImport("+L"), BoltUserError);
  EXPECT_THROW(testSchemaImport("+b"), BoltUserError);
  EXPECT_THROW(testSchemaImport("+z"), BoltUserError);
  EXPECT_THROW(testSchemaImport("+u"), BoltUserError);
  EXPECT_THROW(testSchemaImport("+w"), BoltUserError);
}

class ArrowBridgeSchemaTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void roundtripTest(const TypePtr& inputType) {
    ArrowSchema arrowSchema;
    exportToArrow(inputType, arrowSchema);
    auto outputType = importFromArrow(arrowSchema);
    arrowSchema.release(&arrowSchema);
    EXPECT_EQ(*inputType, *outputType);
  }

  void exportToArrow(const TypePtr& type, ArrowSchema& out) {
    bolt::exportToArrow(
        BaseVector::create(type, 0, pool_.get()), out, options_);
  }

  ArrowOptions options_;
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};

TEST_F(ArrowBridgeSchemaTest, roundtrip) {
  roundtripTest(BOOLEAN());
  roundtripTest(VARCHAR());
  roundtripTest(REAL());
  roundtripTest(VARIANT());
  roundtripTest(ARRAY(DOUBLE()));
  roundtripTest(ARRAY(ARRAY(ARRAY(ARRAY(VARBINARY())))));
  roundtripTest(MAP(VARCHAR(), REAL()));
  roundtripTest(MAP(VARCHAR(), ARRAY(BOOLEAN())));
  roundtripTest(MAP(VARCHAR(), ARRAY(MAP(ARRAY(BIGINT()), BOOLEAN()))));
  roundtripTest(ROW({VARBINARY(), TINYINT(), SMALLINT()}));
  roundtripTest(ROW({VARBINARY(), ROW({DOUBLE(), VARBINARY()}), SMALLINT()}));
  roundtripTest(ROW({
      ARRAY(VARBINARY()),
      MAP(REAL(), ARRAY(DOUBLE())),
      ROW({"a", "b"}, {DOUBLE(), VARBINARY()}),
      INTEGER(),
  }));
}

TEST_F(ArrowBridgeSchemaTest, validateInArrow) {
  const std::pair<TypePtr, std::shared_ptr<arrow::DataType>> kTypes[] = {
      {BOOLEAN(), arrow::boolean()},
      {VARCHAR(), arrow::utf8()},
      {DECIMAL(10, 4), arrow::decimal(10, 4)},
      {DECIMAL(20, 15), arrow::decimal(20, 15)},
      {ARRAY(DOUBLE()), arrow::list(arrow::float64())},
      {ARRAY(ARRAY(DOUBLE())), arrow::list(arrow::list(arrow::float64()))},
      {MAP(VARCHAR(), REAL()), arrow::map(arrow::utf8(), arrow::float32())},
      {ROW({"c0", "c1"}, {VARCHAR(), BIGINT()}),
       arrow::struct_(
           {arrow::field("c0", arrow::utf8()),
            arrow::field("c1", arrow::int64())})},
  };
  for (auto& [tv, ta] : kTypes) {
    VLOG(1) << "Validating conversion between " << tv->toString() << " and "
            << ta->ToString();
    ArrowSchema schema;
    exportToArrow(tv, schema);
    ASSERT_OK_AND_ASSIGN(auto actual, arrow::ImportType(&schema));
    ASSERT_FALSE(schema.release);
    EXPECT_EQ(*actual, *ta);
  }
}

TEST_F(ArrowBridgeSchemaImportTest, dictionaryTypeTest) {
  // Primitive types
  EXPECT_EQ(DOUBLE(), testSchemaDictionaryImport("i", makeArrowSchema("g")));
  EXPECT_EQ(BOOLEAN(), testSchemaDictionaryImport("i", makeArrowSchema("b")));
  EXPECT_EQ(TINYINT(), testSchemaDictionaryImport("i", makeArrowSchema("c")));
  EXPECT_EQ(INTEGER(), testSchemaDictionaryImport("i", makeArrowSchema("i")));
  EXPECT_EQ(SMALLINT(), testSchemaDictionaryImport("i", makeArrowSchema("s")));
  EXPECT_EQ(BIGINT(), testSchemaDictionaryImport("i", makeArrowSchema("l")));
  EXPECT_EQ(REAL(), testSchemaDictionaryImport("i", makeArrowSchema("f")));
  EXPECT_EQ(VARCHAR(), testSchemaDictionaryImport("i", makeArrowSchema("u")));

  std::vector<ArrowSchema> schemas;
  std::vector<ArrowSchema> mapSchemas;
  std::vector<ArrowSchema*> mapSchemaPtrs;
  std::vector<ArrowSchema*> schemaPtrs;

  // Arrays
  EXPECT_EQ(
      *ARRAY(BIGINT()),
      *testSchemaDictionaryImport(
          "i",
          makeComplexArrowSchema(
              schemas, schemaPtrs, mapSchemas, mapSchemaPtrs, "+l", {"l"})));
  EXPECT_EQ(
      *ARRAY(TIMESTAMP()),
      *testSchemaDictionaryImport(
          "i",
          makeComplexArrowSchema(
              schemas, schemaPtrs, mapSchemas, mapSchemaPtrs, "+l", {"ts"})));
  EXPECT_EQ(
      *ARRAY(DATE()),
      *testSchemaDictionaryImport(
          "i",
          makeComplexArrowSchema(
              schemas, schemaPtrs, mapSchemas, mapSchemaPtrs, "+l", {"tdD"})));
  EXPECT_EQ(
      *ARRAY(INTERVAL_YEAR_MONTH()),
      *testSchemaDictionaryImport(
          "i",
          makeComplexArrowSchema(
              schemas, schemaPtrs, mapSchemas, mapSchemaPtrs, "+l", {"tiM"})));
  EXPECT_EQ(
      *ARRAY(VARCHAR()),
      *testSchemaDictionaryImport(
          "i",
          makeComplexArrowSchema(
              schemas, schemaPtrs, mapSchemas, mapSchemaPtrs, "+l", {"U"})));

  // Maps
  EXPECT_EQ(
      *MAP(VARCHAR(), BOOLEAN()),
      *testSchemaDictionaryImport(
          "i",
          makeComplexArrowSchema(
              schemas,
              schemaPtrs,
              mapSchemas,
              mapSchemaPtrs,
              "+m",
              {"U", "b"})));
  EXPECT_EQ(
      *MAP(SMALLINT(), REAL()),
      *testSchemaDictionaryImport(
          "i",
          makeComplexArrowSchema(
              schemas,
              schemaPtrs,
              mapSchemas,
              mapSchemaPtrs,
              "+m",
              {"s", "f"})));

  // Rows
  EXPECT_EQ(
      *ROW({SMALLINT(), REAL()}),
      *testSchemaDictionaryImport(
          "i",
          makeComplexArrowSchema(
              schemas,
              schemaPtrs,
              mapSchemas,
              mapSchemaPtrs,
              "+s",
              {"s", "f"})));

  // Named Row
  EXPECT_EQ(
      *ROW({"col1", "col2"}, {SMALLINT(), REAL()}),
      *testSchemaDictionaryImport(
          "i",
          makeComplexArrowSchema(
              schemas,
              schemaPtrs,
              mapSchemas,
              mapSchemaPtrs,
              "+s",
              {"s", "f"},
              {"col1", "col2"})));
}

TEST_F(ArrowBridgeSchemaTest, exportToArrowWithConstantEncoding) {
  auto* pool = &memory::deprecatedSharedLeafPool();

  auto elementType = INTEGER();
  auto arrayType = ARRAY(elementType);

  auto innerArray = BaseVector::create(elementType, 1, pool);
  auto offsets = allocateOffsets(1, pool);
  auto sizes = allocateSizes(1, pool);

  innerArray->asUnchecked<FlatVector<int32_t>>()->set(0, 42);
  sizes->asMutable<vector_size_t>()[0] = 1;
  offsets->asMutable<vector_size_t>()[0] = 0;

  auto arrayVector = std::make_shared<ArrayVector>(
      pool,
      arrayType,
      nullptr, // nulls
      1, // size
      std::move(offsets),
      std::move(sizes),
      std::move(innerArray));

  auto constantArrayVector = BaseVector::wrapInConstant(10, 0, arrayVector);
  EXPECT_EQ(constantArrayVector->encoding(), VectorEncoding::Simple::CONSTANT);

  auto boolType = BOOLEAN();
  auto constantBoolVector =
      BaseVector::createConstant(boolType, true, 10, pool);
  EXPECT_EQ(constantBoolVector->encoding(), VectorEncoding::Simple::CONSTANT);

  auto doubleType = DOUBLE();
  auto constantDoubleVector =
      BaseVector::createConstant(doubleType, 1.23, 10, pool);
  EXPECT_EQ(constantDoubleVector->encoding(), VectorEncoding::Simple::CONSTANT);

  // Add ROW type constant vector
  auto stringVector = BaseVector::create(VARCHAR(), 10, pool);
  for (int i = 0; i < 10; ++i) {
    stringVector->asUnchecked<FlatVector<StringView>>()->set(i, "Traffic");
  }

  auto intVector1 = BaseVector::create(BIGINT(), 10, pool);
  for (int i = 0; i < 10; ++i) {
    intVector1->asUnchecked<FlatVector<int64_t>>()->set(i, 1);
  }

  auto intVector2 = BaseVector::create(BIGINT(), 10, pool);
  for (int i = 0; i < 10; ++i) {
    intVector2->asUnchecked<FlatVector<int64_t>>()->set(i, 2);
  }

  std::vector<VectorPtr> children = {stringVector, intVector1, intVector2};
  auto rowType = ROW(
      {"string_col", "int_col1", "int_col2"}, {VARCHAR(), BIGINT(), BIGINT()});
  auto rowVector = std::make_shared<RowVector>(
      pool, rowType, nullptr, 10, std::move(children));
  auto constantRowVector = BaseVector::wrapInConstant(10, 0, rowVector);
  EXPECT_EQ(constantRowVector->encoding(), VectorEncoding::Simple::CONSTANT);

  // Test non-REE for exporting constant complex vector
  ArrowSchema arrowSchemaArray;
  EXPECT_NO_THROW(bolt::exportToArrow(
      constantArrayVector, arrowSchemaArray, {.flattenConstant = true}));

  ArrowSchema arrowSchemaBool;
  EXPECT_NO_THROW(bolt::exportToArrow(constantBoolVector, arrowSchemaBool, {}));

  // Test non-REE for exporting constant scalar vector
  ArrowSchema arrowSchemaDouble;
  EXPECT_NO_THROW(bolt::exportToArrow(
      constantDoubleVector, arrowSchemaDouble, {.flattenConstant = true}));

  ArrowSchema arrowSchemaRow;
  EXPECT_NO_THROW(bolt::exportToArrow(constantRowVector, arrowSchemaRow, {}));

  // Roundtrip: Import back to Bolt types
  auto importedArrayType = importFromArrow(arrowSchemaArray);
  auto importedBoolType = importFromArrow(arrowSchemaBool);
  auto importedDoubleType = importFromArrow(arrowSchemaDouble);
  auto importedRowType = importFromArrow(arrowSchemaRow);

  // Validate that the types match after roundtrip
  EXPECT_EQ(*constantArrayVector->type(), *importedArrayType);
  EXPECT_EQ(*constantBoolVector->type(), *importedBoolType);
  EXPECT_EQ(*constantDoubleVector->type(), *importedDoubleType);
  EXPECT_EQ(*constantRowVector->type(), *importedRowType);

  // Clean up the ArrowSchemas
  arrowSchemaArray.release(&arrowSchemaArray);
  arrowSchemaBool.release(&arrowSchemaBool);
  arrowSchemaDouble.release(&arrowSchemaDouble);
  arrowSchemaRow.release(&arrowSchemaRow);
}

TEST_F(ArrowBridgeSchemaImportTest, reeTypeTest) {
  // Ensure REE just returns the type of the inner `values` child.
  EXPECT_EQ(DOUBLE(), testSchemaReeImport("g"));
  EXPECT_EQ(INTEGER(), testSchemaReeImport("i"));
  EXPECT_EQ(BIGINT(), testSchemaReeImport("l"));
}

} // namespace
} // namespace bytedance::bolt::test
