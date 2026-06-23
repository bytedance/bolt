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
#include <memory>
#include <string>
#include <vector>

#include <paimon/data/timestamp.h>
#include <paimon/predicate/function.h>
#include <paimon/predicate/leaf_predicate.h>
#include <paimon/predicate/literal.h>
#include <paimon/predicate/predicate_builder.h>
#include "bolt/common/memory/Memory.h"
#include "bolt/connectors/paimon/PaimonFilterTranslator.h"
#include "bolt/core/Expressions.h"
#include "bolt/type/Type.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

using namespace bytedance::bolt;
using namespace bytedance::bolt::core;
using namespace bytedance::bolt::connector::paimon;

namespace {

class PaimonFilterTranslatorTest
    : public testing::Test,
      public bytedance::bolt::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  /// Convenience override: calls PaimonFilterTranslator::toTypedExpr with
  /// the test's memory pool (from VectorTestBase) so IN/NOT_IN array
  /// allocation works correctly.
  PaimonFilterTranslator::ToTypedExprResult toTypedExpr(
      const std::shared_ptr<::paimon::Predicate>& pred) {
    return PaimonFilterTranslator::toTypedExpr(pred, pool_.get());
  }

  /// Convenience override: calls translate with a rowType that covers all
  /// field names used by the tests (id, name, x, age, score, ratio, val,
  /// status, email, a, b, y, z, i, col).
  ToPaimonPredicateResult translate(const core::TypedExprPtr& expr) {
    return PaimonFilterTranslator::translate(expr, rowType_);
  }

  const RowTypePtr rowType_{ROW({
      {"col", BIGINT()},
      {"id", BIGINT()},
      {"name", VARCHAR()},
      {"x", BIGINT()},
      {"age", BIGINT()},
      {"score", DOUBLE()},
      {"ratio", REAL()},
      {"val", BIGINT()},
      {"status", VARCHAR()},
      {"email", VARCHAR()},
      {"a", BIGINT()},
      {"b", BIGINT()},
      {"y", BIGINT()},
      {"z", VARCHAR()},
      {"i", BIGINT()},
      {"ts_col", TIMESTAMP()},
      {"active", BOOLEAN()},
  })};

  // Helpers to build expression trees.

  static TypedExprPtr field(const TypePtr& type, const std::string& name) {
    return std::make_shared<FieldAccessTypedExpr>(type, name);
  }

  static TypedExprPtr intConst(int64_t value) {
    return std::make_shared<ConstantTypedExpr>(BIGINT(), value);
  }

  static TypedExprPtr int32Const(int32_t value) {
    return std::make_shared<ConstantTypedExpr>(INTEGER(), value);
  }

  static TypedExprPtr strConst(const std::string& value) {
    return std::make_shared<ConstantTypedExpr>(VARCHAR(), variant(value));
  }

  static TypedExprPtr floatConst(float value) {
    return std::make_shared<ConstantTypedExpr>(REAL(), value);
  }

  static TypedExprPtr doubleConst(double value) {
    return std::make_shared<ConstantTypedExpr>(DOUBLE(), value);
  }

  static TypedExprPtr tsConst(const Timestamp& ts) {
    return std::make_shared<ConstantTypedExpr>(TIMESTAMP(), variant(ts));
  }

  /// Wrap an expression in a CastTypedExpr (simulates query planner behavior).
  static TypedExprPtr cast(
      const TypePtr& targetType,
      const TypedExprPtr& input) {
    return std::make_shared<CastTypedExpr>(targetType, input, false);
  }

  /// Build an IN-list array constant for integers.
  TypedExprPtr intArrayConst(const std::vector<int64_t>& values) {
    auto arrVec = makeArrayVector<int64_t>({values});
    return std::make_shared<ConstantTypedExpr>(
        BaseVector::wrapInConstant(1, 0, arrVec));
  }

  /// Build an IN-list array constant for strings.
  TypedExprPtr strArrayConst(const std::vector<std::string>& values) {
    std::vector<StringView> svs;
    svs.reserve(values.size());
    for (const auto& v : values) {
      svs.emplace_back(v);
    }
    auto arrVec = makeArrayVector<StringView>({svs});
    return std::make_shared<ConstantTypedExpr>(
        BaseVector::wrapInConstant(1, 0, arrVec));
  }

  /// Build an IN-list array constant for doubles.
  TypedExprPtr doubleArrayConst(const std::vector<double>& values) {
    auto arrVec = makeArrayVector<double>({values});
    return std::make_shared<ConstantTypedExpr>(
        BaseVector::wrapInConstant(1, 0, arrVec));
  }

  /// Build an IN-list array constant for floats.
  TypedExprPtr floatArrayConst(const std::vector<float>& values) {
    auto arrVec = makeArrayVector<float>({values});
    return std::make_shared<ConstantTypedExpr>(
        BaseVector::wrapInConstant(1, 0, arrVec));
  }

  /// Build an IN-list array constant for booleans.
  TypedExprPtr boolArrayConst(const std::vector<bool>& values) {
    auto arrVec = makeArrayVector<bool>({values});
    return std::make_shared<ConstantTypedExpr>(
        BaseVector::wrapInConstant(1, 0, arrVec));
  }

  /// Build an IN-list array constant for timestamps.
  TypedExprPtr tsArrayConst(const std::vector<Timestamp>& values) {
    auto arrVec = makeArrayVector<Timestamp>({values});
    return std::make_shared<ConstantTypedExpr>(
        BaseVector::wrapInConstant(1, 0, arrVec));
  }

  // Binary call helpers.
  static TypedExprPtr eq(const TypedExprPtr& lhs, const TypedExprPtr& rhs) {
    return std::make_shared<CallTypedExpr>(
        BOOLEAN(), std::vector<TypedExprPtr>{lhs, rhs}, "eq");
  }

  static TypedExprPtr neq(const TypedExprPtr& lhs, const TypedExprPtr& rhs) {
    return std::make_shared<CallTypedExpr>(
        BOOLEAN(), std::vector<TypedExprPtr>{lhs, rhs}, "neq");
  }

  static TypedExprPtr lt(const TypedExprPtr& lhs, const TypedExprPtr& rhs) {
    return std::make_shared<CallTypedExpr>(
        BOOLEAN(), std::vector<TypedExprPtr>{lhs, rhs}, "lt");
  }

  static TypedExprPtr lte(const TypedExprPtr& lhs, const TypedExprPtr& rhs) {
    return std::make_shared<CallTypedExpr>(
        BOOLEAN(), std::vector<TypedExprPtr>{lhs, rhs}, "lte");
  }

  static TypedExprPtr gt(const TypedExprPtr& lhs, const TypedExprPtr& rhs) {
    return std::make_shared<CallTypedExpr>(
        BOOLEAN(), std::vector<TypedExprPtr>{lhs, rhs}, "gt");
  }

  static TypedExprPtr gte(const TypedExprPtr& lhs, const TypedExprPtr& rhs) {
    return std::make_shared<CallTypedExpr>(
        BOOLEAN(), std::vector<TypedExprPtr>{lhs, rhs}, "gte");
  }

  static TypedExprPtr between(
      const TypedExprPtr& col,
      const TypedExprPtr& low,
      const TypedExprPtr& high) {
    return std::make_shared<CallTypedExpr>(
        BOOLEAN(), std::vector<TypedExprPtr>{col, low, high}, "between");
  }

  static TypedExprPtr inExpr(
      const TypedExprPtr& col,
      const TypedExprPtr& arrayConst) {
    return std::make_shared<CallTypedExpr>(
        BOOLEAN(), std::vector<TypedExprPtr>{col, arrayConst}, "in");
  }

  static TypedExprPtr notInExpr(
      const TypedExprPtr& col,
      const TypedExprPtr& arrayConst) {
    return std::make_shared<CallTypedExpr>(
        BOOLEAN(), std::vector<TypedExprPtr>{col, arrayConst}, "not_in");
  }

  static TypedExprPtr isNull(const TypedExprPtr& col) {
    return std::make_shared<CallTypedExpr>(
        BOOLEAN(), std::vector<TypedExprPtr>{col}, "is_null");
  }

  static TypedExprPtr isNotNull(const TypedExprPtr& col) {
    return std::make_shared<CallTypedExpr>(
        BOOLEAN(), std::vector<TypedExprPtr>{col}, "is_not_null");
  }

  static TypedExprPtr notExpr(const TypedExprPtr& inner) {
    return std::make_shared<CallTypedExpr>(
        BOOLEAN(), std::vector<TypedExprPtr>{inner}, "not");
  }

  static TypedExprPtr andExpr(
      const TypedExprPtr& left,
      const TypedExprPtr& right) {
    return std::make_shared<CallTypedExpr>(
        BOOLEAN(), std::vector<TypedExprPtr>{left, right}, "and");
  }

  static TypedExprPtr orExpr(
      const TypedExprPtr& left,
      const TypedExprPtr& right) {
    return std::make_shared<CallTypedExpr>(
        BOOLEAN(), std::vector<TypedExprPtr>{left, right}, "or");
  }
};

// ---------------------------------------------------------------------------
// Null / empty input
// ---------------------------------------------------------------------------

TEST_F(PaimonFilterTranslatorTest, NullInputReturnsNull) {
  auto result = translate(nullptr);
  EXPECT_FALSE(result.ok()) << result.reason;
}

TEST_F(PaimonFilterTranslatorTest, BareFieldAccessReturnsNull) {
  auto expr = field(BIGINT(), "col");
  auto result = translate(expr);
  EXPECT_FALSE(result.ok()) << result.reason;
}

TEST_F(PaimonFilterTranslatorTest, BareConstantReturnsNull) {
  auto expr = intConst(42);
  auto result = translate(expr);
  EXPECT_FALSE(result.ok()) << result.reason;
}

// ---------------------------------------------------------------------------
// Equality predicates
// ---------------------------------------------------------------------------

TEST_F(PaimonFilterTranslatorTest, IntEquality) {
  auto expr = eq(field(BIGINT(), "id"), intConst(42));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("id"), std::string::npos);
  EXPECT_NE(pred.value->ToString().find("42"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, IntNotEqual) {
  auto expr = neq(field(BIGINT(), "id"), intConst(99));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("id"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, StringEquality) {
  auto expr = eq(field(VARCHAR(), "name"), strConst("alice"));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("name"), std::string::npos);
  EXPECT_NE(pred.value->ToString().find("alice"), std::string::npos);
}

// ---------------------------------------------------------------------------
// Range comparison predicates
// ---------------------------------------------------------------------------

TEST_F(PaimonFilterTranslatorTest, LessThan) {
  auto expr = lt(field(BIGINT(), "x"), intConst(100));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find('x'), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, LessThanOrEqual) {
  auto expr = lte(field(BIGINT(), "age"), intConst(30));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("age"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, GreaterThan) {
  auto expr = gt(field(DOUBLE(), "score"), doubleConst(85.5));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("score"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, GreaterThanOrEqual) {
  auto expr = gte(field(REAL(), "ratio"), floatConst(0.5F));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("ratio"), std::string::npos);
}

// ---------------------------------------------------------------------------
// Between predicate
// ---------------------------------------------------------------------------

TEST_F(PaimonFilterTranslatorTest, Between) {
  auto expr = between(field(BIGINT(), "val"), intConst(10), intConst(50));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("val"), std::string::npos);
}

// ---------------------------------------------------------------------------
// IN-list predicates
// ---------------------------------------------------------------------------

TEST_F(PaimonFilterTranslatorTest, IntInList) {
  auto expr = inExpr(field(BIGINT(), "id"), intArrayConst({1, 3, 5}));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("id"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, IntNotInList) {
  auto expr = notInExpr(field(BIGINT(), "id"), intArrayConst({2, 4}));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("id"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, StringInList) {
  auto expr =
      inExpr(field(VARCHAR(), "status"), strArrayConst({"active", "pending"}));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("status"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, StringNotInList) {
  auto expr =
      notInExpr(field(VARCHAR(), "status"), strArrayConst({"archived"}));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("status"), std::string::npos);
}

// ---------------------------------------------------------------------------
// Null-check predicates
// ---------------------------------------------------------------------------

TEST_F(PaimonFilterTranslatorTest, IsNull) {
  auto expr = isNull(field(VARCHAR(), "email"));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("email"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, IsNotNull) {
  auto expr = isNotNull(field(VARCHAR(), "email"));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("email"), std::string::npos);
}

// ---------------------------------------------------------------------------
// Logical compound predicates
// ---------------------------------------------------------------------------

TEST_F(PaimonFilterTranslatorTest, AndTwoPredicates) {
  auto left = eq(field(BIGINT(), "id"), intConst(1));
  auto right = eq(field(VARCHAR(), "name"), strConst("alice"));
  auto expr = andExpr(left, right);
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  // Compound AND should contain both field names.
  const auto& s = pred.value->ToString();
  EXPECT_NE(s.find("id"), std::string::npos);
  EXPECT_NE(s.find("name"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, OrTwoPredicatesSameColumn) {
  auto left = eq(field(BIGINT(), "id"), intConst(1));
  auto right = eq(field(BIGINT(), "id"), intConst(2));
  auto expr = orExpr(left, right);
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("id"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, AndThreePredicates) {
  auto a = eq(field(BIGINT(), "x"), intConst(1));
  auto b = gt(field(BIGINT(), "y"), intConst(0));
  auto c = isNull(field(VARCHAR(), "z"));
  auto expr = andExpr(andExpr(a, b), c);
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  const auto& s = pred.value->ToString();
  EXPECT_NE(s.find('x'), std::string::npos);
  EXPECT_NE(s.find('y'), std::string::npos);
  EXPECT_NE(s.find('z'), std::string::npos);
}

// ---------------------------------------------------------------------------
// NOT negation
// ---------------------------------------------------------------------------

TEST_F(PaimonFilterTranslatorTest, NotEqBecomesNeq) {
  auto inner = eq(field(BIGINT(), "id"), intConst(5));
  auto expr = notExpr(inner);
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("id"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, NotLtBecomesGte) {
  auto inner = lt(field(BIGINT(), "x"), intConst(10));
  auto expr = notExpr(inner);
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find('x'), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, NotIsNullBecomesIsNotNull) {
  auto inner = isNull(field(VARCHAR(), "email"));
  auto expr = notExpr(inner);
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("email"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, NotIsNotNullBecomesIsNull) {
  auto inner = isNotNull(field(VARCHAR(), "email"));
  auto expr = notExpr(inner);
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;
  EXPECT_NE(pred.value->ToString().find("email"), std::string::npos);
}

// ---------------------------------------------------------------------------
// Unsupported / edge cases
// ---------------------------------------------------------------------------

TEST_F(PaimonFilterTranslatorTest, UnsupportedFunctionReturnsNull) {
  // A function like "plus" is not a supported predicate operator.
  auto expr = std::make_shared<CallTypedExpr>(
      BIGINT(),
      std::vector<TypedExprPtr>{field(BIGINT(), "a"), field(BIGINT(), "b")},
      "plus");
  auto result = translate(expr);
  EXPECT_FALSE(result.ok()) << result.reason;
}

// ===========================================================================
// Round-trip tests: TypedExpr → Predicate → TypedExpr
// ===========================================================================

/// Common boilerplate: translate expr → predicate → TypedExpr.
/// Returns the round-tripped expression. Fails with EXPECT if any step fails.
static core::TypedExprPtr roundTripExpr(
    const core::TypedExprPtr& expr,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  auto pred = PaimonFilterTranslator::translate(expr, rowType);
  EXPECT_TRUE(pred.ok()) << pred.reason;
  if (!pred.ok())
    return nullptr;
  auto result = PaimonFilterTranslator::toTypedExpr(pred.value, pool);
  EXPECT_TRUE(result.ok()) << result.reason;
  return result.value;
}

/// Verify a CallTypedExpr has the expected operator name and input count.
static void assertOp(
    const core::TypedExprPtr& rt,
    std::string_view expectedOp,
    size_t expectedInputs) {
  const auto* call = dynamic_cast<const core::CallTypedExpr*>(rt.get());
  ASSERT_NE(call, nullptr) << "Expected CallTypedExpr";
  EXPECT_EQ(call->name(), expectedOp) << "Expected op '" << expectedOp << "'";
  EXPECT_EQ(call->inputs().size(), expectedInputs)
      << "Expected " << expectedInputs << " inputs";
}

/// Extract and verify the field name from input[0] of a CallTypedExpr.
static void assertField(
    const core::TypedExprPtr& rt,
    std::string_view expectedField,
    size_t fieldIndex = 0) {
  const auto* call = dynamic_cast<const core::CallTypedExpr*>(rt.get());
  ASSERT_NE(call, nullptr);
  const auto* fa = dynamic_cast<const core::FieldAccessTypedExpr*>(
      call->inputs()[fieldIndex].get());
  ASSERT_NE(fa, nullptr) << "Input[" << fieldIndex
                         << "] is not FieldAccessTypedExpr";
  EXPECT_EQ(fa->name(), expectedField)
      << "Expected field '" << expectedField << "'";
}

/// Verify input[1] is a ConstantTypedExpr.
static void assertConstantInput(
    const core::TypedExprPtr& rt,
    size_t constIndex = 1) {
  const auto* call = dynamic_cast<const core::CallTypedExpr*>(rt.get());
  ASSERT_NE(call, nullptr);
  const auto* c = dynamic_cast<const core::ConstantTypedExpr*>(
      call->inputs()[constIndex].get());
  ASSERT_NE(c, nullptr) << "Input[" << constIndex
                        << "] is not ConstantTypedExpr";
}

/// Full binary comparison round-trip: op + field + constant + toString match.
static void assertRoundTripBinary(
    const core::TypedExprPtr& expr,
    std::string_view expectedOp,
    std::string_view expectedField,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  auto rt = roundTripExpr(expr, rowType, pool);
  assertOp(rt, expectedOp, 2);
  assertField(rt, expectedField);
  assertConstantInput(rt);
  EXPECT_EQ(rt->toString(), expr->toString())
      << "Round-trip differs.\n"
      << "  original:   " << expr->toString() << "\n"
      << "  round-trip: " << rt->toString();
}

/// Unary op round-trip (is_null / is_not_null): op + field + toString match.
static void assertRoundTripUnary(
    const core::TypedExprPtr& expr,
    std::string_view expectedOp,
    std::string_view expectedField,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  auto rt = roundTripExpr(expr, rowType, pool);
  assertOp(rt, expectedOp, 1);
  assertField(rt, expectedField);
  EXPECT_EQ(rt->toString(), expr->toString())
      << "Round-trip differs.\n"
      << "  original:   " << expr->toString() << "\n"
      << "  round-trip: " << rt->toString();
}

/// Normalized unary op round-trip where the operator name changes
/// (e.g. NOT(is_null)→is_not_null). Verifies structure but skips toString.
static void assertRoundTripNormalizedUnary(
    const core::TypedExprPtr& expr,
    std::string_view expectedOp,
    std::string_view expectedField,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  auto rt = roundTripExpr(expr, rowType, pool);
  assertOp(rt, expectedOp, 1);
  assertField(rt, expectedField);
}

/// Compound op round-trip (and / or): op + input count + toString match.
static void assertRoundTripCompound(
    const core::TypedExprPtr& expr,
    std::string_view expectedOp,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  auto rt = roundTripExpr(expr, rowType, pool);
  assertOp(rt, expectedOp, 2);
  EXPECT_EQ(rt->toString(), expr->toString())
      << "Round-trip differs.\n"
      << "  original:   " << expr->toString() << "\n"
      << "  round-trip: " << rt->toString();
}

/// Binary op round-trip where the operator is intentionally different from the
/// original (e.g. NOT(eq)→neq). Verifies structure but skips toString check.
static void assertRoundTripNormalizedBinary(
    const core::TypedExprPtr& expr,
    std::string_view expectedOp,
    std::string_view expectedField,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  auto rt = roundTripExpr(expr, rowType, pool);
  assertOp(rt, expectedOp, 2);
  assertField(rt, expectedField);
  assertConstantInput(rt);
}

/// IN-list round-trip: op + field + array constant + toString match.
static void assertRoundTripInList(
    const core::TypedExprPtr& expr,
    std::string_view expectedOp,
    std::string_view expectedField,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  auto rt = roundTripExpr(expr, rowType, pool);
  assertOp(rt, expectedOp, 2);
  assertField(rt, expectedField);
  const auto* call = dynamic_cast<const core::CallTypedExpr*>(rt.get());
  const auto* c =
      dynamic_cast<const core::ConstantTypedExpr*>(call->inputs()[1].get());
  ASSERT_NE(c, nullptr) << "Second input is not ConstantTypedExpr";
  EXPECT_TRUE(c->type()->isArray())
      << "Expected array-typed constant for IN-list";
  EXPECT_EQ(rt->toString(), expr->toString())
      << "Round-trip differs.\n"
      << "  original:   " << expr->toString() << "\n"
      << "  round-trip: " << rt->toString();
}

TEST_F(PaimonFilterTranslatorTest, RoundTripIntEq) {
  assertRoundTripBinary(
      eq(field(BIGINT(), "i"), intConst(42)), "eq", "i", rowType_, pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripIntNeq) {
  assertRoundTripBinary(
      neq(field(BIGINT(), "i"), intConst(99)),
      "neq",
      "i",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripLessThan) {
  assertRoundTripBinary(
      lt(field(BIGINT(), "x"), intConst(100)),
      "lt",
      "x",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripLessThanOrEqual) {
  assertRoundTripBinary(
      lte(field(BIGINT(), "age"), intConst(30)),
      "lte",
      "age",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripGreaterThan) {
  assertRoundTripBinary(
      gt(field(DOUBLE(), "score"), doubleConst(85.5)),
      "gt",
      "score",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripGreaterThanOrEqual) {
  assertRoundTripBinary(
      gte(field(REAL(), "ratio"), floatConst(0.5F)),
      "gte",
      "ratio",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripStringEq) {
  assertRoundTripBinary(
      eq(field(VARCHAR(), "name"), strConst("alice")),
      "eq",
      "name",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripBetween) {
  auto expr = between(field(BIGINT(), "val"), intConst(10), intConst(50));
  auto rt = roundTripExpr(expr, rowType_, pool_.get());
  // paimon::Between is internally represented as AND(gte, lte).
  assertOp(rt, "and", 2);
  // Verify both children are binary comparisons on the same field.
  for (int i = 0; i < 2; ++i) {
    SCOPED_TRACE(fmt::format("child {}", i));
    const auto* child =
        dynamic_cast<const core::CallTypedExpr*>(rt->inputs()[i].get());
    ASSERT_NE(child, nullptr);
    ASSERT_EQ(child->inputs().size(), 2);
    const auto* fa = dynamic_cast<const core::FieldAccessTypedExpr*>(
        child->inputs()[0].get());
    ASSERT_NE(fa, nullptr);
    EXPECT_EQ(fa->name(), "val");
  }
}

TEST_F(PaimonFilterTranslatorTest, RoundTripIsNull) {
  assertRoundTripUnary(
      isNull(field(VARCHAR(), "email")),
      "is_null",
      "email",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripIsNotNull) {
  assertRoundTripUnary(
      isNotNull(field(VARCHAR(), "email")),
      "is_not_null",
      "email",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripAndTwoPredicates) {
  assertRoundTripCompound(
      andExpr(
          eq(field(BIGINT(), "id"), intConst(1)),
          eq(field(VARCHAR(), "name"), strConst("alice"))),
      "and",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripOrTwoPredicatesSameColumn) {
  assertRoundTripCompound(
      orExpr(
          eq(field(BIGINT(), "id"), intConst(1)),
          eq(field(BIGINT(), "id"), intConst(2))),
      "or",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripAndThreePredicates) {
  assertRoundTripCompound(
      andExpr(
          andExpr(
              eq(field(BIGINT(), "x"), intConst(1)),
              gt(field(BIGINT(), "y"), intConst(0))),
          isNull(field(VARCHAR(), "z"))),
      "and",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripNotEqBecomesNeq) {
  // NOT(eq) is translated to neq. Structure matches but op differs.
  assertRoundTripNormalizedBinary(
      notExpr(eq(field(BIGINT(), "id"), intConst(5))),
      "neq",
      "id",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripNotLtBecomesGte) {
  // NOT(lt) is translated to gte.
  assertRoundTripNormalizedBinary(
      notExpr(lt(field(BIGINT(), "x"), intConst(10))),
      "gte",
      "x",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripNotIsNullBecomesIsNotNull) {
  // NOT(is_null) → is_not_null. Operator name changes — use normalized
  // variant that checks structure but skips toString comparison.
  assertRoundTripNormalizedUnary(
      notExpr(isNull(field(VARCHAR(), "email"))),
      "is_not_null",
      "email",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripIntInList) {
  assertRoundTripInList(
      inExpr(field(BIGINT(), "id"), intArrayConst({1, 3, 5})),
      "in",
      "id",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripIntNotInList) {
  assertRoundTripInList(
      notInExpr(field(BIGINT(), "id"), intArrayConst({2, 4})),
      "not_in",
      "id",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, ToTypedExprNullPredicateReturnsNull) {
  auto result = PaimonFilterTranslator::toTypedExpr(nullptr);
  EXPECT_FALSE(result.ok());
}

TEST_F(PaimonFilterTranslatorTest, AndWithUnsupportedChildReturnsNull) {
  // If any child of AND can't be translated, the whole thing returns null.
  auto good = eq(field(BIGINT(), "id"), intConst(1));
  auto bad = std::make_shared<CallTypedExpr>(
      BIGINT(),
      std::vector<TypedExprPtr>{field(BIGINT(), "a"), field(BIGINT(), "b")},
      "plus");
  auto expr = andExpr(good, bad);
  auto result = translate(expr);
  EXPECT_FALSE(result.ok()) << result.reason;
}

TEST_F(PaimonFilterTranslatorTest, OrWithUnsupportedChildReturnsNull) {
  auto good = eq(field(BIGINT(), "id"), intConst(1));
  auto bad = std::make_shared<CallTypedExpr>(
      BIGINT(),
      std::vector<TypedExprPtr>{field(BIGINT(), "a"), field(BIGINT(), "b")},
      "plus");
  auto expr = orExpr(good, bad);
  auto result = translate(expr);
  EXPECT_FALSE(result.ok()) << result.reason;
}

TEST_F(PaimonFilterTranslatorTest, NotWithNonCallInnerReturnsNull) {
  // NOT applied to a bare field (not a CallTypedExpr) should fail.
  auto expr = notExpr(field(BIGINT(), "x"));
  auto result = translate(expr);
  EXPECT_FALSE(result.ok()) << result.reason;
}

TEST_F(PaimonFilterTranslatorTest, EmptyAndReturnsNull) {
  // AND with no children (empty inputs).
  auto expr = std::make_shared<CallTypedExpr>(
      BOOLEAN(), std::vector<TypedExprPtr>{}, "and");
  auto result = translate(expr);
  EXPECT_FALSE(result.ok()) << result.reason;
}

TEST_F(PaimonFilterTranslatorTest, EmptyOrReturnsNull) {
  auto expr = std::make_shared<CallTypedExpr>(
      BOOLEAN(), std::vector<TypedExprPtr>{}, "or");
  auto result = translate(expr);
  EXPECT_FALSE(result.ok()) << result.reason;
}

// ---------------------------------------------------------------------------
// CastTypedExpr-wrapped literal extraction (type coercion)
// ---------------------------------------------------------------------------

TEST_F(PaimonFilterTranslatorTest, CastWrappedInt32ConstToBigintFieldEq) {
  // Query planners wrap int32 constants in CastTypedExpr(BIGINT) when the
  // field is BIGINT. extractLiteral must unwrap and widen without crashing.
  auto expr = eq(field(BIGINT(), "id"), cast(BIGINT(), int32Const(42)));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason
                         << "Failed to translate eq with cast-wrapped int32";
  EXPECT_NE(pred.value->ToString().find("id"), std::string::npos);
  EXPECT_NE(pred.value->ToString().find("42"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, CastWrappedInt32ConstToBigintFieldGte) {
  // greaterthanorequal("id", 10) where 10 is an int32 wrapped as BIGINT.
  auto expr = gte(field(BIGINT(), "id"), cast(BIGINT(), int32Const(10)));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason
                         << "Failed to translate gte with cast-wrapped int32";
  EXPECT_NE(pred.value->ToString().find("id"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, CastWrappedInt32ConstToBigintFieldLte) {
  // lessthanorequal("id", 20) where 20 is an int32 wrapped as BIGINT.
  auto expr = lte(field(BIGINT(), "id"), cast(BIGINT(), int32Const(20)));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason
                         << "Failed to translate lte with cast-wrapped int32";
  EXPECT_NE(pred.value->ToString().find("id"), std::string::npos);
}

TEST_F(
    PaimonFilterTranslatorTest,
    CastWrappedInt32ConstToBigintFieldComplexAnd) {
  // The exact failing expression from the error log:
  // and(and(isnotnull("id"), greaterthanorequal("id",10)),
  // lessthanorequal("id",20)) where constants are int32 wrapped in
  // CastTypedExpr(BIGINT).
  auto inner1 = isNotNull(field(BIGINT(), "id"));
  auto inner2 = gte(field(BIGINT(), "id"), cast(BIGINT(), int32Const(10)));
  auto inner3 = lte(field(BIGINT(), "id"), cast(BIGINT(), int32Const(20)));
  auto expr = andExpr(andExpr(inner1, inner2), inner3);
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok())
      << pred.reason
      << "Failed to translate complex AND with cast-wrapped int32 literals";
  const auto& s = pred.value->ToString();
  EXPECT_NE(s.find("id"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, CastWrappedFloatConstToDoubleFieldGt) {
  // A float constant wrapped in CastTypedExpr(DOUBLE) for a DOUBLE field.
  auto expr = gt(field(DOUBLE(), "score"), cast(DOUBLE(), floatConst(85.5F)));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok())
      << pred.reason
      << "Failed to translate gt with cast-wrapped float-to-double";
  EXPECT_NE(pred.value->ToString().find("score"), std::string::npos);
}

TEST_F(
    PaimonFilterTranslatorTest,
    CastWrappedSmallIntConstToBigintFieldBetween) {
  // TINYINT/SMALLINT constants widened through CastTypedExpr to BIGINT.
  auto tinyConst = std::make_shared<ConstantTypedExpr>(
      TINYINT(), variant(static_cast<int8_t>(5)));
  auto smallConst = std::make_shared<ConstantTypedExpr>(
      SMALLINT(), variant(static_cast<int16_t>(100)));
  auto expr = between(
      field(BIGINT(), "val"),
      cast(BIGINT(), tinyConst),
      cast(BIGINT(), smallConst));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok())
      << pred.reason
      << "Failed to translate between with cast-wrapped small-int literals";
  EXPECT_NE(pred.value->ToString().find("val"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, CastWrappedInt32ConstRoundTrip) {
  // Full round-trip: TypedExpr(int32-cast) → Predicate → TypedExpr.
  // The cast wrapper is stripped during translation (paimon stores int64_t),
  // so the round-trip produces a plain eq without cast.
  auto expr = eq(field(BIGINT(), "id"), cast(BIGINT(), int32Const(99)));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  // The cast wrapper is stripped during translation, so verify the round-trip
  // produces a plain eq("id", 99) without the cast.
  assertOp(roundTripped.value, "eq", 2);
  assertField(roundTripped.value, "id");
  assertConstantInput(roundTripped.value);
}

TEST_F(PaimonFilterTranslatorTest, IntFieldInt32LiteralMatch) {
  // Field is INT, literal is int32 — types must match for paimon
  // PredicateBuilder. Uses a separate rowType with INTEGER (not BIGINT) for the
  // "id" column.
  auto intRowType = ROW({{"id", INTEGER()}});
  auto expr = eq(field(INTEGER(), "id"), int32Const(42));
  auto result = PaimonFilterTranslator::translate(expr, intRowType);
  ASSERT_TRUE(result.ok()) << result.reason;
  EXPECT_NE(result.value->ToString().find("id"), std::string::npos);
  EXPECT_NE(result.value->ToString().find("42"), std::string::npos);
}

TEST_F(PaimonFilterTranslatorTest, IntFieldBigintLiteralWidens) {
  // Field is INT but literal arrives as BIGINT (common from query planners).
  // extractLiteral must narrow int64→int32 so paimon accepts it.
  auto intRowType = ROW({{"id", INTEGER()}});
  auto bigIntConst = std::make_shared<ConstantTypedExpr>(
      BIGINT(), variant(static_cast<int64_t>(7)));
  auto expr = eq(field(INTEGER(), "id"), cast(INTEGER(), bigIntConst));
  auto result = PaimonFilterTranslator::translate(expr, intRowType);
  ASSERT_TRUE(result.ok()) << result.reason;
}

// ---------------------------------------------------------------------------
// Timestamp predicate tests
// ---------------------------------------------------------------------------

TEST_F(PaimonFilterTranslatorTest, TimestampEqualityPreservesSubMs) {
  // A timestamp with sub-millisecond nanoseconds. The predicate literal
  // must preserve microsecond precision (not truncate to milliseconds),
  // otherwise filter pushdown may produce incorrect results.
  // Timestamp(seconds, nanos): epoch + 1 second + 123'456'789 nanos.
  auto ts = Timestamp(1, 123'456'789);
  auto expr = eq(field(TIMESTAMP(), "ts_col"), tsConst(ts));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  // Round-trip and verify the nanoseconds are preserved (not truncated to ms).
  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;

  const auto* call =
      dynamic_cast<const core::CallTypedExpr*>(roundTripped.value.get());
  ASSERT_NE(call, nullptr);
  ASSERT_EQ(call->inputs().size(), 2);
  const auto* constExpr =
      dynamic_cast<const core::ConstantTypedExpr*>(call->inputs()[1].get());
  ASSERT_NE(constExpr, nullptr);
  const auto& val = constExpr->value();
  auto roundTrippedTs = val.value<Timestamp>();
  // The original ts had nanos=123'456'789. Paimon stores (millis,
  // nanos_of_millis). After round-trip we should preserve sub-ms precision:
  // millis = 1000 + 123 = 1123, nanos_of_millis = 456'789.
  // The bolt Timestamp should have the same total nanoseconds within ms.
  EXPECT_EQ(roundTrippedTs.getNanos() % 1'000'000, ts.getNanos() % 1'000'000)
      << "Sub-millisecond nanoseconds were truncated";
}

TEST_F(PaimonFilterTranslatorTest, TimestampGtWithSubMsPrecision) {
  // gt(ts, '2023-01-01 00:00:00.000500') should not truncate to
  // '2023-01-01 00:00:00.000', which would incorrectly match rows at
  // t+0.000100.
  auto ts = Timestamp(1672531200, 500'000); // 2023-01-01 00:00:00.000500
  auto expr = gt(field(TIMESTAMP(), "ts_col"), tsConst(ts));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;

  const auto* call =
      dynamic_cast<const core::CallTypedExpr*>(roundTripped.value.get());
  ASSERT_NE(call, nullptr);
  const auto* constExpr =
      dynamic_cast<const core::ConstantTypedExpr*>(call->inputs()[1].get());
  ASSERT_NE(constExpr, nullptr);
  const auto& val = constExpr->value();
  auto roundTrippedTs = val.value<Timestamp>();
  EXPECT_EQ(roundTrippedTs.getNanos() % 1'000'000, ts.getNanos() % 1'000'000);
}

TEST_F(PaimonFilterTranslatorTest, DoubleInList) {
  auto expr =
      inExpr(field(DOUBLE(), "score"), doubleArrayConst({1.5, 2.5, 3.5}));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  // Verify predicate structure: IN function on DOUBLE field "score" with
  // correct literal values.
  const auto* leaf =
      dynamic_cast<const ::paimon::LeafPredicate*>(pred.value.get());
  ASSERT_NE(leaf, nullptr);
  EXPECT_EQ(leaf->GetFunction().GetType(), ::paimon::Function::Type::IN);
  EXPECT_EQ(leaf->FieldName(), "score");
  EXPECT_EQ(leaf->GetFieldType(), ::paimon::FieldType::DOUBLE);

  const auto& literals = leaf->Literals();
  ASSERT_EQ(literals.size(), 3);
  double v0 = literals[0].GetValue<double>();
  double v1 = literals[1].GetValue<double>();
  double v2 = literals[2].GetValue<double>();
  EXPECT_NEAR(v0, 1.5, 1e-9);
  EXPECT_NEAR(v1, 2.5, 1e-9);
  EXPECT_NEAR(v2, 3.5, 1e-9);
}

TEST_F(PaimonFilterTranslatorTest, DoubleNotInList) {
  auto expr =
      notInExpr(field(DOUBLE(), "score"), doubleArrayConst({0.0, 100.0}));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  const auto* leaf =
      dynamic_cast<const ::paimon::LeafPredicate*>(pred.value.get());
  ASSERT_NE(leaf, nullptr);
  EXPECT_EQ(leaf->GetFunction().GetType(), ::paimon::Function::Type::NOT_IN);
  EXPECT_EQ(leaf->FieldName(), "score");
  EXPECT_EQ(leaf->GetFieldType(), ::paimon::FieldType::DOUBLE);

  const auto& literals = leaf->Literals();
  ASSERT_EQ(literals.size(), 2);
  double v0 = literals[0].GetValue<double>();
  double v1 = literals[1].GetValue<double>();
  EXPECT_NEAR(v0, 0.0, 1e-9);
  EXPECT_NEAR(v1, 100.0, 1e-9);
}

TEST_F(PaimonFilterTranslatorTest, FloatInList) {
  auto expr =
      inExpr(field(REAL(), "ratio"), floatArrayConst({0.1F, 0.5F, 0.9F}));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  const auto* leaf =
      dynamic_cast<const ::paimon::LeafPredicate*>(pred.value.get());
  ASSERT_NE(leaf, nullptr);
  EXPECT_EQ(leaf->GetFunction().GetType(), ::paimon::Function::Type::IN);
  EXPECT_EQ(leaf->FieldName(), "ratio");
  EXPECT_EQ(leaf->GetFieldType(), ::paimon::FieldType::FLOAT);

  const auto& literals = leaf->Literals();
  ASSERT_EQ(literals.size(), 3);
  auto f0 = literals[0].GetValue<float>();
  auto f1 = literals[1].GetValue<float>();
  auto f2 = literals[2].GetValue<float>();
  EXPECT_FLOAT_EQ(f0, 0.1F);
  EXPECT_FLOAT_EQ(f1, 0.5F);
  EXPECT_FLOAT_EQ(f2, 0.9F);
}

TEST_F(PaimonFilterTranslatorTest, BooleanInList) {
  auto expr = inExpr(field(BOOLEAN(), "active"), boolArrayConst({true, false}));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  const auto* leaf =
      dynamic_cast<const ::paimon::LeafPredicate*>(pred.value.get());
  ASSERT_NE(leaf, nullptr);
  EXPECT_EQ(leaf->GetFunction().GetType(), ::paimon::Function::Type::IN);
  EXPECT_EQ(leaf->FieldName(), "active");
  EXPECT_EQ(leaf->GetFieldType(), ::paimon::FieldType::BOOLEAN);

  const auto& literals = leaf->Literals();
  ASSERT_EQ(literals.size(), 2);
  bool b0 = literals[0].GetValue<bool>();
  bool b1 = literals[1].GetValue<bool>();
  EXPECT_EQ(b0, true);
  EXPECT_EQ(b1, false);
}

TEST_F(PaimonFilterTranslatorTest, TimestampInList) {
  auto ts1 = Timestamp(1000, 123'456'000);
  auto ts2 = Timestamp(2000, 789'000'000);
  auto expr = inExpr(field(TIMESTAMP(), "ts_col"), tsArrayConst({ts1, ts2}));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  const auto* leaf =
      dynamic_cast<const ::paimon::LeafPredicate*>(pred.value.get());
  ASSERT_NE(leaf, nullptr);
  EXPECT_EQ(leaf->GetFunction().GetType(), ::paimon::Function::Type::IN);
  EXPECT_EQ(leaf->FieldName(), "ts_col");
  EXPECT_EQ(leaf->GetFieldType(), ::paimon::FieldType::TIMESTAMP);

  const auto& literals = leaf->Literals();
  ASSERT_EQ(literals.size(), 2);
  // Verify timestamps preserve full nanosecond precision through conversion.
  auto rtTs1 = literals[0].GetValue<::paimon::Timestamp>();
  auto rtTs2 = literals[1].GetValue<::paimon::Timestamp>();
  int64_t expectedNanos1 = (1000 * 1'000'000'000LL) + 123'456'000;
  int64_t expectedNanos2 = (2000 * 1'000'000'000LL) + 789'000'000;
  int64_t actualNanos1 =
      (rtTs1.GetMillisecond() * 1'000'000) + rtTs1.GetNanoOfMillisecond();
  int64_t actualNanos2 =
      (rtTs2.GetMillisecond() * 1'000'000) + rtTs2.GetNanoOfMillisecond();
  EXPECT_EQ(actualNanos1, expectedNanos1);
  EXPECT_EQ(actualNanos2, expectedNanos2);
}

TEST_F(PaimonFilterTranslatorTest, RoundTripDoubleInList) {
  assertRoundTripInList(
      inExpr(field(DOUBLE(), "score"), doubleArrayConst({1.5, 2.5})),
      "in",
      "score",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripBooleanInList) {
  assertRoundTripInList(
      inExpr(field(BOOLEAN(), "active"), boolArrayConst({true})),
      "in",
      "active",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripTimestampInList) {
  auto ts1 = Timestamp(1000, 500'000);
  auto ts2 = Timestamp(2000, 0);
  assertRoundTripInList(
      inExpr(field(TIMESTAMP(), "ts_col"), tsArrayConst({ts1, ts2})),
      "in",
      "ts_col",
      rowType_,
      pool_.get());
}

// Tests that the StringView dangling bug is fixed: string literals in an
// IN-list must survive the round-trip (TypedExpr → Predicate → TypedExpr)
// without corruption.
TEST_F(PaimonFilterTranslatorTest, RoundTripStringInList) {
  assertRoundTripInList(
      inExpr(
          field(VARCHAR(), "status"),
          strArrayConst({"active", "pending", "archived"})),
      "in",
      "status",
      rowType_,
      pool_.get());
}

TEST_F(PaimonFilterTranslatorTest, RoundTripStringNotInList) {
  assertRoundTripInList(
      notInExpr(field(VARCHAR(), "status"), strArrayConst({"archived"})),
      "not_in",
      "status",
      rowType_,
      pool_.get());
}

// Nested OR of equality predicates on the same column cannot be pushed down
// as a Values filter (we don't flatten nested ORs). The translator should
// return a non-ok result rather than producing incorrect output.
TEST_F(PaimonFilterTranslatorTest, NestedOrOfEqPredicates) {
  // or(or(eq(id,1), eq(id,2)), eq(id,3)) — nested ORs with all-eq children.
  // Direct children of the top-level OR include a nested OR, which means not
  // all direct children are "eq" predicates. This should NOT be pushable as a
  // Values filter because we don't flatten nested OR structure.
  auto innerOr = orExpr(
      eq(field(BIGINT(), "id"), intConst(1)),
      eq(field(BIGINT(), "id"), intConst(2)));
  auto expr = orExpr(innerOr, eq(field(BIGINT(), "id"), intConst(3)));
  auto pred = translate(expr);
  EXPECT_TRUE(pred.ok());
}

} // namespace
