/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

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
  TranslationResult<std::shared_ptr<::paimon::Predicate>> translate(
      const core::TypedExprPtr& expr) {
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

/// Helper: verify a CallTypedExpr's name matches expected.
static void assertOpName(
    const core::TypedExprPtr& expr,
    const std::string& expected) {
  const auto* call = dynamic_cast<const core::CallTypedExpr*>(expr.get());
  ASSERT_NE(call, nullptr);
  EXPECT_EQ(call->name(), expected);
}

TEST_F(PaimonFilterTranslatorTest, RoundTripIntEq) {
  // eq(field("i", BIGINT), intConst(42)) → predicate → expr
  auto expr = eq(field(BIGINT(), "i"), intConst(42));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  assertOpName(roundTripped.value, "eq");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripIntNeq) {
  auto expr = neq(field(BIGINT(), "i"), intConst(99));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  assertOpName(roundTripped.value, "neq");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripLessThan) {
  auto expr = lt(field(BIGINT(), "x"), intConst(100));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  assertOpName(roundTripped.value, "lt");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripLessThanOrEqual) {
  auto expr = lte(field(BIGINT(), "age"), intConst(30));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  assertOpName(roundTripped.value, "lte");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripGreaterThan) {
  auto expr = gt(field(DOUBLE(), "score"), doubleConst(85.5));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  assertOpName(roundTripped.value, "gt");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripGreaterThanOrEqual) {
  auto expr = gte(field(REAL(), "ratio"), floatConst(0.5F));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  assertOpName(roundTripped.value, "gte");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripStringEq) {
  auto expr = eq(field(VARCHAR(), "name"), strConst("alice"));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  assertOpName(roundTripped.value, "eq");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripBetween) {
  auto expr = between(field(BIGINT(), "val"), intConst(10), intConst(50));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  // paimon::Between is internally represented as AND(gte, lte),
  // so the round-trip produces "and" rather than "between".
  assertOpName(roundTripped.value, "and");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripIsNull) {
  auto expr = isNull(field(VARCHAR(), "email"));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  assertOpName(roundTripped.value, "is_null");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripIsNotNull) {
  auto expr = isNotNull(field(VARCHAR(), "email"));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  assertOpName(roundTripped.value, "is_not_null");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripAndTwoPredicates) {
  auto left = eq(field(BIGINT(), "id"), intConst(1));
  auto right = eq(field(VARCHAR(), "name"), strConst("alice"));
  auto expr = andExpr(left, right);
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  assertOpName(roundTripped.value, "and");

  // Verify the AND has two children.
  const auto* call =
      dynamic_cast<const core::CallTypedExpr*>(roundTripped.value.get());
  ASSERT_NE(call, nullptr);
  EXPECT_EQ(call->inputs().size(), 2);
}

TEST_F(PaimonFilterTranslatorTest, RoundTripOrTwoPredicatesSameColumn) {
  auto left = eq(field(BIGINT(), "id"), intConst(1));
  auto right = eq(field(BIGINT(), "id"), intConst(2));
  auto expr = orExpr(left, right);
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  assertOpName(roundTripped.value, "or");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripAndThreePredicates) {
  auto a = eq(field(BIGINT(), "x"), intConst(1));
  auto b = gt(field(BIGINT(), "y"), intConst(0));
  auto c = isNull(field(VARCHAR(), "z"));
  auto expr = andExpr(andExpr(a, b), c);
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  assertOpName(roundTripped.value, "and");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripNotEqBecomesNeq) {
  auto inner = eq(field(BIGINT(), "id"), intConst(5));
  auto expr = notExpr(inner);
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  // NOT(eq) was translated to neq by translate(), so round-trip should give
  // neq.
  assertOpName(roundTripped.value, "neq");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripNotLtBecomesGte) {
  auto inner = lt(field(BIGINT(), "x"), intConst(10));
  auto expr = notExpr(inner);
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  assertOpName(roundTripped.value, "gte");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripNotIsNullBecomesIsNotNull) {
  auto inner = isNull(field(VARCHAR(), "email"));
  auto expr = notExpr(inner);
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  assertOpName(roundTripped.value, "is_not_null");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripIntInList) {
  auto expr = inExpr(field(BIGINT(), "id"), intArrayConst({1, 3, 5}));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  // Instance method provides pool for array constant allocation.
  auto result = toTypedExpr(pred.value);
  ASSERT_TRUE(result.ok()) << result.reason;
  assertOpName(result.value, "in");
}

TEST_F(PaimonFilterTranslatorTest, RoundTripIntNotInList) {
  auto expr = notInExpr(field(BIGINT(), "id"), intArrayConst({2, 4}));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  // Instance method provides pool for array constant allocation.
  auto result = toTypedExpr(pred.value);
  ASSERT_TRUE(result.ok()) << result.reason;
  assertOpName(result.value, "not_in");
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
  auto expr = eq(field(BIGINT(), "id"), cast(BIGINT(), int32Const(99)));
  auto pred = translate(expr);
  ASSERT_TRUE(pred.ok()) << pred.reason;

  auto roundTripped = PaimonFilterTranslator::toTypedExpr(pred.value);
  ASSERT_TRUE(roundTripped.ok()) << roundTripped.reason;
  assertOpName(roundTripped.value, "eq");
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

} // namespace
