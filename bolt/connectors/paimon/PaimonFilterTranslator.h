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

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>
#include "bolt/core/Expressions.h"
#include "bolt/core/ITypedExpr.h"
#include "bolt/type/filter/FilterBase.h"

// Forward declare paimon types to avoid including paimon headers in this
// header.
namespace paimon {
class Predicate;
class Literal;
enum class FieldType : int;
} // namespace paimon

namespace bytedance::bolt::connector::paimon {

/// Result type for TypedExpr -> paimon::Predicate translation.
/// Carries either a successful predicate or a diagnostic reason string.
struct ToPaimonPredicateResult {
  std::shared_ptr<::paimon::Predicate> value;
  std::string reason; // Non-empty when translation failed.

  bool ok() const {
    return reason.empty();
  }
  explicit operator bool() const {
    return ok();
  }
};

/// Translates filter expressions between bolt and paimon representations.
///
/// Two translation directions are supported:
///
/// 1. **TypedExpr → Predicate** (`translate`):
///    Walks a bolt TypedExpr tree (CallTypedExpr for operators,
///    FieldAccessTypedExpr for column references, ConstantTypedExpr for
///    literals) and emits corresponding ::paimon::PredicateBuilder calls. The
///    resulting predicate can be passed to ReadContextBuilder::SetPredicate()
///    for pushdown into the paimon parquet reader.
///
/// 2. **Predicate → TypedExpr** (`toTypedExpr`):
///    Converts a paimon Predicate back into a bolt TypedExpr expression tree.
/// Supported expression patterns:
///   - Comparisons: eq, neq, lt, lte, gt, gte, between, in, not_in
///   - Null checks: is_null, is_not_null
///   - Logical: and, or (fully translatable only when all children are)
///   - Negation: not (applied per-operator where supported)
class PaimonFilterTranslator {
 public:
  // -----------------------------------------------------------------------
  // Direction 1: TypedExpr → paimon::Predicate
  // -----------------------------------------------------------------------

  /// Translate a bolt TypedExpr filter expression into a paimon Predicate,
  /// resolving field indices from the given row type.
  ///
  /// @param expr    The filter expression tree (typically from TableHandle).
  /// @param rowType The output row type used to resolve column names to field
  ///                indices required by Paimon's predicate validation.
  /// @return A ToPaimonPredicateResult with the predicate or failure reason.
  static ToPaimonPredicateResult translate(
      const core::TypedExprPtr& expr,
      const RowTypePtr& rowType);

  // -----------------------------------------------------------------------
  // Direction 2a: paimon::Predicate → TypedExprPtr
  // -----------------------------------------------------------------------

  /// Result of converting a paimon Predicate to a bolt TypedExpr.
  struct ToTypedExprResult {
    core::TypedExprPtr value;
    std::string reason; // Non-empty when conversion failed.

    bool ok() const {
      return reason.empty();
    }
    explicit operator bool() const {
      return ok();
    }
  };

  /// Convert a paimon Predicate back into a bolt TypedExpr expression tree.
  ///
  /// Walks the predicate tree and reconstructs bolt CallTypedExpr nodes
  /// (eq, neq, lt, gt, and, or, is_null, etc.) that represent the same
  /// filter logic. This enables full bidirectional translation for
  /// round-trip verification.
  ///
  /// @param predicate The paimon predicate to convert back.
  /// @param pool      Memory pool for buffer allocation (e.g. array constants).
  ///                   May be nullptr — IN/NOT_IN will fail gracefully in that
  ///                   case.
  /// @return A ToTypedExprResult with the expression or failure reason.
  static ToTypedExprResult toTypedExpr(
      const std::shared_ptr<::paimon::Predicate>& predicate,
      memory::MemoryPool* pool = nullptr);

  // -----------------------------------------------------------------------
  // Direction 2b: TypedExpr → SubfieldFilters (constant-only, no evaluator)
  // -----------------------------------------------------------------------

  /// Result of building a DWIO Filter from a CallTypedExpr.
  struct FilterBuildResult {
    std::unique_ptr<common::Filter> value;
    std::string reason; // Non-empty when conversion failed.

    bool ok() const {
      return reason.empty();
    }
    explicit operator bool() const {
      return ok();
    }
  };

  /// Convert a TypedExpr into SubfieldFilters for DWIO scan spec pushdown.
  ///
  /// Designed for TypedExpr trees produced by toTypedExpr(), where all
  /// literals are ConstantTypedExpr nodes. Does not require an
  /// ExpressionEvaluator — values are read directly from constants.
  ///
  /// Handles AND-flattening. OR is only converted when all children are
  /// EQUAL predicates on the same column (produces a Values filter).
  static common::SubfieldFilters toSubfieldFilters(
      const core::TypedExprPtr& expr);

 private:
  struct FieldInfo {
    std::string name;
    int32_t index;
    ::paimon::FieldType fieldType;
  };

  // --- TypedExpr → Predicate helpers ---

  /// Normalize operator names from various SQL parsers to the canonical short
  /// forms used internally (e.g., SparkSQL's "greaterthanorequal" -> "gte").
  static std::string normalizeOpName(const std::string& opName);

  /// Translate a CallTypedExpr node with row type for field index resolution.
  static ToPaimonPredicateResult translateCall(
      const core::CallTypedExpr& call,
      const RowTypePtr& rowType);

  /// Try to extract field reference info, resolving the index from rowType.
  static std::optional<FieldInfo> extractFieldInfo(
      const core::TypedExprPtr& expr,
      const RowTypePtr& rowType);

  /// Try to extract field reference info from an expression node.
  static std::optional<FieldInfo> extractFieldInfo(
      const core::TypedExprPtr& expr);

  /// Extract a constant value as a paimon Literal.
  static std::optional<::paimon::Literal> extractLiteral(
      const core::TypedExprPtr& expr,
      ::paimon::FieldType fieldType);

  /// Extract IN-list literal values from an array ConstantTypedExpr.
  /// Follows Hive's makeInFilter pattern: uses ArrayVector's
  /// offsetAt/sizeAt/elements() to extract flattened elements.
  static std::optional<std::vector<::paimon::Literal>> extractInListLiterals(
      const core::TypedExprPtr& expr,
      ::paimon::FieldType fieldType);

  /// Map a bolt TypeKind to a paimon FieldType.
  /// Returns nullopt if the type is not supported for pushdown.
  static std::optional<::paimon::FieldType> toPaimonFieldType(
      const TypePtr& type);

  /// Map a function name with negation flag to its non-negated form.
  /// Returns nullopt if the operator doesn't support negation via name swap.
  static std::optional<std::string> applyNegation(const std::string& opName);

  // --- Predicate → TypedExpr helpers ---

  /// Convert a paimon Literal into a bolt ConstantTypedExpr.
  static core::TypedExprPtr literalToConstantExpr(
      const ::paimon::Literal& literal,
      ::paimon::FieldType fieldType);

  /// Map a paimon FieldType back to a bolt TypePtr.
  static TypePtr fromPaimonFieldType(::paimon::FieldType fieldType);

  /// Map a paimon function type (as int) to the corresponding bolt operator
  /// name.
  static std::optional<std::string> functionToOpName(int functionType);
};

} // namespace bytedance::bolt::connector::paimon
