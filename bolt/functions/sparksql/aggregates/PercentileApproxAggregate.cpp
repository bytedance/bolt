/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
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

#include "bolt/functions/sparksql/aggregates/PercentileApproxAggregate.h"
namespace bytedance::bolt::functions::aggregate::sparksql {

PercentileApproxAggregateBase::PercentileApproxAggregateBase(
    bool hasAccuracy,
    const TypePtr& resultType)
    : exec::Aggregate(resultType), hasAccuracy_(hasAccuracy) {}

void PercentileApproxAggregateBase::decodePercentileAndAccuracy(
    const SelectivityVector& rows,
    const std::vector<VectorPtr>& args,
    size_t& argIndex) {
  checkSetPercentile(rows, *args[argIndex++]);
  if (hasAccuracy_) {
    decodedAccuracy_.decode(*args[argIndex++], rows, true);
    checkSetAccuracy();
  }
}

void PercentileApproxAggregateBase::extractPercentiles(
    const ArrayVector* arrays,
    vector_size_t indexInBaseVector,
    const double*& data,
    vector_size_t& len,
    std::vector<bool>& isNull) {
  auto elements = arrays->elements()->asFlatVector<double>();
  auto offset = arrays->offsetAt(indexInBaseVector);
  data = elements->rawValues() + offset;
  len = arrays->sizeAt(indexInBaseVector);
  isNull.resize(len);
  for (auto index = offset; index < offset + len; index++) {
    isNull[index - offset] = elements->isNullAt(index);
  }
}

void PercentileApproxAggregateBase::checkSetPercentile(
    const SelectivityVector& rows,
    const BaseVector& vec) {
  DecodedVector decoded(vec, rows);
  BOLT_USER_CHECK(
      decoded.isConstantMapping(),
      "Percentile argument must be constant for all input rows");
  bool isArray;
  const double* data;
  vector_size_t len;
  std::vector<bool> isNull;
  auto indexInBaseVector = decoded.index(0);
  if (decoded.base()->typeKind() == TypeKind::DOUBLE) {
    isArray = false;
    auto baseVector = decoded.base();
    data = baseVector->asUnchecked<ConstantVector<double>>()->rawValues() +
        indexInBaseVector;
    len = 1;
    isNull = {baseVector->isNullAt(indexInBaseVector)};
  } else if (decoded.base()->typeKind() == TypeKind::ARRAY) {
    isArray = true;
    auto arrays = decoded.base()->asUnchecked<ArrayVector>();
    BOLT_USER_CHECK(
        arrays->elements()->isFlatEncoding(),
        "Only flat encoding is allowed for percentile array elements");
    extractPercentiles(arrays, indexInBaseVector, data, len, isNull);
  } else {
    BOLT_USER_FAIL(
        "Incorrect type for percentile: {}", decoded.base()->typeKind());
  }
  checkSetPercentile(isArray, data, len, isNull);
}

void PercentileApproxAggregateBase::checkSetPercentile(
    bool isArray,
    const double* data,
    vector_size_t len,
    const std::vector<bool>& isNull) {
  if (!percentiles_) {
    BOLT_USER_CHECK_GT(len, 0, "Percentile cannot be empty");
    percentiles_ = {
        .values = std::vector<double>(len),
        .isArray = isArray,
    };
    for (vector_size_t i = 0; i < len; ++i) {
      BOLT_USER_CHECK(!isNull[i], "Percentage value must not be null");
      BOLT_USER_CHECK(
          data[i] >= 0.0 && data[i] <= 1.0,
          "All percentage values must be between 0.0 and 1.0 (current = {})",
          data[i]);
      percentiles_->values[i] = data[i];
    }
  } else {
    BOLT_USER_CHECK_EQ(
        isArray,
        percentiles_->isArray,
        "The percentage provided must be a constant literal");
    BOLT_USER_CHECK_EQ(
        len,
        percentiles_->values.size(),
        "Percentile argument must be constant for all input rows");
    for (vector_size_t i = 0; i < len; ++i) {
      BOLT_USER_CHECK_EQ(
          data[i],
          percentiles_->values[i],
          "Percentile argument must be constant for all input rows");
    }
  }
}

void PercentileApproxAggregateBase::checkSetAccuracy() {
  if (!hasAccuracy_) {
    return;
  }
  BOLT_USER_CHECK(
      decodedAccuracy_.isConstantMapping(),
      "The accuracy provided must be a constant literal");
  TypeKind accuracyType = decodedAccuracy_.base()->type()->kind();
  BOLT_USER_CHECK(
      accuracyType == TypeKind::BIGINT || accuracyType == TypeKind::INTEGER,
      "The accuracy provided must be a literal of integer or bigint (current value = {})",
      decodedAccuracy_.base()->type()->toString());
  if (accuracyType == TypeKind::INTEGER) {
    checkSetAccuracy(decodedAccuracy_.valueAt<int32_t>(0));
  } else {
    checkSetAccuracy(decodedAccuracy_.valueAt<int64_t>(0));
  }
}

void PercentileApproxAggregateBase::checkSetAccuracy(int64_t accuracy) {
  BOLT_USER_CHECK(
      accuracy > 0 && accuracy <= 2147483647,
      "The accuracy provided must be a literal between (0, 2147483647] (current value = {})",
      accuracy);
  const auto checkedAccuracy = static_cast<int32_t>(accuracy);
  if (accuracy_ == kDefaultAccuracy) {
    accuracy_ = checkedAccuracy;
  } else {
    BOLT_USER_CHECK_EQ(
        checkedAccuracy,
        accuracy_,
        "Accuracy argument must be constant for all input rows");
  }
}

bool validPercentileType(const Type& type) {
  if (type.kind() == TypeKind::DOUBLE) {
    return true;
  }
  if (type.kind() != TypeKind::ARRAY) {
    return false;
  }
  return type.as<TypeKind::ARRAY>().elementType()->kind() == TypeKind::DOUBLE;
}

void addSignatures(
    const std::string& inputType,
    const std::string& percentileType,
    const std::string& returnType,
    std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>&
        signatures) {
  auto intermediateType = fmt::format(
      "row(array(double), boolean, integer, {0}, varbinary)", inputType);
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType(intermediateType)
                           .argumentType(inputType)
                           .argumentType(percentileType)
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType(intermediateType)
                           .argumentType(inputType)
                           .argumentType(percentileType)
                           .argumentType("integer")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .returnType(returnType)
                           .intermediateType(intermediateType)
                           .argumentType(inputType)
                           .argumentType(percentileType)
                           .argumentType("bigint")
                           .build());
}

void addDecimalSignatures(
    std::vector<std::shared_ptr<exec::AggregateFunctionSignature>>&
        signatures) {
  auto intermediateType =
      "row(array(double), boolean, integer, DECIMAL(a_precision, a_scale), varbinary)";
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("DECIMAL(a_precision, a_scale)")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("double")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("DECIMAL(a_precision, a_scale)")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("double")
                           .argumentType("integer")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("DECIMAL(a_precision, a_scale)")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("double")
                           .argumentType("bigint")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("array(DECIMAL(a_precision, a_scale))")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("array(double)")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("array(DECIMAL(a_precision, a_scale))")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("array(double)")
                           .argumentType("integer")
                           .build());
  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .integerVariable("a_precision")
                           .integerVariable("a_scale")
                           .returnType("array(DECIMAL(a_precision, a_scale))")
                           .intermediateType(intermediateType)
                           .argumentType("DECIMAL(a_precision, a_scale)")
                           .argumentType("array(double)")
                           .argumentType("bigint")
                           .build());
}

void registerPercentileApproxAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto& inputType :
       {"tinyint",
        "smallint",
        "integer",
        "bigint",
        "hugeint",
        "real",
        "double",
        "timestamp"}) {
    addSignatures(inputType, "double", inputType, signatures);
    addSignatures(
        inputType,
        "array(double)",
        fmt::format("array({})", inputType),
        signatures);
  }
  addDecimalSignatures(signatures);
  auto name = prefix + bolt::aggregate::kPercentileApprox;
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig&
          /*config*/) -> std::unique_ptr<exec::Aggregate> {
        auto isRawInput = exec::isRawInput(step);
        bool hasAccuracy = argTypes.size() == 3 &&
            (argTypes[2]->kind() == TypeKind::BIGINT ||
             argTypes[2]->kind() == TypeKind::INTEGER);
        TypeKind accuracyType = TypeKind::INTEGER;
        if (isRawInput) {
          BOLT_USER_CHECK_EQ(
              argTypes.size(),
              2 + hasAccuracy,
              "Wrong number of arguments passed to {}",
              name);
          if (hasAccuracy) {
            accuracyType = argTypes.back()->kind();
            BOLT_USER_CHECK(
                accuracyType == TypeKind::BIGINT ||
                    accuracyType == TypeKind::INTEGER,
                "The type of the accuracy argument of {} must be BIGINT or INTEGER",
                name);
          }
          BOLT_USER_CHECK(
              validPercentileType(*argTypes[argTypes.size() - 1 - hasAccuracy]),
              "The type of the percentile argument of {} must be DOUBLE or ARRAY(DOUBLE)",
              name);
        } else {
          BOLT_USER_CHECK_EQ(
              argTypes.size(),
              1,
              "The type of partial result for {} must be ROW",
              name);
          BOLT_USER_CHECK_EQ(
              argTypes[0]->kind(),
              TypeKind::ROW,
              "The type of partial result for {} must be ROW",
              name);
        }

        TypePtr type;
        if (!isRawInput && exec::isPartialOutput(step)) {
          type = argTypes[0]->asRow().childAt(kTypePalceHolder);
        } else if (isRawInput) {
          type = argTypes[0];
        } else if (resultType->isArray()) {
          type = resultType->as<TypeKind::ARRAY>().elementType();
        } else {
          type = resultType;
        }
        switch (type->kind()) {
          case TypeKind::TINYINT:
            return PercentileApproxAggregateFactory<
                TypeTraits<TypeKind::TINYINT>::NativeType>::
                create(hasAccuracy, resultType);
          case TypeKind::SMALLINT:
            return PercentileApproxAggregateFactory<
                TypeTraits<TypeKind::SMALLINT>::NativeType>::
                create(hasAccuracy, resultType);
          case TypeKind::INTEGER:
            return PercentileApproxAggregateFactory<
                TypeTraits<TypeKind::INTEGER>::NativeType>::
                create(hasAccuracy, resultType);
          case TypeKind::BIGINT:
            return PercentileApproxAggregateFactory<TypeTraits<
                TypeKind::BIGINT>::NativeType>::create(hasAccuracy, resultType);
          case TypeKind::HUGEINT:
            return PercentileApproxAggregateFactory<
                TypeTraits<TypeKind::HUGEINT>::NativeType>::
                create(hasAccuracy, resultType);
          case TypeKind::REAL:
            return PercentileApproxAggregateFactory<TypeTraits<
                TypeKind::REAL>::NativeType>::create(hasAccuracy, resultType);
          case TypeKind::DOUBLE:
            return PercentileApproxAggregateFactory<TypeTraits<
                TypeKind::DOUBLE>::NativeType>::create(hasAccuracy, resultType);
          case TypeKind::TIMESTAMP:
            return PercentileApproxAggregateFactory<
                TypeTraits<TypeKind::TIMESTAMP>::NativeType>::
                create(hasAccuracy, resultType);
          default:
            BOLT_USER_FAIL(
                "Unsupported input type for {} aggregation {}, isRawInput: {}, setp: {}, result type: {}",
                name,
                type->toString(),
                isRawInput,
                core::AggregationNode::stepName(step),
                resultType->toString());
        }
      },
      withCompanionFunctions,
      overwrite);
}
} // namespace bytedance::bolt::functions::aggregate::sparksql
