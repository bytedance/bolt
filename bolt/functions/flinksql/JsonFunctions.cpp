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

#include "bolt/expression/VectorFunction.h"
#include "bolt/functions/prestosql/json/SIMDJsonWrapper.h"
#include "bolt/functions/prestosql/types/JsonType.h"
#include "bolt/vector/ConstantVector.h"
#include "bolt/vector/FlatVector.h"
namespace bytedance::bolt::functions::flinksql {
namespace {
bool isDigit(char c) {
  return c >= '0' && c <= '9';
}

// Validate JSON numbers syntactically here because json_str_to_map only needs
// to distinguish valid non-object JSON from malformed input. simdjson's DOM
// parser materializes numbers and may reject out-of-range literals like 1e400,
// even though they are valid JSON values for this purpose.
bool isValidJsonNumber(std::string_view number) {
  size_t pos = number[0] == '-';
  if (pos == number.size()) {
    return false;
  }
  if (number[pos] == '0') {
    ++pos;
    if (pos < number.size() && isDigit(number[pos])) {
      return false;
    }
  } else if (number[pos] >= '1' && number[pos] <= '9') {
    while (++pos < number.size() && isDigit(number[pos])) {
    }
  } else {
    return false;
  }
  if (pos < number.size() && number[pos] == '.') {
    if (++pos == number.size() || !isDigit(number[pos])) {
      return false;
    }
    while (++pos < number.size() && isDigit(number[pos])) {
    }
  }
  if (pos < number.size() && (number[pos] == 'e' || number[pos] == 'E')) {
    ++pos;
    if (pos < number.size() && (number[pos] == '+' || number[pos] == '-')) {
      ++pos;
    }
    if (pos == number.size() || !isDigit(number[pos])) {
      return false;
    }
    while (++pos < number.size() && isDigit(number[pos])) {
    }
  }
  return pos == number.size();
}

template <typename NativeType>
VectorPtr checkAndFlatten(const SelectivityVector& rows, VectorPtr& input) {
  BOLT_CHECK_NOT_NULL(input);
  if (input->as<SimpleVector<NativeType>>()) {
    return input;
  }
  DecodedVector decoded(*input, rows);
  auto flatVector = BaseVector::create<FlatVector<NativeType>>(
      input->type(), decoded.size(), input->pool());
  if (std::is_same_v<NativeType, StringView>) {
    flatVector->acquireSharedStringBuffers(input.get());
  }
  if (decoded.mayHaveNulls()) {
    rows.applyToSelected([&](vector_size_t row) {
      if (decoded.isNullAt(row)) {
        flatVector->setNull(row, true);
      } else if constexpr (std::is_same_v<NativeType, StringView>) {
        flatVector->setNoCopy(row, decoded.valueAt<NativeType>(row));
      } else {
        flatVector->set(row, decoded.valueAt<NativeType>(row));
      }
    });
  } else {
    rows.applyToSelected([&](vector_size_t row) {
      if constexpr (std::is_same_v<NativeType, StringView>) {
        flatVector->setNoCopy(row, decoded.valueAt<NativeType>(row));
      } else {
        flatVector->set(row, decoded.valueAt<NativeType>(row));
      }
    });
  }
  return flatVector;
}

class JsonStrToMapFunction : public bytedance::bolt::exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto castFactory =
        dynamic_cast<const JsonCastOperator*>(JsonCastOperator::get().get());

    auto input = checkAndFlatten<StringView>(rows, args[0]);
    auto* inputVector = input->as<SimpleVector<StringView>>();
    VectorPtr logFailuresOnly;
    SimpleVector<bool>* logFailuresOnlyVector{nullptr};
    if (args.size() == 2) {
      logFailuresOnly = checkAndFlatten<bool>(rows, args[1]);
      logFailuresOnlyVector = logFailuresOnly->as<SimpleVector<bool>>();
    }

    exec::VectorWriter<Any> writer;
    exec::LocalSelectivityVector remainingRowsHolder(context);
    SelectivityVector* remainingRows = nullptr;

    auto ensureWritable = [&]() {
      if (remainingRows == nullptr) {
        context.ensureWritable(rows, outputType, result);
        writer.init(*result);
        remainingRows = remainingRowsHolder.get(rows);
      }
    };

    rows.applyToSelected([&](vector_size_t row) {
      if (inputVector->isNullAt(row)) {
        return;
      }
      auto value = inputVector->valueAt(row);
      auto trimmedValue = folly::trimWhitespace(value);
      if (trimmedValue.empty() || trimmedValue.front() == '{') {
        return;
      }
      if (auto error = validateNonObjectJson(trimmedValue)) {
        ensureWritable();
        writer.setOffset(row);
        writer.commitNull();
        remainingRows->setValid(row, false);
        if (logFailuresOnlyVector && logFailuresOnlyVector->valueAt(row)) {
          LOG(WARNING) << "Failed to parse "
                       << std::string_view(value.data(), value.size());
        } else {
          folly::call_once(initializeErrors_, [this] {
            simdjsonErrorsToExceptions(errors_);
          });
          context.setBoltExceptionError(row, errors_[error]);
        }
      } else {
        ensureWritable();
        writer.setOffset(row);
        writer.current().castTo<Map<Any, Any>>();
        writer.commit(true);
        remainingRows->setValid(row, false);
      }
    });

    if (remainingRows == nullptr) {
      castFactory->castFrom(
          *input, context, rows, outputType, result, false, logFailuresOnly);
      return;
    }

    writer.finish();
    remainingRows->updateBounds();
    if (remainingRows->hasSelections()) {
      castFactory->castFrom(
          *input,
          context,
          *remainingRows,
          outputType,
          result,
          false,
          logFailuresOnly);
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        exec::FunctionSignatureBuilder()
            .returnType("map(varchar,varchar)")
            .argumentType("varchar")
            .build(),
        exec::FunctionSignatureBuilder()
            .returnType("map(varchar,varchar)")
            .argumentType("varchar")
            .argumentType("boolean")
            .build()};
  }

 private:
  static simdjson::error_code validateNonObjectJson(
      std::string_view trimmedInput) {
    if (trimmedInput == "true" || trimmedInput == "false" ||
        trimmedInput == "null" || isValidJsonNumber(trimmedInput)) {
      return simdjson::SUCCESS;
    }

    thread_local simdjson::dom::parser parser;
    simdjson::dom::element doc;
    if (auto error = parser.parse(trimmedInput).get(doc)) {
      return error;
    }
    return doc.type() == simdjson::dom::element_type::OBJECT
        ? simdjson::INCORRECT_TYPE
        : simdjson::SUCCESS;
  }

  mutable folly::once_flag initializeErrors_;
  mutable std::exception_ptr errors_[simdjson::NUM_ERROR_CODES];
};

class JsonStrToArrayFunction : public bytedance::bolt::exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto castFactory =
        dynamic_cast<const JsonCastOperator*>(JsonCastOperator::get().get());
    VectorPtr logFailuresOnly;
    if (args.size() == 2) {
      logFailuresOnly = checkAndFlatten<bool>(rows, args[1]);
    }
    castFactory->castFrom(
        *checkAndFlatten<StringView>(rows, args[0]),
        context,
        rows,
        outputType,
        result,
        false,
        std::move(logFailuresOnly));
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        exec::FunctionSignatureBuilder()
            .returnType("array(varchar)")
            .argumentType("varchar")
            .build(),
        exec::FunctionSignatureBuilder()
            .returnType("array(varchar)")
            .argumentType("varchar")
            .argumentType("boolean")
            .build()};
  }
};
} // namespace

BOLT_DECLARE_VECTOR_FUNCTION(
    json_str_to_map,
    JsonStrToMapFunction::signatures(),
    std::make_unique<JsonStrToMapFunction>());
BOLT_DECLARE_VECTOR_FUNCTION(
    json_str_to_array,
    JsonStrToArrayFunction::signatures(),
    std::make_unique<JsonStrToArrayFunction>());
} // namespace bytedance::bolt::functions::flinksql
