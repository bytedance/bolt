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
 */

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

#include "bolt/functions/prestosql/types/JsonType.h"

#include <ryu/ryu.h>
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/encode/Base64.h"
#include "bolt/expression/EvalCtx.h"
#include "bolt/expression/PeeledEncoding.h"
#include "bolt/expression/StringWriter.h"
#include "bolt/functions/lib/RowsTranslationUtil.h"
#include "bolt/functions/prestosql/json/JsonUtil.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt {

namespace {

template <typename T, bool isMapKey = false>
void generateJsonTyped(
    const SimpleVector<T>& input,
    int row,
    std::string& result,
    const TypePtr& type,
    const bool isToJson) {
  auto value = input.valueAt(row);

  if constexpr (std::is_same_v<T, StringView>) {
    if (LIKELY(type->isVarchar())) {
      // TODO Presto escapes Unicode characters using uppercase hex:
      //  SELECT cast(U&'\+01F64F' as json); -- "\uD83D\uDE4F"
      //  Folly uses lowercase hex digits: "\ud83d\ude4f".
      // Figure out how to produce uppercase digits.
      folly::json::serialization_opts opts;
      folly::json::escapeString(value, result, opts);
    } else if (type->isVarbinary()) {
      // Spark's to_json function will encode binary using base64 encoding
      result.append("\"");
      result.append(encoding::Base64::encode(value));
      result.append("\"");
    } else {
      BOLT_UNREACHABLE();
    }
  } else if constexpr (std::is_same_v<T, UnknownValue>) {
    BOLT_FAIL(
        "Casting UNKNOWN to JSON: Vectors of UNKNOWN type should not contain non-null rows");
  } else {
    if constexpr (isMapKey) {
      result.append("\"");
    }

    // The original logic will use the default toString logic when processing
    // decimals, rather than the specific toString logic of decimals. Currently,
    // the modifications will follow the decimal specific toString logic.
    if (isToJson) {
      if constexpr (
          std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
          std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
          std::is_same_v<T, int128_t>) {
        if (type->isDate()) {
          result.append("\"");
          result.append(DATE()->toString(value));
          result.append("\"");
        } else if (type->isDecimal()) {
          result.append(DecimalUtil::toString(value, type));
        } else if (
            type->isTinyint() || type->isSmallint() || type->isInteger() ||
            type->isBigint() || type->isHugeint()) {
          folly::toAppend<std::string, T>(value, &result);
        } else {
          // No other logical types are currently supported
          BOLT_FAIL(fmt::format(
              "Casting {} to JSON is not supported in to_json function.",
              type->toString()));
        }
      } else if constexpr (std::is_same_v<T, double>) {
        if (type->isDouble()) {
          if constexpr (!isMapKey) {
            if (std::isnan(value)) {
              result.append("\"NaN\"");
              return;
            }
          }
          auto doublePtr = d2s(value);
          std::string doubleStr(doublePtr);
          free(doublePtr);
          result.append(doubleStr);

        } else {
          // No other logical types are currently supported
          BOLT_FAIL(fmt::format(
              "Casting {} to JSON is not supported in to_json function.",
              type->toString()));
        }
      } else if constexpr (std::is_same_v<T, float>) {
        if (type->isReal()) {
          if constexpr (!isMapKey) {
            if (std::isnan(value)) {
              result.append("\"NaN\"");
              return;
            }
          }

          auto floatPtr = f2s(value);
          std::string floatStr(floatPtr);
          free(floatPtr);
          result.append(floatStr);
        } else {
          // No other logical types are currently supported
          BOLT_FAIL(fmt::format(
              "Casting {} to JSON is not supported in to_json function.",
              type->toString()));
        }
      } else if constexpr (std::is_same_v<T, bool>) {
        if (type->isBoolean()) {
          result.append(value ? "true" : "false");
        } else {
          // No other logical types are currently supported
          BOLT_FAIL(fmt::format(
              "Casting {} to JSON is not supported in to_json function.",
              type->toString()));
        }
      } else {
        // No other physical types are currently supported
        BOLT_FAIL(fmt::format(
            "Casting {} to JSON is not supported in to_json function.",
            type->toString()));
      }
    } else {
      if constexpr (std::is_same_v<T, bool>) {
        result.append(value ? "true" : "false");
      } else if constexpr (std::is_same_v<T, Timestamp>) {
        result.append(std::to_string(value));
      } else if (type->isDate()) {
        result.append(DATE()->toString(value));
      } else if (type->isDecimal()) {
        result.append(DecimalUtil::toString(value, type));
      } else {
        folly::toAppend<std::string, T>(value, &result);
      }
    }

    if constexpr (isMapKey) {
      result.append("\"");
    }
  }
}

// Casts primitive-type input vectors to Json type.
template <
    TypeKind kind,
    typename std::enable_if_t<TypeTraits<kind>::isPrimitiveType, int> = 0>
void castToJson(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    bool isMapKey = false,
    const bool isToJson = false,
    const bool isTopLevel = false) {
  using T = typename TypeTraits<kind>::NativeType;

  // input is guaranteed to be in flat or constant encodings when passed in.
  auto inputVector = input.as<SimpleVector<T>>();

  std::string result;
  if (!isMapKey) {
    context.applyToSelectedNoThrow(rows, [&](auto row) {
      if (inputVector->isNullAt(row)) {
        flatResult.set(row, "null");
      } else {
        result.clear();
        generateJsonTyped(*inputVector, row, result, input.type(), isToJson);

        flatResult.set(row, StringView{result});
      }
    });
  } else {
    context.applyToSelectedNoThrow(rows, [&](auto row) {
      if (inputVector->isNullAt(row)) {
        BOLT_USER_FAIL("Map keys cannot be null.");
      } else {
        result.clear();
        generateJsonTyped<T, true>(
            *inputVector, row, result, input.type(), isToJson);

        flatResult.set(row, StringView{result});
      }
    });
  }
}

// Forward declaration.
void castToJsonFromArray(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const bool isToJson,
    const bool isTopLevel);

void castToJsonFromMap(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const bool isToJson,
    const bool isTopLevel);

void castToJsonFromRow(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const bool isToJson,
    const bool isTopLevel);

// Casts complex-type input vectors to Json type.
template <
    TypeKind kind,
    typename std::enable_if_t<!TypeTraits<kind>::isPrimitiveType, int> = 0>
void castToJson(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    bool isMapKey = false,
    const bool isToJson = false,
    const bool isTopLevel = false) {
  BOLT_CHECK(
      !isMapKey, "Casting map with complex key type to JSON is not supported");

  if constexpr (kind == TypeKind::ARRAY) {
    castToJsonFromArray(input, context, rows, flatResult, isToJson, isTopLevel);
  } else if constexpr (kind == TypeKind::MAP) {
    castToJsonFromMap(input, context, rows, flatResult, isToJson, isTopLevel);
  } else if constexpr (kind == TypeKind::ROW) {
    castToJsonFromRow(input, context, rows, flatResult, isToJson, isTopLevel);
  } else {
    BOLT_FAIL("Casting {} to JSON is not supported.", input.type()->toString());
  }
}

// Helper struct representing the Json vector of input.
struct AsJson {
  AsJson(
      exec::EvalCtx& context,
      const VectorPtr& input,
      const SelectivityVector& rows,
      const BufferPtr& elementToTopLevelRows,
      bool isMapKey = false,
      const bool isToJson = false,
      const bool isTopLevel = false)
      : decoded_(context) {
    ErrorVectorPtr oldErrors;
    context.swapErrors(oldErrors);
    if (isJsonType(input->type())) {
      json_ = input;
    } else {
      if (!exec::PeeledEncoding::isPeelable(input->encoding()) ||
          !rows.hasSelections()) {
        doCast(context, input, rows, isMapKey, json_, isToJson, isTopLevel);
      } else {
        exec::withContextSaver([&](exec::ContextSaver& saver) {
          exec::LocalSelectivityVector newRowsHolder(*context.execCtx());

          exec::LocalDecodedVector localDecoded(context);
          std::vector<VectorPtr> peeledVectors;
          auto peeledEncoding = exec::PeeledEncoding::peel(
              {input}, rows, localDecoded, true, peeledVectors);
          BOLT_CHECK_EQ(peeledVectors.size(), 1);
          auto newRows =
              peeledEncoding->translateToInnerRows(rows, newRowsHolder);
          // Save context and set the peel.
          context.saveAndReset(saver, rows);
          context.setPeeledEncoding(peeledEncoding);

          doCast(
              context,
              peeledVectors[0],
              *newRows,
              isMapKey,
              json_,
              isToJson,
              isTopLevel);
          json_ = context.getPeeledEncoding()->wrap(
              json_->type(), context.pool(), json_, rows);
        });
      }
    }
    decoded_.get()->decode(*json_, rows);
    jsonStrings_ = decoded_->base()->as<SimpleVector<StringView>>();

    if (isMapKey && decoded_->mayHaveNulls()) {
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        if (decoded_->isNullAt(row)) {
          BOLT_USER_FAIL("Cannot cast map with null keys to JSON.");
        }
      });
    }
    combineErrors(context, rows, elementToTopLevelRows, oldErrors);
  }

  StringView at(vector_size_t i) const {
    return jsonStrings_->valueAt(decoded_->index(i));
  }

  // Returns the length of the json string of the value at i, when this
  // value will be inlined as an element in the json string of an array, map, or
  // row.
  vector_size_t lengthAt(vector_size_t i) const {
    if (decoded_->isNullAt(i)) {
      // Null values are inlined as "null".
      return 4;
    } else {
      return this->at(i).size();
    }
  }

  // Appends the json string of the value at i to a string writer.
  void append(vector_size_t i, exec::StringWriter<>& proxy) const {
    if (decoded_->isNullAt(i)) {
      proxy.append("null");
    } else {
      proxy.append(this->at(i));
    }
  }

  bool isNullAt(vector_size_t i) {
    return decoded_->isNullAt(i);
  }

 private:
  void doCast(
      exec::EvalCtx& context,
      const VectorPtr& input,
      const SelectivityVector& baseRows,
      bool isMapKey,
      VectorPtr& result,
      const bool isToJson,
      const bool isTopLevel) {
    context.ensureWritable(baseRows, JSON(), result);
    auto flatJsonStrings = result->as<FlatVector<StringView>>();

    BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
        castToJson,
        input->typeKind(),
        *input,
        context,
        baseRows,
        *flatJsonStrings,
        isMapKey,
        isToJson,
        isTopLevel);
  }

  // Combine exceptions in oldErrors into context.errors_ with a transformation
  // of rows mapping provided by elementToTopLevelRows. If there are exceptions
  // at the same row in both context.errors_ and oldErrors, the one in oldErrors
  // remains. elementToTopLevelRows can be a nullptr, meaning that the rows in
  // context.errors_ correspond to rows in oldErrors exactly.
  void combineErrors(
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const BufferPtr& elementToTopLevelRows,
      ErrorVectorPtr& oldErrors) {
    if (context.errors()) {
      if (elementToTopLevelRows) {
        context.addElementErrorsToTopLevel(
            rows, elementToTopLevelRows, oldErrors);
      } else {
        context.addErrors(rows, *context.errorsPtr(), oldErrors);
      }
    }
    context.swapErrors(oldErrors);
  }

  exec::LocalDecodedVector decoded_;
  VectorPtr json_;
  const SimpleVector<StringView>* jsonStrings_;
};

void castToJsonFromArray(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const bool isToJson,
    const bool isTopLevel) {
  // input is guaranteed to be in flat encoding when passed in.
  auto inputArray = input.as<ArrayVector>();

  auto elements = inputArray->elements();
  auto elementsRows =
      functions::toElementRows(elements->size(), rows, inputArray);

  if (!elementsRows.hasSelections()) {
    // All arrays are null or empty.
    context.applyToSelectedNoThrow(rows, [&](auto row) {
      if (inputArray->isNullAt(row)) {
        if (isToJson) {
          flatResult.setNull(row, true);
        } else {
          flatResult.set(row, "null");
        }
      } else {
        BOLT_CHECK_EQ(
            inputArray->sizeAt(row),
            0,
            "All arrays are expected to be null or empty");
        flatResult.set(row, "[]");
      }
    });
    return;
  }

  auto elementToTopLevelRows = functions::getElementToTopLevelRows(
      elements->size(), rows, inputArray, context.pool());
  AsJson elementsAsJson{
      context,
      elements,
      elementsRows,
      elementToTopLevelRows,
      /*isMapKey=*/false,
      isToJson,
      /*isTopLevel=*/false};

  // Estimates an upperbound of the total length of all Json strings for the
  // input according to the length of all elements Json strings and the
  // delimiters to be added.
  size_t elementsStringSize = 0;
  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (inputArray->isNullAt(row)) {
      // "null" will be inlined in the StringView.
      return;
    }

    auto offset = inputArray->offsetAt(row);
    auto size = inputArray->sizeAt(row);
    for (auto i = offset, end = offset + size; i < end; ++i) {
      elementsStringSize += elementsAsJson.lengthAt(i);
    }

    // Extra length for commas and brackets.
    elementsStringSize += size > 0 ? size + 1 : 2;
  });

  flatResult.getBufferWithSpace(elementsStringSize);

  // Constructs the Json string of each array from Json strings of its elements.
  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (inputArray->isNullAt(row)) {
      if (isToJson) {
        flatResult.setNull(row, true);
      } else {
        flatResult.set(row, "null");
      }
      return;
    }

    auto offset = inputArray->offsetAt(row);
    auto size = inputArray->sizeAt(row);

    auto proxy = exec::StringWriter<>(&flatResult, row);

    proxy.append("["_sv);
    for (int i = offset, end = offset + size; i < end; ++i) {
      if (i > offset) {
        proxy.append(","_sv);
      }
      elementsAsJson.append(i, proxy);
    }
    proxy.append("]"_sv);

    proxy.finalize();
  });
}

void castToJsonFromMap(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const bool isToJson,
    const bool isTopLevel) {
  // input is guaranteed to be in flat encoding when passed in.
  auto inputMap = input.as<MapVector>();

  auto mapKeys = inputMap->mapKeys();
  auto mapValues = inputMap->mapValues();
  auto elementsRows = functions::toElementRows(mapKeys->size(), rows, inputMap);
  if (!elementsRows.hasSelections()) {
    // All maps are null or empty.
    context.applyToSelectedNoThrow(rows, [&](auto row) {
      if (inputMap->isNullAt(row)) {
        if (isToJson) {
          flatResult.setNull(row, true);
        } else {
          flatResult.set(row, "null");
        }
      } else {
        BOLT_CHECK_EQ(
            inputMap->sizeAt(row),
            0,
            "All maps are expected to be null or empty");
        flatResult.set(row, "{}");
      }
    });
    return;
  }

  auto elementToTopLevelRows = functions::getElementToTopLevelRows(
      mapKeys->size(), rows, inputMap, context.pool());
  // Maps with unsupported key types should have already been rejected by
  // JsonCastOperator::isSupportedType() beforehand.
  AsJson keysAsJson{
      context,
      mapKeys,
      elementsRows,
      elementToTopLevelRows,
      /*isMapKey=*/true,
      isToJson,
      /*isTopLevel=*/false};
  AsJson valuesAsJson{
      context,
      mapValues,
      elementsRows,
      elementToTopLevelRows,
      /*isMapKey=*/false,
      isToJson,
      /*isTopLevel=*/false};

  // Estimates an upperbound of the total length of all Json strings for the
  // input according to the length of all elements Json strings and the
  // delimiters to be added.
  size_t elementsStringSize = 0;
  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (inputMap->isNullAt(row)) {
      // "null" will be inlined in the StringView.
      return;
    }

    auto offset = inputMap->offsetAt(row);
    auto size = inputMap->sizeAt(row);
    for (auto i = offset, end = offset + size; i < end; ++i) {
      // The construction of keysAsJson ensured there is no null in keysAsJson.
      elementsStringSize += keysAsJson.at(i).size() + valuesAsJson.lengthAt(i);
    }

    // Extra length for commas, semicolons, and curly braces.
    elementsStringSize += size > 0 ? size * 2 + 1 : 2;
  });

  flatResult.getBufferWithSpace(elementsStringSize);

  // Constructs the Json string of each map from Json strings of its keys and
  // values.
  std::vector<std::pair<StringView, vector_size_t>> sortedKeys;
  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (inputMap->isNullAt(row)) {
      if (isToJson) {
        flatResult.setNull(row, true);
      } else {
        flatResult.set(row, "null");
      }
      return;
    }

    auto offset = inputMap->offsetAt(row);
    auto size = inputMap->sizeAt(row);

    // Sort entries by keys in each map.
    sortedKeys.clear();
    for (int i = offset, end = offset + size; i < end; ++i) {
      sortedKeys.push_back(std::make_pair(keysAsJson.at(i), i));
    }

    if (!isToJson) {
      std::sort(sortedKeys.begin(), sortedKeys.end());
    }

    auto proxy = exec::StringWriter<>(&flatResult, row);

    proxy.append("{"_sv);
    for (auto it = sortedKeys.begin(); it != sortedKeys.end(); ++it) {
      if (it != sortedKeys.begin()) {
        proxy.append(","_sv);
      }
      proxy.append(it->first);
      proxy.append(":"_sv);
      valuesAsJson.append(it->second, proxy);
    }
    proxy.append("}"_sv);

    proxy.finalize();
  });
}

void castToJsonFromRow(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<StringView>& flatResult,
    const bool isToJson,
    const bool isTopLevel) {
  // input is guaranteed to be in flat encoding when passed in.
  BOLT_CHECK_EQ(input.encoding(), VectorEncoding::Simple::ROW);
  auto inputRow = input.as<RowVector>();
  auto childrenSize = inputRow->childrenSize();

  // Estimates an upperbound of the total length of all Json strings for the
  // input according to the length of all children Json strings and the
  // delimiters to be added.
  size_t childrenStringSize = 0;
  std::vector<AsJson> childrenAsJson;
  for (int i = 0; i < childrenSize; ++i) {
    childrenAsJson.emplace_back(
        context,
        inputRow->childAt(i),
        rows,
        /*elementToTopLevelRows=*/nullptr,
        /*isMapKey=*/false,
        isToJson,
        /*isTopLevel=*/false);

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      if (inputRow->isNullAt(row)) {
        // "null" will be inlined in the StringView.
        return;
      }
      childrenStringSize += childrenAsJson[i].lengthAt(row);
    });
  }

  // Extra length for commas and brackets.
  childrenStringSize +=
      rows.countSelected() * (childrenSize > 0 ? childrenSize + 1 : 2);
  flatResult.getBufferWithSpace(childrenStringSize);

  // Constructs Json string of each row from Json strings of its children.
  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (inputRow->isNullAt(row)) {
      if (isToJson) {
        if (isTopLevel) {
          flatResult.setNull(row, true);
        } else {
          flatResult.set(row, "{}");
        }
      } else {
        flatResult.set(row, "null");
      }
      return;
    }

    auto proxy = exec::StringWriter<>(&flatResult, row);

    if (isToJson) {
      proxy.append("{"_sv);
    } else {
      proxy.append("["_sv);
    }
    bool hasNonNullField = false;
    for (int i = 0; i < childrenSize; ++i) {
      if (isToJson) {
        if (childrenAsJson[i].isNullAt(row) ||
            childrenAsJson[i].at(row) == "null") {
          continue;
        }
        if (hasNonNullField) {
          proxy.append(","_sv);
        }
      } else {
        if (i > 0) {
          proxy.append(","_sv);
        }
      }
      if (isToJson) {
        std::string ans;
        std::string key =
            dynamic_cast<const RowType*>(inputRow->type().get())->nameOf(i);
        folly::json::escapeString(key, ans, folly::json::serialization_opts{});
        proxy.append(ans);
        proxy.append(":"_sv);
      }
      childrenAsJson[i].append(row, proxy);
      hasNonNullField = true;
    }
    if (isToJson) {
      proxy.append("}"_sv);
    } else {
      proxy.append("]"_sv);
    }
    proxy.finalize();
  });
}

template <typename T>
simdjson::simdjson_result<T> fromString(const std::string_view& s) {
  auto result = folly::tryTo<T>(s);
  if (result.hasError()) {
    return simdjson::INCORRECT_TYPE;
  }
  return std::move(*result);
}

template <>
simdjson::simdjson_result<UnknownValue> fromString(const std::string_view& s) {
  return simdjson::INCORRECT_TYPE;
}

// Write x to writer if x is in the range of writer type `To'.  Only the
// following cases are supported:
//
// Signed Integer -> Signed Integer
// Float | Double -> Float | Double | Signed Integer
template <typename To, typename From>
simdjson::error_code convertIfInRange(From x, exec::GenericWriter& writer) {
  static_assert(std::is_signed_v<From> && std::is_signed_v<To>);
  static_assert(std::is_integral_v<To> || !std::is_integral_v<From>);
  if constexpr (!std::is_same_v<To, From>) {
    constexpr From kMin = static_cast<From>(std::numeric_limits<To>::lowest());
    constexpr From kMax = static_cast<From>(std::numeric_limits<To>::max());
    if (!(kMin <= x && x <= kMax)) {
      return simdjson::NUMBER_OUT_OF_RANGE;
    }
  }
  writer.castTo<To>() = x;
  return simdjson::SUCCESS;
}

template <TypeKind kind>
simdjson::error_code appendMapKey(
    const std::string_view& value,
    exec::GenericWriter& writer) {
  using T = typename TypeTraits<kind>::NativeType;
  if constexpr (std::is_same_v<T, void>) {
    return simdjson::INCORRECT_TYPE;
  } else {
    SIMDJSON_ASSIGN_OR_RAISE(writer.castTo<T>(), fromString<T>(value));
    return simdjson::SUCCESS;
  }
}

template <>
simdjson::error_code appendMapKey<TypeKind::VARCHAR>(
    const std::string_view& value,
    exec::GenericWriter& writer) {
  writer.castTo<Varchar>().append(value);
  return simdjson::SUCCESS;
}

template <>
simdjson::error_code appendMapKey<TypeKind::VARBINARY>(
    const std::string_view& /*value*/,
    exec::GenericWriter& /*writer*/) {
  return simdjson::INCORRECT_TYPE;
}

template <>
simdjson::error_code appendMapKey<TypeKind::TIMESTAMP>(
    const std::string_view& /*value*/,
    exec::GenericWriter& /*writer*/) {
  return simdjson::INCORRECT_TYPE;
}

template <typename Input>
struct CastFromJsonTypedImpl {
  template <TypeKind kind>
  static simdjson::error_code
  apply(Input input, exec::GenericWriter& writer, const bool isFromJson) {
    return KindDispatcher<kind>::apply(input, writer, isFromJson);
  }

 private:
  // Dummy is needed because full/explicit specialization is not allowed inside
  // class.
  template <TypeKind kind, typename Dummy = void>
  struct KindDispatcher {
    static simdjson::error_code apply(Input, exec::GenericWriter&, const bool) {
      BOLT_NYI(
          "Casting from JSON to {} is not supported.", TypeTraits<kind>::name);
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::VARCHAR, Dummy> {
    static simdjson::error_code
    apply(Input value, exec::GenericWriter& writer, const bool /*isFromJson*/) {
      SIMDJSON_ASSIGN_OR_RAISE(auto type, value.type());
      std::string_view s;
      if (isJsonType(writer.type())) {
        SIMDJSON_ASSIGN_OR_RAISE(s, rawJson(value, type));
      } else {
        switch (type) {
          case simdjson::ondemand::json_type::string: {
            SIMDJSON_ASSIGN_OR_RAISE(s, value.get_string(true));
            break;
          }
          case simdjson::ondemand::json_type::number:
          case simdjson::ondemand::json_type::boolean:
            s = value.raw_json_token();
            break;
          case simdjson::ondemand::json_type::array:
          case simdjson::ondemand::json_type::object: {
            // Due to the fact that when value is an object, the native logic
            // will throw an exception, and Java logic needs to cast the object
            // into a string.
            // simdjson::to_json_string generates spaces by default, while
            // functions::JsonUtil::boltToJsonString<simdjson::ondemand::value>
            // does not generate spaces by default and aligns with Java
            std::string ans;
            if (auto error = functions::JsonUtil::boltToJsonString<
                    simdjson::ondemand::value>(value, true, 0, ans)) {
              writer.castTo<Varchar>().append("null");
            } else {
              writer.castTo<Varchar>().append(ans);
            }
            return simdjson::SUCCESS;
          }
          default:
            return simdjson::INCORRECT_TYPE;
        }
      }
      writer.castTo<Varchar>().append(s);
      return simdjson::SUCCESS;
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::BOOLEAN, Dummy> {
    static simdjson::error_code
    apply(Input value, exec::GenericWriter& writer, const bool /*isFromJson*/) {
      SIMDJSON_ASSIGN_OR_RAISE(auto type, value.type());
      auto& w = writer.castTo<bool>();
      switch (type) {
        case simdjson::ondemand::json_type::boolean: {
          SIMDJSON_ASSIGN_OR_RAISE(w, value.get_bool());
          break;
        }
        case simdjson::ondemand::json_type::number: {
          SIMDJSON_ASSIGN_OR_RAISE(auto num, value.get_number());
          switch (num.get_number_type()) {
            case simdjson::ondemand::number_type::floating_point_number:
              w = num.get_double() != 0;
              break;
            case simdjson::ondemand::number_type::signed_integer:
              w = num.get_int64() != 0;
              break;
            case simdjson::ondemand::number_type::unsigned_integer:
              w = num.get_uint64() != 0;
              break;
            case simdjson::ondemand::number_type::big_integer:
              BOLT_UNREACHABLE(); // value.get_number() would have failed
                                  // already.
          }
          break;
        }
        case simdjson::ondemand::json_type::string: {
          SIMDJSON_ASSIGN_OR_RAISE(auto s, value.get_string(true));
          SIMDJSON_ASSIGN_OR_RAISE(w, fromString<bool>(s));
          break;
        }
        default:
          return simdjson::INCORRECT_TYPE;
      }
      return simdjson::SUCCESS;
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::TINYINT, Dummy> {
    static simdjson::error_code
    apply(Input value, exec::GenericWriter& writer, const bool /*isFromJson*/) {
      return castJsonToInt<int8_t>(value, writer);
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::SMALLINT, Dummy> {
    static simdjson::error_code
    apply(Input value, exec::GenericWriter& writer, const bool /*isFromJson*/) {
      return castJsonToInt<int16_t>(value, writer);
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::INTEGER, Dummy> {
    static simdjson::error_code
    apply(Input value, exec::GenericWriter& writer, const bool /*isFromJson*/) {
      return castJsonToInt<int32_t>(value, writer);
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::BIGINT, Dummy> {
    static simdjson::error_code
    apply(Input value, exec::GenericWriter& writer, const bool /*isFromJson*/) {
      return castJsonToInt<int64_t>(value, writer);
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::REAL, Dummy> {
    static simdjson::error_code
    apply(Input value, exec::GenericWriter& writer, const bool /*isFromJson*/) {
      return castJsonToFloatingPoint<float>(value, writer);
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::DOUBLE, Dummy> {
    static simdjson::error_code
    apply(Input value, exec::GenericWriter& writer, const bool /*isFromJson*/) {
      return castJsonToFloatingPoint<double>(value, writer);
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::ARRAY, Dummy> {
    static simdjson::error_code
    apply(Input value, exec::GenericWriter& writer, const bool isFromJson) {
      auto& writerTyped = writer.castTo<Array<Any>>();
      auto& elementType = writer.type()->childAt(0);
      SIMDJSON_ASSIGN_OR_RAISE(auto array, value.get_array());
      for (auto elementResult : array) {
        SIMDJSON_ASSIGN_OR_RAISE(auto element, elementResult);
        // If casting to array of JSON, nulls in array elements should become
        // the JSON text "null".
        if (!isJsonType(elementType) && element.is_null()) {
          writerTyped.add_null();
        } else {
          SIMDJSON_TRY(BOLT_DYNAMIC_TYPE_DISPATCH(
              CastFromJsonTypedImpl<simdjson::ondemand::value>::apply,
              elementType->kind(),
              element,
              writerTyped.add_item(),
              isFromJson));
        }
      }
      return simdjson::SUCCESS;
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::MAP, Dummy> {
    static simdjson::error_code
    apply(Input value, exec::GenericWriter& writer, const bool isFromJson) {
      auto& writerTyped = writer.castTo<Map<Any, Any>>();
      auto& keyType = writer.type()->childAt(0);
      auto& valueType = writer.type()->childAt(1);
      SIMDJSON_ASSIGN_OR_RAISE(auto object, value.get_object());
      for (auto fieldResult : object) {
        SIMDJSON_ASSIGN_OR_RAISE(auto field, fieldResult);
        SIMDJSON_ASSIGN_OR_RAISE(auto key, field.unescaped_key(true));
        // If casting to map of JSON values, nulls in map values should become
        // the JSON text "null".
        if (!isJsonType(valueType) && field.value().is_null()) {
          SIMDJSON_TRY(BOLT_DYNAMIC_TYPE_DISPATCH(
              appendMapKey, keyType->kind(), key, writerTyped.add_null()));
        } else {
          auto writers = writerTyped.add_item();
          SIMDJSON_TRY(BOLT_DYNAMIC_TYPE_DISPATCH(
              appendMapKey, keyType->kind(), key, std::get<0>(writers)));
          SIMDJSON_TRY(BOLT_DYNAMIC_TYPE_DISPATCH(
              CastFromJsonTypedImpl<simdjson::ondemand::value>::apply,
              valueType->kind(),
              field.value(),
              std::get<1>(writers),
              isFromJson));
        }
      }
      return simdjson::SUCCESS;
    }
  };

  template <typename Dummy>
  struct KindDispatcher<TypeKind::ROW, Dummy> {
    static simdjson::error_code
    apply(Input value, exec::GenericWriter& writer, const bool isFromJson) {
      auto& rowType = writer.type()->asRow();
      auto& writerTyped = writer.castTo<DynamicRow>();
      SIMDJSON_ASSIGN_OR_RAISE(auto type, value.type());
      if (type == simdjson::ondemand::json_type::array) {
        SIMDJSON_ASSIGN_OR_RAISE(auto array, value.get_array());
        SIMDJSON_ASSIGN_OR_RAISE(auto arraySize, array.count_elements());
        if (arraySize != writer.type()->size()) {
          return simdjson::INCORRECT_TYPE;
        }
        column_index_t i = 0;
        for (auto elementResult : array) {
          SIMDJSON_ASSIGN_OR_RAISE(auto element, elementResult);
          if (element.is_null()) {
            writerTyped.set_null_at(i);
          } else {
            auto err = BOLT_DYNAMIC_TYPE_DISPATCH(
                CastFromJsonTypedImpl<simdjson::ondemand::value>::apply,
                rowType.childAt(i)->kind(),
                element,
                writerTyped.get_writer_at(i),
                isFromJson);
            if (err) {
              if (isFromJson) {
                writerTyped.set_null_at(i);
              } else {
                return err;
              }
            }
          }
          ++i;
        }
      } else {
        SIMDJSON_ASSIGN_OR_RAISE(auto object, value.get_object());
        folly::F14FastMap<std::string, column_index_t> fieldKey2Idx;
        std::unordered_set<column_index_t> appearedIdxs;
        std::string jsonKey, rowKey;

        for (column_index_t numFields = rowType.size(), i = 0; i < numFields;
             ++i) {
          rowKey = rowType.nameOf(i);
          boost::algorithm::to_lower(rowKey);
          fieldKey2Idx[rowKey] = i;
          appearedIdxs.insert(i);
        }

        /*
          The previous code used `folly::F14FastMap<std::string,
          simdjson::ondemand::value>` to store simdjson::ondemand::value, but
          simdjson is stream parsing, so at the end of parsing,
          simdjson::ondemand::value will become ineffective.
          Therefore, we need to process all the fields in one loop.
        */
        for (auto fieldResult : object) {
          SIMDJSON_ASSIGN_OR_RAISE(auto field, fieldResult);
          SIMDJSON_ASSIGN_OR_RAISE(jsonKey, field.unescaped_key(true));
          boost::algorithm::to_lower(jsonKey);
          auto it = fieldKey2Idx.find(jsonKey);
          if (it == fieldKey2Idx.end()) {
            continue;
          }
          column_index_t jsonKeyIdx = it->second;
          if (!field.value().is_null()) {
            auto err = BOLT_DYNAMIC_TYPE_DISPATCH(
                CastFromJsonTypedImpl<simdjson::ondemand::value>::apply,
                rowType.childAt(jsonKeyIdx)->kind(),
                field.value(),
                writerTyped.get_writer_at(jsonKeyIdx),
                isFromJson);
            if (err) {
              if (isFromJson) {
                writerTyped.set_null_at(jsonKeyIdx);
              } else {
                return err;
              }
            }
          } else {
            writerTyped.set_null_at(jsonKeyIdx);
          }
          appearedIdxs.erase(jsonKeyIdx);
        }

        // set all non appearing columns to null
        for (auto idx : appearedIdxs) {
          writerTyped.set_null_at(idx);
        }
      }
      return simdjson::SUCCESS;
    }
  };

  static simdjson::simdjson_result<std::string_view> rawJson(
      Input value,
      simdjson::ondemand::json_type type) {
    switch (type) {
      case simdjson::ondemand::json_type::array: {
        SIMDJSON_ASSIGN_OR_RAISE(auto array, value.get_array());
        return array.raw_json();
      }
      case simdjson::ondemand::json_type::object: {
        SIMDJSON_ASSIGN_OR_RAISE(auto object, value.get_object());
        return object.raw_json();
      }
      default:
        return value.raw_json_token();
    }
  }

  template <typename T>
  static simdjson::error_code castJsonToInt(
      Input value,
      exec::GenericWriter& writer) {
    SIMDJSON_ASSIGN_OR_RAISE(auto type, value.type());
    switch (type) {
      case simdjson::ondemand::json_type::number: {
        SIMDJSON_ASSIGN_OR_RAISE(auto num, value.get_number());
        switch (num.get_number_type()) {
          case simdjson::ondemand::number_type::floating_point_number:
            return convertIfInRange<T>(num.get_double(), writer);
          case simdjson::ondemand::number_type::signed_integer:
            return convertIfInRange<T>(num.get_int64(), writer);
          case simdjson::ondemand::number_type::unsigned_integer:
            return simdjson::NUMBER_OUT_OF_RANGE;
          case simdjson::ondemand::number_type::big_integer:
            BOLT_UNREACHABLE(); // value.get_number() would have failed
                                // already.
        }
        break;
      }
      case simdjson::ondemand::json_type::boolean: {
        writer.castTo<T>() = value.get_bool();
        break;
      }
      case simdjson::ondemand::json_type::string: {
        SIMDJSON_ASSIGN_OR_RAISE(auto s, value.get_string(true));
        SIMDJSON_ASSIGN_OR_RAISE(writer.castTo<T>(), fromString<T>(s));
        break;
      }
      default:
        return simdjson::INCORRECT_TYPE;
    }
    return simdjson::SUCCESS;
  }

  template <typename T>
  static simdjson::error_code castJsonToFloatingPoint(
      Input value,
      exec::GenericWriter& writer) {
    SIMDJSON_ASSIGN_OR_RAISE(auto type, value.type());
    switch (type) {
      case simdjson::ondemand::json_type::number: {
        SIMDJSON_ASSIGN_OR_RAISE(auto num, value.get_double());
        return convertIfInRange<T>(num, writer);
      }
      case simdjson::ondemand::json_type::boolean: {
        writer.castTo<T>() = value.get_bool();
        break;
      }
      case simdjson::ondemand::json_type::string: {
        SIMDJSON_ASSIGN_OR_RAISE(auto s, value.get_string(true));
        SIMDJSON_ASSIGN_OR_RAISE(writer.castTo<T>(), fromString<T>(s));
        break;
      }
      default:
        return simdjson::INCORRECT_TYPE;
    }
    return simdjson::SUCCESS;
  }
};

template <TypeKind kind>
simdjson::error_code castFromJsonOneRow(
    const simdjson::padded_string_view& input,
    exec::VectorWriter<Any>& writer,
    const bool isFromJson) {
  simdjson::ondemand::document doc;
  if (auto error = simdjsonParse(input).get(doc)) {
    if (isFromJson) {
      writer.commitNull();
      return simdjson::SUCCESS;
    }
    return error;
  }

  if (doc.is_null()) {
    writer.commitNull();
    return simdjson::SUCCESS;
  }

  if (isFromJson) {
    // For compatibility with Java, when encountering a invalid JSON string,
    // the result is null, and only an error is recorded instead of throwing
    // an exception
    try {
      if (auto error =
              CastFromJsonTypedImpl<simdjson::ondemand::document&>::apply<kind>(
                  doc, writer.current(), /* isFromJson*/ true)) {
        writer.commitNull();
      } else {
        writer.commit(true);
      }
    } catch (const simdjson::simdjson_error& e) {
      LOG(INFO) << fmt::format(
          "encountered `simdjson::simdjson_error` exception while parsing json, json string : `{}`, exception msg : {} ",
          input,
          e.what());
      writer.commitNull();
    } catch (const std::exception& e) {
      LOG(INFO) << fmt::format(
          "encountered `std::exception` exception while parsing json, json string: `{}`, exception msg: {}",
          input,
          e.what());
      writer.commitNull();
    }
  } else {
    SIMDJSON_TRY(
        CastFromJsonTypedImpl<simdjson::ondemand::document&>::apply<kind>(
            doc, writer.current(), /* isFromJson*/ false));
    writer.commit(true);
  }

  return simdjson::SUCCESS;
}

bool isSupportedBasicType(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::BIGINT:
    case TypeKind::INTEGER:
    case TypeKind::SMALLINT:
    case TypeKind::TINYINT:
    case TypeKind::DOUBLE:
    case TypeKind::REAL:
    case TypeKind::VARCHAR:
      return true;
    default:
      return false;
  }
}

class JsonTypeFactories : public CustomTypeFactories {
 public:
  JsonTypeFactories() = default;

  TypePtr getType() const override {
    return JSON();
  }

  exec::CastOperatorPtr getCastOperator() const override {
    return std::make_shared<JsonCastOperator>();
  }
};

} // namespace

bool JsonCastOperator::isSupportedFromType(const TypePtr& other) const {
  if (isSupportedBasicType(other)) {
    return true;
  }

  switch (other->kind()) {
    case TypeKind::UNKNOWN:
    case TypeKind::TIMESTAMP:
      return true;
    case TypeKind::ARRAY:
      return isSupportedFromType(other->childAt(0));
    case TypeKind::ROW:
      for (const auto& child : other->as<TypeKind::ROW>().children()) {
        if (!isSupportedFromType(child)) {
          return false;
        }
      }
      return true;
    case TypeKind::MAP:
      return (
          isSupportedBasicType(other->childAt(0)) &&
          isSupportedFromType(other->childAt(1)));
    default:
      return false;
  }
}

bool JsonCastOperator::isSupportedToType(const TypePtr& other) const {
  if (other->isDate()) {
    return false;
  }

  if (isSupportedBasicType(other)) {
    return true;
  }

  switch (other->kind()) {
    case TypeKind::ARRAY:
      return isSupportedToType(other->childAt(0));
    case TypeKind::ROW:
      for (const auto& child : other->as<TypeKind::ROW>().children()) {
        if (!isSupportedToType(child)) {
          return false;
        }
      }
      return true;
    case TypeKind::MAP:
      return (
          isSupportedBasicType(other->childAt(0)) &&
          isSupportedToType(other->childAt(1)) &&
          !isJsonType(other->childAt(0)));
    default:
      return false;
  }
}

/// Converts an input vector of a supported type to Json type. The
/// implementation follows the structure below.
/// JsonOperator::castTo: type dispatch for castToJson
/// +- castToJson (simple types)
///    +- generateJsonTyped: appends actual data to string
/// +- castToJson (complex types, via SFINAE)
///    +- castToJsonFrom{Row, Map, Array}:
///         Generates data for child vectors in temporary vectors. Copies this
///         data and adds delimiters and separators.
///       +- castToJson (recursive)
void JsonCastOperator::castTo(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    const TypePtr& resultType,
    VectorPtr& result) const {
  this->castTo(
      input,
      context,
      rows,
      resultType,
      result,
      /*isToJson=*/false,
      /*This value won't be used because isToJson is false, isTopLevel=*/true);
}

void JsonCastOperator::castTo(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    const TypePtr& resultType,
    VectorPtr& result,
    const bool isToJson,
    const bool isTopLevel) const {
  context.ensureWritable(rows, resultType, result);
  auto* flatResult = result->as<FlatVector<StringView>>();

  // Casting from VARBINARY and OPAQUE are not supported and should have been
  // rejected by isSupportedType() in the caller.
  BOLT_DYNAMIC_TYPE_DISPATCH_ALL(
      castToJson,
      input.typeKind(),
      input,
      context,
      rows,
      *flatResult,
      /*isMapKey=*/false,
      isToJson,
      isTopLevel);
}

/// Converts an input vector from Json type to the type of result vector.
void JsonCastOperator::castFrom(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    const TypePtr& resultType,
    VectorPtr& result) const {
  castFrom(input, context, rows, resultType, result, /*isFromJson*/ false);
}

/// If `isFromJson` is ture, using in `from_json` udf, converts an input vector
/// from Json type to the type of result vector.
void JsonCastOperator::castFrom(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    const TypePtr& resultType,
    VectorPtr& result,
    const bool isFromJson,
    const VectorPtr& logFailuresOnly) const {
  // Initialize errors here so that we get the proper exception context.
  folly::call_once(
      initializeErrors_, [this] { simdjsonErrorsToExceptions(errors_); });
  context.ensureWritable(rows, resultType, result);
  // Casting to unsupported types should have been rejected by isSupportedType()
  // in the caller.
  BOLT_DYNAMIC_TYPE_DISPATCH(
      castFromJson,
      result->typeKind(),
      input,
      context,
      rows,
      *result,
      isFromJson,
      logFailuresOnly);
}

template <TypeKind kind>
void JsonCastOperator::castFromJson(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    BaseVector& result,
    const bool isFromJson,
    const VectorPtr& logFailuresOnly) const {
  // Result is guaranteed to be a flat writable vector.
  auto* flatResult = result.as<typename KindToFlatVector<kind>::type>();
  exec::VectorWriter<Any> writer;
  writer.init(*flatResult);
  // Input is guaranteed to be in flat or constant encodings when passed in.
  auto* inputVector = input.as<SimpleVector<StringView>>();
  SimpleVector<bool>* logFailuresOnlyVector{nullptr};
  if (logFailuresOnly) {
    logFailuresOnlyVector = logFailuresOnly->as<SimpleVector<bool>>();
  }
  size_t maxSize = 0;

  rows.applyToSelected([&](auto row) {
    if (inputVector->isNullAt(row)) {
      return;
    }
    auto& input = inputVector->valueAt(row);
    maxSize = std::max(maxSize, input.size());
  });

  paddedInput_.resize(maxSize + simdjson::SIMDJSON_PADDING);
  auto flinkCompatible =
      context.execCtx()->queryCtx()->queryConfig().enableFlinkCompatible();

  rows.applyToSelected([&](auto row) {
    writer.setOffset(row);
    if (inputVector->isNullAt(row)) {
      if (flinkCompatible &&
          (!logFailuresOnlyVector || !logFailuresOnlyVector->valueAt(row))) {
        BOLT_FAIL("input is null.");
      } else {
        writer.commitNull();
      }
      return;
    }
    const auto& input = inputVector->valueAt(row);
    if ((isFromJson || flinkCompatible) &&
        (input.empty() || folly::trimWhitespace(input).empty())) {
      writer.commitNull();
      return;
    }
    std::memset(paddedInput_.data(), 0, paddedInput_.size());
    std::memcpy(paddedInput_.data(), input.data(), input.size());
    simdjson::padded_string_view paddedInput(
        paddedInput_.data(), input.size(), paddedInput_.size());
    if (auto error =
            castFromJsonOneRow<kind>(paddedInput, writer, isFromJson)) {
      if (!isFromJson) {
        if (logFailuresOnlyVector && logFailuresOnlyVector->valueAt(row)) {
          LOG(WARNING) << "Failed to parse "
                       << std::string_view(input.data(), input.size());
        } else {
          context.setBoltExceptionError(row, errors_[error]);
        }
      }
      writer.commitNull();
    }
  });
  writer.finish();
}

void registerJsonType() {
  registerCustomType("json", std::make_unique<const JsonTypeFactories>());
}

} // namespace bytedance::bolt
