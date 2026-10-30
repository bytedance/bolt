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

#include "bolt/functions/sparksql/specialforms/JsonSplit.h"

#include "bolt/common/base/Exceptions.h"
#include "bolt/expression/PeeledEncoding.h"
#include "bolt/expression/SpecialForm.h"
#include "bolt/functions/lib/DateTimeFormatterBuilder.h"
#include "bolt/functions/prestosql/json/SIMDJsonWrapper.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/SelectivityVector.h"

#include "expression/ScopedVarSetter.h"
#include "sonic/dom/parser.h"
#include "sonic/sonic.h"
using namespace bytedance::bolt::exec;
namespace bytedance::bolt::functions::sparksql {

std::shared_ptr<DateTimeFormatter> getDateTimeFormatter() {
  static std::shared_ptr<DateTimeFormatter> formatter = nullptr;
  if (formatter) {
    return formatter;
  } else {
    // yyyy-MM-dd'T'HH:mm:ssZZ
    DateTimeFormatterBuilder builder(100);
    auto formatter = builder.appendYear(4)
                         .appendLiteral("-")
                         .appendMonthOfYear(2)
                         .appendLiteral("-")
                         .appendDayOfMonth(2)
                         .appendLiteral("T")
                         .appendHourOfDay(2)
                         .appendLiteral(":")
                         .appendMinuteOfHour(2)
                         .appendLiteral(":")
                         .appendSecondOfMinute(2)
                         .appendTimeZoneOffsetId(2)
                         .setType(DateTimeFormatterType::JODA)
                         .build();
    return formatter;
  }
}

bool hasJsonStringType(TypePtr type) {
  if (type->kind() == TypeKind::VARCHAR ||
      type->kind() == TypeKind::VARBINARY ||
      type->kind() == TypeKind::TIMESTAMP) {
    return true;
  } else if (type->kind() == TypeKind::ARRAY) {
    return hasJsonStringType(type->asArray().elementType());
  } else if (type->kind() == TypeKind::MAP) {
    return hasJsonStringType(type->asMap().keyType()) ||
        hasJsonStringType(type->asMap().valueType());
  } else if (type->kind() == TypeKind::ROW) {
    for (auto child : type->asRow().children()) {
      if (hasJsonStringType(child)) {
        return true;
      }
    }
  }
  return false;
}

class JsonSplitExpr : public SpecialForm {
 public:
  /// @param type The target type of the cast expression
  /// @param expr The expression to cast
  /// @param trackCpuUsage Whether to track CPU usage
  JsonSplitExpr(TypePtr type, ExprPtr&& expr, bool trackCpuUsage)
      : SpecialForm(
            type,
            std::vector<ExprPtr>({expr}),
            "json_split",
            false /* supportsFlatNoNullsFastPath */,
            trackCpuUsage),
        hasJsonStringType_(hasJsonStringType(type)) {}

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override;

  void computePropagatesNulls() override {
    propagatesNulls_ = inputs_[0]->propagatesNulls();
  }

  // Apply the cast to a vector after vector encodings being peeled off. The
  // input vector is guaranteed to be flat or constant.
  void applyPeeled(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& resultType,
      VectorPtr& result);

  template <TypeKind kind>
  void jsonToArray(
      const SelectivityVector& rows,
      const BaseVector& input,
      exec::EvalCtx& context,
      const TypePtr& resultType,
      VectorPtr& result,
      bool hasJsonStringType);

 private:
  bool hasJsonStringType_;

  mutable folly::once_flag initUseSonic_;
  mutable bool useSonic_ = true;
};

void JsonSplitExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  VectorPtr input;
  inputs_[0]->eval(rows, context, input);
  auto resultType = std::const_pointer_cast<const Type>(type_);

  LocalSelectivityVector remainingRows(context, rows);

  context.deselectErrors(*remainingRows);

  LocalDecodedVector decoded(context, *input, *remainingRows);
  auto* rawNulls = decoded->nulls(&rows);

  if (rawNulls) {
    remainingRows->deselectNulls(
        rawNulls, remainingRows->begin(), remainingRows->end());
  }

  VectorPtr localResult;
  if (!remainingRows->hasSelections()) {
    localResult =
        BaseVector::createNullConstant(resultType, rows.end(), context.pool());
  } else if (decoded->isIdentityMapping()) {
    applyPeeled(
        *remainingRows, *decoded->base(), context, resultType, localResult);
  } else {
    withContextSaver([&](ContextSaver& saver) {
      LocalSelectivityVector newRowsHolder(*context.execCtx());

      LocalDecodedVector localDecoded(context);
      std::vector<VectorPtr> peeledVectors;
      auto peeledEncoding = PeeledEncoding::peel(
          {input}, *remainingRows, localDecoded, true, peeledVectors);
      BOLT_CHECK_EQ(peeledVectors.size(), 1);
      if (peeledVectors[0]->isLazy()) {
        peeledVectors[0] =
            peeledVectors[0]->as<LazyVector>()->loadedVectorShared();
      }
      auto newRows =
          peeledEncoding->translateToInnerRows(*remainingRows, newRowsHolder);
      // Save context and set the peel.
      context.saveAndReset(saver, *remainingRows);
      context.setPeeledEncoding(peeledEncoding);
      applyPeeled(
          *newRows, *peeledVectors[0], context, resultType, localResult);

      localResult = context.getPeeledEncoding()->wrap(
          resultType, context.pool(), localResult, *remainingRows);
    });
  }
  context.moveOrCopyResult(localResult, *remainingRows, result);
  context.releaseVector(localResult);

  // If there are nulls or rows that encountered errors in the input, add
  // nulls
  // to the result at the same rows.
  BOLT_CHECK_NOT_NULL(result);
  if (rawNulls || context.errors()) {
    EvalCtx::addNulls(
        rows, remainingRows->asRange().bits(), context, resultType, result);
  }

  // Return 'input' back to the vector pool in 'context' so it can be reused.
  context.releaseVector(input);
}

template <TypeKind kind>
struct JsonTypeMapping {
  using type = void;
};

#define DECLARE_JSON_TYPE_MAPPING(KIND, JSON_TYPE) \
  template <>                                      \
  struct JsonTypeMapping<KIND> {                   \
    using type = JSON_TYPE;                        \
  };

DECLARE_JSON_TYPE_MAPPING(TypeKind::BOOLEAN, bool);
DECLARE_JSON_TYPE_MAPPING(TypeKind::TINYINT, int64_t);
DECLARE_JSON_TYPE_MAPPING(TypeKind::SMALLINT, int64_t);
DECLARE_JSON_TYPE_MAPPING(TypeKind::INTEGER, int64_t);
DECLARE_JSON_TYPE_MAPPING(TypeKind::BIGINT, int64_t);
DECLARE_JSON_TYPE_MAPPING(TypeKind::REAL, double);
DECLARE_JSON_TYPE_MAPPING(TypeKind::DOUBLE, double);
DECLARE_JSON_TYPE_MAPPING(TypeKind::VARCHAR, std::string_view);
DECLARE_JSON_TYPE_MAPPING(TypeKind::VARBINARY, std::string_view);
DECLARE_JSON_TYPE_MAPPING(TypeKind::TIMESTAMP, std::string_view);
// Once simdjson is removed replace below to std::string
DECLARE_JSON_TYPE_MAPPING(TypeKind::ARRAY, simdjson::ondemand::array);
DECLARE_JSON_TYPE_MAPPING(TypeKind::MAP, simdjson::ondemand::object);
DECLARE_JSON_TYPE_MAPPING(TypeKind::ROW, simdjson::ondemand::object);

template <TypeKind kind>
inline constexpr bool TypeNotSupported =
    std::is_same_v<typename JsonTypeMapping<kind>::type, void>;

template <TypeKind kind>
inline constexpr bool IsArithmeticType = !TypeNotSupported<kind> &&
    std::is_arithmetic_v<typename JsonTypeMapping<kind>::type>;

template <TypeKind kind>
inline constexpr bool IsStringType =
    std::is_same_v<typename JsonTypeMapping<kind>::type, std::string_view>;

// for unsupported type, just return null
template <TypeKind kind>
typename std::enable_if<TypeNotSupported<kind>>::type
parseElement(VectorPtr& vector, size_t index, simdjson::ondemand::value value) {
  vector->setNull(index, true);
}

template <TypeKind kind>
typename std::enable_if<TypeNotSupported<kind>>::type parseElement(
    VectorPtr& vector,
    size_t index,
    sonic_json::DNode<sonic_json::MemoryPoolAllocator<>>*) {
  vector->setNull(index, true);
}

template <TypeKind kind>
typename std::enable_if<IsArithmeticType<kind>>::type
parseElement(VectorPtr& vector, size_t index, simdjson::ondemand::value value) {
  if (value.type() == simdjson::ondemand::json_type::null) {
    vector->setNull(index, true);
    return;
  }
  using NativeType = typename bolt::TypeTraits<kind>::NativeType;
  auto flatVector = vector->as<FlatVector<NativeType>>();
  typename JsonTypeMapping<kind>::type jsonValue = 0;
  try {
    value.get(jsonValue);
  } catch (simdjson::simdjson_error& error) {
  }
  flatVector->set(index, jsonValue);
}

template <TypeKind kind>
typename std::enable_if<IsArithmeticType<kind>>::type parseElement(
    VectorPtr& vector,
    size_t index,
    sonic_json::DNode<sonic_json::MemoryPoolAllocator<>>* value) {
  if (value->IsNull()) {
    vector->setNull(index, true);
    return;
  }
  using NativeType = typename bolt::TypeTraits<kind>::NativeType;
  auto flatVector = vector->as<FlatVector<NativeType>>();
  if (!value->IsNumber()) {
    flatVector->set(index, 0);
    return;
  } else if (value->IsInt64()) {
    auto jsonValue = value->GetInt64();
    flatVector->set(index, jsonValue);
  } else if (value->IsDouble()) {
    auto jsonValue = value->GetDouble();
    flatVector->set(index, jsonValue);
  }
}

template <typename ToType>
ToType castFromJsonString(const std::string_view& value) {
  return ToType(value.data(), value.size());
}

template <>
Timestamp castFromJsonString<Timestamp>(const std::string_view& value) {
  auto result = getDateTimeFormatter()->parse(value).value();
  result.timestamp.toGMT(result.timezoneId);
  return result.timestamp;
}

template <TypeKind kind>
typename std::enable_if<IsStringType<kind>>::type
parseElement(VectorPtr& vector, size_t index, simdjson::ondemand::value value) {
  if (value.type() == simdjson::ondemand::json_type::null) {
    vector->setNull(index, true);
    return;
  }
  using NativeType = typename bolt::TypeTraits<kind>::NativeType;
  auto flatVector = vector->as<FlatVector<NativeType>>();
  std::string_view str;
  if (value.type() == simdjson::ondemand::json_type::string) {
    str = value.get_string(true);
  } else {
    str = simdjson::to_json_string(value);
  }
  flatVector->set(index, castFromJsonString<NativeType>(str));
}

template <TypeKind kind>
typename std::enable_if<IsStringType<kind>>::type parseElement(
    VectorPtr& vector,
    size_t index,
    sonic_json::DNode<sonic_json::MemoryPoolAllocator<>>* v) {
  std::string str;
  if (v->IsNull()) {
    vector->setNull(index, true);
    return;
  } else if (v->IsString()) {
    str = v->GetString();
  } else if (v->IsBool()) {
    str = v->GetBool() ? "true" : "false";
  } else {
    sonic_json::WriteBuffer wb;
    v->Serialize(wb);
    str = wb.ToString();
  }

  using NativeType = typename bolt::TypeTraits<kind>::NativeType;
  auto flatVector = vector->as<FlatVector<NativeType>>();
  std::string_view sv(str.c_str(), str.size());
  flatVector->set(index, castFromJsonString<NativeType>(sv));
}

void resizeVector(VectorPtr vectorPtr, size_t newSize) {
  if (newSize > vectorPtr->size()) {
    vectorPtr->resize(vectorPtr->size() + newSize);
  }
}

template <TypeKind kind>
typename std::enable_if<kind == TypeKind::ARRAY>::type
parseElement(VectorPtr& vector, size_t index, simdjson::ondemand::value value);

template <TypeKind kind>
typename std::enable_if<kind == TypeKind::MAP>::type
parseElement(VectorPtr& vector, size_t index, simdjson::ondemand::value value);

template <TypeKind kind>
typename std::enable_if<kind == TypeKind::ROW>::type
parseElement(VectorPtr& vector, size_t index, simdjson::ondemand::value value);

template <TypeKind kind>
typename std::enable_if<kind == TypeKind::ARRAY>::type parseElement(
    VectorPtr& vector,
    size_t index,
    sonic_json::DNode<sonic_json::MemoryPoolAllocator<>>* value);

template <TypeKind kind>
typename std::enable_if<kind == TypeKind::MAP>::type parseElement(
    VectorPtr& vector,
    size_t index,
    sonic_json::DNode<sonic_json::MemoryPoolAllocator<>>* value);

template <TypeKind kind>
typename std::enable_if<kind == TypeKind::ROW>::type parseElement(
    VectorPtr& vector,
    size_t index,
    sonic_json::DNode<sonic_json::MemoryPoolAllocator<>>* value);

template <TypeKind kind>
typename std::enable_if<kind == TypeKind::ARRAY>::type
parseElement(VectorPtr& vector, size_t index, simdjson::ondemand::value value) {
  auto* arrayVector = vector->as<ArrayVector>();
  auto elementVector = arrayVector->elements();
  size_t offset = index == 0
      ? 0
      : arrayVector->offsetAt(index - 1) + arrayVector->sizeAt(index - 1);
  size_t end = offset;
  if (value.type() == simdjson::ondemand::json_type::array) {
    elementVector->resize(offset + value.count_elements());
    for (auto elem : value) {
      BOLT_DYNAMIC_TYPE_DISPATCH(
          parseElement,
          elementVector->type()->kind(),
          elementVector,
          end,
          elem.value());
      end += 1;
    }
  } else if (value.type() == simdjson::ondemand::json_type::object) {
    // return array of values
    elementVector->resize(offset + value.count_fields());
    for (auto elem : value.get_object()) {
      BOLT_DYNAMIC_TYPE_DISPATCH(
          parseElement,
          elementVector->type()->kind(),
          elementVector,
          end,
          elem.value().value());
      end += 1;
    }
  } else {
    // return empty array
  }
  arrayVector->setNull(
      index, value.type() == simdjson::ondemand::json_type::null);
  arrayVector->setOffsetAndSize(index, offset, end - offset);
}

template <TypeKind kind>
typename std::enable_if<kind == TypeKind::ARRAY>::type parseElement(
    VectorPtr& vector,
    size_t index,
    sonic_json::DNode<sonic_json::MemoryPoolAllocator<>>* value) {
  auto* arrayVector = vector->as<ArrayVector>();
  auto elementVector = arrayVector->elements();
  size_t offset = index == 0
      ? 0
      : arrayVector->offsetAt(index - 1) + arrayVector->sizeAt(index - 1);
  size_t end = offset;
  if (value->IsArray()) {
    elementVector->resize(offset + value->Size());
    for (auto v = value->Begin(); v != value->End(); v++) {
      BOLT_DYNAMIC_TYPE_DISPATCH(
          parseElement, elementVector->type()->kind(), elementVector, end, v);
      end += 1;
    }
  } else if (value->IsObject()) {
    // return array of values
    elementVector->resize(offset + value->Size());
    for (auto m = value->MemberBegin(); m != value->MemberEnd(); ++m) {
      auto* val = &m->value;
      BOLT_DYNAMIC_TYPE_DISPATCH(
          parseElement, elementVector->type()->kind(), elementVector, end, val);
      end += 1;
    }
  } else {
    // return empty array
  }
  arrayVector->setNull(index, value->IsNull());
  arrayVector->setOffsetAndSize(index, offset, end - offset);
}

template <TypeKind kind>
typename std::enable_if<kind == TypeKind::MAP>::type
parseElement(VectorPtr& vector, size_t index, simdjson::ondemand::value value) {
  auto* mapVector = vector->as<MapVector>();
  auto keyVector = mapVector->mapKeys();
  auto valueVector = mapVector->mapValues();
  size_t offset = index == 0
      ? 0
      : mapVector->offsetAt(index - 1) + mapVector->sizeAt(index - 1);
  size_t end = offset;
  if (value.type() == simdjson::ondemand::json_type::null) {
    vector->setNull(index, true);
  } else {
    vector->setNull(index, false);
    size_t count = value.count_fields();
    keyVector->resize(offset + count);
    valueVector->resize(offset + count);
    for (auto kv : value.get_object()) {
      std::string_view keyStr = kv.unescaped_key(true).value();
      keyVector->as<FlatVector<StringView>>()->set(
          end, StringView(keyStr.data(), keyStr.size()));
      BOLT_DYNAMIC_TYPE_DISPATCH(
          parseElement,
          valueVector->type()->kind(),
          valueVector,
          end,
          kv.value().value());
      end += 1;
    }
  }
  mapVector->setOffsetAndSize(index, offset, end - offset);
}

template <TypeKind kind>
typename std::enable_if<kind == TypeKind::MAP>::type parseElement(
    VectorPtr& vector,
    size_t index,
    sonic_json::DNode<sonic_json::MemoryPoolAllocator<>>* value) {
  auto* mapVector = vector->as<MapVector>();
  auto keyVector = mapVector->mapKeys();
  auto valueVector = mapVector->mapValues();
  size_t offset = index == 0
      ? 0
      : mapVector->offsetAt(index - 1) + mapVector->sizeAt(index - 1);
  size_t end = offset;
  if (value->IsNull()) {
    vector->setNull(index, true);
  } else {
    vector->setNull(index, false);
    size_t count = value->Size();
    keyVector->resize(offset + count);
    valueVector->resize(offset + count);
    for (auto m = value->MemberBegin(); m != value->MemberEnd(); ++m) {
      auto* val = &m->value;
      std::string_view keyStr = m->name.GetStringView();
      keyVector->as<FlatVector<StringView>>()->set(
          end, StringView(keyStr.data(), keyStr.size()));
      BOLT_DYNAMIC_TYPE_DISPATCH(
          parseElement, valueVector->type()->kind(), valueVector, end, val);
      end += 1;
    }
  }
  mapVector->setOffsetAndSize(index, offset, end - offset);
}

template <TypeKind kind>
typename std::enable_if<kind == TypeKind::ROW>::type
parseElement(VectorPtr& vector, size_t index, simdjson::ondemand::value value) {
  auto* rowVector = vector->as<RowVector>();
  for (size_t i = 0; i < rowVector->childrenSize(); i++) {
    auto child = rowVector->childAt(i);
    auto name = rowVector->type()->asRow().nameOf(i);
    child->resize(index + 1);
    auto elem = value[name].value();
    BOLT_DYNAMIC_TYPE_DISPATCH(
        parseElement, child->type()->kind(), child, index, elem);
  }
}

template <TypeKind kind>
typename std::enable_if<kind == TypeKind::ROW>::type parseElement(
    VectorPtr& vector,
    size_t index,
    sonic_json::DNode<sonic_json::MemoryPoolAllocator<>>* value) {
  auto* rowVector = vector->as<RowVector>();
  for (size_t i = 0; i < rowVector->childrenSize(); i++) {
    auto child = rowVector->childAt(i);
    auto name = rowVector->type()->asRow().nameOf(i);
    child->resize(index + 1);
    auto elem = &((*value)[name]);
    BOLT_DYNAMIC_TYPE_DISPATCH(
        parseElement, child->type()->kind(), child, index, elem);
  }
}

template <TypeKind kind>
void jsonToArray_SimdJson(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& resultType,
    VectorPtr& result,
    bool hasJsonStringType) {
  auto* inputVector = input.as<SimpleVector<StringView>>();
  auto* arrayVector = result->as<ArrayVector>();
  auto elementVector = arrayVector->elements();
  size_t offset = 0, lastOffset = 0;
  // reserve memory for element
  elementVector->resize(rows.size());
  simdjson::ondemand::parser parser;
  std::string minifiedJson;
  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (input.isNullAt(row)) {
      result->setNull(row, true);
    } else {
      const auto& jsonStr = inputVector->valueAt(row);
      try {
        simdjson::padded_string paddedStr;
        // the root node (the document) needs special handling and cannot be
        // treated as a normal value.
        simdjson::ondemand::document doc;
        if (hasJsonStringType) {
          // minify json so we don't need to rebuild minified json string
          // representation later
          minifiedJson.resize(jsonStr.size() + simdjson::SIMDJSON_PADDING);
          size_t minifiedSize = minifiedJson.size();
          simdjson::minify(
              jsonStr.data(),
              jsonStr.size(),
              minifiedJson.data(),
              minifiedSize);
          doc = parser.iterate(
              minifiedJson.data(), minifiedSize, minifiedJson.size());
        } else {
          paddedStr = simdjson::padded_string(jsonStr.data(), jsonStr.size());
          doc = parser.iterate(paddedStr);
        }
        if (doc.type() == simdjson::ondemand::json_type::array) {
          resizeVector(elementVector, offset + doc.count_elements());
          for (auto elem : doc) {
            parseElement<kind>(elementVector, offset++, elem.value());
          }
        } else if (doc.type() == simdjson::ondemand::json_type::object) {
          // return array of values
          resizeVector(elementVector, offset + doc.count_fields());
          for (auto elem : doc.get_object()) {
            parseElement<kind>(elementVector, offset++, elem.value().value());
          }
        } else {
          // return empty array
        }
        // json null see as null
        arrayVector->setNull(
            row, doc.type() == simdjson::ondemand::json_type::null);
        arrayVector->setOffsetAndSize(row, lastOffset, offset - lastOffset);
        lastOffset = offset;
      } catch (const simdjson::simdjson_error&) {
        arrayVector->setOffsetAndSize(row, lastOffset, 0);
        result->setNull(row, true);
        offset = lastOffset;
        return;
      } catch (const BoltException& e) {
        if (!e.isUserError()) {
          throw;
        }
        arrayVector->setOffsetAndSize(row, lastOffset, 0);
        result->setNull(row, true);
        offset = lastOffset;
        return;
      } catch (const std::exception&) {
        arrayVector->setOffsetAndSize(row, lastOffset, 0);
        result->setNull(row, true);
        offset = lastOffset;
        throw;
      }
    }
  });
  elementVector->resize(offset);
}

template <TypeKind kind>
void jsonToArray_Sonic(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& resultType,
    VectorPtr& result,
    bool hasJsonStringType) {
  auto* inputVector = input.as<SimpleVector<StringView>>();
  auto* arrayVector = result->as<ArrayVector>();
  auto elementVector = arrayVector->elements();
  size_t offset = 0, lastOffset = 0;
  // reserve memory for element
  elementVector->resize(rows.size());

  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (input.isNullAt(row)) {
      result->setNull(row, true);
    } else {
      const auto& jsonStr = inputVector->valueAt(row);

      sonic_json::Document jsonDoc;
      jsonDoc.Parse(jsonStr.data(), jsonStr.size());

      if (jsonDoc.HasParseError()) {
        arrayVector->setOffsetAndSize(row, lastOffset, 0);
        result->setNull(row, true);
        offset = lastOffset;
        return;
      }

      try {
        if (jsonDoc.IsArray()) {
          resizeVector(elementVector, offset + jsonDoc.Size());
          for (auto v = jsonDoc.Begin(); v != jsonDoc.End(); v++) {
            parseElement<kind>(elementVector, offset++, v);
          }
        } else if (jsonDoc.IsObject()) {
          resizeVector(elementVector, offset + jsonDoc.Size());

          for (auto m = jsonDoc.MemberBegin(); m != jsonDoc.MemberEnd(); ++m) {
            auto* val = &m->value;
            parseElement<kind>(elementVector, offset++, val);
          }
        }
      } catch (const BoltException& e) {
        if (!e.isUserError()) {
          throw;
        }
        arrayVector->setOffsetAndSize(row, lastOffset, 0);
        result->setNull(row, true);
        offset = lastOffset;
        return;
      } catch (const std::exception&) {
        arrayVector->setOffsetAndSize(row, lastOffset, 0);
        result->setNull(row, true);
        offset = lastOffset;
        throw;
      }

      arrayVector->setNull(row, jsonDoc.IsNull());
      arrayVector->setOffsetAndSize(row, lastOffset, offset - lastOffset);
      lastOffset = offset;
    }
  });

  elementVector->resize(offset);
}

template <TypeKind kind>
void JsonSplitExpr::jsonToArray(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& resultType,
    VectorPtr& result,
    bool hasJsonStringType) {
  folly::call_once(initUseSonic_, [&] {
    useSonic_ =
        context.execCtx()->queryCtx()->queryConfig().enableSonicJsonParse();
  });

  ScopedVarSetter holder{context.mutableThrowOnError(), false};
  if (useSonic_) {
    jsonToArray_Sonic<kind>(
        rows, input, context, resultType, result, hasJsonStringType);
  } else {
    jsonToArray_SimdJson<kind>(
        rows, input, context, resultType, result, hasJsonStringType);
  }
}

void JsonSplitExpr::applyPeeled(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& resultType,
    VectorPtr& result) {
  context.ensureWritable(rows, resultType, result);
  BOLT_DYNAMIC_TYPE_DISPATCH(
      jsonToArray,
      result->as<ArrayVector>()->elements()->type()->kind(),
      rows,
      input,
      context,
      resultType,
      result,
      hasJsonStringType_);
}

TypePtr JsonSplitToSpecialForm::resolveType(
    const std::vector<TypePtr>& argTypes) {
  BOLT_FAIL("JSON_SPLIT expressions do not support type resolution.");
}

ExprPtr JsonSplitToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const core::QueryConfig& config) {
  if (compiledChildren.size() != 1 && compiledChildren.size() != 2) {
    BOLT_FAIL(
        "JSON_SPLIT statements expect one or two argument, received {}.",
        compiledChildren.size());
  }
  return std::make_shared<JsonSplitExpr>(
      type, std::move(compiledChildren[0]), trackCpuUsage);
}
} // namespace bytedance::bolt::functions::sparksql
