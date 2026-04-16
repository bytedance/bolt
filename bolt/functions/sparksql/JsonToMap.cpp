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

#include <glog/logging.h>

#include "bolt/expression/VectorFunction.h"
#include "bolt/expression/VectorWriters.h"
#include "bolt/functions/prestosql/json/SIMDJsonWrapper.h"

#include <sonic/sonic.h>
#include "sonic/dom/parser.h"
namespace bytedance::bolt::functions::sparksql {
namespace {
class JsonToMapFunction : public exec::VectorFunction {
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
    folly::call_once(initUseSonic_, [&] {
      useSonic_ =
          context.execCtx()->queryCtx()->queryConfig().enableSonicJsonParse();
    });

    if (useSonic_) {
      applySonic(rows, args, outputType, context, result);
    } else {
      applySimdJson(rows, args, outputType, context, result);
    }
  }

  void applySimdJson(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    BaseVector::ensureWritable(
        rows, MAP(VARCHAR(), VARCHAR()), context.pool(), result);
    exec::LocalDecodedVector input(context, *args[0], rows);
    exec::VectorWriter<Map<Varchar, Varchar>> resultWriter;
    resultWriter.init(*result->as<MapVector>());
    simdjson::ondemand::parser parser;
    std::string padded_data;
    rows.applyToSelected([&](auto row) {
      resultWriter.setOffset(row);
      if (input->isNullAt(row)) {
        resultWriter.commitNull();
      } else {
        auto& mapWriter = resultWriter.current();
        folly::F14FastMap<std::string_view, std::string_view> keyValues;
        auto sv = input->valueAt<StringView>(row);
        const std::string_view current = std::string_view(sv.data(), sv.size());
        padded_data = current;
        if (padded_data.capacity() < sv.size() + simdjson::SIMDJSON_PADDING) {
          padded_data.reserve(std::max(
              sv.size() + simdjson::SIMDJSON_PADDING,
              padded_data.capacity() + padded_data.capacity() / 2));
        }
        try {
          simdjson::ondemand::document doc = parser.iterate(padded_data);
          simdjson::ondemand::value val = doc;
          if (val.type() != simdjson::ondemand::json_type::object) {
            // hiveudf returns empty map for valid non-object JSON
            // (null, number, boolean, string, array), not SQL NULL.
            resultWriter.commit();
            return;
          }
          for (auto field : val.get_object()) {
            std::string_view key = field.unescaped_key(true);
            simdjson::ondemand::value value = field.value();
            std::string_view view;
            if (value.type() == simdjson::ondemand::json_type::string) {
              view = value.get_string(true);
            } else {
              view = simdjson::to_json_string(value);
            }
            keyValues.insert_or_assign(key, view);
          }
          for (const auto& [key, value] : keyValues) {
            auto [keyWriter, valueWriter] = mapWriter.add_item();
            keyWriter.append(StringView(key));
            valueWriter.append(StringView(value));
          }
          resultWriter.commit();
        } catch (std::exception& e) {
          resultWriter.commitNull();
        }
      }
    });
    resultWriter.finish();
  }

  void applySonic(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    BaseVector::ensureWritable(
        rows, MAP(VARCHAR(), VARCHAR()), context.pool(), result);
    exec::LocalDecodedVector input(context, *args[0], rows);
    exec::VectorWriter<Map<Varchar, Varchar>> resultWriter;
    resultWriter.init(*result->as<MapVector>());
    rows.applyToSelected([&](auto row) {
      resultWriter.setOffset(row);
      if (input->isNullAt(row)) {
        resultWriter.commitNull();
      } else {
        auto& mapWriter = resultWriter.current();
        folly::F14FastMap<std::string, std::string> keyValues;
        auto sv = input->valueAt<StringView>(row);
        const std::string_view current = std::string_view(sv.data(), sv.size());
        sonic_json::Document doc;
        doc.Parse(current);
        if (doc.HasParseError()) {
          resultWriter.commitNull();
          return;
        }
        if (!doc.IsObject()) {
          // hiveudf returns empty map for valid non-object JSON
          // (null, number, boolean, string, array), not SQL NULL.
          resultWriter.commit();
          return;
        }

        for (auto m = doc.MemberBegin(); m != doc.MemberEnd(); ++m) {
          auto& val = m->value;
          std::string_view key = m->name.GetStringView();
          std::string str;
          if (m->value.IsString()) {
            str = m->value.GetString();
          } else {
            sonic_json::WriteBuffer wb;
            m->value.Serialize(wb);
            str = wb.ToString();
          }
          keyValues.insert_or_assign(key, str);
        }

        for (const auto& [key, value] : keyValues) {
          auto [keyWriter, valueWriter] = mapWriter.add_item();
          keyWriter.append(StringView(key));
          valueWriter.append(StringView(value));
        }
        resultWriter.commit();
      }
    });
    resultWriter.finish();
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder()
                .returnType("map(varchar,varchar)")
                .argumentType("varchar")
                .build()};
  }

 private:
  mutable folly::once_flag initUseSonic_;
  mutable bool useSonic_ = true;
};
} // namespace

BOLT_DECLARE_VECTOR_FUNCTION(
    udf_json_to_map,
    JsonToMapFunction::signatures(),
    std::make_unique<JsonToMapFunction>());
} // namespace bytedance::bolt::functions::sparksql
