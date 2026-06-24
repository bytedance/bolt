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
// Escape raw (unescaped) control chars (\x00 ~ \x1f) that appear *inside*
// string literals (keys or values), producing strictly valid JSON whose
// decoded contents are unchanged. Control chars outside strings are valid
// JSON whitespace and are left untouched. Both parser backends fall back to
// this so they accept the same inputs as the reference Hive UDF
// (com.jsoniter), which tolerates raw control chars and keeps them verbatim.
std::string escapeUnescapedControlChars(std::string_view in) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out;
  out.reserve(in.size());
  bool inString = false;
  bool escaped = false;
  for (unsigned char c : in) {
    if (!inString) {
      if (c == '"') {
        inString = true;
      }
      out.push_back(c);
      continue;
    }
    if (escaped) {
      out.push_back(c);
      escaped = false;
      continue;
    }
    if (c == '\\') {
      out.push_back(c);
      escaped = true;
      continue;
    }
    if (c == '"') {
      out.push_back(c);
      inString = false;
      continue;
    }
    if (c < 0x20) {
      switch (c) {
        case '\n':
          out += "\\n";
          break;
        case '\r':
          out += "\\r";
          break;
        case '\t':
          out += "\\t";
          break;
        case '\b':
          out += "\\b";
          break;
        case '\f':
          out += "\\f";
          break;
        default:
          out += "\\u00";
          out.push_back(kHex[(c >> 4) & 0xf]);
          out.push_back(kHex[c & 0xf]);
      }
      continue;
    }
    out.push_back(c);
  }
  return out;
}

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

        // Parse the given buffer into keyValues. Returns true on success
        // (including valid non-object JSON, which yields an empty map);
        // returns false if the JSON could not be parsed. The string_views
        // stored in keyValues remain valid as long as `buffer` and `parser`
        // are not mutated/reused, so the caller writes them out before any
        // subsequent parse attempt.
        auto parseInto = [&](std::string& buffer) -> bool {
          keyValues.clear();
          if (buffer.capacity() < buffer.size() + simdjson::SIMDJSON_PADDING) {
            buffer.reserve(std::max(
                buffer.size() + simdjson::SIMDJSON_PADDING,
                buffer.capacity() + buffer.capacity() / 2));
          }
          try {
            simdjson::ondemand::document doc = parser.iterate(buffer);
            simdjson::ondemand::value val = doc;
            if (val.type() != simdjson::ondemand::json_type::object) {
              // hiveudf returns empty map for valid non-object JSON
              // (null, number, boolean, string, array), not SQL NULL.
              return true;
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
            return true;
          } catch (std::exception& e) {
            return false;
          }
        };

        padded_data = current;
        bool ok = parseInto(padded_data);
        if (!ok) {
          // simdjson, unlike the reference Hive UDF (com.jsoniter), rejects
          // raw control chars inside strings. Escape them and retry so both
          // backends behave identically; this only runs on the (rare) failure
          // path, so valid JSON pays no extra cost.
          padded_data = escapeUnescapedControlChars(current);
          ok = parseInto(padded_data);
        }
        if (!ok) {
          resultWriter.commitNull();
          return;
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
        std::string escaped;
        if (doc.HasParseError()) {
          // The reference Hive UDF (com.jsoniter) accepts raw (unescaped)
          // control chars inside keys/values, while strict parsers reject
          // them. Escape such chars and retry so both behave the same and the
          // control chars are preserved in the result; `escaped` must outlive
          // the member iteration below.
          escaped = escapeUnescapedControlChars(current);
          doc.Parse(escaped);
        }
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
