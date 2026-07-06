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
// Escape raw control chars (U+0000-U+001F) inside JSON string literals, leaving
// the decoded value unchanged, so a strict parser accepts them. RFC 8259 sec.7
// requires these to be escaped inside strings (outside, they are only
// whitespace, so we leave them). Lets both backends match the Hive reference
// UDF (com.jsoniter), which tolerates raw control chars.
std::string escapeUnescapedControlChars(std::string_view in) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out;
  out.reserve(in.size());
  bool inString = false;
  bool escaped = false;
  for (unsigned char c : in) {
    // \u00XX covers the whole range (RFC 8259 sec.7) and decodes to the same
    // byte, so the \n/\t/... two-char forms are not needed.
    if (inString && !escaped && c < 0x20) {
      out += "\\u00";
      out.push_back(kHex[c >> 4]);
      out.push_back(kHex[c & 0xf]);
      continue;
    }
    out.push_back(c);
    if (escaped) {
      escaped = false; // this char was consumed by the preceding backslash
    } else if (c == '\\' && inString) {
      escaped = true;
    } else if (c == '"') {
      inString = !inString;
    }
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

        // Parse `buffer` into keyValues; true on success (valid non-object JSON
        // yields an empty map), false if unparsable. The stored string_views
        // point into `buffer`/`parser`, so the caller must write them out
        // before the next parse attempt.
        auto parseInto = [&](std::string& buffer) -> bool {
          keyValues.clear();
          if (buffer.capacity() < buffer.size() + simdjson::SIMDJSON_PADDING) {
            buffer.reserve(std::max(
                buffer.size() + simdjson::SIMDJSON_PADDING,
                buffer.capacity() + buffer.capacity() / 2));
          }
          try {
            simdjson::ondemand::document doc = parser.iterate(buffer);
            // Check the document type instead of coercing to a value: a
            // top-level scalar throws SCALAR_DOCUMENT_AS_VALUE, but is still
            // valid non-object JSON -> empty map (like hiveudf), not NULL.
            if (doc.type() != simdjson::ondemand::json_type::object) {
              return true;
            }
            for (auto field : doc.get_object()) {
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
          // On failure, escape raw control chars and retry (only the rare
          // failure path; valid JSON is unaffected).
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
        // Keep numeric values as their original source text rather than
        // round-tripping through a double (which loses precision and reformats,
        // e.g. 1e10 -> 10000000000.0) or failing on out-of-range exponents
        // (e.g. 1e400). Matches the Hive reference UDF (com.jsoniter).
        constexpr unsigned kParseFlags =
            kParseIntegerAsRaw | kParseOverflowNumAsNumStr;
        doc.Parse<kParseFlags>(current);
        std::string escaped;
        if (doc.HasParseError()) {
          // On failure, escape raw control chars and retry (matching jsoniter);
          // `escaped` must outlive the member iteration below.
          escaped = escapeUnescapedControlChars(current);
          doc.Parse<kParseFlags>(escaped);
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
