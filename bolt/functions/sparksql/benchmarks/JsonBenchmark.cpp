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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "bolt/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/functions/sparksql/registration/Register.h"
using namespace bytedance::bolt;

namespace {

constexpr int kNumRows = 1'000;

// About 300 bytes per row.
std::string makeEvent(int i) {
  std::string json;
  json.append(R"({"id":)").append(std::to_string(i));
  json.append(R"(,"event":"page_view","ts":"2024-05-01T12:)");
  json.append(std::to_string(10 + i % 50)).append(":");
  json.append(std::to_string(10 + i % 50)).append(R"(Z")");
  json.append(R"(,"user":{"id":"u)").append(std::to_string(i * 7919));
  json.append(R"(","name":"User Name )").append(std::to_string(i));
  json.append(R"(","email":"user)").append(std::to_string(i));
  json.append(R"(@example.com","verified":)");
  json.append(i % 3 == 0 ? "true" : "false");
  json.append(R"(},"device":{"os":"android","version":"14.)");
  json.append(std::to_string(i % 10));
  json.append(R"("},"tags":["sports","news","video"],"score":)");
  json.append(std::to_string(i / 7.0));
  json.append(R"(,"geo":{"lat":37.77,"lon":-122.41}})");
  return json;
}

// About 4 KB per row.
std::string makeBatch(int i) {
  std::string json;
  json.append(R"({"id":)").append(std::to_string(i));
  json.append(R"(,"source":"mobile","items":[)");
  for (int j = 0; j < 13; ++j) {
    if (j > 0) {
      json.append(",");
    }
    json.append(makeEvent(i * 13 + j));
  }
  json.append("]}");
  return json;
}

class JsonBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  JsonBenchmark() {
    functions::sparksql::registerFunctions("");
    functions::prestosql::registerJsonFunctions("presto_");
    setConfig({{core::QueryConfig::kEnableSonicIsJsonScalar, "false"}});

    std::vector<std::string> small;
    std::vector<std::string> large;
    for (int i = 0; i < kNumRows; ++i) {
      small.push_back(makeEvent(i));
      large.push_back(makeBatch(i));
    }
    small_ = vectorMaker_.rowVector({vectorMaker_.flatVector(small)});
    large_ = vectorMaker_.rowVector({vectorMaker_.flatVector(large)});
  }

  size_t run(const std::string& expression, bool large, size_t times) {
    folly::BenchmarkSuspender suspender;
    const auto& data = large ? large_ : small_;
    auto exprSet = compileExpression(expression, asRowType(data->type()));
    suspender.dismiss();

    size_t count = 0;
    for (size_t i = 0; i < times; ++i) {
      count += evaluate(exprSet, data)->size();
    }
    return count;
  }

 private:
  RowVectorPtr small_;
  RowVectorPtr large_;
};

JsonBenchmark& benchmark() {
  static JsonBenchmark instance;
  return instance;
}

BENCHMARK_MULTI(parseJsonSmall, n) {
  return benchmark().run("parse_json(c0)", false, n);
}

BENCHMARK_MULTI(parseJsonLarge, n) {
  return benchmark().run("parse_json(c0)", true, n);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(jsonObjectKeysSmall, n) {
  return benchmark().run("json_object_keys(c0)", false, n);
}

BENCHMARK_MULTI(jsonObjectKeysLarge, n) {
  return benchmark().run("json_object_keys(c0)", true, n);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(getJsonObjectDetectBadJsonSmall, n) {
  return benchmark().run(
      "presto_get_json_object_detect_bad_json(c0, '$.user.name')", false, n);
}

BENCHMARK_MULTI(getJsonObjectDetectBadJsonLarge, n) {
  return benchmark().run(
      "presto_get_json_object_detect_bad_json(c0, '$.source')", true, n);
}

BENCHMARK_DRAW_LINE();

BENCHMARK_MULTI(simdIsJsonScalarSmall, n) {
  return benchmark().run("presto_is_json_scalar(c0)", false, n);
}

BENCHMARK_MULTI(simdIsJsonScalarLarge, n) {
  return benchmark().run("presto_is_json_scalar(c0)", true, n);
}

} // namespace

int main(int argc, char** argv) {
  memory::MemoryManager::initialize({});
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
