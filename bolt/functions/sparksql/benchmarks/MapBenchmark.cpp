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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "bolt/functions/Registerer.h"
#include "bolt/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
#include "bolt/functions/sparksql/registration/Register.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;

namespace {

class MapBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  MapBenchmark() {
    functions::sparksql::registerFunctions();

    const auto Size = 10'000;
    VectorFuzzer::Options options;
    options.vectorSize = Size;

    auto rowType =
        ROW({"int_const0",
             "int_const1",
             "int_const2",
             "str_const0",
             "str_const1",
             "str_const2"},
            {INTEGER(), INTEGER(), INTEGER(), VARCHAR(), VARCHAR(), VARCHAR()});
    std::vector<VectorPtr> children{
        BaseVector::createConstant(INTEGER(), 1000, Size, pool()),
        BaseVector::createConstant(INTEGER(), 10000, Size, pool()),
        BaseVector::createConstant(INTEGER(), 99999999, Size, pool()),
        BaseVector::createConstant(
            VARCHAR(), "playable_custom_download_click_count", Size, pool()),
        BaseVector::createConstant(
            VARCHAR(),
            "play_duration_no_greater_than_1500ms_count",
            Size,
            pool()),
        BaseVector::createConstant(VARCHAR(), "abc", Size, pool())};

    VectorFuzzer fuzzer(options, pool());
    auto argsRow = std::dynamic_pointer_cast<RowVector>(fuzzer.fuzzFlat(
        ROW({"int_key0",
             "int_key1",
             "int_key2",
             "int_value0",
             "int_value1",
             "int_value2",
             "str_key0",
             "str_key1",
             "str_key2",
             "str_value0",
             "str_value1",
             "str_value2"},
            {INTEGER(),
             INTEGER(),
             INTEGER(),
             INTEGER(),
             INTEGER(),
             INTEGER(),
             VARCHAR(),
             VARCHAR(),
             VARCHAR(),
             VARCHAR(),
             VARCHAR(),
             VARCHAR()})));
    TypePtr type = argsRow->type();
    rowType =
        rowType->unionWith(std::dynamic_pointer_cast<const RowType>(type));
    auto& argsRowChildren = argsRow->children();
    children.insert(
        children.end(), argsRowChildren.begin(), argsRowChildren.end());

    data_ = std::make_shared<RowVector>(
        pool(),
        std::move(rowType),
        nullptr,
        argsRow->size(),
        std::move(children));
  }

  size_t runStringConstantKeys(size_t times) {
    return run(
        "map(str_const0, str_value0, str_const1, str_value1, str_const2, str_value2)",
        times);
  }

  size_t runStringFlatKeys(size_t times) {
    return run(
        "map(str_key0, str_value0, str_key1, str_value1, str_key2, str_value2)",
        times);
  }

  size_t runint_constantKeys(size_t times) {
    return run(
        "map(int_const0, int_value0, int_const1, int_value1, int_const2, int_value2)",
        times);
  }

  size_t runIntFlatKeys(size_t times) {
    return run(
        "map(int_key0, int_value0, int_key1, int_value1, int_key2, int_value2)",
        times);
  }

 private:
  size_t run(const std::string& expression, size_t times) {
    folly::BenchmarkSuspender suspender;
    auto exprSet = compileExpression(expression, asRowType(data_->type()));
    suspender.dismiss();

    int cnt = 0;
    for (auto i = 0; i < times * 1'000; i++) {
      cnt += evaluate(exprSet, data_)->size();
    }
    return cnt;
  }

  RowVectorPtr data_;
};

BENCHMARK_MULTI(strConstantKeys, n) {
  MapBenchmark benchmark;
  return benchmark.runStringConstantKeys(n);
}

BENCHMARK_MULTI(strFlatKeys, n) {
  MapBenchmark benchmark;
  return benchmark.runStringFlatKeys(n);
}

BENCHMARK_MULTI(intConstantKeys, n) {
  MapBenchmark benchmark;
  return benchmark.runint_constantKeys(n);
}

BENCHMARK_MULTI(intFlatKeys, n) {
  MapBenchmark benchmark;
  return benchmark.runIntFlatKeys(n);
}
} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  memory::MemoryManager::initialize({});
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
