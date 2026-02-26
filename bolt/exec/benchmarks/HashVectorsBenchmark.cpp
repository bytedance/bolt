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

#include <common/base/Exceptions.h>
#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/BenchmarkUtil.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <type/Type.h>
#include <vector/BaseVector.h>
#include <vector/DecodedVector.h>
#include <vector/TypeAliases.h>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>
#include "bolt/exec/VectorHasher.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");
DEFINE_int64(iterations, 1, "run count of each benchmark");
DEFINE_string(keys, "", "keys to compare equal");
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec::test;
using namespace bytedance::bolt::exec;

static constexpr int32_t kNumVectors = 1;
static constexpr int32_t kRowsPerVector = 4096;

namespace {

class HashVectorsBenchmark : public OperatorTestBase {
 public:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    OperatorTestBase::TearDownTestCase();
  }

  explicit HashVectorsBenchmark() {
    OperatorTestBase::SetUp();

    inputType_ = ROW(
        {{"bool", BOOLEAN()},
         {"i8", TINYINT()},
         {"i16", SMALLINT()},
         {"i32", INTEGER()},
         {"i64", BIGINT()},
         {"i128", HUGEINT()},
         {"f32", REAL()},
         {"f64", DOUBLE()},
         {"ts", TIMESTAMP()},
         {"array", ARRAY(INTEGER())},
         {"map", MAP(INTEGER(), VARCHAR())},
         {"struct",
          ROW({{"i64", BIGINT()}, {"f32", REAL()}, {"str", VARCHAR()}})},
         {"bool_dict", BOOLEAN()},
         {"i8_dict", TINYINT()},
         {"i16_dict", SMALLINT()},
         {"i32_dict", INTEGER()},
         {"i64_dict", BIGINT()},
         {"i128_dict", HUGEINT()},
         {"f32_dict", REAL()},
         {"f64_dict", DOUBLE()},
         {"ts_dict", TIMESTAMP()},
         {"array_dict", ARRAY(INTEGER())},
         {"map_dict", MAP(INTEGER(), VARCHAR())},
         {"struct_dict",
          ROW({{"i64", BIGINT()}, {"f32", REAL()}, {"str", VARCHAR()}})},
         {"bool_halfnull", BOOLEAN()},
         {"i8_halfnull", TINYINT()},
         {"i16_halfnull", SMALLINT()},
         {"i32_halfnull", INTEGER()},
         {"i64_halfnull", BIGINT()},
         {"i128_halfnull", HUGEINT()},
         {"f32_halfnull", REAL()},
         {"f64_halfnull", DOUBLE()},
         {"ts_halfnull", TIMESTAMP()},
         {"array_halfnull", ARRAY(INTEGER())},
         {"map_halfnull", MAP(INTEGER(), VARCHAR())},
         {"struct_halfnull",
          ROW({{"i64", BIGINT()}, {"f32", REAL()}, {"str", VARCHAR()}})},
         {"bool_halfnull_dict", BOOLEAN()},
         {"i8_halfnull_dict", TINYINT()},
         {"i16_halfnull_dict", SMALLINT()},
         {"i32_halfnull_dict", INTEGER()},
         {"i64_halfnull_dict", BIGINT()},
         {"i128_halfnull_dict", HUGEINT()},
         {"f32_halfnull_dict", REAL()},
         {"f64_halfnull_dict", DOUBLE()},
         {"ts_halfnull_dict", TIMESTAMP()},
         {"array_halfnull_dict", ARRAY(INTEGER())},
         {"map_halfnull_dict", MAP(INTEGER(), VARCHAR())},
         {"struct_halfnull_dict",
          ROW({{"i64", BIGINT()}, {"f32", REAL()}, {"str", VARCHAR()}})},
         {"str", VARCHAR()},
         {"str_dict", VARCHAR()},
         {"str_inline", VARCHAR()},
         {"str_inline_dict", VARCHAR()},
         {"str_halfnull", VARCHAR()},
         {"str_halfnull_dict", VARCHAR()},
         {"str_inline_halfnull", VARCHAR()},
         {"str_inline_halfnull_dict", VARCHAR()}});

    VectorFuzzer::Options opts;
    opts.vectorSize = kRowsPerVector;
    opts.nullRatio = 0;
    VectorFuzzer fuzzer(opts, pool(), FLAGS_fuzzer_seed);

    auto getDictVector = [&](TypePtr type) {
      int dictCount = rand() % kRowsPerVector;
      dictCount = std::max(dictCount, 3);
      return fuzzer.fuzzDictionary(
          fuzzer.fuzzFlat(type, dictCount), opts.vectorSize);
    };

    std::vector<VectorPtr> children;
    // Generate random values without nulls.
    children.emplace_back(fuzzer.fuzzFlat(BOOLEAN()));
    children.emplace_back(fuzzer.fuzzFlat(TINYINT()));
    children.emplace_back(fuzzer.fuzzFlat(SMALLINT()));
    children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
    children.emplace_back(fuzzer.fuzzFlat(BIGINT()));
    children.emplace_back(fuzzer.fuzzFlat(HUGEINT()));
    children.emplace_back(fuzzer.fuzzFlat(REAL()));
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));
    children.emplace_back(fuzzer.fuzzFlat(TIMESTAMP()));
    children.emplace_back(fuzzer.fuzzFlat(ARRAY(INTEGER())));
    children.emplace_back(fuzzer.fuzzFlat(MAP(INTEGER(), VARCHAR())));
    std::vector<VectorPtr> structChildren;
    structChildren.emplace_back(fuzzer.fuzzFlat(BIGINT()));
    structChildren.emplace_back(fuzzer.fuzzFlat(REAL()));
    structChildren.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
    children.emplace_back(makeRowVector({"a", "b", "c"}, structChildren));
    children.emplace_back(getDictVector(BOOLEAN()));
    children.emplace_back(getDictVector(TINYINT()));
    children.emplace_back(getDictVector(SMALLINT()));
    children.emplace_back(getDictVector(INTEGER()));
    children.emplace_back(getDictVector(BIGINT()));
    children.emplace_back(getDictVector(HUGEINT()));
    children.emplace_back(getDictVector(REAL()));
    children.emplace_back(getDictVector(DOUBLE()));
    children.emplace_back(getDictVector(TIMESTAMP()));
    children.emplace_back(getDictVector(ARRAY(INTEGER())));
    children.emplace_back(getDictVector(MAP(INTEGER(), VARCHAR())));
    structChildren.clear();
    structChildren.emplace_back(getDictVector(BIGINT()));
    structChildren.emplace_back(getDictVector(REAL()));
    structChildren.emplace_back(getDictVector(VARCHAR()));
    children.emplace_back(makeRowVector({"a", "b", "c"}, structChildren));

    // Generate random values with nulls.
    opts.nullRatio = 0.5; // 50%
    fuzzer.setOptions(opts);
    children.emplace_back(fuzzer.fuzzFlat(BOOLEAN()));
    children.emplace_back(fuzzer.fuzzFlat(TINYINT()));
    children.emplace_back(fuzzer.fuzzFlat(SMALLINT()));
    children.emplace_back(fuzzer.fuzzFlat(INTEGER()));
    children.emplace_back(fuzzer.fuzzFlat(BIGINT()));
    children.emplace_back(fuzzer.fuzzFlat(HUGEINT()));
    children.emplace_back(fuzzer.fuzzFlat(REAL()));
    children.emplace_back(fuzzer.fuzzFlat(DOUBLE()));
    children.emplace_back(fuzzer.fuzzFlat(TIMESTAMP()));
    children.emplace_back(fuzzer.fuzzFlat(ARRAY(INTEGER())));
    children.emplace_back(fuzzer.fuzzFlat(MAP(INTEGER(), VARCHAR())));
    structChildren.clear();
    structChildren.emplace_back(fuzzer.fuzzFlat(BIGINT()));
    structChildren.emplace_back(fuzzer.fuzzFlat(REAL()));
    structChildren.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
    children.emplace_back(makeRowVector({"a", "b", "c"}, structChildren));
    children.emplace_back(getDictVector(BOOLEAN()));
    children.emplace_back(getDictVector(TINYINT()));
    children.emplace_back(getDictVector(SMALLINT()));
    children.emplace_back(getDictVector(INTEGER()));
    children.emplace_back(getDictVector(BIGINT()));
    children.emplace_back(getDictVector(HUGEINT()));
    children.emplace_back(getDictVector(REAL()));
    children.emplace_back(getDictVector(DOUBLE()));
    children.emplace_back(getDictVector(TIMESTAMP()));
    children.emplace_back(getDictVector(ARRAY(INTEGER())));
    children.emplace_back(getDictVector(MAP(INTEGER(), VARCHAR())));
    structChildren.clear();
    structChildren.emplace_back(getDictVector(BIGINT()));
    structChildren.emplace_back(getDictVector(REAL()));
    structChildren.emplace_back(getDictVector(VARCHAR()));
    children.emplace_back(makeRowVector({"a", "b", "c"}, structChildren));

    opts.nullRatio = 0;
    opts.stringLength = 100;
    fuzzer.setOptions(opts);
    children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
    children.emplace_back(getDictVector(VARCHAR()));

    opts.stringLength = StringView::kInlineSize;
    fuzzer.setOptions(opts);
    children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
    children.emplace_back(getDictVector(VARCHAR()));

    opts.nullRatio = 0.5; // 50%
    opts.stringLength = 100;
    fuzzer.setOptions(opts);
    children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
    children.emplace_back(getDictVector(VARCHAR()));

    opts.stringLength = StringView::kInlineSize;
    fuzzer.setOptions(opts);
    children.emplace_back(fuzzer.fuzzFlat(VARCHAR()));
    children.emplace_back(getDictVector(VARCHAR()));

    inputVector_ = makeRowVector(inputType_->names(), children);
  }

  ~HashVectorsBenchmark() override {
    OperatorTestBase::TearDown();
  }

  void TestBody() override {}

  void run(std::string keys) {
    folly::BenchmarkSuspender suspender;
    std::vector<std::string> keyVec;
    std::vector<BaseVector*> vectors;
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    auto rowSize = inputVector_->size();
    SelectivityVector allRows = SelectivityVector(rowSize);
    folly::split(":", keys, keyVec);
    for (auto i = 0; i < keyVec.size(); i++) {
      auto type = inputType_->findChild(keyVec[i]);
      BOLT_CHECK(type);
      BOLT_CHECK(inputVector_->childAt(keyVec[i]));
      BOLT_CHECK(inputVector_->childAt(keyVec[i])->loadedVector());
      vectors.emplace_back(inputVector_->childAt(keyVec[i])->loadedVector());
      hashers.push_back(VectorHasher::create(type, i));
    }
    raw_vector<uint64_t> hashes(rowSize);
    for (auto i = 0; i < rowSize; i++) {
      hashes[i] = 0;
    }
    suspender.dismiss();
    for (auto i = 0; i < hashers.size(); i++) {
      hashers[i]->decode(*vectors[i], allRows);
      hashers[i]->hash(allRows, i > 0, hashes);
    }
    folly::doNotOptimizeAway(hashes[rowSize - 1]);
  }

 private:
  RowVectorPtr inputVector_;
  RowTypePtr inputType_;
}; // namespace

std::unique_ptr<HashVectorsBenchmark> benchmark;

void doRun(uint32_t it, const std::string& keys) {
  for (int i = 0; i < FLAGS_iterations; i++) {
    benchmark->run(keys);
  }
}

// BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench, FLAGS_keys);
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_i8, "i8");
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_i16, "i16");
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_i32, "i32");
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_i64, "i64");
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_f32, "f32");
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_f64, "f64");
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_ts, "ts");
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_array, "array");
// str
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_str, "str");
// str_inline
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_str_inline, "str_inline");
BENCHMARK_DRAW_LINE();
// i8_halfnull
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_i8_halfnull, "i8_halfnull");
// i16_halfnull
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_i16_halfnull, "i16_halfnull");
// i32_halfnull
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_i32_halfnull, "i32_halfnull");
// i64_halfnull
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_i64_halfnull, "i64_halfnull");
// f32_halfnull
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_f32_halfnull, "f32_halfnull");
// f64_halfnull
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_f64_halfnull, "f64_halfnull");
// ts_halfnull
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_ts_halfnull, "ts_halfnull");
// array_halfnull
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_array_halfnull, "array_halfnull");
// str_halfnull
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_str_halfnull, "str_halfnull");
// str_inline_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_str_inline_halfnull,
    "str_inline_halfnull");
BENCHMARK_DRAW_LINE();
// i8_dict
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_i8_dict, "i8_dict");
// i16_dict
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_i16_dict, "i16_dict");
// i32_dict
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_i32_dict, "i32_dict");
// i64_dict
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_i64_dict, "i64_dict");
// f32_dict
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_f32_dict, "f32_dict");
// f64_dict
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_f64_dict, "f64_dict");
// ts_dict
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_ts_dict, "ts_dict");
// array_dict
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_array_dict, "array_dict");
// str_dict
BENCHMARK_NAMED_PARAM(doRun, hashVectorsBench_str_dict, "str_dict");
// str_inline_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_str_inline_dict,
    "str_inline_dict");
BENCHMARK_DRAW_LINE();
// i8_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_i8_halfnull_dict,
    "i8_halfnull_dict");
// i16_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_i16_halfnull_dict,
    "i16_halfnull_dict");
// i32_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_i32_halfnull_dict,
    "i32_halfnull_dict");
// i64_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_i64_halfnull_dict,
    "i64_halfnull_dict");
// f32_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_f32_halfnull_dict,
    "f32_halfnull_dict");
// f64_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_f64_halfnull_dict,
    "f64_halfnull_dict");
// ts_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_ts_halfnull_dict,
    "ts_halfnull_dict");
// array_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_array_halfnull_dict,
    "array_halfnull_dict");
// str_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_str_halfnull_dict,
    "str_halfnull_dict");
// str_inline_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_str_inline_halfnull_dict,
    "str_inline_halfnull_dict");
BENCHMARK_DRAW_LINE();

// i64_halfnull:str_inline:i32:str
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_i64_halfnull_str_inline_i32_str,
    "i64_halfnull:str_inline:i32:str");

// i64_halfnull:i32:i32_halfnull:i64
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_i64_halfnull_i32_i32_halfnull_i64,
    "i64_halfnull:i32:i32_halfnull:i64");
// i64_halfnull:str:str_inline_halfnull:i64
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_i64_halfnull_str_str_inline_halfnull_i64_halfnull,
    "i64_halfnull:str:str_inline_halfnull:i64");
// str:str_inline_halfnull:str_halfnull:str_inline
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_str_str_inline_halfnull_str_halfnull_str_inline,
    "str:str_inline_halfnull:str_halfnull:str_inline");
// str_dict:str_inline_dict:i64_dict:i32_halfnull:str_inline_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_str_dict_str_inline_dict_i64_dict_i32_halfnull_str_inline_halfnull,
    "str_dict:str_inline_dict:i64_dict:i32_halfnull:str_inline_halfnull");

// i32:str_inline_halfnull_dict:i64_dict:str:i64_halfnull:str_inline_halfnull:str_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    hashVectorsBench_i32_str_inline_halfnull_dict_i64_dict_str_i64_halfnull_str_inline_halfnull_str_dict,
    "i32:str_inline_halfnull_dict:i64_dict:str:i64_halfnull:str_inline_halfnull:str_dict");
BENCHMARK_DRAW_LINE();

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  HashVectorsBenchmark::SetUpTestCase();
  benchmark = std::make_unique<HashVectorsBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();
  HashVectorsBenchmark::TearDownTestCase();
  return 0;
}
