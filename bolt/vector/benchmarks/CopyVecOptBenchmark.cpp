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
#include <unordered_map>
#include <vector>
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
        {{"booll", BOOLEAN()},
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

    // set selectivity vector
    rowsAll_ = SelectivityVector(kRowsPerVector);
    rows99PerCent_ = SelectivityVector(kRowsPerVector);
    rows50PerCent_ = SelectivityVector(kRowsPerVector);
    rows30PerCent_ = SelectivityVector(kRowsPerVector);
    rows20PerCent_ = SelectivityVector(kRowsPerVector);
    rows10PerCent_ = SelectivityVector(kRowsPerVector);
    rows1PerCent_ = SelectivityVector(kRowsPerVector);

    for (size_t i = 0; i < kRowsPerVector; ++i) {
      // Set half to invalid.
      if (fuzzer.coinToss(0.5)) {
        rows50PerCent_.setValid(i, false);
      }

      // Set 70% to invalid.
      if (fuzzer.coinToss(0.7)) {
        rows30PerCent_.setValid(i, false);
      }

      // Set 80% to invalid.
      if (fuzzer.coinToss(0.8)) {
        rows20PerCent_.setValid(i, false);
      }

      // Set 90% to invalid.
      if (fuzzer.coinToss(0.9)) {
        rows10PerCent_.setValid(i, false);
      }

      // Set 99% to invalid.
      if (fuzzer.coinToss(0.99)) {
        rows1PerCent_.setValid(i, false);
      }

      // Set 1% to invalid.
      if (fuzzer.coinToss(0.01)) {
        rows99PerCent_.setValid(i, false);
      }
    }

    rowsAll_.updateBounds();
    rows99PerCent_.updateBounds();
    rows50PerCent_.updateBounds();
    rows30PerCent_.updateBounds();
    rows20PerCent_.updateBounds();
    rows10PerCent_.updateBounds();
    rows1PerCent_.updateBounds();
    sel2vec_ = {
        {"100%", &rowsAll_},
        {"99%", &rows99PerCent_},
        {"50%", &rows50PerCent_},
        {"30%", &rows30PerCent_},
        {"20%", &rows20PerCent_},
        {"10%", &rows10PerCent_},
        {"1%", &rows1PerCent_},
    };
  }

  ~HashVectorsBenchmark() override {
    OperatorTestBase::TearDown();
  }

  void TestBody() override {}

  void run(std::string keys, std::string percent, bool canCopyAll) {
    folly::BenchmarkSuspender suspender;
    std::vector<std::string> keyVec;
    std::vector<BaseVector*> vectors;
    std::vector<VectorPtr> results;
    auto rowSize = inputVector_->size();
    SelectivityVector* selRows = sel2vec_[percent];
    folly::split(":", keys, keyVec);
    for (auto i = 0; i < keyVec.size(); i++) {
      auto type = inputType_->findChild(keyVec[i]);
      BOLT_CHECK(type);
      BOLT_CHECK(inputVector_->childAt(keyVec[i]));
      BOLT_CHECK(inputVector_->childAt(keyVec[i])->loadedVector());
      vectors.push_back(inputVector_->childAt(keyVec[i])->loadedVector());
      results.push_back(BaseVector::create(type, rowSize, pool()));
    }
    suspender.dismiss();
    for (auto j = 0; j < FLAGS_iterations; j++) {
      for (auto i = 0; i < results.size(); i++) {
        results[i]->copy(vectors[i], *selRows, nullptr, canCopyAll);
      }
    }
    folly::doNotOptimizeAway(results);
  }

 private:
  RowVectorPtr inputVector_;
  RowTypePtr inputType_;
  SelectivityVector rowsAll_;
  SelectivityVector rows99PerCent_;
  SelectivityVector rows50PerCent_;
  SelectivityVector rows30PerCent_;
  SelectivityVector rows20PerCent_;
  SelectivityVector rows10PerCent_;
  SelectivityVector rows1PerCent_;
  std::unordered_map<std::string, SelectivityVector*> sel2vec_;
}; // namespace

std::unique_ptr<HashVectorsBenchmark> benchmark;

void doRun(
    uint32_t it,
    const std::string& keys,
    std::string percent,
    bool canCopyAll = false) {
  benchmark->run(keys, percent, canCopyAll);
}

#define GENERATE_SELECTIVITY_BENCHMARKS(key)                                \
  BENCHMARK_NAMED_PARAM(doRun, copy_##key##_base_100, #key, "100%", false); \
  BENCHMARK_RELATIVE_NAMED_PARAM(                                           \
      doRun, copy_##key##_opti_100, #key, "100%", true);                    \
  BENCHMARK_NAMED_PARAM(doRun, copy_##key##_base_99, #key, "99%", false);   \
  BENCHMARK_RELATIVE_NAMED_PARAM(                                           \
      doRun, copy_##key##_opti_99, #key, "99%", true);                      \
  BENCHMARK_NAMED_PARAM(doRun, copy_##key##_base_50, #key, "50%", false);   \
  BENCHMARK_RELATIVE_NAMED_PARAM(                                           \
      doRun, copy_##key##_opti_50, #key, "50%", true);                      \
  BENCHMARK_NAMED_PARAM(doRun, copy_##key##_base_30, #key, "30%", false);   \
  BENCHMARK_RELATIVE_NAMED_PARAM(                                           \
      doRun, copy_##key##_opti_30, #key, "30%", true);                      \
  BENCHMARK_NAMED_PARAM(doRun, copy_##key##_base_20, #key, "20%", false);   \
  BENCHMARK_RELATIVE_NAMED_PARAM(                                           \
      doRun, copy_##key##_opti_20, #key, "20%", true);                      \
  BENCHMARK_NAMED_PARAM(doRun, copy_##key##_base_10, #key, "10%", false);   \
  BENCHMARK_RELATIVE_NAMED_PARAM(                                           \
      doRun, copy_##key##_opti_10, #key, "10%", true);                      \
  BENCHMARK_NAMED_PARAM(doRun, copy_##key##_base_1, #key, "1%", false);     \
  BENCHMARK_RELATIVE_NAMED_PARAM(                                           \
      doRun, copy_##key##_opti_1, #key, "1%", true);                        \
  BENCHMARK_DRAW_LINE();

GENERATE_SELECTIVITY_BENCHMARKS(booll)
GENERATE_SELECTIVITY_BENCHMARKS(i8)
GENERATE_SELECTIVITY_BENCHMARKS(i16)
GENERATE_SELECTIVITY_BENCHMARKS(i32)
GENERATE_SELECTIVITY_BENCHMARKS(i64)
GENERATE_SELECTIVITY_BENCHMARKS(i128)
GENERATE_SELECTIVITY_BENCHMARKS(f32)
GENERATE_SELECTIVITY_BENCHMARKS(f64)
GENERATE_SELECTIVITY_BENCHMARKS(ts)
GENERATE_SELECTIVITY_BENCHMARKS(array)
GENERATE_SELECTIVITY_BENCHMARKS(map)
GENERATE_SELECTIVITY_BENCHMARKS(struct)
BENCHMARK_DRAW_LINE();

GENERATE_SELECTIVITY_BENCHMARKS(bool_dict)
GENERATE_SELECTIVITY_BENCHMARKS(i8_dict)
GENERATE_SELECTIVITY_BENCHMARKS(i16_dict)
GENERATE_SELECTIVITY_BENCHMARKS(i32_dict)
GENERATE_SELECTIVITY_BENCHMARKS(i64_dict)
GENERATE_SELECTIVITY_BENCHMARKS(i128_dict)
GENERATE_SELECTIVITY_BENCHMARKS(f32_dict)
GENERATE_SELECTIVITY_BENCHMARKS(f64_dict)
GENERATE_SELECTIVITY_BENCHMARKS(ts_dict)
GENERATE_SELECTIVITY_BENCHMARKS(array_dict)
GENERATE_SELECTIVITY_BENCHMARKS(map_dict)
GENERATE_SELECTIVITY_BENCHMARKS(struct_dict)
BENCHMARK_DRAW_LINE();

GENERATE_SELECTIVITY_BENCHMARKS(bool_halfnull)
GENERATE_SELECTIVITY_BENCHMARKS(i8_halfnull)
GENERATE_SELECTIVITY_BENCHMARKS(i16_halfnull)
GENERATE_SELECTIVITY_BENCHMARKS(i32_halfnull)
GENERATE_SELECTIVITY_BENCHMARKS(i64_halfnull)
GENERATE_SELECTIVITY_BENCHMARKS(i128_halfnull)
GENERATE_SELECTIVITY_BENCHMARKS(f32_halfnull)
GENERATE_SELECTIVITY_BENCHMARKS(f64_halfnull)
GENERATE_SELECTIVITY_BENCHMARKS(ts_halfnull)
GENERATE_SELECTIVITY_BENCHMARKS(array_halfnull)
GENERATE_SELECTIVITY_BENCHMARKS(map_halfnull)
GENERATE_SELECTIVITY_BENCHMARKS(struct_halfnull)
BENCHMARK_DRAW_LINE();

GENERATE_SELECTIVITY_BENCHMARKS(bool_halfnull_dict)
GENERATE_SELECTIVITY_BENCHMARKS(i8_halfnull_dict)
GENERATE_SELECTIVITY_BENCHMARKS(i16_halfnull_dict)
GENERATE_SELECTIVITY_BENCHMARKS(i32_halfnull_dict)
GENERATE_SELECTIVITY_BENCHMARKS(i64_halfnull_dict)
GENERATE_SELECTIVITY_BENCHMARKS(i128_halfnull_dict)
GENERATE_SELECTIVITY_BENCHMARKS(f32_halfnull_dict)
GENERATE_SELECTIVITY_BENCHMARKS(f64_halfnull_dict)
GENERATE_SELECTIVITY_BENCHMARKS(ts_halfnull_dict)
GENERATE_SELECTIVITY_BENCHMARKS(array_halfnull_dict)
GENERATE_SELECTIVITY_BENCHMARKS(map_halfnull_dict)
GENERATE_SELECTIVITY_BENCHMARKS(struct_halfnull_dict)
BENCHMARK_DRAW_LINE();

GENERATE_SELECTIVITY_BENCHMARKS(str)
GENERATE_SELECTIVITY_BENCHMARKS(str_dict)
GENERATE_SELECTIVITY_BENCHMARKS(str_inline)
GENERATE_SELECTIVITY_BENCHMARKS(str_inline_dict)
BENCHMARK_DRAW_LINE();

GENERATE_SELECTIVITY_BENCHMARKS(str_halfnull)
GENERATE_SELECTIVITY_BENCHMARKS(str_halfnull_dict)
GENERATE_SELECTIVITY_BENCHMARKS(str_inline_halfnull)
GENERATE_SELECTIVITY_BENCHMARKS(str_inline_halfnull_dict)
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
