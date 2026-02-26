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

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/BenchmarkUtil.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <type/Type.h>
#include <vector/BaseVector.h>
#include <vector/DecodedVector.h>
#include <vector/TypeAliases.h>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>
#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"

DEFINE_int64(fuzzer_seed, 99887766, "Seed for random input dataset generator");
DEFINE_int64(iterations, 1, "run count of each benchmark");
DEFINE_string(keys, "", "keys to compare equal");
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec::test;

static constexpr int32_t kNumVectors = 1;
static constexpr int32_t kRowsPerVector = 10'000;

#define DEBUG 0
namespace {

class rowEqvectorsBenchmark : public OperatorTestBase {
 public:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    OperatorTestBase::TearDownTestCase();
  }

  explicit rowEqvectorsBenchmark() {
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

  ~rowEqvectorsBenchmark() override {
    OperatorTestBase::TearDown();
  }

  void TestBody() override {}

  std::vector<char*> store(
      exec::RowContainer& rowContainer,
      std::vector<std::shared_ptr<DecodedVector>> decodedVectors,
      vector_size_t size) {
    std::vector<char*> rows(size);
    for (size_t row = 0; row < size; ++row) {
      rows[row] = rowContainer.newRow();
      for (auto keyId = 0; keyId < decodedVectors.size(); keyId++) {
        auto decodedVector = decodedVectors[keyId];
        rowContainer.store(*decodedVector, row, rows[row], keyId);
      }
    }
    return rows;
  }

  template <bool mayHaveNulls>
  int32_t RowEqualVectors(
      exec::RowContainer* const rowContainer,
      std::vector<char*> const rows,
      std::vector<std::shared_ptr<DecodedVector>> const decodedVectors,
      folly::BenchmarkSuspender& suspender) {
    suspender.dismiss();
    int32_t equalNum = 0;
    int32_t vectorSize = decodedVectors[0]->size();
    int32_t keysNum = decodedVectors.size();
    std::vector<char*> vectors;
    for (auto& decoded : decodedVectors) {
      vectors.emplace_back((char*)(decoded.get()));
    }

    for (auto& row : rows) {
      for (auto vecId = 0; vecId < vectorSize; vecId++) {
        bool equal = true;
        if (!useJit_) {
          for (auto keyId = 0; (keyId < keysNum) && equal; keyId++) {
            auto decoded = decodedVectors[keyId];
            equal = rowContainer->equals<mayHaveNulls>(
                row, rowContainer->columnAt(keyId), *decoded, vecId);
          }
        } else {
          BOLT_CHECK(eqFunc_);
          equal = eqFunc_(row, vecId, vectors.data());
        }
        equalNum += equal;
#if DEBUG
        if (!useJit_) {
          bool jitEqual = eqFunc_(row, vecId, vectors.data());
          if (equal != jitEqual) {
            std::cout << vecId << " equal: " << (int)equal
                      << " jitEqual: " << (int)jitEqual
                      << " row:  " << rowContainer->toString(row) << " , vec:";
            for (auto d = 0; d < decodedVectors.size(); d++) {
              auto decoded = decodedVectors[d];
              // get cpp type from types_[d]
              auto type = types_[d];
              switch (type->kind()) {
                case TypeKind::BIGINT:
                  std::cout << " null = " << (int)decoded->isNullAt(vecId)
                            << ",val= " << decoded->valueAt<int64_t>(vecId);
                  break;
                case TypeKind::INTEGER:
                  std::cout << " null = " << (int)decoded->isNullAt(vecId)
                            << ",val= " << decoded->valueAt<int32_t>(vecId);
                  break;
                case TypeKind::SMALLINT:
                  std::cout << " null = " << (int)decoded->isNullAt(vecId)
                            << ",val= " << decoded->valueAt<int16_t>(vecId);
                  break;
                case TypeKind::TINYINT:
                  std::cout << " null = " << (int)decoded->isNullAt(vecId)
                            << ",val= " << decoded->valueAt<int8_t>(vecId);
                  break;
                case TypeKind::REAL:
                  std::cout << " null = " << (int)decoded->isNullAt(vecId)
                            << ",val= " << decoded->valueAt<float>(vecId);
                  break;
                case TypeKind::DOUBLE:
                  std::cout << " null = " << (int)decoded->isNullAt(vecId)
                            << ",val= " << decoded->valueAt<double>(vecId);
                  break;
                case TypeKind::VARCHAR:
                  std::cout << " null = " << (int)decoded->isNullAt(vecId)
                            << ",val= " << decoded->valueAt<StringView>(vecId);
                  break;
                case TypeKind::TIMESTAMP:
                  std::cout << " null = " << (int)decoded->isNullAt(vecId)
                            << ",val= " << decoded->valueAt<Timestamp>(vecId);
                  break;
                default:
                  std::cout << " null = " << (int)decoded->isNullAt(vecId)
                            << ",val= ignored for complex type";
                  break;
              }
            }
            std::cout << std::endl;
            BOLT_CHECK(false);
          }
        }
#endif
      }
    }
    folly::doNotOptimizeAway(equalNum);
    suspender.rehire();
    return equalNum;
  } // namespace

  void prepare(const std::string& keys) {
    types_.clear();
    decodedVectors_.clear();
    hasNulls_ = false;
    if (keys.empty()) {
      hasNulls_ = true;
      types_ = inputType_->children();
      for (auto& vec : inputVector_->children()) {
        decodedVectors_.emplace_back(std::make_shared<DecodedVector>(*vec));
      }
    } else { // find subset of keys from RowType
      std::vector<std::string> keyVec;
      folly::split(":", keys, keyVec);
      for (auto& key : keyVec) {
        if (key.find("halfnull") != std::string::npos) {
          hasNulls_ = true;
        }
        auto type = inputType_->findChild(key);
        types_.emplace_back(type);
        decodedVectors_.emplace_back(
            std::make_shared<DecodedVector>(*inputVector_->childAt(key)));
      }
    }
    rowContainer_ = std::make_shared<exec::RowContainer>(types_, pool());
#ifdef ENABLE_BOLT_JIT
    if (rowContainer_->JITable(types_)) {
      auto [jitMod, funcName] =
          rowContainer_->codegenRowEqVectors(types_, hasNulls_);
      jitModule_ = std::move(jitMod);
      eqFunc_ = (exec::RowEqVectors)jitModule_->getFuncPtr(funcName);
    }
#endif

    rows_ = store(*rowContainer_, decodedVectors_, inputVector_->size());
#if DEBUG
    std::cout << "keys = " << keys << ", sub size = " << types_.size()
              << " , row size = " << inputVector_->size()
              << (hasNulls_ ? ", nullable" : ", no null")
              << (eqFunc_ == nullptr ? ", no jit" : ", use JIT") << std::endl;
#endif
  }

  void run() {
    // benchmark
    folly::BenchmarkSuspender suspender;
    int32_t equalNum = 0;
    if (hasNulls_) {
      equalNum = RowEqualVectors<true>(
          rowContainer_.get(), rows_, decodedVectors_, suspender);
    } else {
      equalNum = RowEqualVectors<false>(
          rowContainer_.get(), rows_, decodedVectors_, suspender);
    }
    folly::doNotOptimizeAway(equalNum);
#if DEBUG
    std::cout << "equal num = " << equalNum << std::endl;
#endif
  }

  void setUseJit(bool useJit) {
    useJit_ = useJit;
  }

 private:
  std::shared_ptr<exec::RowContainer> rowContainer_;
  RowVectorPtr inputVector_;
  RowTypePtr inputType_;
  std::vector<TypePtr> types_;
  bool hasNulls_ = false;
  std::vector<std::shared_ptr<DecodedVector>> decodedVectors_;
  std::vector<char*> rows_;
  exec::RowEqVectors eqFunc_{nullptr};
  bool useJit_ = false;
#ifdef ENABLE_BOLT_JIT
  bytedance::bolt::jit::CompiledModuleSP jitModule_;
#endif
}; // namespace

std::unique_ptr<rowEqvectorsBenchmark> benchmark;

void doRun(uint32_t it, const std::string& keys, const bool useJit) {
  benchmark->setUseJit(useJit);
  benchmark->prepare(keys);
  for (int i = 0; i < FLAGS_iterations; i++) {
    benchmark->run();
  }
}

BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT, FLAGS_keys, false);
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, rowEQvecBenchmark_JIT, FLAGS_keys, true);
BENCHMARK_DRAW_LINE();

// bool_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_bool_dict,
    "bool_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_bool_dict,
    "bool_dict",
    true);
BENCHMARK_DRAW_LINE();
// i8_dict
BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT_i8_dict, "i8_dict", false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i8_dict,
    "i8_dict",
    true);
BENCHMARK_DRAW_LINE();
// i16_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_i16_dict,
    "i16_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i16_dict,
    "i16_dict",
    true);
BENCHMARK_DRAW_LINE();
// i32_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_i32_dict,
    "i32_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i32_dict,
    "i32_dict",
    true);
BENCHMARK_DRAW_LINE();
// i64_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_i64_dict,
    "i64_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i64_dict,
    "i64_dict",
    true);
BENCHMARK_DRAW_LINE();
// f32_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_f32_dict,
    "f32_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_f32_dict,
    "f32_dict",
    true);
BENCHMARK_DRAW_LINE();
// f64_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_f64_dict,
    "f64_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_f64_dict,
    "f64_dict",
    true);
BENCHMARK_DRAW_LINE();
// ts_dict
BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT_ts_dict, "ts_dict", false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_ts_dict,
    "ts_dict",
    true);
BENCHMARK_DRAW_LINE();
// str_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_str_dict,
    "str_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_str_dict,
    "str_dict",
    true);
BENCHMARK_DRAW_LINE();
// str_inline_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_str_inline_dict,
    "str_inline_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_str_inline_dict,
    "str_inline_dict",
    true);
BENCHMARK_DRAW_LINE();
// array_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_array_dict,
    "array_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_array_dict,
    "array_dict",
    true);
BENCHMARK_DRAW_LINE();
// map_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_map_dict,
    "map_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_map_dict,
    "map_dict",
    true);
BENCHMARK_DRAW_LINE();
// struct_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_struct_dict,
    "struct_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_struct_dict,
    "struct_dict",
    true);
BENCHMARK_DRAW_LINE();

// bool_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_bool_halfnull_dict,
    "bool_halfnull_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_bool_halfnull_dict,
    "bool_halfnull_dict",
    true);
BENCHMARK_DRAW_LINE();
// i8_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_i8_halfnull_dict,
    "i8_halfnull_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i8_halfnull_dict,
    "i8_halfnull_dict",
    true);
BENCHMARK_DRAW_LINE();
// i16_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_i16_halfnull_dict,
    "i16_halfnull_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i16_halfnull_dict,
    "i16_halfnull_dict",
    true);
BENCHMARK_DRAW_LINE();
// i32_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_i32_halfnull_dict,
    "i32_halfnull_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i32_halfnull_dict,
    "i32_halfnull_dict",
    true);
BENCHMARK_DRAW_LINE();
// i64_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_i64_halfnull_dict,
    "i64_halfnull_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i64_halfnull_dict,
    "i64_halfnull_dict",
    true);
BENCHMARK_DRAW_LINE();
// f32_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_f32_halfnull_dict,
    "f32_halfnull_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_f32_halfnull_dict,
    "f32_halfnull_dict",
    true);
BENCHMARK_DRAW_LINE();
// f64_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_f64_halfnull_dict,
    "f64_halfnull_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_f64_halfnull_dict,
    "f64_halfnull_dict",
    true);
BENCHMARK_DRAW_LINE();
// ts_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_ts_halfnull_dict,
    "ts_halfnull_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_ts_halfnull_dict,
    "ts_halfnull_dict",
    true);
BENCHMARK_DRAW_LINE();
// str_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_str_halfnull_dict,
    "str_halfnull_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_str_halfnull_dict,
    "str_halfnull_dict",
    true);
BENCHMARK_DRAW_LINE();
// str_inline_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_str_inline_halfnull_dict,
    "str_inline_halfnull_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_str_inline_halfnull_dict,
    "str_inline_halfnull_dict",
    true);
BENCHMARK_DRAW_LINE();
// array_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_array_halfnull_dict,
    "array_halfnull_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_array_halfnull_dict,
    "array_halfnull_dict",
    true);
BENCHMARK_DRAW_LINE();
// map_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_map_halfnull_dict,
    "map_halfnull_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_map_halfnull_dict,
    "map_halfnull_dict",
    true);
BENCHMARK_DRAW_LINE();
// struct_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_struct_halfnull_dict,
    "struct_halfnull_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_struct_halfnull_dict,
    "struct_halfnull_dict",
    true);
BENCHMARK_DRAW_LINE();
// bool_dict:i8_dict:i16_dict:i32_dict:i64_dict:f32_dict:f64_dict:ts_dict:str_dict:str_inline_dict:array_dict:map_dict:struct_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_bool_i8_i16_i32_i64_f32_f64_ts_str_str_inline_array_map_struct,
    "bool_dict:i8_dict:i16_dict:i32_dict:i64_dict:f32_dict:f64_dict:ts_dict:str_dict:str_inline_dict:array_dict:map_dict:struct_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_bool_i8_i16_i32_i64_f32_f64_ts_str_str_inline_array_map_struct,
    "bool_dict:i8_dict:i16_dict:i32_dict:i64_dict:f32_dict:f64_dict:ts_dict:str_dict:str_inline_dict:array_dict:map_dict:struct_dict",
    true);
BENCHMARK_DRAW_LINE();
// bool_halfnull_dict:i8_halfnull_dict:i16_halfnull_dict:i32_halfnull_dict:i64_halfnull_dict:f32_halfnull_dict:f64_halfnull_dict:ts_halfnull_dict:str_halfnull_dict:str_inline_halfnull_dict:array_halfnull_dict:map_halfnull_dict:struct_halfnull_dict
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_bool_halfnull_i8_halfnull_i16_halfnull_i32_halfnull_i64_halfnull_f32_halfnull_f64_halfnull_ts_halfnull_str_halfnull_str_inline_halfnull_array_halfnull_map_halfnull_struct_halfnull,
    "bool_halfnull_dict:i8_halfnull_dict:i16_halfnull_dict:i32_halfnull_dict:i64_halfnull_dict:f32_halfnull_dict:f64_halfnull_dict:ts_halfnull_dict:str_halfnull_dict:str_inline_halfnull_dict:array_halfnull_dict:map_halfnull_dict:struct_halfnull_dict",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_bool_halfnull_i8_halfnull_i16_halfnull_i32_halfnull_i64_halfnull_f32_halfnull_f64_halfnull_ts_halfnull_str_halfnull_str_inline_halfnull_array_halfnull_map_halfnull_struct_halfnull,
    "bool_halfnull_dict:i8_halfnull_dict:i16_halfnull_dict:i32_halfnull_dict:i64_halfnull_dict:f32_halfnull_dict:f64_halfnull_dict:ts_halfnull_dict:str_halfnull_dict:str_inline_halfnull_dict:array_halfnull_dict:map_halfnull_dict:struct_halfnull_dict",
    true);
BENCHMARK_DRAW_LINE();

// bool:bool_halfnull_dict:str:str_dict:array_dict:f32_dict:bool_halfnull:str_inline
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_bool_str_array_f32_bool_halfnull_str_inline,
    "bool:bool_halfnull_dict:str:str_dict:array_dict:f32_dict:bool_halfnull:str_inline",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_bool_str_array_f32_bool_halfnull_str_inline,
    "bool:bool_halfnull_dict:str:str_dict:array_dict:f32_dict:bool_halfnull:str_inline",
    true);
BENCHMARK_DRAW_LINE();

// array
BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT_array, "array", false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_array,
    "array",
    true);
BENCHMARK_DRAW_LINE();
// map
BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT_map, "map", false);
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, rowEQvecBenchmark_JIT_map, "map", true);
BENCHMARK_DRAW_LINE();
// struct
BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT_struct, "struct", false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_struct,
    "struct",
    true);
BENCHMARK_DRAW_LINE();

// array_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_array_halfnull,
    "array_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_array_halfnull,
    "array_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// map_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_map_halfnull,
    "map_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_map_halfnull,
    "map_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// struct_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_struct_halfnull,
    "struct_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_struct_halfnull,
    "struct_halfnull",
    true);
BENCHMARK_DRAW_LINE();

// array:map:struct
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_array_map_struct,
    "array:map:struct",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_array_map_struct,
    "array:map:struct",
    true);
BENCHMARK_DRAW_LINE();
// array_halfnull:map_halfnull:struct_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_array_halfnull_map_halfnull_struct_halfnull,
    "array_halfnull:map_halfnull:struct_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_array_halfnull_map_halfnull_struct_halfnull,
    "array_halfnull:map_halfnull:struct_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// array:map:struct:array_halfnull:map_halfnull:struct_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_array_array_halfnull_map_halfnull_struct_halfnull,
    "array:map:struct:array_halfnull:map_halfnull:struct_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_array_array_halfnull_map_halfnull_struct_halfnull,
    "array:map:struct:array_halfnull:map_halfnull:struct_halfnull",
    true);
BENCHMARK_DRAW_LINE();

// array:str_halfnull:map:struct:str_halfnull:map_halfnull:struct_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_array_str_halfnull_map_halfnull_struct_halfnull,
    "array:str_halfnull:map:struct:str_halfnull:map_halfnull:struct_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_array_str_halfnull_map_halfnull_struct_halfnull,
    "array:str_halfnull:map:struct:str_halfnull:map_halfnull:struct_halfnull",
    true);
BENCHMARK_DRAW_LINE();

// bool
BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT_bool, "bool", false);
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, rowEQvecBenchmark_JIT_bool, "bool", true);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT_i8, "i8", false);
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, rowEQvecBenchmark_JIT_i8, "i8", true);
BENCHMARK_DRAW_LINE();

// i16
BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT_i16, "i16", false);
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, rowEQvecBenchmark_JIT_i16, "i16", true);
BENCHMARK_DRAW_LINE();
// i32
BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT_i32, "i32", false);
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, rowEQvecBenchmark_JIT_i32, "i32", true);
BENCHMARK_DRAW_LINE();
// i64
BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT_i64, "i64", false);
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, rowEQvecBenchmark_JIT_i64, "i64", true);
BENCHMARK_DRAW_LINE();
// f32
BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT_f32, "f32", false);
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, rowEQvecBenchmark_JIT_f32, "f32", true);
BENCHMARK_DRAW_LINE();
// f64
BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT_f64, "f64", false);
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, rowEQvecBenchmark_JIT_f64, "f64", true);
BENCHMARK_DRAW_LINE();
// ts
BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT_ts, "ts", false);
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, rowEQvecBenchmark_JIT_ts, "ts", true);
BENCHMARK_DRAW_LINE();
// str
BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT_str, "str", false);
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, rowEQvecBenchmark_JIT_str, "str", true);
BENCHMARK_DRAW_LINE();
// str_inline
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_str_inline,
    "str_inline",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_str_inline,
    "str_inline",
    true);
BENCHMARK_DRAW_LINE();

// bool_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_bool_halfnull,
    "bool_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_bool_halfnull,
    "bool_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// i8_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_i8_halfnull,
    "i8_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i8_halfnull,
    "i8_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// i16_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_i16_halfnull,
    "i16_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i16_halfnull,
    "i16_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// i32_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_i32_halfnull,
    "i32_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i32_halfnull,
    "i32_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// i64_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_i64_halfnull,
    "i64_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i64_halfnull,
    "i64_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// f32_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_f32_halfnull,
    "f32_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_f32_halfnull,
    "f32_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// f64_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_f64_halfnull,
    "f64_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_f64_halfnull,
    "f64_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// ts_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_ts_halfnull,
    "ts_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_ts_halfnull,
    "ts_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// str_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_str_halfnull,
    "str_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_str_halfnull,
    "str_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// str_inline_halfnull
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_str_inline_halfnull,
    "str_inline_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_str_inline_halfnull,
    "str_inline_halfnull",
    true);
BENCHMARK_DRAW_LINE();

// ""
BENCHMARK_NAMED_PARAM(doRun, rowEQvecBenchmark_noJIT_empty, "", false);
BENCHMARK_RELATIVE_NAMED_PARAM(doRun, rowEQvecBenchmark_JIT_empty, "", true);
BENCHMARK_DRAW_LINE();

// "bool:bool_halfnull"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_bool_bool_halfnull,
    "bool:bool_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_bool_bool_halfnull,
    "bool:bool_halfnull",
    true);
BENCHMARK_DRAW_LINE();

// "i8:i8_halfnull"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_i8_i8_halfnull,
    "i8:i8_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i8_i8_halfnull,
    "i8:i8_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// "i16:i16_halfnull"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_i16_i16_halfnull,
    "i16:i16_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i16_i16_halfnull,
    "i16:i16_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// "i32:i32_halfnull"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_i32_i32_halfnull,
    "i32:i32_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i32_i32_halfnull,
    "i32:i32_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// "i64:i64_halfnull"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_i64_i64_halfnull,
    "i64:i64_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_i64_i64_halfnull,
    "i64:i64_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// "f32:f32_halfnull"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_f32_f32_halfnull,
    "f32:f32_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_f32_f32_halfnull,
    "f32:f32_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// "f64:f64_halfnull"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_f64_f64_halfnull,
    "f64:f64_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_f64_f64_halfnull,
    "f64:f64_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// "ts:ts_halfnull"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_ts_ts_halfnull,
    "ts:ts_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_ts_ts_halfnull,
    "ts:ts_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// "str:str_halfnull"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_str_str_halfnull,
    "str:str_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_str_str_halfnull,
    "str:str_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// "str_inline:str_inline_halfnull"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_str_inline_str_inline_halfnull,
    "str_inline:str_inline_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_str_inline_str_inline_halfnull,
    "str_inline:str_inline_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// "bool:bool_halfnull:str:str_halfnull"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_bool_str_halfnull,
    "bool:bool_halfnull:str:str_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_bool_str_halfnull,
    "bool:bool_halfnull:str:str_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// "bool:bool_halfnull:i8:i8_halfnull"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_bool_i8_halfnull,
    "bool:bool_halfnull:i8:i8_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_bool_i8_halfnull,
    "bool:bool_halfnull:i8:i8_halfnull",
    true);
BENCHMARK_DRAW_LINE();
// "bool:i8:i16:i32:i64:f32:f64:str:str_inline:ts"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_all_notnull,
    "bool:i8:i16:i32:i64:f32:f64:str:str_inline:ts",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_all_notnull,
    "bool:i8:i16:i32:i64:f32:f64:str:str_inline:ts",
    true);
BENCHMARK_DRAW_LINE();

// "bool_halfnull:i8_halfnull:i16_halfnull:i32_halfnull:i64_halfnull:f32_halfnull:f64_halfnull:str_halfnull:str_inline_halfnull:ts_halfnull"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_all_halfnull,
    "bool_halfnull:i8_halfnull:i16_halfnull:i32_halfnull:i64_halfnull:f32_halfnull:f64_halfnull:str_halfnull:str_inline_halfnull:ts_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_all_halfnull,
    "bool_halfnull:i8_halfnull:i16_halfnull:i32_halfnull:i64_halfnull:f32_halfnull:f64_halfnull:str_halfnull:str_inline_halfnull:ts_halfnull",
    true);
BENCHMARK_DRAW_LINE();

// "str_inline_halfnull:str_halfnull:str:str_inline"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_str_inline_str_halfnull,
    "str_inline_halfnull:str_halfnull:str:str_inline",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_str_inline_str_halfnull,
    "str_inline_halfnull:str_halfnull:str:str_inline",
    true);
BENCHMARK_DRAW_LINE();

// "bool:i8:i16:i32:i64:f32:f64:str:str_inline:ts:array:map:struct"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_all_notnull1,
    "bool:i8:i16:i32:i64:f32:f64:str:str_inline:ts:array:map:struct",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_all_notnull1,
    "bool:i8:i16:i32:i64:f32:f64:str:str_inline:ts:array:map:struct",
    true);
BENCHMARK_DRAW_LINE();

// "bool_halfnull:i8_halfnull:i16_halfnull:i32_halfnull:i64_halfnull:f32_halfnull:f64_halfnull:str_halfnull:str_inline_halfnull:ts_halfnull:array_halfnull:map_halfnull:struct_halfnull"
BENCHMARK_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_noJIT_all_halfnull1,
    "bool_halfnull:i8_halfnull:i16_halfnull:i32_halfnull:i64_halfnull:f32_halfnull:f64_halfnull:str_halfnull:str_inline_halfnull:ts_halfnull:array_halfnull:map_halfnull:struct_halfnull",
    false);
BENCHMARK_RELATIVE_NAMED_PARAM(
    doRun,
    rowEQvecBenchmark_JIT_all_halfnull1,
    "bool_halfnull:i8_halfnull:i16_halfnull:i32_halfnull:i64_halfnull:f32_halfnull:f64_halfnull:str_halfnull:str_inline_halfnull:ts_halfnull:array_halfnull:map_halfnull:struct_halfnull",
    true);
BENCHMARK_DRAW_LINE();

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  rowEqvectorsBenchmark::SetUpTestCase();
  benchmark = std::make_unique<rowEqvectorsBenchmark>();
  folly::runBenchmarks();
  benchmark.reset();
  rowEqvectorsBenchmark::TearDownTestCase();
  return 0;
}
