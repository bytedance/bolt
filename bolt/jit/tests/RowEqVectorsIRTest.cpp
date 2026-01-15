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
#include <optional>
#include <string>
#include <vector>
#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/RowContainer.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/fuzzer/VectorFuzzer.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec::test;

static constexpr int32_t kNumVectors = 1;
static constexpr int32_t kRowsPerVector = 1'000;

#define DEBUG 0
namespace {

class RowEqvectorsTest : public OperatorTestBase {
 public:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
  }

  static void TearDownTestCase() {
    OperatorTestBase::TearDownTestCase();
  }

  void SetUp() override {
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
    VectorFuzzer fuzzer(opts, pool(), rand());

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

  void TearDown() override {
    rowContainer_ = nullptr;
    inputVector_ = nullptr;
    inputType_ = nullptr;
    types_.clear();
    decodedVectors_.clear();
    rows_.clear();
#ifdef ENABLE_BOLT_JIT
    jitModule_ = nullptr;
    eqFunc_ = nullptr;
#endif
    OperatorTestBase::TearDown();
  }

  ~RowEqvectorsTest() = default;

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
      exec::RowEqVectors eqFunc) {
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
        for (auto keyId = 0; (keyId < keysNum) && equal; keyId++) {
          auto decoded = decodedVectors[keyId];
          equal = rowContainer->equals<mayHaveNulls>(
              row, rowContainer->columnAt(keyId), *decoded, vecId);
        }
        equalNum += equal;

        bool jitEqual = eqFunc(row, vecId, vectors.data());

        if (equal != jitEqual) {
          std::stringstream ss;
          ss << vecId << " equal: " << (int)equal
             << " jitEqual: " << (int)jitEqual
             << " row:  " << rowContainer->toString(row) << " , vec:";
          for (auto d = 0; d < decodedVectors.size(); d++) {
            auto decoded = decodedVectors[d];
            auto null = decoded->isNullAt(vecId);
            // get cpp type from types_[d]
            auto type = rowContainer->keyTypes()[d];
            if (null) {
              ss << " null = " << (int)null;
              ss << " type = " << type->toString();
              continue;
            } else {
              ss << "type = " << type->toString();
            }
            switch (type->kind()) {
              case TypeKind::BIGINT:
                ss << ",val= " << decoded->valueAt<int64_t>(vecId);
                break;
              case TypeKind::INTEGER:
                ss << ",val= " << decoded->valueAt<int32_t>(vecId);
                break;
              case TypeKind::SMALLINT:
                ss << ",val= " << decoded->valueAt<int16_t>(vecId);
                break;
              case TypeKind::TINYINT:
                ss << ",val= " << decoded->valueAt<int8_t>(vecId);
                break;
              case TypeKind::REAL:
                ss << ",val= " << decoded->valueAt<float>(vecId);
                break;
              case TypeKind::DOUBLE:
                ss << ",val= " << decoded->valueAt<double>(vecId);
                break;
              case TypeKind::VARCHAR:
                ss << ",val= " << decoded->valueAt<StringView>(vecId);
                break;
              case TypeKind::TIMESTAMP:
                ss << ",val= " << decoded->valueAt<Timestamp>(vecId);
                break;
              default:
                ss << ",val= ignored for complex type";
                break;
            }
          }
          std::cerr << ss.str() << std::endl;
          BOLT_CHECK(false);
        }
      }
    }
    return equalNum;
  }

  void runOnce(
      std::vector<TypePtr>& types,
      std::vector<std::shared_ptr<DecodedVector>>& decodedVectors,
      bool nullable) {
    auto rowContainer = std::make_shared<exec::RowContainer>(types, pool());
    auto rows = store(*rowContainer, decodedVectors, decodedVectors[0]->size());
    auto [jitMod, funcName] =
        rowContainer->codegenRowEqVectors(types, nullable);
    auto eqFunc = (exec::RowEqVectors)jitMod->getFuncPtr(funcName);
    if (nullable) {
      RowEqualVectors<true>(rowContainer.get(), rows, decodedVectors, eqFunc);
    } else {
      RowEqualVectors<false>(rowContainer.get(), rows, decodedVectors, eqFunc);
    }
  }

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
    if (rowContainer_->JITable(types_)) {
      auto [jitMod, funcName] =
          rowContainer_->codegenRowEqVectors(types_, hasNulls_);
      jitModule_ = std::move(jitMod);
      eqFunc_ = (exec::RowEqVectors)jitModule_->getFuncPtr(funcName);
    }

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
    int32_t equalNum = 0;
    if (hasNulls_) {
      equalNum = RowEqualVectors<true>(
          rowContainer_.get(), rows_, decodedVectors_, eqFunc_);
    } else {
      equalNum = RowEqualVectors<false>(
          rowContainer_.get(), rows_, decodedVectors_, eqFunc_);
    }
#if DEBUG
    std::cout << "equal num = " << equalNum << std::endl;
#endif
  }

  void setUseJit(bool useJit) {
    useJit_ = useJit;
  }

  std::shared_ptr<exec::RowContainer> rowContainer_{nullptr};
  RowVectorPtr inputVector_{nullptr};
  RowTypePtr inputType_{nullptr};
  std::vector<TypePtr> types_;
  bool hasNulls_ = false;
  std::vector<std::shared_ptr<DecodedVector>> decodedVectors_;
  std::vector<char*> rows_;
#ifdef ENABLE_BOLT_JIT
  bytedance::bolt::jit::CompiledModuleSP jitModule_;
  exec::RowEqVectors eqFunc_{nullptr};
  bool useJit_ = false;
#endif
};

TEST_F(RowEqvectorsTest, singleKey) {
  for (auto& name : inputType_->names()) {
    prepare(name);
    run();
  }
}

TEST_F(RowEqvectorsTest, twoKeys) {
  for (auto i = 0; i < inputType_->names().size(); i++) {
    for (auto j = i + 1; j < inputType_->names().size(); j++) {
      prepare(inputType_->names()[i] + ":" + inputType_->names()[j]);
      run();
    }
  }
}

// all halfnull
TEST_F(RowEqvectorsTest, halfnull) {
  // get subset of xx_halfnull_yy types from inputType_
  std::string halfnullTypes = "bool";
  for (auto& name : inputType_->names()) {
    if (name.find("halfnull") != std::string::npos) {
      halfnullTypes = halfnullTypes + ":" + name;
    }
  }
  prepare(halfnullTypes);
  run();
}

// all non-null
TEST_F(RowEqvectorsTest, nonnull) {
  std::string nonnullTypes = "bool";
  for (auto& name : inputType_->names()) {
    if (name.find("halfnull") == std::string::npos) {
      nonnullTypes = nonnullTypes + ":" + name;
    }
  }
  prepare(nonnullTypes);
  run();
}

// all
TEST_F(RowEqvectorsTest, all) {
  std::string nonnullTypes = "";
  prepare(nonnullTypes);
  run();
}

// get random subset of keys from inputType_
TEST_F(RowEqvectorsTest, random) {
  for (auto t = 0; t < 1000; t++) {
    std::string randomTypes = "str";
    auto size = rand() % 100;
    for (auto i = 0; i < size; i++) {
      auto idx = rand() % inputType_->names().size();
      randomTypes = randomTypes + ":" + inputType_->names()[idx];
    }
    prepare(randomTypes);
    run();
  }
}

// test Nan for double
TEST_F(RowEqvectorsTest, Nan) {
  constexpr double nan = std::numeric_limits<double>::quiet_NaN();
  std::vector<std::optional<double>> child{
      1,
      nan,
      3.12,
      -4.12,
      nan,
      0,
      -0.0,
      0.0,
      std::nullopt,
      1.0,
      nan,
      nan,
      0.0,
      std::numeric_limits<double>::max(),
      std::numeric_limits<float>::quiet_NaN(),
      std::numeric_limits<double>::infinity()};
  auto col = makeNullableFlatVector<double>(child, DOUBLE());
  std::vector<TypePtr> types = {std::vector<TypePtr>{DOUBLE()}};
  std::vector<std::shared_ptr<DecodedVector>> decodedVectors = {
      std::vector<std::shared_ptr<DecodedVector>>{
          std::make_shared<DecodedVector>(*col)}};
  runOnce(types, decodedVectors, true);
}

TEST_F(RowEqvectorsTest, String) {
  auto col = makeNullableFlatVector<std::string>(
      {"A",
       "A",
       "",
       "BBBBBBBBBBBB",
       "BBBBBBBBBBBB",
       "BBBBBBBBBB",
       "BBBBBBBBBBBX",
       "BBBBBBBBBBBBB",
       "BBBBBBBBBBBBx",
       "ABCD",
       "ABCE",
       "abcde",
       "\n .         𣈯 、你 \\t .   ",
       "ABCDE",
       "ABCED",
       "ABCEDx",
       "🐷古力 [朋友]",
       "🐷古力 [朋友]x",
       "🐷古力 [朋友] ",
       "\\🐷古力 [朋友]\n ",
       "\\🐷古力 [朋友]\n",
       "\\古力 [朋友]\t ",
       "\\n",
       "\\n ",
       std::nullopt});
  std::vector<TypePtr> types = {std::vector<TypePtr>{VARCHAR()}};
  std::vector<std::shared_ptr<DecodedVector>> decodedVectors = {
      std::vector<std::shared_ptr<DecodedVector>>{
          std::make_shared<DecodedVector>(*col)}};
  runOnce(types, decodedVectors, true);
}

TEST_F(RowEqvectorsTest, 2String) {
  auto col = makeNullableFlatVector<std::string>(
      {"A",
       "A",
       "",
       "BBBBBBBBBBBB",
       "BBBBBBBBBBBB",
       "BBBBBBBBBB",
       "BBBBBBBBBBBX",
       "BBBBBBBBBBBBB",
       "BBBBBBBBBBBBx",
       "ABCD",
       "ABCE",
       "abcde",
       "\n .         𣈯 、你 \\t .   ",
       "ABCDE",
       "ABCED",
       "ABCEDx",
       "🐷古力 [朋友]",
       "🐷古力 [朋友]x",
       "🐷古力 [朋友] ",
       "\\🐷古力 [朋友]\n ",
       "\\🐷古力 [朋友]\n",
       "\\古力 [朋友]\t ",
       "\\n",
       "\\n ",
       std::nullopt});
  std::vector<TypePtr> types = {std::vector<TypePtr>{VARCHAR(), VARCHAR()}};
  std::vector<std::shared_ptr<DecodedVector>> decodedVectors = {
      std::vector<std::shared_ptr<DecodedVector>>{
          std::make_shared<DecodedVector>(*col),
          std::make_shared<DecodedVector>(*col)}};
  runOnce(types, decodedVectors, true);
}

TEST_F(RowEqvectorsTest, multiCol) {
  auto col0 = makeNullableFlatVector<int64_t>({3696237840770924569}, BIGINT());
  auto col1 = makeNullableFlatVector<std::string>({std::nullopt}, VARCHAR());
  auto col2 = makeNullableFlatVector<std::string>({"20241211"}, VARCHAR());
  auto col3 = makeNullableFlatVector<int32_t>({2}, INTEGER());
  std::vector<TypePtr> types = {
      std::vector<TypePtr>{BIGINT(), VARCHAR(), VARCHAR(), INTEGER()}};
  std::vector<std::shared_ptr<DecodedVector>> decodedVectors = {
      std::vector<std::shared_ptr<DecodedVector>>{
          std::make_shared<DecodedVector>(*col0),
          std::make_shared<DecodedVector>(*col1),
          std::make_shared<DecodedVector>(*col2),
          std::make_shared<DecodedVector>(*col3)}};
  runOnce(types, decodedVectors, true);
}

TEST_F(RowEqvectorsTest, ts) {
  auto col = makeNullableFlatVector<Timestamp>(
      {util::fromTimestampString("2022-11-27 16:00:00.000000000", nullptr),
       util::fromTimestampString("2012-11-27 16:00:00.000000000", nullptr)},
      TIMESTAMP());
  std::vector<TypePtr> types = {std::vector<TypePtr>{col->type()}};
  std::vector<std::shared_ptr<DecodedVector>> decodedVectors = {
      std::vector<std::shared_ptr<DecodedVector>>{
          std::make_shared<DecodedVector>(*col)}};
  runOnce(types, decodedVectors, true);
}

} // namespace
