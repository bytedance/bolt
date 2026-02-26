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

#include <DataSketches/kll_sketch.hpp>

#include "SketchTestBase.hpp"
namespace bytedance::bolt::aggregate::sketches::test {

class SketchKLLTests : public SketchTestBase {};

TEST_F(SketchKLLTests, example) {
  // Publicly documented kll data sketch example, guaranteeing correct linking
  // https://datasketches.apache.org/docs/KLL/KLLCppExample.html

  // this section generates two sketches from random data and serializes them
  // into files
  {
    std::default_random_engine generator(
        std::chrono::system_clock::now().time_since_epoch().count());
    std::normal_distribution<float> normalDistribution(
        0, 1); // mean=0, stddev=1

    datasketches::kll_sketch<float> sketch1; // default k=200
    for (int i = 0; i < 10000; i++) {
      sketch1.update(normalDistribution(generator)); // mean=0, stddev=1
    }
    std::ofstream os1("kll_sketch_float1.bin");
    sketch1.serialize(os1);

    datasketches::kll_sketch<float> sketch2; // default k=200
    for (int i = 0; i < 10000; i++) {
      sketch2.update(
          normalDistribution(generator) +
          1); // shift the mean for the second sketch
    }
    std::ofstream os2("kll_sketch_float2.bin");
    sketch2.serialize(os2);
  }

  // this section deserializes the sketches, produces a union and prints some
  // results
  {
    std::ifstream is1("kll_sketch_float1.bin");
    auto sketch1 = datasketches::kll_sketch<float>::deserialize(is1);

    std::ifstream is2("kll_sketch_float2.bin");
    auto sketch2 = datasketches::kll_sketch<float>::deserialize(is2);

    // we could merge sketch2 into sketch1 or the other way around
    // this is an example of using a new sketch as a union and keeping the
    // original sketches intact
    datasketches::kll_sketch<float> u; // default k=200
    u.merge(sketch1);
    u.merge(sketch2);

    auto max = u.get_quantile(0);
    auto min = u.get_quantile(1);
    auto avg = u.get_quantile(0.5);
    EXPECT_GE(min, avg);
    EXPECT_GE(avg, max);
  }
}

TEST_F(SketchKLLTests, basicPartialAgg) {
  vector_size_t size = 10'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .partialAggregation({}, {"kll(c0, 200, 0.5)"})
                .finalAggregation()
                .planNode();
  int result = readSingleValue(op).value<int>();
  EXPECT_LT(result, 5100);
  EXPECT_LT(4900, result);
}

TEST_F(SketchKLLTests, singleAggregation) {
  vector_size_t size = 10'000;
  auto values = makeFlatVector<double>(size, [](auto row) { return row; });

  auto op = PlanBuilder()
                .values({makeRowVector({values})})
                .singleAggregation({}, {"kll(c0, 200, 0.5)"})
                .planNode();

  double result = readSingleValue(op).value<double>();
  EXPECT_LT(result, 5100);
  EXPECT_LT(4900, result);
}

TEST_F(SketchKLLTests, DISABLED_globalAggFloats) {
  vector_size_t size = 10'000;
  auto values = makeFlatVector<float>(size, [](auto row) { return row * 5; });

  testGlobalAgg(values, "kll(c0, 200, 0.5)", 12500.0f, 30500.0f);
}

TEST_F(SketchKLLTests, globalAggStrings) {
  vector_size_t size = kFruits.size();

  auto values = makeFlatVector<StringView>(
      size, [&](auto row) { return StringView(kFruits[row]); });

  testGlobalAgg(
      values,
      "kll(c0, 200, 0.5)",
      kFruits[(size - 3) / 2],
      kFruits[(size + 3) / 2]);
}

TEST_F(SketchKLLTests, globalAggHighCardinalityIntegers) {
  vector_size_t size = 1'000'000;
  auto values =
      makeFlatVector<int32_t>(size, [size](auto row) { return row + size; });

  testGlobalAgg<int>(values, "kll(c0, 200, 0.75)", size * 1.73, size * 1.77);
}

TEST_F(SketchKLLTests, globalAggVeryLowCardinalityIntegers) {
  vector_size_t size = 100;
  auto values =
      makeFlatVector<int32_t>(size, [size](auto row) { return row + size; });

  testGlobalAgg<int>(values, "kll(c0, 200, 0.75)", size * 1.73, size * 1.77);
}

TEST_F(SketchKLLTests, globalAggIntegersWithDifferentRank) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  testGlobalAgg(values, "kll(c0, 200, 0.0)", 0, 3);
  testGlobalAgg(values, "kll(c0, 200, 0.5)", 475, 525);
  testGlobalAgg(values, "kll(c0, 200, 1.0)", 950, 1050);
}

TEST_F(SketchKLLTests, globalAggIntegersWithDifferentK) {
  vector_size_t size = 1'000;
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  testGlobalAgg(values, "kll(c0, 100, 0.0)", 0, 3);
  testGlobalAgg(values, "kll(c0, 200, 0.5)", 475, 525);
  testGlobalAgg(values, "kll(c0, 300, 1.0)", 950, 1050);
}

TEST_F(SketchKLLTests, groupByIntegers) {
  vector_size_t size = 10'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? row * 3 : row * 5; });

  std::unordered_map<int32_t, int32_t> expectedResultMin{
      {0, 14500}, {1, 24000}};
  std::unordered_map<int32_t, int32_t> expectedResultMax{
      {0, 15500}, {1, 26000}};
  testGroupByAgg(
      keys, values, "kll(c1, 200, 0.5)", expectedResultMin, expectedResultMax);
}

TEST_F(SketchKLLTests, groupByStrings) {
  vector_size_t size = 1'000 * kFruits.size() * kVegetables.size();

  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<StringView>(size, [&](auto row) {
    return StringView(
        row % 2 == 0 ? kFruits[row % kFruits.size()]
                     : kVegetables[row % kVegetables.size()]);
  });

  double fSz = kFruits.size();
  double vSz = kVegetables.size();
  std::unordered_map<int32_t, std::string> expectedMin{
      {0, kFruits[fSz / 2 - 2]}, {1, kVegetables[vSz / 2 - 2]}};
  std::unordered_map<int32_t, std::string> expectedMax{
      {0, kFruits[fSz / 2 + 2]}, {1, kVegetables[vSz / 2 + 2]}};

  testGroupByAgg(keys, values, "kll(c1, 200, 0.5)", expectedMin, expectedMax);
}

TEST_F(SketchKLLTests, groupByHighCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(size, [](auto row) { return row; });

  std::unordered_map<int, int> expectedResultMin{{0, 480}, {1, 480}};
  std::unordered_map<int, int> expectedResultMax{{0, 520}, {1, 520}};
  testGroupByAgg(
      keys, values, "kll(c1, 200, 0.5)", expectedResultMin, expectedResultMax);
}

TEST_F(SketchKLLTests, groupByVeryLowCardinalityIntegers) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : row * 2; });

  std::unordered_map<int, int> expectedResultMin{{0, 26}, {1, 950}};
  std::unordered_map<int, int> expectedResultMax{{0, 28}, {1, 1050}};
  testGroupByAgg(
      keys, values, "kll(c1, 200, 0.5)", expectedResultMin, expectedResultMax);
}

TEST_F(SketchKLLTests, groupByAllNulls) {
  vector_size_t size = 1'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<int32_t>(
      size, [](auto row) { return row % 2 == 0 ? 27 : row % 3; }, nullEvery(2));

  auto expectedMin = toRowVector(std::unordered_map<int, int>{{0, 0}, {1, 1}});
  auto expectedMax = toRowVector(std::unordered_map<int, int>{{0, 1}, {1, 3}});
  auto op = PlanBuilder()
                .values({makeRowVector({keys, values})})
                .singleAggregation({"c0"}, {"kll(c1, 200, 0.5)"})
                .planNode();
  assertQueryBetween(op, expectedMin, expectedMax);

  op = PlanBuilder()
           .values({makeRowVector({keys, values})})
           .partialAggregation({"c0"}, {"kll(c1, 300, 0.75)"})
           .finalAggregation()
           .planNode();
  assertQueryBetween(op, expectedMin, expectedMax);
}

TEST_F(SketchKLLTests, groupByMultiple) {
  vector_size_t size = 1'000;
  auto keys1 = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto keys2 = makeFlatVector<int32_t>(size, [](auto row) { return row % 3; });
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row % 18; });

  auto plan = PlanBuilder()
                  .values({makeRowVector({keys1, keys2, values})})
                  .partialAggregation({"c0", "c1"}, {"kll(c2, 300, 0.5)"})
                  .finalAggregation()
                  .planNode();

  CursorParameters params;
  params.planNode = plan;
  auto result = readCursor(params, [](Task*) {});
  auto actual = result.second;

  auto col3 = actual[0]->childAt(2)->template as<FlatVector<int32_t>>();
  EXPECT_EQ(col3->size(), 2 * 3);
  for (int i = 0; i < col3->size(); i++) {
    EXPECT_BETWEEN(1, col3->valueAt(i), 17);
  }

  result.first->task();
}

} // namespace bytedance::bolt::aggregate::sketches::test
