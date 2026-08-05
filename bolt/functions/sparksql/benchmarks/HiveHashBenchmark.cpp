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

#include "bolt/benchmarks/ExpressionBenchmarkBuilder.h"
#include "bolt/functions/sparksql/registration/Register.h"

DEFINE_int32(bolt_benchmark_string_length, 50, "string length");
DEFINE_double(bolt_benchmark_null_ratio, 0.0, "null ratio");
using namespace bytedance;
using namespace bytedance::bolt;

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  functions::sparksql::registerFunctions("");

  ExpressionBenchmarkBuilder benchmarkBuilder;
  benchmarkBuilder
      .addBenchmarkSet(
          "HiveHash",
          ROW({"i32",
               "i64",
               "f32",
               "f64",
               "d64",
               "d128",
               "str",
               "row_i32_i32",
               "row_i64_i64",
               "row_str_str",
               "array_i32",
               "array_i64",
               "array_str",
               "map_i32_i32",
               "map_i64_i64",
               "map_str_str"},
              {INTEGER(),
               BIGINT(),
               REAL(),
               DOUBLE(),
               DECIMAL(18, 6),
               DECIMAL(38, 16),
               VARCHAR(),
               ROW({INTEGER(), INTEGER()}),
               ROW({BIGINT(), BIGINT()}),
               ROW({VARCHAR(), VARCHAR()}),
               ARRAY(INTEGER()),
               ARRAY(BIGINT()),
               ARRAY(VARCHAR()),
               MAP(INTEGER(), INTEGER()),
               MAP(BIGINT(), BIGINT()),
               MAP(VARCHAR(), VARCHAR())}))
      .withFuzzerOptions(
          {.vectorSize = 1000,
           .nullRatio = FLAGS_bolt_benchmark_null_ratio,
           .stringLength = (size_t)FLAGS_bolt_benchmark_string_length})
      .addExpression("i32", "hive_hash(i32)")
      .addExpression("i64", "hive_hash(i64)")
      .addExpression("f32", "hive_hash(f32)")
      .addExpression("f64", "hive_hash(f64)")
      .addExpression("d64", "hive_hash(d64)")
      .addExpression("d128", "hive_hash(d128)")
      .addExpression("str", "hive_hash(str)")
      .addExpression("row_i32_i32", "hive_hash(row_i32_i32)")
      .addExpression("row_i64_i64", "hive_hash(row_i64_i64)")
      .addExpression("row_str_str", "hive_hash(row_str_str)")
      .addExpression("array_i32", "hive_hash(array_i32)")
      .addExpression("array_i64", "hive_hash(array_i64)")
      .addExpression("array_str", "hive_hash(array_str)")
      .addExpression("map_i32_i32", "hive_hash(map_i32_i32)")
      .addExpression("map_i64_i64", "hive_hash(map_i64_i64)")
      .addExpression("map_str_str", "hive_hash(map_str_str)")
      .withIterations(10000);

  benchmarkBuilder.registerBenchmarks();
  folly::runBenchmarks();
  return 0;
}
