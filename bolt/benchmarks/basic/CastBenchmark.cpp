/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "bolt/type/DecimalUtil.h"

#include "bolt/benchmarks/ExpressionBenchmarkBuilder.h"

DEFINE_bool(
    bolt_benchmark_enable_cast_optimize,
    true,
    "Enable cast optimization in the benchmark");

DEFINE_bool(bolt_benchmark_primitive, true, "benchmark all primitive case");
using namespace bytedance;
using namespace bytedance::bolt;

struct ConvertCase {
  std::string name;
  std::string expression;
  VectorPtr input;
};

template <typename T>
T shrinkTo(T v, const std::pair<std::string, std::string>& type) {
  T scale = 1;
  if (type.first == "short_decimal") {
    scale = (T)1000000;
  } else if (type.first == "long_decimal") {
    scale = (T)1000000000000000000;
  }
  if (type.second == "tinyint") {
    return (int8_t)(v)*scale;
  } else if (type.second == "smallint") {
    return (int16_t)(v)*scale;
  } else if (type.second == "int") {
    return (int32_t)(v)*scale;
  } else if (type.second == "bigint") {
    return (int64_t)(v)*scale;
  } else if (type.second == "short_decimal") {
    v = std::fmod(v, pow(10, 17));
    return std::isnan(v) ? 0 : v;
  } else if (type.second == "long_decimal") {
    v = std::isnan(v) ? 0 : v;
    v = std::isinf(v) ? 0 : v;
    return v;
  }
  return v;
}

void benchmarkAll(
    const std::vector<std::pair<std::string, std::string>>& convList) {
  ExpressionBenchmarkBuilder benchmarkBuilder;
  benchmarkBuilder.setConfig(
      {{core::QueryConfig::kEnableOptimizedCast,
        FLAGS_bolt_benchmark_enable_cast_optimize ? "true" : "false"}});
  const vector_size_t vectorSize = 1024 * 32;
  auto vectorMaker = benchmarkBuilder.vectorMaker();
  std::map<std::string, VectorPtr> cols;
  std::vector<std::pair<std::string, std::string>> cases;
  for (auto v : convList) {
    VectorPtr input;
    std::string name = v.first + "_" + v.second;
    std::string toTypeName = v.second;
    if (toTypeName == "short_decimal") {
      toTypeName = "decimal(18, 6)";
    } else if (toTypeName == "long_decimal") {
      toTypeName = "decimal(38, 18)";
    }
    cases.emplace_back(
        v.first + "_to_" + v.second,
        "cast(" + name + " AS " + toTypeName + ")");
    if (v.first == "bool") {
      input = vectorMaker.flatVector<bool>(
          vectorSize, [&](auto j) { return j % 2 == 0; });
    } else if (v.first == "tinyint") {
      input = vectorMaker.flatVector<int8_t>(
          vectorSize, [&](auto j) { return 1234 * j; });
    } else if (v.first == "smallint") {
      input = vectorMaker.flatVector<int16_t>(
          vectorSize, [&](auto j) { return shrinkTo<int16_t>(1234 * j, v); });
    } else if (v.first == "int") {
      input = vectorMaker.flatVector<int32_t>(
          vectorSize, [&](auto j) { return shrinkTo<int32_t>(1234 * j, v); });
    } else if (v.first == "bigint") {
      input = vectorMaker.flatVector<int64_t>(vectorSize, [&](auto j) {
        return shrinkTo<int64_t>(123456789 * j, v);
      });
    } else if (v.first == "float") {
      input = vectorMaker.flatVector<float>(vectorSize, [&](auto j) {
        return shrinkTo<float>(12345678.0 * j, v);
      });
    } else if (v.first == "double") {
      input = vectorMaker.flatVector<double>(vectorSize, [&](auto j) {
        return shrinkTo<double>(-123456.7 / j, v);
      });
    } else if (v.first == "short_decimal") {
      input = vectorMaker.flatVector<int64_t>(
          vectorSize,
          [&](auto j) { return shrinkTo<int64_t>(1234 * j, v); },
          nullptr,
          DECIMAL(18, 6));
    } else if (v.first == "long_decimal") {
      input = vectorMaker.flatVector<int128_t>(
          vectorSize,
          [&](auto j) {
            return shrinkTo<int128_t>(
                bytedance::bolt::HugeInt::build(12345 * j, 56789 * j + 12345),
                v);
          },
          nullptr,
          DECIMAL(38, 18));
    } else if (v.first == "string") {
      if (v.second == "bool") {
        input = vectorMaker.flatVector<std::string>(vectorSize, [&](auto j) {
          return std::string(j % 2 == 0 ? "true" : "false");
        });
      } else if (v.second == "tinyint") {
        input = vectorMaker.flatVector<std::string>(vectorSize, [&](auto j) {
          return std::to_string((int8_t)(1234 * j));
        });
      } else if (v.second == "smallint") {
        input = vectorMaker.flatVector<std::string>(vectorSize, [&](auto j) {
          return std::to_string((int16_t)(1234 * j));
        });
      } else if (v.second == "int") {
        input = vectorMaker.flatVector<std::string>(vectorSize, [&](auto j) {
          return std::to_string((int32_t)(1234 * j));
        });
      } else if (v.second == "bigint") {
        input = vectorMaker.flatVector<std::string>(vectorSize, [&](auto j) {
          return std::to_string((int64_t)(1234567890123456789 * j));
        });
      } else if (v.second == "float") {
        input = vectorMaker.flatVector<std::string>(vectorSize, [&](auto j) {
          return std::to_string(12345678.0f * j);
        });
      } else if (v.second == "double") {
        input = vectorMaker.flatVector<std::string>(
            vectorSize, [&](auto j) { return std::to_string(-123456.7 / j); });
      } else if (v.second == "short_decimal") {
        input = vectorMaker.flatVector<std::string>(vectorSize, [&](auto j) {
          char buf[32];
          auto size =
              DecimalUtil::convertToString<int64_t>(123456789 * j, 6, 32, buf);
          return std::string(buf, size);
        });
      } else if (v.second == "long_decimal") {
        input = vectorMaker.flatVector<std::string>(vectorSize, [&](auto j) {
          char buf[128];
          auto v =
              bytedance::bolt::HugeInt::build(12345 * j, 56789 * j + 12345);
          auto size = DecimalUtil::convertToString<int64_t>(v, 18, 128, buf);
          return std::string(buf, size);
        });
      } else {
        throw std::runtime_error("Unsupported type from string to " + v.second);
      }
    } else if (v.first == "timestamp") {
      input = vectorMaker.flatVector<Timestamp>(vectorSize, [&](auto j) {
        return Timestamp(1695859694 + j / 1000, j % 1000 * 1'000'000);
      });
    } else if (v.first == "date") {
      input = vectorMaker.flatVector<int32_t>(
          vectorSize, [&](auto j) { return j * 365 + j; }, nullptr, DATE());
    }
    cols[name] = input;
  }

  std::vector<std::string> colNames;
  std::vector<VectorPtr> colVecs;
  for (auto& c : cols) {
    colNames.push_back(c.first);
    colVecs.push_back(c.second);
  }
  auto& benches = benchmarkBuilder.addBenchmarkSet(
      "cast", vectorMaker.rowVector(colNames, colVecs));

  for (auto& c : cases) {
    benches.addExpression(c.first, c.second);
  }
  benches.withIterations(100).disableTesting();

  benchmarkBuilder.registerBenchmarks();
  folly::runBenchmarks();
}

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});

  if (FLAGS_bolt_benchmark_primitive) {
    std::vector<std::string> types = {
        "bool",
        "tinyint",
        "smallint",
        "int",
        "bigint",
        "float",
        "double",
        "short_decimal",
        "long_decimal",
        "string",
    };

    std::vector<std::pair<std::string, std::string>> convList;
    for (const auto& type : types) {
      for (const auto& fromType : types) {
        if (type != fromType) {
          convList.emplace_back(fromType, type);
        }
      }
    }
    convList.emplace_back("timestamp", "string");
    convList.emplace_back("timestamp", "date");
    convList.emplace_back("date", "string");
    convList.emplace_back("date", "timestamp");

    benchmarkAll(convList);

  } else {
    ExpressionBenchmarkBuilder benchmarkBuilder;
    benchmarkBuilder.setConfig(
        {{core::QueryConfig::kEnableOptimizedCast,
          FLAGS_bolt_benchmark_enable_cast_optimize ? "true" : "false"}});
    const vector_size_t vectorSize = 1024 * 32;
    auto vectorMaker = benchmarkBuilder.vectorMaker();
    auto invalidInput =
        vectorMaker.flatVector<bytedance::bolt::StringView>({""});
    auto validInput = vectorMaker.flatVector<bytedance::bolt::StringView>({""});
    auto nanInput = vectorMaker.flatVector<bytedance::bolt::StringView>({""});
    auto decimalInput = vectorMaker.flatVector<int64_t>(
        vectorSize, [&](auto j) { return 12345 * j; }, nullptr, DECIMAL(9, 2));
    auto shortDecimalInput = vectorMaker.flatVector<int64_t>(
        vectorSize,
        [&](auto j) { return 123456789 * j; },
        nullptr,
        DECIMAL(18, 6));
    auto longDecimalInput = vectorMaker.flatVector<int128_t>(
        vectorSize,
        [&](auto j) {
          return bytedance::bolt::HugeInt::build(12345 * j, 56789 * j + 12345);
        },
        nullptr,
        DECIMAL(38, 16));
    auto largeRealInput = vectorMaker.flatVector<float>(
        vectorSize, [&](auto j) { return 12345678.0 * j; });
    auto smallRealInput = vectorMaker.flatVector<float>(
        vectorSize, [&](auto j) { return 1.2345678 * j; });
    auto smallDoubleInput = vectorMaker.flatVector<double>(
        vectorSize, [&](auto j) { return -0.00012345678 / j; });
    auto largeDoubleInput = vectorMaker.flatVector<double>(
        vectorSize, [&](auto j) { return -123456.7 / j; });
    auto timestampInput =
        vectorMaker.flatVector<Timestamp>(vectorSize, [&](auto j) {
          return Timestamp(1695859694 + j / 1000, j % 1000 * 1'000'000);
        });
    auto arrayIntInput = vectorMaker.arrayVector<int64_t>(
        vectorSize,
        [&](auto j) { return 10; },
        [&](auto j) { return 1234567 * j; });
    auto mapIntInput = vectorMaker.mapVector<int64_t, int64_t>(
        vectorSize,
        [&](auto j) { return 10; },
        [&](auto j) { return 1234567 * j; },
        [&](auto j) { return 1234567 * j + 1; });
    auto rowInput =
        vectorMaker.rowVector({"a", "b"}, {smallRealInput, largeRealInput});
    auto complexInput = vectorMaker.rowVector(
        {"a", "b", "c"}, {arrayIntInput, mapIntInput, rowInput});

    auto invalidDateStr = vectorMaker.flatVector<bytedance::bolt::StringView>(
        vectorSize, [&](auto j) { return "abcd"; });

    invalidInput->resize(vectorSize);
    validInput->resize(vectorSize);
    nanInput->resize(vectorSize);

    for (int i = 0; i < vectorSize; i++) {
      nanInput->set(i, "$"_sv);
      invalidInput->set(i, StringView::makeInline(std::string("")));
      validInput->set(i, StringView::makeInline(std::to_string(i)));
    }

    benchmarkBuilder
        .addBenchmarkSet(
            "cast",
            vectorMaker.rowVector(
                {"valid",
                 "empty",
                 "nan",
                 "decimal",
                 "short_decimal",
                 "long_decimal",
                 "large_real",
                 "small_real",
                 "small_double",
                 "large_double",
                 "timestamp",
                 "array_int",
                 "map_int",
                 "row",
                 "complex",
                 "invalid_date"},
                {validInput,
                 invalidInput,
                 nanInput,
                 decimalInput,
                 shortDecimalInput,
                 longDecimalInput,
                 largeRealInput,
                 smallRealInput,
                 smallDoubleInput,
                 largeDoubleInput,
                 timestampInput,
                 arrayIntInput,
                 mapIntInput,
                 rowInput,
                 complexInput,
                 invalidDateStr}))
        .addExpression(
            "try_cast_invalid_empty_input", "try_cast (empty as int)")
        .addExpression(
            "tryexpr_cast_invalid_empty_input", "try (cast (empty as int))")
        .addExpression("try_cast_invalid_nan", "try_cast (nan as int)")
        .addExpression("tryexpr_cast_invalid_nan", "try (cast (nan as int))")
        .addExpression("try_cast_valid", "try_cast (valid as int)")
        .addExpression("tryexpr_cast_valid", "try (cast (valid as int))")
        .addExpression("string_to_int", "cast(valid as int)")
        .addExpression("decimal_to_inline_string", "cast (decimal as varchar)")
        .addExpression(
            "short_decimal_to_string", "cast (short_decimal as varchar)")
        .addExpression(
            "long_decimal_to_string", "cast (long_decimal as varchar)")
        .addExpression("large_real_to_string", "cast(large_real as varchar)")
        .addExpression("small_real_to_string", "cast(small_real as varchar)")
        .addExpression(
            "small_double_to_string", "cast(small_double as varchar)")
        .addExpression(
            "large_double_to_string", "cast(large_double as varchar)")
        .addExpression("timestamp_to_string", "cast (timestamp as varchar)")
        .addExpression("real_to_int", "cast (small_real as integer)")
        .addExpression("array_int_to_string", "cast (array_int as varchar)")
        .addExpression("map_int_to_string", "cast (map_int as varchar)")
        .addExpression("row_to_string", "cast (row as varchar)")
        .addExpression("complex_to_string", "cast (complex as varchar)")
        .addExpression(
            "invalid_timestamp", "try_cast(invalid_date as timestamp)")
        .addExpression("invalid_date", "try_cast(invalid_date as date)")
        .withIterations(100)
        .disableTesting();

    benchmarkBuilder.registerBenchmarks();
    folly::runBenchmarks();
  }
  return 0;
}
