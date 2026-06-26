#include "bolt/common/file/FileSystems.h"
#include "bolt/duckdb/conversion/DuckParser.h"
#include "bolt/exec/Aggregate.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/Window.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"
#include "bolt/exec/tests/utils/TempDirectoryPath.h"
#include "bolt/functions/prestosql/window/WindowFunctionsRegistration.h"

#include <folly/String.h>

#include <algorithm>
#include <iterator>
#include <set>

using namespace bytedance::bolt::exec::test;

namespace bytedance::bolt::exec {
namespace {

class BmWindowSemanticCoverageTest : public OperatorTestBase {
 public:
  void SetUp() override {
    OperatorTestBase::SetUp();
    bytedance::bolt::window::prestosql::registerAllWindowFunctions();
    filesystems::registerLocalFileSystem();
  }

 protected:
  RowVectorPtr makeCoverageInput(vector_size_t size = 18) {
    return makeRowVector(
        {"c0",
         "c1",
         "c2",
         "c3",
         "row_id",
         "bigint_value",
         "double_value",
         "real_value",
         "bool_value",
         "varchar_value",
         "varbinary_value",
         "second_double",
         "weight",
         "bucket_count",
         "capacity",
         "percentile_value",
         "default_bigint"},
        {
            makeFlatVector<int32_t>(
                size,
                [](auto row) { return row % 4; },
                [](auto row) { return row % 11 == 0; }),
            makeFlatVector<int32_t>(
                size,
                [](auto row) { return (row * 7) % 9; },
                [](auto row) { return row % 7 == 0; }),
            makeFlatVector<int64_t>(size, [](auto row) {
              return static_cast<int64_t>(row % 5) + 1;
            }),
            makeFlatVector<int64_t>(size, [](auto row) {
              return static_cast<int64_t>((row + 1) % 4) + 1;
            }),
            makeFlatVector<int64_t>(size, [](auto row) { return row; }),
            makeFlatVector<int64_t>(
                size,
                [](auto row) { return row * 3 - 17; },
                [](auto row) { return row % 6 == 0; }),
            makeFlatVector<double>(
                size,
                [](auto row) { return 0.25 + row * 1.5; },
                [](auto row) { return row % 8 == 0; }),
            makeFlatVector<float>(
                size,
                [](auto row) { return static_cast<float>(row) / 3.0f; },
                [](auto row) { return row % 10 == 0; }),
            makeFlatVector<bool>(
                size,
                [](auto row) { return row % 3 != 0; },
                [](auto row) { return row % 13 == 0; }),
            makeFlatVector<std::string>(
                size,
                [](auto row) {
                  return fmt::format("value-{:02d}", static_cast<int>(row % 9));
                },
                [](auto row) { return row % 12 == 0; }),
            makeFlatVector<std::string>(
                size,
                [](auto row) {
                  return fmt::format("bin-{:02d}", static_cast<int>(row % 7));
                },
                [](auto row) { return row % 14 == 0; },
                VARBINARY()),
            makeFlatVector<double>(
                size,
                [](auto row) { return 10.0 - row * 0.75; },
                [](auto row) { return row % 9 == 0; }),
            makeFlatVector<int64_t>(
                size, [](auto row) { return static_cast<int64_t>(row % 5) + 1; }),
            makeConstant<int64_t>(3, size),
            makeConstant<int64_t>(31, size),
            makeConstant<double>(0.5, size),
            makeFlatVector<int64_t>(size, [](auto row) { return -100 - row; }),
        });
  }

  RowVectorPtr makeComplexCoverageInput() {
    constexpr vector_size_t size = 8;
    auto rowValue = makeRowVector(
        {"x", "y"},
        {
            makeFlatVector<int64_t>(
                size,
                [](auto row) { return row * 10; },
                [](auto row) { return row % 5 == 0; }),
            makeFlatVector<std::string>(
                size,
                [](auto row) { return fmt::format("row-{}", row); },
                [](auto row) { return row % 3 == 0; }),
        });

    return makeRowVector(
        {"p",
         "s",
         "array_value",
         "days_value",
         "map_varchar_int_value",
         "map_int_value",
         "map_double_value",
         "row_value"},
        {
            makeFlatVector<int64_t>(size, [](auto row) { return row % 2; }),
            makeFlatVector<int64_t>(size, [](auto row) { return row; }),
            makeArrayVector<int64_t>(
                {{0, 1},
                 {10, 11},
                 {2, 3},
                 {12, 13},
                 {4, 5},
                 {14, 15},
                 {6, 7},
                 {16, 17}}),
            makeArrayVector<int64_t>(
                {{7, 8, 9},
                 {4, 5, 6},
                 {6, 7, 8},
                 {7, 8, 9},
                 {1, 2, 3},
                 {2, 4, 8},
                 {8, 16, 32},
                 {3, 6, 12}}),
            makeMapVector<std::string, int64_t>(
                {{{"a", 10}, {"b", 20}},
                 {{"a", 11}, {"c", 30}},
                 {{"b", 21}, {"d", 40}},
                 {{"c", 31}, {"e", 50}},
                 {{"a", 12}, {"d", 41}},
                 {{"b", 22}, {"e", 51}},
                 {{"c", 32}, {"f", 60}},
                 {{"d", 42}, {"f", 61}}}),
            makeMapVector<int64_t, int64_t>(
                {{{1, 10}, {2, 20}},
                 {{1, 11}, {3, 30}},
                 {{2, 21}, {4, 40}},
                 {{3, 31}, {5, 50}},
                 {{1, 12}, {4, 41}},
                 {{2, 22}, {5, 51}},
                 {{3, 32}, {6, 60}},
                 {{4, 42}, {6, 61}}}),
            makeMapVector<int64_t, double>(
                {{{1, 1.5}, {2, 2.5}},
                 {{1, 2.5}, {3, 3.5}},
                 {{2, 3.5}, {4, 4.5}},
                 {{3, 4.5}, {5, 5.5}},
                 {{1, 5.5}, {4, 6.5}},
                 {{2, 6.5}, {5, 7.5}},
                 {{3, 7.5}, {6, 8.5}},
                 {{4, 8.5}, {6, 9.5}}}),
            rowValue,
        });
  }
};

bool isCompanionFunctionName(
    const std::string& name,
    const std::unordered_map<std::string, AggregateFunctionEntry>& functions) {
  auto suffixOffset = name.rfind("_partial");
  if (suffixOffset == std::string::npos) {
    suffixOffset = name.rfind("_merge_extract");
  }
  if (suffixOffset == std::string::npos) {
    suffixOffset = name.rfind("_merge");
  }
  if (suffixOffset == std::string::npos) {
    suffixOffset = name.rfind("_extract");
  }
  if (suffixOffset == std::string::npos) {
    return false;
  }
  return functions.count(name.substr(0, suffixOffset)) > 0;
}

std::set<std::string> registeredMainAggregateNames() {
  std::set<std::string> names;
  aggregateFunctions().withRLock([&](const auto& functions) {
    for (const auto& [name, entry] : functions) {
      if (!isCompanionFunctionName(name, functions)) {
        names.insert(name);
      }
    }
  });
  return names;
}

std::set<std::string> registeredPlainWindowNames() {
  std::set<std::string> names;
  aggregateFunctions().withRLock([&](const auto& aggregateFunctions) {
    for (const auto& [name, entry] : windowFunctions()) {
      if (!isCompanionFunctionName(name, aggregateFunctions) &&
          !aggregateFunctions.count(name)) {
        names.insert(name);
      }
    }
  });
  return names;
}

std::vector<std::string> streamingSortKeysForWindow(
    const std::string& windowSql) {
  std::vector<std::string> orderByClauses;
  auto windowExpr = duckdb::parseWindowExpr(windowSql, {});
  for (const auto& partition : windowExpr.partitionBy) {
    orderByClauses.push_back(partition->toString() + " NULLS FIRST");
  }
  for (const auto& orderBy : windowExpr.orderBy) {
    orderByClauses.push_back(
        orderBy.first->toString() + " " + orderBy.second.toString());
  }
  return orderByClauses;
}

std::string windowSql(
    const std::string& functionCall,
    const std::string& overClause,
    const std::string& frameClause = "") {
  return fmt::format(
      "{} over ({}{})",
      functionCall,
      overClause,
      frameClause.empty() ? "" : " " + frameClause);
}

std::unordered_map<std::string, std::string> bmConfigs() {
  return {
      {core::QueryConfig::kBufferManagerEnabled, "true"},
      {core::QueryConfig::kSpillEnabled, "true"},
      {core::QueryConfig::kWindowSpillEnabled, "true"},
  };
}

std::unordered_map<std::string, std::string> bmFeatureConfigs() {
  auto configs = bmConfigs();
  configs[core::QueryConfig::kBmStreamingWindowBuildEnabled] = "true";
  return configs;
}

RowVectorPtr runWindowPlan(
    memory::MemoryPool* pool,
    const RowVectorPtr& data,
    const std::string& windowFunction,
    WindowBuildType buildType) {
  core::PlanNodePtr plan;
  if (buildType == WindowBuildType::kBmStreamingWindowBuild) {
    auto sortKeys = streamingSortKeysForWindow(windowFunction);
    auto builder = PlanBuilder().values({data});
    if (!sortKeys.empty()) {
      builder.orderBy(sortKeys, false);
    }
    plan = builder.streamingWindow({windowFunction}).planNode();
  } else {
    plan = PlanBuilder().values({data}).window({windowFunction}).planNode();
  }

  TestWindowInjection windowInjection(buildType);
  auto spillDirectory = TempDirectoryPath::create();
  return AssertQueryBuilder(plan)
      .configs(buildType == WindowBuildType::kBmStreamingWindowBuild
                   ? bmConfigs()
                   : std::unordered_map<std::string, std::string>{})
      .spillDirectory(spillDirectory->path)
      .copyResults(pool);
}

RowVectorPtr runAutoBmFeatureWindowPlan(
    memory::MemoryPool* pool,
    const RowVectorPtr& data,
    const std::string& windowFunction) {
  auto sortKeys = streamingSortKeysForWindow(windowFunction);
  auto builder = PlanBuilder().values({data});
  if (!sortKeys.empty()) {
    builder.orderBy(sortKeys, false);
  }
  auto plan = builder.streamingWindow({windowFunction}).planNode();
  auto spillDirectory = TempDirectoryPath::create();
  return AssertQueryBuilder(plan)
      .configs(bmFeatureConfigs())
      .spillDirectory(spillDirectory->path)
      .copyResults(pool);
}

void assertBmMatchesSortWindow(
    memory::MemoryPool* pool,
    const RowVectorPtr& data,
    const std::string& windowFunction) {
  SCOPED_TRACE(windowFunction);
  auto expected =
      runWindowPlan(pool, data, windowFunction, WindowBuildType::kSortWindowBuild);
  auto actual = runWindowPlan(
      pool, data, windowFunction, WindowBuildType::kBmStreamingWindowBuild);
  ASSERT_TRUE(assertEqualResults({expected}, {actual}));
}

void assertAutoBmFeatureMatchesSortWindow(
    memory::MemoryPool* pool,
    const RowVectorPtr& data,
    const std::string& windowFunction) {
  SCOPED_TRACE(windowFunction);
  auto expected =
      runWindowPlan(pool, data, windowFunction, WindowBuildType::kSortWindowBuild);
  auto actual = runAutoBmFeatureWindowPlan(pool, data, windowFunction);
  ASSERT_TRUE(assertEqualResults({expected}, {actual}));
}

std::vector<std::string> partitionClauses() {
  return {"", "partition by c0", "partition by c0, c2"};
}

std::vector<std::string> rowOrderClauses() {
  return {
      "order by c1 asc nulls first, c3, row_id",
      "order by c1 asc nulls last, c3, row_id",
      "order by c1 desc nulls first, c3, row_id",
      "order by c1 desc nulls last, c3, row_id",
      "order by c1 asc nulls first, c2, c3, row_id",
      "order by c1 desc nulls last, c2, c3, row_id",
      "order by c2 asc nulls first, c1 desc nulls last, c3, row_id",
  };
}

std::vector<std::string> rangeOrderClauses() {
  return {
      "order by c2 asc nulls last",
      "order by c2 desc nulls last",
  };
}

std::vector<std::string> rowsFrameClauses() {
  return {
      "rows unbounded preceding",
      "rows current row",
      "rows between unbounded preceding and unbounded following",
      "rows between unbounded preceding and current row",
      "rows between current row and unbounded following",
      "rows between 2 preceding and current row",
      "rows between 2 preceding and unbounded following",
      "rows between current row and 2 following",
      "rows between unbounded preceding and 2 following",
      "rows between 2 preceding and 2 following",
      "rows between c2 preceding and current row",
      "rows between current row and c2 following",
      "rows between c2 preceding and c3 following",
      "rows between unbounded preceding and 1 preceding",
      "rows between 1 preceding and 3 preceding",
      "rows between 1 following and unbounded following",
      "rows between 3 following and 1 following",
  };
}

std::vector<std::string> rangeFrameClauses() {
  return {
      "range unbounded preceding",
      "range current row",
      "range between unbounded preceding and unbounded following",
      "range between unbounded preceding and current row",
      "range between current row and unbounded following",
      "range between c2 preceding and current row",
      "range between current row and c2 following",
      "range between c2 preceding and c3 following",
  };
}

std::vector<std::string> orderedOverClauses(
    const std::vector<std::string>& orders) {
  std::vector<std::string> clauses;
  const std::vector<std::string> keys{"c0", "c1", "c2", "c3", "row_id"};
  for (const auto& partition : partitionClauses()) {
    for (const auto& order : orders) {
      const auto overlapsPartition = std::any_of(keys.begin(), keys.end(), [&](auto& key) {
        return partition.find(key) != std::string::npos &&
            order.find(key) != std::string::npos;
      });
      if (overlapsPartition) {
        continue;
      }
      clauses.push_back(
          partition.empty() ? order : fmt::format("{} {}", partition, order));
    }
  }
  return clauses;
}

struct AggregateCoverageCase {
  std::string name;
  std::string call;
};

std::vector<AggregateCoverageCase> nativeAggregateCoverageCases() {
  return {
      {"any_value", "any_value(bigint_value)"},
      {"approx_distinct", "approx_distinct(bigint_value)"},
      {"approx_most_frequent",
       "approx_most_frequent(3, bigint_value, 31)"},
      {"approx_percentile", "approx_percentile(double_value, 0.5)"},
      {"approx_set", "approx_set(bigint_value)"},
      {"arbitrary", "arbitrary(bigint_value)"},
      {"array_agg", "array_agg(bigint_value)"},
      {"avg", "avg(double_value)"},
      {"bitwise_and_agg", "bitwise_and_agg(bigint_value)"},
      {"bitwise_or_agg", "bitwise_or_agg(bigint_value)"},
      {"bitwise_xor_agg", "bitwise_xor_agg(bigint_value)"},
      {"bool_and", "bool_and(bool_value)"},
      {"bool_or", "bool_or(bool_value)"},
      {"checksum", "checksum(bigint_value)"},
      {"collect_list", "collect_list(bigint_value)"},
      {"collect_set", "collect_set(bigint_value)"},
      {"corr", "corr(double_value, second_double)"},
      {"count", "count(bigint_value)"},
      {"count_if", "count_if(bool_value)"},
      {"covar_pop", "covar_pop(double_value, second_double)"},
      {"covar_samp", "covar_samp(double_value, second_double)"},
      {"entropy", "entropy(c2)"},
      {"every", "every(bool_value)"},
      {"first", "first(bigint_value)"},
      {"geometric_mean", "geometric_mean(double_value)"},
      {"histogram", "histogram(bigint_value)"},
      {"kurtosis", "kurtosis(double_value)"},
      {"last", "last(bigint_value)"},
      {"map_agg", "map_agg(row_id, varchar_value)"},
      {"max", "max(bigint_value)"},
      {"max_by", "max_by(varchar_value, bigint_value)"},
      {"max_data_size_for_stats", "max_data_size_for_stats(varchar_value)"},
      {"min", "min(bigint_value)"},
      {"min_by", "min_by(varchar_value, bigint_value)"},
      {"multimap_agg", "multimap_agg(row_id, varchar_value)"},
      {"percentile", "percentile(bigint_value, 0.5)"},
      {"regr_avgx", "regr_avgx(double_value, second_double)"},
      {"regr_avgy", "regr_avgy(double_value, second_double)"},
      {"regr_count", "regr_count(double_value, second_double)"},
      {"regr_intercept", "regr_intercept(double_value, second_double)"},
      {"regr_r2", "regr_r2(double_value, second_double)"},
      {"regr_slope", "regr_slope(double_value, second_double)"},
      {"regr_sxx", "regr_sxx(double_value, second_double)"},
      {"regr_sxy", "regr_sxy(double_value, second_double)"},
      {"regr_syy", "regr_syy(double_value, second_double)"},
      {"set_agg", "set_agg(bigint_value)"},
      {"skewness", "skewness(double_value)"},
      {"std", "std(double_value)"},
      {"stddev", "stddev(double_value)"},
      {"stddev_pop", "stddev_pop(double_value)"},
      {"stddev_samp", "stddev_samp(double_value)"},
      {"sum", "sum(bigint_value)"},
      {"sum_data_size_for_stats", "sum_data_size_for_stats(varchar_value)"},
      {"var_pop", "var_pop(double_value)"},
      {"var_samp", "var_samp(double_value)"},
      {"variance", "variance(double_value)"},
  };
}

std::set<std::string> complexInputFallbackAggregateNames() {
  return {
      "aggregate_map_sum",
      "array_addition",
      "array_count",
      "bit_days_or",
      "map_union",
      "map_union_avg",
      "map_union_count",
      "map_union_max",
      "map_union_min",
      "map_union_sum",
      "non_null_count",
      "set_union",
  };
}

std::set<std::string> baselineUnsupportedAggregateNames() {
  return {
      "merge",
      "reduce_agg",
  };
}

std::set<std::string> nativeAggregateCoverageNames() {
  std::set<std::string> names;
  for (const auto& testCase : nativeAggregateCoverageCases()) {
    names.insert(testCase.name);
  }
  return names;
}

TEST_F(BmWindowSemanticCoverageTest, registeredFunctionCensusIsClassified) {
  EXPECT_EQ(
      (std::set<std::string>{"cume_dist",
                             "dense_rank",
                             "first_value",
                             "lag",
                             "last_value",
                             "lead",
                             "nth_value",
                             "ntile",
                             "percent_rank",
                             "rank",
                             "row_number"}),
      registeredPlainWindowNames());

  auto aggregateNames = registeredMainAggregateNames();
  auto classified = nativeAggregateCoverageNames();
  auto fallback = complexInputFallbackAggregateNames();
  classified.insert(fallback.begin(), fallback.end());
  auto baselineUnsupported = baselineUnsupportedAggregateNames();
  classified.insert(baselineUnsupported.begin(), baselineUnsupported.end());

  std::vector<std::string> missing;
  std::set_difference(
      aggregateNames.begin(),
      aggregateNames.end(),
      classified.begin(),
      classified.end(),
      std::back_inserter(missing));
  EXPECT_TRUE(missing.empty())
      << "Unclassified aggregate window functions: "
      << folly::join(", ", missing);
}

TEST_F(
    BmWindowSemanticCoverageTest,
    plainWindowFunctionsMatchSortWindowAcrossFrameOrderNullMatrix) {
  auto data = makeCoverageInput();

  const std::vector<std::string> rankingFunctions{
      "row_number()", "rank()", "dense_rank()", "percent_rank()", "cume_dist()",
      "ntile(3)"};
  for (const auto& functionCall : rankingFunctions) {
    for (const auto& over : orderedOverClauses(rowOrderClauses())) {
      assertBmMatchesSortWindow(pool(), data, windowSql(functionCall, over));
    }
  }

  const std::vector<std::string> valueFunctions{
      "first_value(bigint_value)",
      "last_value(bigint_value)",
      "nth_value(bigint_value, c2)",
      "lead(bigint_value, c2, default_bigint)",
      "lag(bigint_value, c2, default_bigint)",
      "first_value(bigint_value IGNORE NULLS)",
      "last_value(bigint_value IGNORE NULLS)",
      "nth_value(bigint_value, c2 IGNORE NULLS)",
      "lead(bigint_value, c2, default_bigint IGNORE NULLS)",
      "lag(bigint_value, c2, default_bigint IGNORE NULLS)",
  };

  for (const auto& functionCall : valueFunctions) {
    for (const auto& over : orderedOverClauses(rowOrderClauses())) {
      for (const auto& frame : rowsFrameClauses()) {
        assertBmMatchesSortWindow(pool(), data, windowSql(functionCall, over, frame));
      }
    }
    for (const auto& over : orderedOverClauses(rangeOrderClauses())) {
      for (const auto& frame : rangeFrameClauses()) {
        assertBmMatchesSortWindow(pool(), data, windowSql(functionCall, over, frame));
      }
    }
  }
}

TEST_F(
    BmWindowSemanticCoverageTest,
    aggregateWindowFunctionsMatchSortWindowAcrossFrameOrderNullMatrix) {
  auto data = makeCoverageInput();

  for (const auto& aggregateCase : nativeAggregateCoverageCases()) {
    for (const auto& over : orderedOverClauses(rowOrderClauses())) {
      for (const auto& frame : rowsFrameClauses()) {
        assertBmMatchesSortWindow(
            pool(), data, windowSql(aggregateCase.call, over, frame));
      }
    }
    for (const auto& over : orderedOverClauses(rangeOrderClauses())) {
      for (const auto& frame : rangeFrameClauses()) {
        assertBmMatchesSortWindow(
            pool(), data, windowSql(aggregateCase.call, over, frame));
      }
    }
  }
}

TEST_F(BmWindowSemanticCoverageTest, supportedInputTypesMatchSortWindow) {
  constexpr vector_size_t size = 16;
  auto data = makeRowVector(
      {"p",
       "s",
       "tiny_value",
       "small_value",
       "int_value",
       "big_value",
       "real_value",
       "double_value",
       "bool_value",
       "varchar_value",
       "varbinary_value",
       "date_value",
       "timestamp_value",
       "short_decimal_value",
       "long_decimal_value"},
      {
          makeFlatVector<int32_t>(size, [](auto row) { return row % 4; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row; }),
          makeFlatVector<int8_t>(
              size, [](auto row) { return static_cast<int8_t>(row % 7); }),
          makeFlatVector<int16_t>(
              size, [](auto row) { return static_cast<int16_t>(row % 11); }),
          makeFlatVector<int32_t>(size, [](auto row) { return row % 13; }),
          makeFlatVector<int64_t>(size, [](auto row) { return row % 17; }),
          makeFlatVector<float>(
              size, [](auto row) { return static_cast<float>(row) / 2.0f; }),
          makeFlatVector<double>(size, [](auto row) { return row / 3.0; }),
          makeFlatVector<bool>(size, [](auto row) { return row % 2 == 0; }),
          makeFlatVector<std::string>(
              size, [](auto row) { return fmt::format("s{}", row % 5); }),
          makeFlatVector<std::string>(
              size,
              [](auto row) { return fmt::format("b{}", row % 5); },
              nullptr,
              VARBINARY()),
          makeFlatVector<int32_t>(
              size, [](auto row) { return 18'000 + row; }, nullptr, DATE()),
          makeFlatVector<Timestamp>(size, [](auto row) {
            return Timestamp(1'700'000'000 + row, row * 1'000);
          }),
          makeFlatVector<int64_t>(
              size,
              [](auto row) { return static_cast<int64_t>(row * 100 - 7); },
              nullptr,
              DECIMAL(10, 2)),
          makeFlatVector<int128_t>(
              size,
              [](auto row) { return static_cast<int128_t>(row) * 10'000 - 17; },
              nullptr,
              DECIMAL(30, 4)),
      });

  const std::vector<std::string> functions{
      "min(tiny_value)",
      "min(small_value)",
      "min(int_value)",
      "min(big_value)",
      "min(real_value)",
      "min(double_value)",
      "bool_and(bool_value)",
      "min(varchar_value)",
      "min(varbinary_value)",
      "min(date_value)",
      "min(timestamp_value)",
      "min(short_decimal_value)",
      "min(long_decimal_value)",
  };

  for (const auto& functionCall : functions) {
    assertBmMatchesSortWindow(
        pool(),
        data,
        windowSql(
            functionCall,
            "partition by p order by s",
            "rows between 2 preceding and 1 following"));
  }
}

TEST_F(
    BmWindowSemanticCoverageTest,
    complexInputAggregatesMatchSortWindowThroughFeatureFlagFallback) {
  auto data = makeComplexCoverageInput();
  const std::vector<std::string> functionCalls{
      "array_agg(array_value)",
      "aggregate_map_sum(map_varchar_int_value)",
      "array_addition(array_value)",
      "array_count(array_value)",
      "bit_days_or(days_value)",
      "map_union(map_int_value)",
      "map_union_avg(map_double_value)",
      "map_union_count(map_int_value)",
      "map_union_max(map_int_value)",
      "map_union_min(map_int_value)",
      "map_union_sum(map_int_value)",
      "non_null_count(row_value)",
      "set_union(array_value)",
  };

  for (const auto& functionCall : functionCalls) {
    assertAutoBmFeatureMatchesSortWindow(
        pool(),
        data,
        windowSql(
            functionCall,
            "partition by p order by s",
            "rows between 2 preceding and current row"));
  }
}

TEST_F(
    BmWindowSemanticCoverageTest,
    baselineUnsupportedAggregateWindowsAreClassified) {
  auto data = makeCoverageInput();
  EXPECT_THROW(
      runWindowPlan(
          pool(),
          data,
          windowSql(
              "merge(bigint_value)",
              "partition by c0 order by row_id",
              "rows between unbounded preceding and current row"),
          WindowBuildType::kSortWindowBuild),
      BoltException);
  EXPECT_THROW(
      runWindowPlan(
          pool(),
          data,
          windowSql(
              "reduce_agg(bigint_value, 0, (x, y) -> (x + y), (x, y) -> (x + y))",
              "partition by c0 order by row_id",
              "rows between unbounded preceding and current row"),
          WindowBuildType::kSortWindowBuild),
      BoltException);
}

} // namespace
} // namespace bytedance::bolt::exec
