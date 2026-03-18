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

#ifdef ENABLE_BOLT_EXPR_JIT

#include "bolt/dwio/dwrf/test/utils/BatchMaker.h"
#include "bolt/exec/tests/utils/OperatorTestBase.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"

#include <thrust/jit/expr.h>

#include <span>
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::exec::test;

using bytedance::bolt::test::BatchMaker;
namespace bytedance::bolt::jit::test {

template <typename T> // int64_t
VectorPtr CreateFlatVector(
    std::span<T> raw,
    std::span<int8_t> raw_nulls,
    memory::MemoryPool& pool) {
  auto size = raw.size();
  BufferPtr values = AlignedBuffer::allocate<T>(size, &pool);
  auto valuesPtr = values->asMutableRange<T>();

  BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
  auto* nullsPtr = nulls->asMutable<uint64_t>();

  size_t nullCount = 0;
  for (size_t i = 0; i < size; ++i) {
    bool isNull = raw_nulls.size() != 0 && raw_nulls[i] == 0;
    bits::setNull(nullsPtr, i, isNull);
    if (!isNull) {
      valuesPtr[i] = raw[i];
    } else {
      nullCount++;
    }
  }

  return std::make_shared<FlatVector<T>>(
      &pool, nulls, size, values, std::vector<BufferPtr>{});
}

template <typename T>
VectorPtr
CreateVectorTestData(memory::MemoryPool& pool, size_t size, int batch_no = 0) {
  auto make_null_data = [](size_t size, size_t null_pos) {
    std::vector<int8_t> res;
    for (auto i = 0; i < size; i++) {
      int8_t v = (i != null_pos) ? 1 : 0; // 0 means null
      res.push_back(v);
    }
    return res;
  };

  if constexpr (std::is_integral_v<T> || std::is_floating_point_v<T>) {
    std::vector<T> data;
    for (auto i = 1; i <= size; i++) {
      data.emplace_back((T)(batch_no * 100 + i));
    }

    return CreateFlatVector(data, make_null_data(size, size / 3), pool);
  } else {
    throw std::logic_error("todo");
  }
}

VectorPtr CreateRow(
    const std::shared_ptr<const Type>& type,
    size_t size,
    std::span<void*> colums_data,
    std::span<std::span<int8_t>> colums_null,
    std::span<int8_t> row_nulls,
    memory::MemoryPool& pool) {
  BufferPtr nulls;
  size_t nullCount = 0;

  if (row_nulls.size() > 0) {
    nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    for (size_t i = 0; i < row_nulls.size(); ++i) {
      bits::setNull(nullsPtr, i, !row_nulls[i]); // '0' means  null.
      if (row_nulls[i] == 0) {
        nullCount++;
      }
    }
  }

  auto& row = type->asRow();
  std::vector<VectorPtr> children(row.size());
  for (size_t i = 0; i < row.size(); ++i) {
    auto child = row.childAt(i);
    // TODO type dispatching
    if (child->kind() == TypeKind::BOOLEAN) {
      // TODO
      auto raw = std::span{((int8_t*)colums_data[i]), size};
      children[i] = CreateFlatVector<int8_t>(raw, colums_null[i], pool);
    } else if (child->kind() == TypeKind::TINYINT) {
      auto raw = std::span{((int8_t*)colums_data[i]), size};
      children[i] = CreateFlatVector<int8_t>(raw, colums_null[i], pool);
    } else if (child->kind() == TypeKind::SMALLINT) {
      auto raw = std::span{((int16_t*)colums_data[i]), size};
      children[i] = CreateFlatVector<int16_t>(raw, colums_null[i], pool);
    } else if (child->kind() == TypeKind::INTEGER) {
      auto raw = std::span{((int32_t*)colums_data[i]), size};
      children[i] = CreateFlatVector<int32_t>(raw, colums_null[i], pool);
    } else if (child->kind() == TypeKind::BIGINT) {
      auto raw = std::span{((int64_t*)colums_data[i]), size};
      children[i] = CreateFlatVector<int64_t>(raw, colums_null[i], pool);
    } else if (child->kind() == TypeKind::REAL) {
      auto raw = std::span{((float*)colums_data[i]), size};
      children[i] = CreateFlatVector<float>(raw, colums_null[i], pool);
    } else if (child->kind() == TypeKind::DOUBLE) {
      auto raw = std::span{((double*)colums_data[i]), size};
      children[i] = CreateFlatVector<double>(raw, colums_null[i], pool);
    } else if (child->kind() == TypeKind::VARBINARY) {
      BOLT_UNSUPPORTED("TODO Unsupported");

    } else if (child->kind() == TypeKind::DATE) {
      BOLT_UNSUPPORTED("TODO Unsupported");
    }
  }
  return std::make_shared<RowVector>(
      &pool, type, nulls, size, children, nullCount);
}

class JitFilterProjectTest : public OperatorTestBase {
 public:
  RowVectorPtr CreateRowBatch(
      const std::shared_ptr<const Type>& type,
      std::vector<VectorPtr>&& children,
      std::span<int8_t> row_nulls) {
    size_t size = row_nulls.size();
    BufferPtr nulls;
    size_t nullCount = 0;

    if (row_nulls.size() > 0) {
      nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), pool_.get());
      auto* nullsPtr = nulls->asMutable<uint64_t>();
      for (size_t i = 0; i < row_nulls.size(); ++i) {
        bits::setNull(nullsPtr, i, !row_nulls[i]); // '0' means  null.
        if (row_nulls[i] == 0) {
          nullCount++;
        }
      }
    }
    return std::make_shared<RowVector>(
        pool_.get(), type, nulls, size, children, nullCount);
  }
};

TEST_F(JitFilterProjectTest, filterProject) {
  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2"}, {BIGINT(), BIGINT(), BIGINT()})}; // , DOUBLE()

  constexpr int32_t batches = 2;
  constexpr int32_t batch_size = 50;

  auto make_raw_data = []<typename T>(size_t size, T init, T inc) {
    std::vector<int64_t> res;
    for (auto i = 0; i < size; i++) {
      res.emplace_back(init + i * inc);
    }
    return res;
  };
  auto make_null_data = [](size_t size, size_t null_pos) {
    std::vector<int8_t> res;
    for (auto i = 0; i < size; i++) {
      int8_t v = (i != null_pos) ? 1 : 0; // 0 means null
      res.push_back(v);
    }
    return res;
  };

  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < batches; ++i) {
    std::vector<int8_t> raw_nulls;

    std::vector<int64_t> c0_data = make_raw_data(batch_size, (i + 1) * 10L, 1L);
    std::vector<int64_t> c1_data = make_raw_data(batch_size, (i + 2) * 10L, 1L);
    std::vector<int64_t> c2_data = make_raw_data(batch_size, (i + 3) * 10L, 1L);

    std::vector<int8_t> c0_null = make_null_data(batch_size, 1);
    std::vector<int8_t> c1_null = make_null_data(batch_size, 3);
    std::vector<int8_t> c2_null = make_null_data(batch_size, 5);

    std::vector<void*> data;
    data.emplace_back((void*)c0_data.data());
    data.emplace_back((void*)c1_data.data());
    data.emplace_back((void*)c2_data.data());
    std::vector<std::span<int8_t>> columns_nulls;
    columns_nulls.emplace_back(c0_null);
    columns_nulls.emplace_back(c1_null);
    columns_nulls.emplace_back(c2_null);

    std::vector<int8_t> row_nulls = make_null_data(batch_size, 0);

    auto batch = std::dynamic_pointer_cast<RowVector>(CreateRow(
        rowType_, batch_size, data, columns_nulls, row_nulls, *pool_));

    vectors.push_back(batch);
  }
  createDuckDbTable(vectors);

  auto plan =
      PlanBuilder()
          .values(vectors)
          .filter("c0 < 20 AND 2*c1 > c2")
          .project({"c2 + c1 * 2", "c0 + c2 / c1 "}) //   , "c0 - c1 / c2 + 100"
          .planNode();

  assertQuery(
      plan,
      "SELECT c2 + c1 * 2 AS p0, c0 + c2 / c1  as p1 FROM tmp WHERE c0 < 20 AND 2*c1 > c2 "); // WHERE c0 < 1000 AND c1 > c2
}

TEST_F(JitFilterProjectTest, testTypes) {
  auto make_null_data = [](size_t size, size_t null_pos) {
    std::vector<int8_t> res;
    for (auto i = 0; i < size; i++) {
      int8_t v = (i != null_pos) ? 1 : 0; // 0 means null
      res.push_back(v);
    }
    return res;
  };

  constexpr int32_t batches = 1;
  constexpr int32_t batch_size = 16;

  std::shared_ptr<const RowType> rowType{ROW(
      {
          "c_int8",
          "c_int16",
          "c_int32",
          "c_int64",
          "c_real",
          "c_double",
          "c_varchar",
          "c_bool",
          "c_timestamp",
          "c_date",
      },
      {
          TINYINT(),
          SMALLINT(),
          INTEGER(),
          BIGINT(),
          REAL(),
          DOUBLE(),
          VARCHAR(),
          BOOLEAN(),
          TIMESTAMP(),
          DATE(),
      })};

  // VARBINARY() =>  VARCHAR()

  std::vector<RowVectorPtr> vectors;
  std::vector<int8_t> row_nulls; //{1,1,1,1, 1,1,1,1, 1,1,1,1, 1,1,1,1};
  for (auto i = 0; i < batch_size; i++) {
    row_nulls.push_back(1);
  }

  for (size_t i = 0; i < batches; ++i) {
    std::vector<VectorPtr> children{
        makeFlatVector<int8_t>(
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
        makeFlatVector<int16_t>(
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
        makeFlatVector<int32_t>(
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
        makeNullableFlatVector<int64_t>(
            {std::nullopt,
             2,
             std::nullopt,
             4,
             5,
             6,
             7,
             8,
             9,
             10,
             11,
             12,
             13,
             14,
             15,
             16}), // 1, 3 is null
        makeFlatVector<float>(
            {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
        makeNullableFlatVector<double>(
            {1,
             std::nullopt,
             3,
             std::nullopt,
             5,
             6,
             7,
             8,
             9,
             10,
             11,
             12,
             13,
             14,
             15,
             16}), // 2, 4 is null
        makeNullableFlatVector<StringView>({
            "CHINA",
            "ALGERIA",
            "ARGENTINA",
            "BRAZIL",
            "CANADA",
            "EGYPT",
            "The United States of America",
            "Afghanistan",
            std::nullopt,
            std::nullopt,
            "Principality of Andorra",
            std::nullopt,
            std::nullopt,
            std::nullopt,
            std::nullopt,
            "Russia",
        }),
        makeFlatVector<bool>(
            {true,
             false,
             true,
             false,
             true,
             false,
             true,
             false,
             true,
             false,
             true,
             false,
             true,
             false,
             true,
             false}),
        makeFlatVector<Timestamp>(
            batch_size,
            [](vector_size_t row) {
              return Timestamp{row, 100};
            },
            nullEvery(7)),
        makeFlatVector<Date>(
            batch_size,
            [](vector_size_t row) { return Date{row}; },
            nullEvery(7)),
    };

    auto&& batch = CreateRowBatch(rowType, std::move(children), row_nulls);

    vectors.emplace_back(std::move(batch));
  }
  createDuckDbTable(vectors);

  // Test a single expression
  if (false) {
    auto plan =
        PlanBuilder()
            .values(vectors)
            .filter("c_int64 < 100 AND c_date > cast('1970-1-1' as date) ")
            .project({
                "c_int8",
                "c_real",
            })
            .planNode();

    assertQuery(
        plan,
        R"sql(
        SELECT
          c_int8 , c_real
        FROM tmp
        WHEREc_int64 < 100 AND c_date > '1970-1-1'
        )sql");
  }

  // NOTE: bolt doesn't support >= <= yet

  // 1. constant literal expression
  // 2.  + - * / for types integer, real, double
  // 3. String
  //  OR c_bool = true
  // c_int64 > 0
  // OR c_varchar = 'EGYPT'
  // OR c_date > cast('1970-1-1' as date)
  // OR c_bool = true
  // OR c_bool
  // OR c_int64 > 0 OR c_int8 < 100 OR c_int16 > 0 OR c_int32 < 200
  const std::string filters = R"sql(
       c_int64 > 0
       OR c_int8 < 100
  )sql";

  const std::vector<std::string> projects{
      "c_int8 + c_int16 * c_int8",
      "c_real / cast(c_int64 as real) - c_double * cast(2 as double)  ",
  };

  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter(filters)
                  .project(projects)
                  .planNode();

  std::string duckDbSql = R"sql(
      SELECT
        c_int8 + c_int16 * c_int8
        , c_real / cast(c_int64 as real) - c_double * cast(2 as double)
      FROM tmp
      WHERE
            c_int64 > 0
          -- OR c_varchar = 'EGYPT'
          -- OR c_date > '1970-1-1'
          -- OR c_bool = true
          -- OR c_bool
          OR c_int8 < 100
      )sql";

  CursorParameters params;
  params.queryCtx = core::QueryCtx::createForTest();

  params.queryCtx->setConfigOverridesUnsafe({
      {core::QueryConfig::kThrustJITenabled, "false"},
  });

  params.planNode = plan;
  assertQuery(params, duckDbSql);
}

} // namespace bytedance::bolt::jit::test

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  // folly::init(&argc, &argv);

  return RUN_ALL_TESTS();
}

#endif // #ifdef ENABLE_BOLT_EXPR_JIT
