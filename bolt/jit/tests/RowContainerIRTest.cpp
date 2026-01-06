/*
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

#ifdef ENABLE_BOLT_JIT

#include <dwio/parquet/arrow/util/safe-math.h>
#include <type/Type.h>

#include <gtest/gtest.h>
#include "bolt/jit/RowContainer/RowContainerCodeGenerator.h"
#include "bolt/jit/ThrustJIT.h"
#include "bolt/jit/tests/JitTestBase.h"

#include <cmath>
#include <limits>
#include <tuple>
#include <type_traits>

using int128_t = __int128;

extern "C" {

// Just for JIT test
int StringViewCompareWrapper(char* l, char* r) {
  auto l_len = *(int32_t*)l;
  auto r_len = *(int32_t*)r;

  std::string_view left_sv;
  std::string_view right_sv;
  if (l_len <= 12) {
    left_sv = std::string_view((char*)(l + 4), l_len);
  } else {
    left_sv = std::string_view(*(char**)(l + 8), l_len);
  }
  if (r_len <= 12) {
    right_sv = std::string_view((char*)(r + 4), r_len);
  } else {
    right_sv = std::string_view(*(char**)(r + 8), r_len);
  }

  auto res = left_sv.compare(right_sv);
  return res;
}
}

namespace bytedance::bolt::jit::test {

template <typename T>
concept TupleLike = requires(T a) {
  std::tuple_size_v<T>;
  // std::tuple_element<0, T>::type;
};

struct RowDataBuilder {
  template <TupleLike T>
  RowContainerCodeGenerator MakeGeneratorForRow(
      const T& row,
      std::vector<CompareFlags> flags) {
    idx = 0;
    keyTypes.clear();
    offsets.clear();
    nullByteMasks.clear();
    nullByteOffsets.clear();
    value_len = 0;

    auto appendField = [this](auto&& x) {
      nullByteOffsets.emplace_back(idx / 8);

      if constexpr (std::is_same_v<std::decay_t<decltype(x)>, bool>) {
        keyTypes.emplace_back(bytedance::bolt::Type::create<
                              bytedance::bolt::TypeKind::BOOLEAN>());
      } else if constexpr (std::is_same_v<std::decay_t<decltype(x)>, int8_t>) {
        keyTypes.emplace_back(bytedance::bolt::Type::create<
                              bytedance::bolt::TypeKind::TINYINT>());
      } else if constexpr (std::is_same_v<std::decay_t<decltype(x)>, int16_t>) {
        keyTypes.emplace_back(bytedance::bolt::Type::create<
                              bytedance::bolt::TypeKind::SMALLINT>());
      } else if constexpr (std::is_same_v<std::decay_t<decltype(x)>, int32_t>) {
        keyTypes.emplace_back(bytedance::bolt::Type::create<
                              bytedance::bolt::TypeKind::INTEGER>());
      } else if constexpr (std::is_same_v<std::decay_t<decltype(x)>, int64_t>) {
        keyTypes.emplace_back(
            bytedance::bolt::Type::create<bytedance::bolt::TypeKind::BIGINT>());
      } else if constexpr (std::
                               is_same_v<std::decay_t<decltype(x)>, int128_t>) {
        keyTypes.emplace_back(bytedance::bolt::Type::create<
                              bytedance::bolt::TypeKind::HUGEINT>());
      } else if constexpr (std::is_same_v<std::decay_t<decltype(x)>, float>) {
        keyTypes.emplace_back(
            bytedance::bolt::Type::create<bytedance::bolt::TypeKind::REAL>());
      } else if constexpr (std::is_same_v<std::decay_t<decltype(x)>, double>) {
        keyTypes.emplace_back(
            bytedance::bolt::Type::create<bytedance::bolt::TypeKind::DOUBLE>());
      } else if constexpr (std::is_same_v<
                               std::decay_t<decltype(x)>,
                               StringView>) {
        keyTypes.emplace_back(bytedance::bolt::Type::create<
                              bytedance::bolt::TypeKind::VARCHAR>());
      } else if constexpr (std::is_same_v<
                               std::decay_t<decltype(x)>,
                               Timestamp>) {
        keyTypes.emplace_back(bytedance::bolt::Type::create<
                              bytedance::bolt::TypeKind::TIMESTAMP>());
      }

      offsets.push_back(value_len);
      value_len += sizeof(decltype(x));
      nullByteMasks.push_back(1 << (idx % 8));
      ++idx;
    };

    std::apply(
        [&appendField, this](auto&&... xs) { ((appendField(xs)), ...); }, row);

    for (auto i = 0; i < nullByteOffsets.size(); ++i) {
      nullByteOffsets[i] += value_len;
    }

    RowContainerCodeGenerator gen;
    {
      auto f = flags;
      auto off = offsets;
      auto nullOff = nullByteOffsets;
      auto nullMask = nullByteMasks;
      auto kt = keyTypes;
      gen.setCompareFlags(std::move(f))
          .setKeyOffsets(std::move(off))
          .setNullByteOffsets(std::move(nullOff))
          .setNullMasks(std::move(nullMask))
          .setKeyTypes(std::move(kt));
    }

    return gen;
  }

  template <TupleLike T>
  std::vector<char*> MakeRows(const std::vector<T>& rows) {
    std::vector<char*> result;
    for (auto r : rows) {
      auto sz = (value_len + ((std::tuple_size_v<T> + 7) / 8) + 63) &
          ~63; // 64 align buffer
      char* row = (char*)aligned_alloc(sizeof(int128_t), sz);
      memset(row, 0, sz);

      idx = 0;

      auto appendFieldValue = [&row, this](auto&& x) {
        using xt = std::decay_t<decltype(x)>;
        *(xt*)(row + offsets[idx]) = x;

        // TODO set nullbit
        ++idx;
      };

      std::apply(
          [&appendFieldValue, this](auto&&... xs) {
            ((appendFieldValue(xs)), ...);
          },
          r);
      result.emplace_back(row);
    }
    return result;
  }

  size_t idx = 0;

  std::vector<int32_t> nullByteOffsets;
  std::vector<bytedance::bolt::TypePtr> keyTypes;
  std::vector<int32_t> offsets;
  std::vector<int8_t> nullByteMasks;
  size_t value_len{0};
};

class RowContainerJitTest : public ::testing::Test {
 public:
  RowContainerJitTest() {
    jit = bytedance::bolt::jit::ThrustJIT::getInstance();
  };

 protected:
  bytedance::bolt::jit::ThrustJIT* jit{nullptr};
};

using namespace bytedance::bolt::jit;

TEST_F(RowContainerJitTest, all_types) {
  RowDataBuilder rb;
  using int128_t = __int128;
  using RowTuple = std::tuple<
      int128_t,
      bool,
      int8_t,
      int16_t,
      int32_t,
      int64_t,
      double,
      bytedance::bolt::jit::test::StringView,
      bytedance::bolt::jit::test::Timestamp>;

  std::vector<RowTuple> data;
  data.emplace_back(
      0, false, 0, 0, 0, 0, 0.0, "aaaa", Timestamp{.seconds = 0, .nanos = 0});
  data.emplace_back(
      0, false, 0, 0, 0, 0, 0.0, "aaaa", Timestamp{.seconds = 0, .nanos = 1});
  data.emplace_back(
      0, false, 0, 0, 0, 0, 0.0, "aaaa", Timestamp{.seconds = 1, .nanos = 1});
  data.emplace_back(
      0, false, 0, 0, 0, 0, 0.0, "bbbb", Timestamp{.seconds = 1, .nanos = 1});
  data.emplace_back(
      0, false, 0, 0, 0, 0, 1.0, "bbbb", Timestamp{.seconds = 1, .nanos = 1});
  data.emplace_back(
      0, false, 0, 0, 0, 1, 1.0, "bbbb", Timestamp{.seconds = 1, .nanos = 1});
  data.emplace_back(
      0, false, 0, 0, 1, 1, 1.0, "bbbb", Timestamp{.seconds = 1, .nanos = 1});
  data.emplace_back(
      0, false, 0, 1, 1, 1, 1.0, "bbbb", Timestamp{.seconds = 1, .nanos = 1});
  data.emplace_back(
      0, false, 1, 1, 1, 1, 1.0, "bbbb", Timestamp{.seconds = 1, .nanos = 1});
  data.emplace_back(
      0, true, 1, 1, 1, 1, 1.0, "bbbb", Timestamp{.seconds = 1, .nanos = 1});
  data.emplace_back(
      1, true, 1, 1, 1, 1, 1.0, "bbbb", Timestamp{.seconds = 1, .nanos = 1});

  for (bool nullFirst : {false, true}) {
    for (bool asc : {true, false}) {
      std::vector<bolt::jit::CompareFlags> flags{
          {.nullsFirst = nullFirst, .ascending = asc},
          {.nullsFirst = nullFirst, .ascending = asc},
          {.nullsFirst = nullFirst, .ascending = asc},
          {.nullsFirst = nullFirst, .ascending = asc},
          {.nullsFirst = nullFirst, .ascending = asc},
          {.nullsFirst = nullFirst, .ascending = asc},
          {.nullsFirst = nullFirst, .ascending = asc},
          {.nullsFirst = nullFirst, .ascending = asc},
          {.nullsFirst = nullFirst, .ascending = asc}};

      auto gen = rb.MakeGeneratorForRow(data[0], flags);
      auto rows = rb.MakeRows(data);

      auto fn = gen.GetCmpFuncName();
      auto modSP = gen.codegen();
      auto func = (bool (*)(char*, char*))(modSP->getFuncPtr(fn));
      ASSERT_TRUE(func != nullptr);

      for (size_t i = 0; i < rows.size() - 1; ++i) {
        auto cmp = func(rows[i], rows[i + 1]);
        ASSERT_TRUE(cmp == asc);
      }

      for (auto r : rows) {
        free(r);
      }
      rows.clear();
    }
  }
}

TEST_F(RowContainerJitTest, two_float_point) {
  RowDataBuilder rb;

  using RowTuple = std::tuple<float, double>;

  std::vector<RowTuple> data;
  data.emplace_back(0.0, 0.0);
  data.emplace_back(0.0, 1.0);
  data.emplace_back(1.0, 2.0);
  data.emplace_back(1.0, std::numeric_limits<double>::max());
  data.emplace_back(
      std::numeric_limits<float>::max(), std::numeric_limits<double>::max());
  data.emplace_back(
      std::numeric_limits<float>::quiet_NaN(),
      std::numeric_limits<double>::max());
  data.emplace_back(
      std::numeric_limits<float>::quiet_NaN(),
      std::numeric_limits<double>::quiet_NaN());
  data.emplace_back(
      std::numeric_limits<float>::quiet_NaN(),
      std::numeric_limits<double>::quiet_NaN());

  std::vector<bolt::jit::CompareFlags> flags{
      {.nullsFirst = false, .ascending = true},
      {.nullsFirst = false, .ascending = true}};

  auto gen = rb.MakeGeneratorForRow(data[0], flags);
  auto rows = rb.MakeRows(data);

  auto fn = gen.GetCmpFuncName();
  auto modSP = gen.codegen();
  auto func = (bool (*)(char*, char*))(modSP->getFuncPtr(fn));
  ASSERT_TRUE(func != nullptr);

  // NaN as the Max
  for (auto i = 0; i < data.size() - 2; ++i) {
    ASSERT_TRUE(func(rows[i], rows[i + 1]));
  }
  ASSERT_FALSE(func(rows[data.size() - 2], rows[data.size() - 1]));

  for (auto r : rows) {
    free(r);
  }
  rows.clear();
}

TEST_F(RowContainerJitTest, float_with_nulls) {
  RowDataBuilder rb;
  using RowTuple = std::tuple<double>;
  std::vector<RowTuple> data;
  data.emplace_back(0.0);
  data.emplace_back(1.0);

  for (auto asc : {true, false}) {
    for (auto null_first : {true, false}) {
      std::vector<bolt::jit::CompareFlags> flags{
          {.nullsFirst = null_first, .ascending = asc},
      };
      auto gen = rb.MakeGeneratorForRow(data[0], flags);
      auto rows = rb.MakeRows(data);

      auto fn = gen.GetCmpFuncName();
      auto modSP = gen.codegen();

      auto func = (bool (*)(char*, char*))(modSP->getFuncPtr(fn));

      // {0.0, 1.0}
      auto res = func(rows[0], rows[1]);
      ASSERT_TRUE(res == asc);

      // {null, 1.0}
      *(rows[0] + sizeof(double)) = (char)1;
      *(rows[1] + sizeof(double)) = (char)0;
      res = func(rows[0], rows[1]);
      ASSERT_TRUE(res == null_first);

      // {1.0, null}
      *(rows[0] + sizeof(double)) = (char)0;
      *(rows[1] + sizeof(double)) = (char)1;
      res = func(rows[0], rows[1]);
      ASSERT_TRUE(res != null_first);

      // {null, null}
      *(rows[0] + sizeof(double)) = (char)1;
      *(rows[1] + sizeof(double)) = (char)1;
      res = func(rows[0], rows[1]);
      ASSERT_FALSE(res);
      for (auto r : rows) {
        free(r);
      }
    }
  }
}

TEST_F(RowContainerJitTest, float_point_nan_test) {
  RowDataBuilder rb;
  using RowTuple = std::tuple<double>;
  std::vector<RowTuple> data;

  // 0.0 < 1.0 => true
  // 1.0 < max => true
  // max < NaN => true
  // NaN < NaN => false
  data.emplace_back(0.0);
  data.emplace_back(1.0);
  data.emplace_back(std::numeric_limits<double>::max());
  data.emplace_back(std::numeric_limits<double>::infinity());
  data.emplace_back(std::numeric_limits<float>::quiet_NaN());
  data.emplace_back(std::numeric_limits<float>::quiet_NaN());

  for (auto asc : {true, false}) {
    std::vector<bolt::jit::CompareFlags> flags{
        {.nullsFirst = true, .ascending = asc},
    };
    auto gen = rb.MakeGeneratorForRow(data[0], flags);
    auto rows = rb.MakeRows(data);

    auto fn = gen.GetCmpFuncName();
    auto modSP = gen.codegen();
    auto func = (bool (*)(char*, char*))(modSP->getFuncPtr(fn));
    ASSERT_TRUE(func != nullptr);

    for (auto i = 0; i < data.size() - 2; ++i) {
      auto l = i;
      auto r = i + 1;
      if (!asc) {
        l = i + 1;
        r = i;
      }
      ASSERT_TRUE(func(rows[l], rows[r]));
    }
    ASSERT_FALSE(func(rows[data.size() - 2], rows[data.size() - 1]));
    for (auto r : rows) {
      free(r);
    }
    rows.clear();
  }
}

TEST_F(RowContainerJitTest, timestamp) {
  RowDataBuilder rb;

  using RowTuple = std::tuple<Timestamp, int64_t, Timestamp>;

  std::vector<RowTuple> data;
  data.emplace_back(
      Timestamp{.seconds = 0, .nanos = 0},
      0,
      Timestamp{.seconds = 0, .nanos = 0});
  data.emplace_back(
      Timestamp{.seconds = 0, .nanos = 0},
      0,
      Timestamp{.seconds = 0, .nanos = 1});
  data.emplace_back(
      Timestamp{.seconds = 0, .nanos = 0},
      0,
      Timestamp{.seconds = 1, .nanos = 1});
  data.emplace_back(
      Timestamp{.seconds = 0, .nanos = 0},
      1,
      Timestamp{.seconds = 1, .nanos = 1});
  data.emplace_back(
      Timestamp{.seconds = 0, .nanos = 1},
      1,
      Timestamp{.seconds = 1, .nanos = 1});
  data.emplace_back(
      Timestamp{.seconds = 1, .nanos = 1},
      1,
      Timestamp{.seconds = 1, .nanos = 1});

  std::vector<bolt::jit::CompareFlags> flags{
      {.nullsFirst = false, .ascending = true},
      {.nullsFirst = false, .ascending = true},
      {.nullsFirst = false, .ascending = true}};

  auto gen = rb.MakeGeneratorForRow(data[0], flags);
  auto rows = rb.MakeRows(data);

  auto fn = gen.GetCmpFuncName();
  auto modSP = gen.codegen();
  auto func = (bool (*)(char*, char*))(modSP->getFuncPtr(fn));
  ASSERT_TRUE(func != nullptr);

  for (auto i = 0; i < rows.size() - 1; ++i) {
    ASSERT_TRUE(func(rows[i], rows[i + 1]));
  }

  for (auto r : rows) {
    free(r);
  }
  rows.clear();
}

TEST_F(RowContainerJitTest, singleKey) {
  RowDataBuilder rb;

  using RowTuple = std::tuple<int64_t>;

  std::vector<RowTuple> data;
  data.emplace_back(0);
  data.emplace_back(1);

  std::vector<bolt::jit::CompareFlags> flags{
      {.nullsFirst = false, .ascending = true}};

  auto gen = rb.MakeGeneratorForRow(data[0], flags);
  auto rows = rb.MakeRows(data);

  auto fn = gen.GetCmpFuncName();
  auto modSP = gen.codegen();
  auto func = (bool (*)(char*, char*))(modSP->getFuncPtr(fn));
  ASSERT_TRUE(func != nullptr);

  ASSERT_TRUE(func(rows[0], rows[1]));

  for (auto r : rows) {
    free(r);
  }
  rows.clear();
}

TEST_F(RowContainerJitTest, stringview) {
  RowDataBuilder rb;
  std::vector<std::string> strings{
      "aa", "bbbb", "bbbbbb", "bbbbbb", "bbbbbbb", "bbbbbbc"};

  using RowTuple = std::tuple<
      bolt::jit::test::StringView,
      bytedance::bolt::jit::test::StringView>;

  std::vector<RowTuple> data;
  for (auto&& s : strings) {
    data.emplace_back(s, s);
  }

  std::vector<bolt::jit::CompareFlags> flags{
      {.nullsFirst = false, .ascending = true},
      {.nullsFirst = false, .ascending = true}};

  auto gen = rb.MakeGeneratorForRow(data[0], flags);
  auto rows = rb.MakeRows(data);

  auto fn = gen.GetCmpFuncName();
  auto modSP = gen.codegen();
  auto func = (bool (*)(char*, char*))(modSP->getFuncPtr(fn));
  ASSERT_TRUE(func != nullptr);

  for (auto i = 0; i < rows.size() - 1; ++i) {
    auto jit_res = func(rows[i], rows[i + 1]);
    auto res = strings[i].compare(strings[i + 1]) < 0;
    ASSERT_TRUE(jit_res == res);
  }

  for (auto r : rows) {
    free(r);
  }
  rows.clear();
}

} // namespace bytedance::bolt::jit::test

#endif
