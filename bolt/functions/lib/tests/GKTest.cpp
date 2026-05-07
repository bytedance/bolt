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

#include <gtest/gtest.h>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/memory/Memory.h"

#include "bolt/type/Type.h"

#include "bolt/dwio/common/tests/utils/DataFiles.h"

#include "bolt/functions/lib/GreenwaldKhanna.h"
namespace bytedance::bolt::functions::gk::test {
namespace {

constexpr double kEpsilon = 1.0F / 10000;

class GKTest : public testing::TestWithParam<bool> {
 public:
  std::string getDataFilePath(const std::string& name) {
    return bolt::test::getDataFilePath(fmt::format("data/{}", name));
  }

  template <typename T>
  void
  insertRandomData(int seed, int n, GKQuantileSummaries<T>& gk, T* values) {
    std::default_random_engine gen(seed);
    if constexpr (std::is_integral_v<T> || std::is_same_v<T, Timestamp>) {
      if constexpr (std::is_same_v<T, Timestamp>) {
        std::uniform_int_distribution<uint64_t> dist(0, Timestamp::kMaxNanos);
        for (int i = 0; i < n; ++i) {
          int64_t v1 = dist(gen);
          int64_t v2 = dist(gen);
          auto v = Timestamp(v1, v2);
          gk.insert(v);
          if (values) {
            values[i] = v;
          }
        }
      } else {
        std::uniform_int_distribution<T> dist;
        for (int i = 0; i < n; ++i) {
          auto v = static_cast<T>(dist(gen));
          gk.insert(v);
          if (values) {
            values[i] = v;
          }
        }
      }
    } else if constexpr (std::is_floating_point_v<T>) {
      std::uniform_real_distribution<T> dist;
      for (int i = 0; i < n; ++i) {
        auto v = static_cast<T>(dist(gen));
        gk.insert(v);
        if (values) {
          values[i] = v;
        }
      }
    }
  }

  // Generate linearly spaced values between [0, 1].
  std::vector<double> linspace(int len) {
    BOLT_DCHECK_GE(len, 2);
    std::vector<double> out(len);
    double step = 1.0 / (len - 1);
    for (int i = 0; i < len; ++i) {
      out[i] = i * step;
    }
    return out;
  }
  template <typename T>
  T getPercentile(GKQuantileSummaries<T>& gk, double level) {
    if (!gk.isCompressed()) {
      gk.compress();
    }
    return gk.query(level);
  }

  template <typename T>
  std::vector<T> getPercentiles(
      GKQuantileSummaries<T>& gk,
      const std::vector<double>& levels) {
    if (!gk.isCompressed()) {
      gk.compress();
    }
    return gk.query(levels);
  }

  template <typename T>
  void testSerde() {
    constexpr int N = 1e5;
    constexpr int M = 1001;
    GKQuantileSummaries<T> gk(
        pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
    insertRandomData<T>(0, N, gk, nullptr);
    gk.compress();
    std::unique_ptr<char[]> buf(new char[gk.serializedByteSize()]);
    gk.serialize(buf.get());
    GKQuantileSummaries<T> gk2(
        pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
    gk2.deserialize(buf.get(), gk.serializedByteSize());

    auto q = linspace(M);
    auto v = getPercentiles(gk, q);
    auto v2 = getPercentiles(gk2, q);
    EXPECT_EQ(v, v2);
    EXPECT_TRUE(gk.equals(gk2));
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    // This test throws a lot of exceptions, so turn off stack trace
    // capturing.
    FLAGS_bolt_exception_user_stacktrace_enabled = false;
    memory::MemoryManager::Options options;
    options.alignment = 32;
    manager_ = std::make_shared<memory::MemoryManager>(options);
    pool_ = manager_->addLeafPool("gktest", 100000000);
  }
  std::shared_ptr<memory::MemoryManager> manager_;
  std::shared_ptr<memory::MemoryPool> pool_;
};
TEST_F(GKTest, oneItem) {
  GKQuantileSummaries<double> gk(
      pool_.get(),
      kDefaultRelativeError,
      kDefaultCompressThreshold,
      kDefaultHeadSize);
  gk.insert(1.0);
  gk.compress();
  EXPECT_EQ(getPercentile(gk, 0.0), 1);
  EXPECT_EQ(getPercentile(gk, 0.5), 1);
  EXPECT_EQ(getPercentile(gk, 1.0), 1);
}

TEST_F(GKTest, MultiItems) {
  GKQuantileSummaries<double> gk(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  for (int i = 1; i <= 1000; ++i) {
    gk.insert(i);
  }
  gk.compress();
  EXPECT_EQ(getPercentile(gk, 0.25), 250);
  EXPECT_EQ(getPercentile(gk, 0.5), 500);
  EXPECT_EQ(getPercentile(gk, 0.75), 750);
  EXPECT_EQ(getPercentile(gk, 0.0), 1);
  EXPECT_EQ(getPercentile(gk, 1.0), 1000);
  EXPECT_EQ(getPercentile(gk, 0), 1);
  EXPECT_EQ(getPercentile(gk, 1), 1000);
  std::vector<double> result =
      getPercentiles(gk, {0.25, 0.5, 0.75, 0.0, 1.0, 0, 1});
  EXPECT_EQ(getPercentile(gk, 0.25), result[0]);
  EXPECT_EQ(getPercentile(gk, 0.5), result[1]);
  EXPECT_EQ(getPercentile(gk, 0.75), result[2]);
  EXPECT_EQ(getPercentile(gk, 0.0), result[3]);
  EXPECT_EQ(getPercentile(gk, 1.0), result[4]);
  EXPECT_EQ(getPercentile(gk, 0), result[5]);
  EXPECT_EQ(getPercentile(gk, 1), result[6]);
}

TEST_F(GKTest, MultiItemsSmallPercentages) {
  GKQuantileSummaries<double> gk(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  for (int i = 1; i <= 10; ++i) {
    gk.insert(i);
  }
  gk.compress();
  EXPECT_EQ(getPercentile(gk, 0.01), 1);
  EXPECT_EQ(getPercentile(gk, 0.1), 1);
  EXPECT_EQ(getPercentile(gk, 0.11), 2);
}

TEST_F(GKTest, MultiItemsRepeat) {
  GKQuantileSummaries<double> gk(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  std::vector<int> numbers = {1, 1, 2, 1, 1, 3, 1, 1, 4, 1, 1, 5};
  for (int i = 0; i < numbers.size(); ++i) {
    gk.insert(numbers[i]);
  }
  gk.compress();
  EXPECT_EQ(getPercentile(gk, 0.5), 1);
}

TEST_F(GKTest, DifferentAccuracies) {
  std::vector<int> accuracies = {1, 10, 100, 1000, 10000};
  std::vector<int> errors;
  for (auto acc : accuracies) {
    GKQuantileSummaries<double> gk(
        pool_.get(), 1.0F / acc, kDefaultCompressThreshold, kDefaultHeadSize);

    for (int i = 1; i <= 1000; ++i) {
      gk.insert(i);
    }
    gk.compress();
    errors.push_back(std::abs(getPercentile(gk, 0.25) - 250));
  }
  std::reverse(errors.begin(), errors.end());
  EXPECT_TRUE(std::is_sorted(errors.begin(), errors.end()));
}

TEST_F(GKTest, randomInput) {
  constexpr int N = 1e5;
  constexpr int M = 1001;

  GKQuantileSummaries<double> gk(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  double values[N];
  insertRandomData<double>(0, N, gk, values);
  gk.compress();
  std::sort(std::begin(values), std::end(values));
  auto q = linspace(M);
  std::vector<double> v = getPercentiles(gk, q);
  ASSERT_TRUE(std::is_sorted(std::begin(v), std::end(v)));
  for (int i = 0; i < M; ++i) {
    auto it = std::lower_bound(std::begin(values), std::end(values), v[i]);
    double actualQ = 1.0 * (it - std::begin(values)) / N;
    EXPECT_NEAR(q[i], actualQ, kEpsilon * N);
  }
}

TEST_F(GKTest, doubleNaNSingleSummaryFollowsSparkHeadSort) {
  const auto nan = std::numeric_limits<double>::quiet_NaN();

  GKQuantileSummaries<double> gk(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  gk.insert(1.0);
  gk.insert(2.0);
  gk.insert(nan);
  gk.compress();

  const auto result = getPercentiles(gk, {0.0, 0.5, 0.9999, 1.0});
  ASSERT_EQ(result.size(), 4);
  EXPECT_EQ(result[0], 1.0);
  EXPECT_EQ(result[1], 2.0);
  EXPECT_TRUE(std::isnan(result[2]));
  EXPECT_TRUE(std::isnan(result[3]));
}

TEST_F(GKTest, doubleSignedZeroSingleSummaryFollowsSparkHeadSort) {
  GKQuantileSummaries<double> gk(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  gk.insert(0.0);
  gk.insert(-0.0);
  gk.compress();

  const auto result = getPercentiles(gk, {0.0, 1.0});
  ASSERT_EQ(result.size(), 2);
  EXPECT_TRUE(std::signbit(result[0]));
  EXPECT_FALSE(std::signbit(result[1]));
}

TEST_F(GKTest, doubleNaNPartialMergeMatchesSparkCompress) {
  const auto nan = std::numeric_limits<double>::quiet_NaN();

  GKQuantileSummaries<double> gk(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  gk.insert(1.0);
  gk.insert(2.0);
  gk.compress();

  GKQuantileSummaries<double> other(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  other.insert(nan);
  other.compress();
  gk.merge(other);

  const auto result = getPercentiles(gk, {0.5, 0.9, 0.99, 0.9999, 1.0});
  ASSERT_EQ(result.size(), 5);
  for (const auto value : result) {
    EXPECT_EQ(value, 2.0);
  }
}

TEST_F(GKTest, doublePercentilesWithNaNAreNonDecreasing) {
  const auto nan = std::numeric_limits<double>::quiet_NaN();
  const std::vector<double> firstPartial{
      8814.79, 7960.1,  8492.04, 6837.15, 8429.88, 6034.46, 4464.18, 6628.85,
      7465.74, 8335.13, 7883.56, 6673.07, 7160.19, 7303.34, 6371.66, 7277.8,
      4463.77, 8233.78, 8605.39, 8370.71, 6054.61, 4520.48, 5278.29, 6743.67,
      8697.81, 8828.3,  6975.8,  5624.5,  5544.61, 6099.35, 6562.43, 6507.85,
      4379.5,  7273.95, 6862.91, 6767.65, 6664.36, 0.0,     5624.5,  7143.87,
      6277.58, 5624.5,  7123.34, 6662.73, 7083.62, 4863.93, 0.0,     6831.25,
      6827.66, 6179.52, 6111.47, 6430.53, 644.48,  7262.85, 6425.22, 6804.15,
      0.0};
  const std::vector<double> secondPartial{nan, nan, nan, 0.0};

  GKQuantileSummaries<double> gk(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  for (auto value : firstPartial) {
    gk.insert(value);
  }
  gk.compress();

  GKQuantileSummaries<double> other(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  for (auto value : secondPartial) {
    other.insert(value);
  }
  other.compress();
  gk.merge(other);

  const auto result = getPercentiles(
      gk,
      {0.10,
       0.20,
       0.30,
       0.40,
       0.50,
       0.60,
       0.70,
       0.80,
       0.90,
       0.91,
       0.92,
       0.93,
       0.94,
       0.95,
       0.96,
       0.97,
       0.98,
       0.99});

  bool seenNaN = false;
  std::optional<double> previous;
  for (auto i = 0; i < result.size(); ++i) {
    if (std::isnan(result[i])) {
      seenNaN = true;
      continue;
    }
    ASSERT_FALSE(seenNaN) << "finite value after NaN at index: " << i;
    if (previous.has_value()) {
      ASSERT_LE(previous.value(), result[i]) << "index: " << i;
    }
    previous = result[i];
  }
}

TEST_F(GKTest, serializeAndDeserialize) {
  testSerde<double>();
  testSerde<float>();
  testSerde<int128_t>();
  testSerde<int64_t>();
  testSerde<int32_t>();
  testSerde<int16_t>();
  testSerde<int8_t>();
  testSerde<Timestamp>();
}

TEST_F(GKTest, deserializeClearsExistingState) {
  GKQuantileSummaries<double> source(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  std::unique_ptr<char[]> buffer(new char[source.serializedByteSize()]);
  source.serialize(buffer.get());

  GKQuantileSummaries<double> target(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  target.insert(1.0);
  target.insert(2.0);
  target.compress();

  target.deserialize(buffer.get(), source.serializedByteSize());
  EXPECT_TRUE(target.empty());
  EXPECT_EQ(target.count(), 0);
}

TEST_F(GKTest, deserializeRejectsInvalidSize) {
  GKQuantileSummaries<double> source(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  source.insert(1.0);
  source.insert(2.0);
  source.compress();
  const auto serializedSize = source.serializedByteSize();
  std::unique_ptr<char[]> buffer(new char[serializedSize]);
  source.serialize(buffer.get());

  GKQuantileSummaries<double> target(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  BOLT_ASSERT_THROW(
      target.deserialize(buffer.get(), serializedSize - 1), "Invalid size");
  BOLT_ASSERT_THROW(
      target.deserialize(buffer.get(), serializedSize + 1), "Invalid size");
}

TEST_F(GKTest, resetClearsState) {
  GKQuantileSummaries<double> gk(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  gk.insert(1.0);
  gk.insert(2.0);
  gk.compress();

  gk.reset();
  EXPECT_TRUE(gk.empty());
  EXPECT_EQ(gk.count(), 0);
  EXPECT_TRUE(gk.isCompressed());

  gk.insert(3.0);
  gk.compress();
  EXPECT_EQ(gk.query(0.5), 3.0);
}

TEST_F(GKTest, queryEmptyPercentiles) {
  GKQuantileSummaries<double> gk(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  gk.insert(1.0);
  gk.compress();

  EXPECT_TRUE(gk.query(std::vector<double>{}).empty());
}

TEST_F(GKTest, merge) {
  constexpr int N = 1e4;
  constexpr int M = 1001;
  GKQuantileSummaries<double> gk1(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  GKQuantileSummaries<double> gk2(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  for (int i = 0; i < N; ++i) {
    gk1.insert(i);
    gk2.insert(2 * N - i - 1);
  }
  gk1.compress();
  gk2.compress();
  gk1.merge(gk2);
  auto q = linspace(M);

  std::vector<double> v = getPercentiles(gk1, q);
  ASSERT_TRUE(std::is_sorted(std::begin(v), std::end(v)));
  for (int i = 0; i < M; ++i) {
    EXPECT_NEAR(q[i], v[i] / (2 * N), kEpsilon * N);
  }
}

TEST_F(GKTest, mergeDeserialized) {
  constexpr int N = 1e4;
  constexpr int M = 1001;
  GKQuantileSummaries<double> gk1(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  GKQuantileSummaries<double> gk2(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  for (int i = 0; i < N; ++i) {
    gk1.insert(i);
    gk2.insert(2 * N - i - 1);
  }
  gk1.compress();
  gk2.compress();
  auto q = linspace(M);
  std::vector<double> v1 = getPercentiles(gk1, q);

  std::vector<char> data(gk2.serializedByteSize());
  gk2.serialize(data.data());
  auto tmp = GKQuantileSummaries<double>(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  tmp.deserialize(data.data(), gk2.serializedByteSize());
  gk1.merge(tmp);

  std::vector<double> v = getPercentiles(gk1, q);
  EXPECT_TRUE(std::is_sorted(std::begin(v), std::end(v)));
  for (int i = 0; i < M; ++i) {
    EXPECT_NEAR(q[i], v[i] / (2 * N), kEpsilon * N);
  }
}

TEST_F(GKTest, fromRepeatedValue) {
  constexpr int N = 1000;
  constexpr int kTotal = (1 + N) * N / 2;
  constexpr int M = 1001;
  std::vector<GKQuantileSummaries<int>> gks;
  for (int n = 0; n <= N; ++n) {
    auto gk = GKQuantileSummaries<int>::fromRepeatedValue(
        n,
        n,
        pool_.get(),
        kEpsilon,
        kDefaultCompressThreshold,
        kDefaultHeadSize);
    if (n > 0) {
      std::vector<double> q = {0, 0.25, 0.5, 0.75, 1};
      std::vector<int> v = getPercentiles(gk, q);
      for (int x : v) {
        EXPECT_EQ(x, n);
      }
    }
    gks.push_back(std::move(gk));
  }
  GKQuantileSummaries<int> gk(
      pool_.get(), kEpsilon, kDefaultCompressThreshold, kDefaultHeadSize);
  for (auto& g : gks) {
    gk.merge(g);
  }
  auto q = linspace(M);
  std::vector<int> v = getPercentiles(gk, q);
  for (int i = 0; i < M; ++i) {
    double realQ = 0.5 * v[i] * (v[i] - 1) / kTotal;
    EXPECT_NEAR(q[i], realQ, kEpsilon * kTotal);
  }
}

// a test use a huge dataset to ensure the result is same with spark
TEST_F(GKTest, normal) {
  auto filePath = getDataFilePath("gk-data");
  CHECK(std::filesystem::exists(filePath)) << "file not found: " << filePath;
  std::ifstream inFile(filePath, std::ios::binary);
  int32_t length;
  inFile.read(reinterpret_cast<char*>(&length), sizeof(length));
  EXPECT_EQ(length, 10852024);
  std::unique_ptr<float[]> buf(new float[length]);
  inFile.read(reinterpret_cast<char*>(buf.get()), length * sizeof(float));
  std::vector<double> percentiles = {
      0.0,  0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1,  0.11,
      0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.2,  0.21, 0.22, 0.23,
      0.24, 0.25, 0.26, 0.27, 0.28, 0.29, 0.3,  0.31, 0.32, 0.33, 0.34, 0.35,
      0.36, 0.37, 0.38, 0.39, 0.4,  0.41, 0.42, 0.43, 0.44, 0.45, 0.46, 0.47,
      0.48, 0.49, 0.5,  0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59,
      0.6,  0.61, 0.62, 0.63, 0.64, 0.65, 0.66, 0.67, 0.68, 0.69, 0.7,  0.71,
      0.72, 0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.79, 0.8,  0.81, 0.82, 0.83,
      0.84, 0.85, 0.86, 0.87, 0.88, 0.89, 0.9,  0.91, 0.92, 0.93, 0.94, 0.95,
      0.96, 0.97, 0.98, 0.99, 1.0};
  std::vector<double> expectedDecile = {
      0.008767819963395596, 0.008767819963395596, 0.008767819963395596,
      0.008767819963395596, 0.008767819963395596, 0.008767819963395596,
      0.008767819963395596, 0.008767819963395596, 0.008767819963395596,
      0.008767819963395596, 0.008767819963395596, 0.10680899769067764,
      0.10680899769067764,  0.10680899769067764,  0.10680899769067764,
      0.10680899769067764,  0.10680899769067764,  0.10680899769067764,
      0.10680899769067764,  0.10680899769067764,  0.10680899769067764,
      0.10680899769067764,  0.10680899769067764,  0.10680899769067764,
      0.10680899769067764,  0.10680899769067764,  0.10680899769067764,
      0.10680899769067764,  0.15591099858283997,  0.15591099858283997,
      0.15591099858283997,  0.15591099858283997,  0.15591099858283997,
      0.15591099858283997,  0.15591099858283997,  0.15591099858283997,
      0.15591099858283997,  0.15591099858283997,  0.15591099858283997,
      0.15591099858283997,  0.15591099858283997,  0.15591099858283997,
      0.21520599722862244,  0.21520599722862244,  0.21520599722862244,
      0.21520599722862244,  0.21520599722862244,  0.21520599722862244,
      0.21520599722862244,  0.21520599722862244,  0.21520599722862244,
      0.21520599722862244,  0.21520599722862244,  0.2942110002040863,
      0.2942110002040863,   0.2942110002040863,   0.2942110002040863,
      0.2942110002040863,   0.2942110002040863,   0.2942110002040863,
      0.2942110002040863,   0.2942110002040863,   0.2942110002040863,
      0.4325909912586212,   0.4325909912586212,   0.4325909912586212,
      0.4325909912586212,   0.4325909912586212,   0.4325909912586212,
      0.4325909912586212,   0.4325909912586212,   0.4325909912586212,
      0.4325909912586212,   0.4325909912586212,   0.4325909912586212,
      0.4325909912586212,   0.4325909912586212,   0.5854589939117432,
      0.5854589939117432,   0.5854589939117432,   0.5854589939117432,
      0.5854589939117432,   0.5854589939117432,   0.5854589939117432,
      0.5854589939117432,   0.5854589939117432,   0.5854589939117432,
      0.5854589939117432,   0.5854589939117432,   0.5854589939117432,
      0.9976000189781189,   0.9976000189781189,   0.9976000189781189,
      0.9976000189781189,   0.9976000189781189,   0.9976000189781189,
      0.9976000189781189,   0.9976000189781189,   0.9976000189781189,
      0.9976000189781189,   0.9976000189781189};
  std::vector<double> expectedPercent = {
      0.008767819963395596, 0.008767819963395596, 0.03919050097465515,
      0.04650060087442398,  0.054197199642658234, 0.054197199642658234,
      0.06548760086297989,  0.06548760086297989,  0.07444079965353012,
      0.07444079965353012,  0.07765810191631317,  0.08547569811344147,
      0.08659099787473679,  0.09312579780817032,  0.09312579780817032,
      0.09612789750099182,  0.10143599659204483,  0.10143599659204483,
      0.10938099771738052,  0.10938099771738052,  0.11393199861049652,
      0.11393199861049652,  0.11748000234365463,  0.1237649992108345,
      0.1237649992108345,   0.12849299609661102,  0.12849299609661102,
      0.13380199670791626,  0.14050599932670593,  0.14050599932670593,
      0.1483979970216751,   0.1483979970216751,   0.15651799738407135,
      0.16325800120830536,  0.16325800120830536,  0.17049899697303772,
      0.17888200283050537,  0.17888200283050537,  0.1842849999666214,
      0.19016599655151367,  0.19697299599647522,  0.19697299599647522,
      0.2050590068101883,   0.2139430046081543,   0.22302499413490295,
      0.23180900514125824,  0.24161100387573242,  0.24161100387573242,
      0.2508639991283417,   0.2587909996509552,   0.2672550082206726,
      0.2753719985485077,   0.2912749946117401,   0.2912749946117401,
      0.3074609935283661,   0.3074609935283661,   0.3231079876422882,
      0.3386259973049164,   0.3386259973049164,   0.35495200753211975,
      0.35495200753211975,  0.36942198872566223,  0.385019987821579,
      0.385019987821579,    0.39973101019859314,  0.39973101019859314,
      0.4140529930591583,   0.427047997713089,    0.44044700264930725,
      0.44044700264930725,  0.4590179920196533,   0.47113800048828125,
      0.47113800048828125,  0.4827660024166107,   0.49394699931144714,
      0.505765974521637,    0.5180040001869202,   0.5399839878082275,
      0.5399839878082275,   0.5546029806137085,   0.5688149929046631,
      0.5829330086708069,   0.5975800156593323,   0.5975800156593323,
      0.6243690252304077,   0.6243690252304077,   0.6417340040206909,
      0.6592990159988403,   0.675337016582489,    0.7071869969367981,
      0.7071869969367981,   0.7241849899291992,   0.7606019973754883,
      0.7606019973754883,   0.8002099990844727,   0.8002099990844727,
      0.8247630000114441,   0.8766149878501892,   0.8766149878501892,
      0.9976000189781189,   0.9976000189781189};
  std::vector<double> expectedPermille = {
      0.008767819963395596, 0.03762609884142876,  0.04245520010590553,
      0.046553898602724075, 0.052361298352479935, 0.0577378012239933,
      0.06353580206632614,  0.06750209629535675,  0.07271970063447952,
      0.07700250297784805,  0.08066020160913467,  0.08547569811344147,
      0.08890269696712494,  0.09217250347137451,  0.09564200043678284,
      0.09766600281000137,  0.10087200254201889,  0.10425099730491638,
      0.10735700279474258,  0.11091499775648117,  0.11347299814224243,
      0.1158749982714653,   0.11903200298547745,  0.1226079985499382,
      0.12456600368022919,  0.12793900072574615,  0.12984700500965118,
      0.13356100022792816,  0.1379919946193695,   0.14222000539302826,
      0.14727100729942322,  0.15184800326824188,  0.156918004155159,
      0.1614769995212555,   0.16613399982452393,  0.17205600440502167,
      0.17624300718307495,  0.17987799644470215,  0.18532299995422363,
      0.19077199697494507,  0.19587400555610657,  0.20258000493049622,
      0.21027199923992157,  0.21750999987125397,  0.2246129959821701,
      0.23264600336551666,  0.24046899378299713,  0.24921700358390808,
      0.2568120062351227,   0.2645060122013092,   0.27225399017333984,
      0.28073400259017944,  0.2893050014972687,   0.29828399419784546,
      0.3066520094871521,   0.3150149881839752,   0.3239159882068634,
      0.3331269919872284,   0.3417310118675232,   0.35053399205207825,
      0.3596619963645935,   0.3699379861354828,   0.3797900080680847,
      0.3884600102901459,   0.398811012506485,    0.40828999876976013,
      0.4186890125274658,   0.4282270073890686,   0.4383910000324249,
      0.44861799478530884,  0.45913198590278625,  0.46898001432418823,
      0.4795669913291931,   0.4908890128135681,   0.5020949840545654,
      0.5146539807319641,   0.5251780152320862,   0.5355640053749084,
      0.5477719902992249,   0.5593370199203491,   0.5712190270423889,
      0.5837739706039429,   0.5966209769248962,   0.6091799736022949,
      0.6230369806289673,   0.6364700198173523,   0.651764988899231,
      0.6682270169258118,   0.6816549897193909,   0.6981620192527771,
      0.7150869965553284,   0.7317479848861694,   0.7517729997634888,
      0.7713689804077148,   0.7931650280952454,   0.8148429989814758,
      0.8417270183563232,   0.8696349859237671,   0.9047200083732605,
      0.9523159861564636,   0.9976000189781189};
  std::vector<double> expectedPermyriad = {
      0.008767819963395596, 0.03691849857568741, 0.04195829853415489,
      0.0470414012670517,   0.05238720029592514, 0.0577378012239933,
      0.06365779787302017,  0.06788890063762665, 0.07302729785442352,
      0.07712270319461823,  0.08095099776983261, 0.08547569811344147,
      0.08890269696712494,  0.09235189855098724, 0.09566400200128555,
      0.0979309007525444,   0.10120300203561783, 0.10449299961328506,
      0.10766299813985825,  0.1112150028347969,  0.11369699984788895,
      0.1158749982714653,   0.11894399672746658, 0.12265399843454361,
      0.12457600235939026,  0.12793900072574615, 0.1299699991941452,
      0.13372300565242767,  0.13821400701999664, 0.14262700080871582,
      0.14753299951553345,  0.1519940048456192,  0.1570120006799698,
      0.16187900304794312,  0.16653099656105042, 0.17184099555015564,
      0.17641399800777435,  0.18018600344657898, 0.18522900342941284,
      0.19057799875736237,  0.19618000090122223, 0.20297099649906158,
      0.21011599898338318,  0.21770599484443665, 0.22539299726486206,
      0.23305700719356537,  0.2409439980983734,  0.2489989995956421,
      0.25675898790359497,  0.2646639943122864,  0.2725290060043335,
      0.28078100085258484,  0.2893050014972687,  0.2978079915046692,
      0.3066520094871521,   0.3153490126132965,  0.3241559863090515,
      0.33283498883247375,  0.341964989900589,   0.35104501247406006,
      0.3600740134716034,   0.36945998668670654, 0.3791239857673645,
      0.3888300061225891,   0.3986530005931854,  0.4085710048675537,
      0.41858500242233276,  0.4285709857940674,  0.4386090040206909,
      0.4487529993057251,   0.4590980112552643,  0.4696420133113861,
      0.4804939925670624,   0.4913420081138611,  0.502465009689331,
      0.513725996017456,    0.5249860286712646,  0.5364440083503723,
      0.5481969714164734,   0.5600060224533081,  0.572022020816803,
      0.5840359926223755,   0.5965080261230469,  0.6093530058860779,
      0.6230639815330505,   0.6370409727096558,  0.6516339778900146,
      0.6670269966125488,   0.6827020049095154,  0.6989390254020691,
      0.7158650159835815,   0.7328259944915771,  0.7511510252952576,
      0.770654022693634,    0.7919960021972656,  0.8155080080032349,
      0.8421859741210938,   0.8723049759864807,  0.9085760116577148,
      0.9534059762954712,   0.9976000189781189};
  std::vector<double> expectedMin = {
      0.008767819963395596, 0.03703559935092926, 0.04200449958443642,
      0.0470414012670517,   0.052565798163414,   0.0577378012239933,
      0.06376879662275314,  0.06793840229511261, 0.07299529761075974,
      0.07711149752140045,  0.08095099776983261, 0.08547569811344147,
      0.08890269696712494,  0.09242279827594757, 0.09566400200128555,
      0.09798219799995422,  0.10118500143289566, 0.10449299961328506,
      0.10770100355148315,  0.1112150028347969,  0.11365299671888351,
      0.1158749982714653,   0.11889699846506119, 0.12267100065946579,
      0.12457499653100967,  0.12793900072574615, 0.12995600700378418,
      0.133774995803833,    0.13824200630187988, 0.14265799522399902,
      0.14754900336265564,  0.15202400088310242, 0.1570120006799698,
      0.16187900304794312,  0.1664939969778061,  0.1718740016222,
      0.1764249950647354,   0.180185005068779,   0.1851940006017685,
      0.19062399864196777,  0.19618800282478333, 0.20294800400733948,
      0.2101379930973053,   0.2176630049943924,  0.22535300254821777,
      0.23311899602413177,  0.2409989982843399,  0.2490050047636032,
      0.2567870020866394,   0.26461300253868103, 0.2725619971752167,
      0.2808240056037903,   0.28930601477622986, 0.2978689968585968,
      0.30662399530410767,  0.3153989911079407,  0.3241310119628906,
      0.3329240083694458,   0.34192800521850586, 0.351065993309021,
      0.3601189851760864,   0.36953499913215637, 0.37912899255752563,
      0.3888719975948334,   0.3986879885196686,  0.40855398774147034,
      0.4186759889125824,   0.42855700850486755, 0.4386099874973297,
      0.448745995759964,    0.45913898944854736, 0.4697139859199524,
      0.4804140031337738,   0.4913569986820221,  0.5024459958076477,
      0.513700008392334,    0.5250880122184753,  0.5365549921989441,
      0.5482339859008789,   0.5599470138549805,  0.5719509720802307,
      0.5840590000152588,   0.5965250134468079,  0.609466016292572,
      0.6230790019035339,   0.6371380090713501,  0.6517689824104309,
      0.6670420169830322,   0.6827870011329651,  0.6990770101547241,
      0.7157599925994873,   0.7330009937286377,  0.7510910034179688,
      0.7706050276756287,   0.7920269966125488,  0.8156099915504456,
      0.8421949744224548,   0.8725550174713135,  0.9086949825286865,
      0.953777015209198,    0.9976000189781189};

  {
    GKQuantileSummaries<double> gkDecile(pool_.get(), 0.1);
    for (int i = 0; i < length; ++i) {
      gkDecile.insert(buf[i]);
    }
    gkDecile.compress();
    // std::cout << gkDecile.toString() << std::endl;
    for (size_t i = 0; i < percentiles.size(); ++i) {
      EXPECT_NEAR(gkDecile.query(percentiles[i]), expectedDecile[i], 0.0001);
    }
    EXPECT_EQ(gkDecile.sampleSize(), 8);
  }
  {
    GKQuantileSummaries<double> gkPercent(pool_.get(), 0.01);
    for (int i = 0; i < length; ++i) {
      gkPercent.insert(buf[i]);
    }
    gkPercent.compress();
    for (size_t i = 0; i < percentiles.size(); ++i) {
      EXPECT_NEAR(gkPercent.query(percentiles[i]), expectedPercent[i], 0.0001);
    }
    EXPECT_EQ(gkPercent.sampleSize(), 74);
  }
  {
    GKQuantileSummaries<double> gkPermille(pool_.get(), 0.001);
    for (int i = 0; i < length; ++i) {
      gkPermille.insert(buf[i]);
    }
    gkPermille.compress();
    for (size_t i = 0; i < percentiles.size(); ++i) {
      EXPECT_NEAR(
          gkPermille.query(percentiles[i]), expectedPermille[i], 0.0001);
    }
    EXPECT_EQ(gkPermille.sampleSize(), 792);
  }
  {
    GKQuantileSummaries<double> gkPermyriad(pool_.get(), 0.0001);
    for (int i = 0; i < length; ++i) {
      gkPermyriad.insert(buf[i]);
    }
    gkPermyriad.compress();
    for (size_t i = 0; i < percentiles.size(); ++i) {
      EXPECT_NEAR(
          gkPermyriad.query(percentiles[i]), expectedPermyriad[i], 0.00001);
    }
    EXPECT_EQ(gkPermyriad.sampleSize(), 10403);
  }
  {
    GKQuantileSummaries<double> gkMin(pool_.get(), 0.0000001);
    for (int i = 0; i < length; ++i) {
      gkMin.insert(buf[i]);
    }
    gkMin.compress();
    for (size_t i = 0; i < percentiles.size(); ++i) {
      EXPECT_NEAR(gkMin.query(percentiles[i]), expectedMin[i], 0.0001);
    }
    EXPECT_EQ(gkMin.sampleSize(), 8120294);
  }
}
} // namespace
} // namespace bytedance::bolt::functions::gk::test
