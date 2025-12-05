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
 */
/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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
#include "bolt/functions/Macros.h"
#include "bolt/functions/Registerer.h"
#include "bolt/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "bolt/functions/prestosql/registration/RegistrationFunctions.h"
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;

namespace {

template <typename T>
struct WidthBucketFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      double operand,
      double bound1,
      double bound2,
      int64_t bucketCount) {
    double lower = std::min(bound1, bound2);
    double upper = std::max(bound1, bound2);

    if (operand < lower) {
      result = 0;
    } else if (operand > upper) {
      result = bucketCount + 1;
    } else {
      result =
          (int64_t)((double)bucketCount * (operand - lower) / (upper - lower) + 1);
    }

    if (bound1 > bound2) {
      result = bucketCount - result + 1;
    }
    return true;
  }
};

class WidthBucketBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  WidthBucketBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerArithmeticFunctions();

    registerFunction<
        WidthBucketFunction,
        int64_t,
        double,
        double,
        double,
        int64_t>({"width_bucket_no_check"});
  }

  void run(const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    vector_size_t size = 1'000;
    auto operandVector = vectorMaker_.flatVector<double>(
        size, [](auto row) { return row + 3.14; });
    auto bound1Vector =
        vectorMaker_.flatVector<double>(size, [](auto row) { return row; });
    auto bound2Vector =
        vectorMaker_.flatVector<double>(size, [](auto row) { return row + 4; });
    auto bucketCountVector =
        BaseVector::createConstant(INTEGER(), 3, size, execCtx_.pool());

    auto rowVector = vectorMaker_.rowVector(
        {operandVector, bound1Vector, bound2Vector, bucketCountVector});
    auto exprSet = compileExpression(
        fmt::format("{}(c0, c1, c2, c3)", functionName), rowVector->type());
    suspender.dismiss();

    doRun(exprSet, rowVector);
  }

  void doRun(ExprSet& exprSet, const RowVectorPtr& rowVector) {
    int cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

BENCHMARK(widthBucketNoCheck) {
  WidthBucketBenchmark benchmark;
  benchmark.run("width_bucket_no_check");
}

BENCHMARK_RELATIVE(widthBucket) {
  WidthBucketBenchmark benchmark;
  benchmark.run("width_bucket");
}

} // namespace

int main(int argc, char** argv) {
  // todo: use folly::Init init after upgrade folly lib
  folly::init(&argc, &argv);

  folly::runBenchmarks();
  return 0;
}
