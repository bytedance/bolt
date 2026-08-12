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

#pragma once

#include <folly/Benchmark.h>
#include <iostream>

#include "bolt/cudf/tests/CudfResource.h"
#include "bolt/exec/PlanNodeStats.h"
#include "bolt/exec/tests/utils/AssertQueryBuilder.h"
#include "bolt/exec/tests/utils/PlanBuilder.h"

DEFINE_int32(bolt_benchmark_drivers, 1, "The number of drivers to run a query");

namespace {
constexpr const double nsToMs(const uint64_t ns) {
  return ns * 1e-6;
}
} // namespace

namespace bolt::cudf::benchmark {

template <bool useGPU>
class BenchmarkBase {
 public:
  explicit BenchmarkBase(
      std::shared_ptr<std::vector<bytedance::bolt::RowVectorPtr>> input)
      : input_(input) {
    if constexpr (useGPU) {
      bolt::cudf::test::CudfResource::getInstance().initialize();
    }
  }

  virtual ~BenchmarkBase() {
    if constexpr (useGPU) {
      bolt::cudf::test::CudfResource::getInstance().finalize();
    }
  }

  inline virtual const std::string name() const {
    if constexpr (useGPU) {
      return "GPU";
    }
    return "CPU";
  }

  virtual void addBenchmark() {
    folly::addBenchmark(__FILE__, this->name(), [this]() {
      std::shared_ptr<bytedance::bolt::exec::Task> task;
      bytedance::bolt::exec::test::AssertQueryBuilder(this->plan_)
          .maxDrivers(FLAGS_bolt_benchmark_drivers)
          .runWithoutResults(task);

      folly::BenchmarkSuspender benchSuspender;
      BOLT_CHECK(task, "Something wrong if task is null.");
      this->tasks_.push_back(task);
      benchSuspender.dismiss();

      return 1;
    });
  }

  virtual void reportStats() {
    std::cout << "Number of tasks: " << tasks_.size() << std::endl;
    std::cout << __func__ << "(" << name() << ")" << std::endl;
    for (const auto& task : tasks_) {
      std::cout << printPlanWithStats(*plan_, task->taskStats(), true)
                << std::endl;
      break; // print only one
    }
  }

 protected:
  std::shared_ptr<const bytedance::bolt::core::PlanNode> plan_;
  std::vector<std::shared_ptr<bytedance::bolt::exec::Task>> tasks_;
  std::shared_ptr<std::vector<bytedance::bolt::RowVectorPtr>> input_;
};

} // namespace bolt::cudf::benchmark
