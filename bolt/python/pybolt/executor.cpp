/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include <pybind11/functional.h>
#include <pybind11/gil.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>

#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "bolt/core/PlanFragment.h"
#include "bolt/core/PlanNode.h"
#include "bolt/exec/HashPartitionFunction.h"
#include "bolt/exec/Split.h"
#include "bolt/exec/Task.h"
#include "bolt/exec/VectorHasher.h"
#include "bolt/python/Aggregate.h"
#include "bolt/python/Function.h"
#include "bolt/python/pybolt/context.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/arrow/Abi.h"
#include "bolt/vector/arrow/Bridge.h"

using namespace ::bytedance::bolt;
using namespace ::bytedance::bolt::python;

namespace {

class BoltTaskExecutor {
 public:
  explicit BoltTaskExecutor() {}

  RowVectorPtr execute(
      const core::PlanFragment& planFragment,
      const std::string& taskName) {
    // Single-driver kSerial execution: drive the task with the
    // iterator pattern (``task->next()`` loop) and concatenate the
    // RowVectors it produces.
    auto task = exec::Task::create(
        taskName,
        planFragment,
        0,
        PyBoltContext::getSingletonInstance().queryCtx(),
        exec::Task::ExecutionMode::kSerial,
        // Explicitly-typed empty Consumer: a bare ``nullptr`` is
        // ambiguous between the ``Consumer`` and ``ConsumerSupplier``
        // overloads of ``Task::create``. kSerial ignores the consumer.
        /*consumer=*/exec::Consumer{});

    for (auto& [planNodeId, splits] : splitsMap_) {
      for (auto& split : splits) {
        task->addSplit(planNodeId, std::move(split));
      }
      // Always signal ``noMoreSplits`` for every node the caller
      // explicitly registered in ``splitsMap_``, even when the split
      // vector is empty. Without it the executor's source operator
      // hangs in kSerial waiting for splits that will never arrive
      // (``"Serial execution requires all splits to be added before
      // calling Task::next()."``). Callers that legitimately have
      // zero splits for a registered source (empty-shard fan-out,
      // virtual_table-only plans, etc.) need the same signal —
      // they're saying "this source has no rows" not "I forgot to
      // call addSplits".
      task->noMoreSplits(planNodeId);
    }
    splitsMap_.clear();

    RowVectorPtr result = nullptr;
    while (auto rowVector = task->next()) {
      if (result == nullptr) {
        result = BaseVector::create<RowVector>(
            rowVector->type(),
            rowVector->size(),
            PyBoltContext::getSingletonInstance().pool());
        result->copy(rowVector.get(), 0, 0, rowVector->size());
      } else {
        result->append(rowVector.get());
      }
    }

    // Original ``boltml`` contract: when no rows are produced
    // (e.g. filter eliminates every input row), return ``nullptr``
    // / Python ``None``. Callers that need a schema-bearing empty
    // RowVector for downstream use (e.g. distributed exchange-
    // publish, where the consumer descriptor needs ``outputNames`` /
    // ``outputTypes``) construct one explicitly on the Python side
    // from the plan node's ``outputType()``.
    return result;
  }

  void addSplits(core::PlanNodeId nodeId, std::vector<exec::Split> splits) {
    auto it = splitsMap_.find(nodeId);
    if (it != splitsMap_.end()) {
      it->second.insert(
          it->second.end(),
          std::make_move_iterator(splits.begin()),
          std::make_move_iterator(splits.end()));
    } else {
      splitsMap_.emplace_hint(it, std::move(nodeId), std::move(splits));
    }
  }

 private:
  std::unordered_map<core::PlanNodeId, std::vector<exec::Split>> splitsMap_;
};
} // namespace

namespace bytedance::bolt::python {

void addExecutorBindings( // NOLINT
    pybind11::module& m,
    bool asModuleLocalDefinitions) {
  pybind11::class_<BoltTaskExecutor>(
      m, "BoltTaskExecutor", pybind11::module_local(asModuleLocalDefinitions))
      .def(pybind11::init(), "Create a BoltTaskExecutor instance")
      .def(
          "execute",
          &BoltTaskExecutor::execute,
          pybind11::arg("planFragment"),
          pybind11::arg("taskName") = "PyboltTask",
          "Execute the task and return the result RowVector "
          "(single-driver kSerial execution).")
      .def(
          "addSplits",
          [](BoltTaskExecutor& self,
             core::PlanNodeId nodeId,
             std::vector<exec::Split> splits) {
            self.addSplits(std::move(nodeId), std::move(splits));
          },
          pybind11::arg("nodeId"),
          pybind11::arg("splits"),
          "Add splits to be consumed by next execute() call.");

  m.def(
      "registerPythonFunction",
      &registerPythonFunction,
      pybind11::arg("callable"),
      pybind11::arg("functionName"),
      pybind11::arg("returnType"),
      pybind11::arg("nArgs"),
      pybind11::arg("defaultArgs") = std::vector<pybind11::object>{},
      pybind11::arg("mapBatch") = false,
      "Register a python function as a bolt UDF.");

  m.def(
      "registerPythonAggregator",
      &PythonAggregate::registerAggregationFunction,
      pybind11::arg("pyAggregatorClass"),
      pybind11::arg("fnName"),
      pybind11::arg("returnType"),
      pybind11::arg("nArgs"),
      "Register a python class as a bolt aggregate function.");

  // Hash-partition / row-hash bindings.
  //
  // boltml's distributed runtime needs to bucket a result vector into
  // ``numPartitions`` exchange partitions on the producer side
  // (``boltml/ray/runtime.py::_splitProducedResult``). The bucket
  // assignment must be deterministic across the driver and every
  // remote worker, AND must agree with what Bolt's own
  // ``HashPartitionFunction`` would produce so that any future plan
  // that pushes partitioning into a Bolt operator stays consistent
  // with the boltml-orchestrated split.
  m.def(
      "computePartitionIndices",
      [](const RowVectorPtr& input,
         const std::vector<int32_t>& keyChannels,
         int32_t numPartitions) {
        BOLT_CHECK_GT(numPartitions, 0, "numPartitions must be positive");
        const auto rowType =
            std::dynamic_pointer_cast<const RowType>(input->type());
        BOLT_CHECK_NOT_NULL(rowType, "input must be a RowVector");
        std::vector<column_index_t> channels;
        channels.reserve(keyChannels.size());
        for (auto c : keyChannels) {
          BOLT_CHECK_GE(c, 0, "key channel must be non-negative");
          BOLT_CHECK_LT(
              static_cast<size_t>(c),
              rowType->size(),
              "key channel {} out of range for {}-column row",
              c,
              rowType->size());
          channels.emplace_back(static_cast<column_index_t>(c));
        }
        exec::HashPartitionFunction fn(
            numPartitions, rowType, channels, /*constValues*/ {});
        std::vector<uint32_t> partitions;
        fn.partition(*input, partitions);
        return partitions;
      },
      pybind11::arg("input"),
      pybind11::arg("keyChannels"),
      pybind11::arg("numPartitions"),
      "Compute the partition index for each row of ``input`` using "
      "Bolt's HashPartitionFunction over ``keyChannels`` (0-based "
      "column indices into the row schema). Returns a list of "
      "``numPartitions``-bounded indices, one per row.");

  m.def(
      "computeRowHashes",
      [](const RowVectorPtr& input, const std::vector<int32_t>& keyChannels) {
        const auto rowType =
            std::dynamic_pointer_cast<const RowType>(input->type());
        BOLT_CHECK_NOT_NULL(rowType, "input must be a RowVector");
        const auto size = input->size();
        std::vector<std::unique_ptr<exec::VectorHasher>> hashers;
        hashers.reserve(keyChannels.size());
        for (auto c : keyChannels) {
          BOLT_CHECK_GE(c, 0, "key channel must be non-negative");
          BOLT_CHECK_LT(
              static_cast<size_t>(c),
              rowType->size(),
              "key channel {} out of range for {}-column row",
              c,
              rowType->size());
          hashers.emplace_back(exec::VectorHasher::create(
              rowType->childAt(static_cast<column_index_t>(c)),
              static_cast<column_index_t>(c)));
        }
        SelectivityVector rows(size);
        rows.setAll();
        raw_vector<uint64_t> hashes;
        hashes.resize(size);
        for (size_t i = 0; i < hashers.size(); ++i) {
          auto& hasher = hashers[i];
          hasher->decode(*input->childAt(hasher->channel()), rows);
          hasher->hash(rows, /*mix*/ i > 0, hashes);
        }
        return std::vector<uint64_t>(hashes.begin(), hashes.end());
      },
      pybind11::arg("input"),
      pybind11::arg("keyChannels"),
      "Compute the per-row uint64 hash of ``input`` over the given "
      "key channels using Bolt's VectorHasher.");
}
} // namespace bytedance::bolt::python
