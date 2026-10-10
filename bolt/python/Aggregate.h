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
#pragma once

#include <pybind11/pytypes.h>

#include "bolt/exec/Aggregate.h"

namespace bytedance::bolt::python {

class PythonAggregate final : public bytedance::bolt::exec::Aggregate {
 public:
  ~PythonAggregate() override {}

  static void registerAggregationFunction(
      pybind11::object pyAggregatorClass,
      const std::string& fnName,
      const bytedance::bolt::TypePtr& returnType,
      const size_t nArgs);

 protected:
  // Returns the fixed number of bytes the accumulator takes on a group
  // row. Variable width accumulators will reference the variable
  // width part of the state from the fixed part.
  //
  // Note that since the accumulator implementation is user defined,
  // we don't know what number of bytes the accumulator really takes.
  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(std::unique_ptr<pybind11::object>);
  }

  bool accumulatorUsesExternalMemory() const override {
    return true;
  }

  // We are cheating here because we don't really know what the user
  // allocates as far as memory goes. It could very well be a variable
  // size accumulator. However, we store the python object in a pointer and
  // the size of the pointer is a fixed size.
  bool isFixedSize() const override {
    return true;
  }

  // We need to free each individual pointer to python objects manually.
  void destroy(folly::Range<char**> /*groups*/) override;

  void initializeNewGroups(
      char** groups,
      folly::Range<const bytedance::bolt::vector_size_t*> indices) override;

  void addSingleGroupRawInput(
      char* group,
      const bytedance::bolt::SelectivityVector& rows,
      const std::vector<bytedance::bolt::VectorPtr>& args,
      bool mayPushdown) override;

  void addRawInput(
      char** groups,
      const bytedance::bolt::SelectivityVector& rows,
      const std::vector<bytedance::bolt::VectorPtr>& args,
      bool mayPushdown) override;

  void extractAccumulators(
      char** groups,
      int32_t numGroups,
      bytedance::bolt::VectorPtr* result) override;

  void addSingleGroupIntermediateResults(
      char* group,
      const bytedance::bolt::SelectivityVector& rows,
      const std::vector<bytedance::bolt::VectorPtr>& args,
      bool mayPushdown) override;

  void addIntermediateResults(
      char** groups,
      const bytedance::bolt::SelectivityVector& rows,
      const std::vector<bytedance::bolt::VectorPtr>& args,
      bool mayPushdown) override;

  void extractValues(
      char** groups,
      int32_t numGroups,
      bytedance::bolt::VectorPtr* result) override;

 private:
  PythonAggregate(
      pybind11::object pyAggregatorClass,
      const bytedance::bolt::TypePtr& returnType);

  pybind11::object aggregatorClass_;
};
} // namespace bytedance::bolt::python
