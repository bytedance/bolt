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
 * 2026-10-10.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#pragma once

#include "bolt/common/memory/Memory.h"
#include "bolt/core/QueryCtx.h"

namespace folly {
class Executor;
} // namespace folly
namespace bytedance::bolt::python {

/// PyBoltContext is used only during function binding time. Its a utility
/// that manages pool, query and exec context for Bolt expressions and vectors.
struct PyBoltContext {
  PyBoltContext(const PyBoltContext&) = delete;
  PyBoltContext(const PyBoltContext&&) = delete;
  PyBoltContext& operator=(const PyBoltContext&) = delete;
  PyBoltContext& operator=(const PyBoltContext&&) = delete;

  static PyBoltContext& getSingletonInstance() {
    static PyBoltContext instance;
    return instance;
  }

  // Cleanup instances that relies on the python interpreter.
  //
  // This is required because some python objects from the python interpreter
  // live on the C++ side, their lifetime needs to not outlive the
  // interpreter's. For instance, when registering a python function to run as
  // a UDF, the python function object needs be freed before the interpreter.
  // Since udf functions in bolt are stored in a static lifetime object,
  // it can be tricky to ensure it outlives the interpreter that spawned it.
  void cleanup();

  bytedance::bolt::memory::MemoryPool* pool() {
    return leafPool_.get();
  }

  std::shared_ptr<bytedance::bolt::core::QueryCtx> queryCtx() {
    return queryCtx_;
  }

  bytedance::bolt::core::ExecCtx* execCtx() {
    return execCtx_.get();
  }

 private:
  explicit PyBoltContext();

  void initMemoryManager();
  void registerHiveConnector();
  void registerTpchConnector();
  void registerAllFunctions();

  std::shared_ptr<folly::Executor> executor_;
  std::shared_ptr<bytedance::bolt::memory::MemoryPool> rootPool_{nullptr};
  std::shared_ptr<bytedance::bolt::memory::MemoryPool> leafPool_{nullptr};
  std::unique_ptr<bytedance::bolt::core::ExecCtx> execCtx_{nullptr};
  std::shared_ptr<bytedance::bolt::core::QueryCtx> queryCtx_{nullptr};

  static constexpr const char* kDefaultContextName = "pybolt";
  static inline const std::string kFunctionPrefix;
};

} // namespace bytedance::bolt::python
