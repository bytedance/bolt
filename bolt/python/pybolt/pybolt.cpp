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

#include <pybind11/pybind11.h>

#include <glog/logging.h>

#include "bolt/python/pybolt/context.h"

using namespace ::bytedance::bolt;

namespace bytedance::bolt::python {

void addArrowAdapterBindings(pybind11::module&);
void addDataTypeBindings(pybind11::module&, bool);
void addExecutorBindings(pybind11::module&, bool);
void addFileFormatBindings(pybind11::module&, bool);
void addFileSystemBindings(pybind11::module&, bool);
void addJoinTypeBindings(pybind11::module&, bool);
void addSortOrderBindings(pybind11::module&, bool);
void addSplitBindings(pybind11::module&, bool);
void addPlanNodeBindings(pybind11::module&, bool);
void addPlanFragmentBindings(pybind11::module&, bool);
void addPlanNodeIdBindings(pybind11::module&, bool);
void addPlanBindings(pybind11::module&, bool);
void addSubstraitBindings(pybind11::module&, bool);
void addTpchBindings(pybind11::module&, bool);
void addVectorBindings(pybind11::module&, bool);

PYBIND11_MODULE(pybolt, m) {
  m.doc() = R"pbdoc(
      PyBolt native code module
      --------------------------

      .. currentmodule:: pybolt.pybolt

      .. autosummary::
         :toctree: _generate

  )pbdoc";

  google::InitGoogleLogging("pybolt");

  addArrowAdapterBindings(m);
  addDataTypeBindings(m, true);
  addExecutorBindings(m, true);
  addFileFormatBindings(m, true);
  addFileSystemBindings(m, true);
  addJoinTypeBindings(m, true);
  addSortOrderBindings(m, true);
  addSplitBindings(m, true);
  addPlanNodeBindings(m, true);
  addPlanFragmentBindings(m, true);
  addPlanNodeIdBindings(m, true);
  addPlanBindings(m, true);
  addSubstraitBindings(m, true);
  addTpchBindings(m, true);
  addVectorBindings(m, true);

  { auto& _ = PyBoltContext::getSingletonInstance(); }

  auto atexit = pybind11::module_::import("atexit");
  atexit.attr("register")(pybind11::cpp_function([]() {
    PyBoltContext& context = PyBoltContext::getSingletonInstance();
    context.cleanup();
  }));
}

} // namespace bytedance::bolt::python
