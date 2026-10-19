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

#include <pybind11/pybind11.h>

#include "bolt/dwio/common/Options.h"

using namespace ::bytedance::bolt;

namespace bytedance::bolt::python {

void addFileFormatBindings( // NOLINT
    pybind11::module& m,
    bool asModuleLocalDefinitions) {
  pybind11::enum_<dwio::common::FileFormat>(
      m, "FileFormat", pybind11::module_local(asModuleLocalDefinitions))
      .value("UNKNOWN", dwio::common::FileFormat::UNKNOWN)
      .value("DWRF", dwio::common::FileFormat::DWRF)
      .value("RC", dwio::common::FileFormat::RC)
      .value("RC_TEXT", dwio::common::FileFormat::RC_TEXT)
      .value("RC_BINARY", dwio::common::FileFormat::RC_BINARY)
      .value("TEXT", dwio::common::FileFormat::TEXT)
      .value("JSON", dwio::common::FileFormat::JSON)
      .value("PARQUET", dwio::common::FileFormat::PARQUET)
      .value("ALPHA", dwio::common::FileFormat::ALPHA)
      .value("ORC", dwio::common::FileFormat::ORC)
      .value("LANCE", dwio::common::FileFormat::LANCE)
      .export_values();

  m.def(
      "fileFormatFromString",
      &dwio::common::toFileFormat,
      "Convert string to FileFormat");
  m.def(
      "fileFormatToString",
      static_cast<std::string (*)(dwio::common::FileFormat)>(
          &dwio::common::toString),
      "Convert FileFormat to string");
}

} // namespace bytedance::bolt::python
