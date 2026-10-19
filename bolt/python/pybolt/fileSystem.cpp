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
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>
#include <pyerrors.h>
#include <exception>

#include "bolt/common/config/Config.h"
#include "bolt/common/file/File.h"
#include "bolt/common/file/FileSystems.h"

using namespace ::bytedance::bolt;

namespace pybind11 {
// Pybind11 does not define this one.
PYBIND11_RUNTIME_EXCEPTION(runtime_error, PyExc_RuntimeError)
} // namespace pybind11

namespace bytedance::bolt::python {

void addFileSystemBindings( // NOLINT
    pybind11::module& m,
    bool asModuleLocalDefinitions) {
  using FileSystemPtr = std::shared_ptr<filesystems::FileSystem>;
  pybind11::class_<filesystems::FileSystem, FileSystemPtr> filesystem(
      m, "FileSystem", pybind11::module_local(asModuleLocalDefinitions));
  filesystem.def_static(
      "get",
      [](std::string_view path) -> FileSystemPtr {
        auto cfg = std::make_shared<config::ConfigBase>(
            std::unordered_map<std::string, std::string>());
        return filesystems::getFileSystem(path, cfg);
      },
      pybind11::arg("path"));
  filesystem.def("__str__", [](FileSystemPtr& fs) { return fs->name(); });
  filesystem.def("exists", [](FileSystemPtr& fs, const std::string& path) {
    return fs->exists(path);
  });
  filesystem.def("isDirectory", [](FileSystemPtr& fs, const std::string& path) {
    return fs->isDirectory(path);
  });
  filesystem.def("list", [](FileSystemPtr& fs, const std::string& path) {
    try {
      return fs->list(path);
    } catch (const std::exception& e) {
      throw pybind11::runtime_error(e.what());
    }
  });
  filesystem.def("read", [](FileSystemPtr& fs, const std::string& path) {
    auto file = fs->openFileForRead(path);
    try {
      return pybind11::bytes(file->pread(0, file->size()));
    } catch (const std::exception& e) {
      throw pybind11::runtime_error(e.what());
    }
  });
}

} // namespace bytedance::bolt::python
