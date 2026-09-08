# Copyright (c) ByteDance Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import (
    apply_conandata_patches,
    copy,
    export_conandata_patches,
    get,
)


class OpenZlConan(ConanFile):
    name = "openzl"
    package_type = "static-library"
    license = "BSD-3-Clause"
    url = "https://github.com/facebook/openzl"
    description = "OpenZL format-aware lossless data compression"
    topics = ("compression", "openzl", "zstd", "lz4")

    settings = "os", "arch", "compiler", "build_type"
    options = {"fPIC": [True, False]}
    default_options = {"fPIC": True}

    def config_options(self):
        if self.settings.os == "Windows":
            self.options.rm_safe("fPIC")

    def requirements(self):
        self.requires("zstd/1.5.7", transitive_headers=True, transitive_libs=True)
        self.requires("lz4/1.10.0", transitive_headers=True, transitive_libs=True)

    def export_sources(self):
        export_conandata_patches(self)

    def source(self):
        source = self.conan_data["sources"][str(self.version)]
        get(
            self,
            url=source["url"],
            sha256=source["sha256"],
            strip_root=True,
        )
        apply_conandata_patches(self)

    def layout(self):
        cmake_layout(self)

    def generate(self):
        dependencies = CMakeDeps(self)
        dependencies.generate()

        toolchain = CMakeToolchain(self)
        toolchain.variables["OPENZL_USE_CONAN_DEPS"] = True
        toolchain.variables["OPENZL_BUILD_ALL"] = False
        toolchain.variables["OPENZL_BUILD_SHARED_LIBS"] = False
        toolchain.variables["OPENZL_BUILD_TESTS"] = False
        toolchain.variables["OPENZL_BUILD_BENCHMARKS"] = False
        toolchain.variables["OPENZL_BUILD_PARQUET_TOOLS"] = False
        toolchain.variables["OPENZL_BUILD_PYTHON_EXT"] = False
        toolchain.variables["OPENZL_BUILD_PYTHON_EXT_TESTS"] = False
        toolchain.variables["OPENZL_ALLOW_INTROSPECTION"] = False
        toolchain.variables["OPENZL_INSTALL"] = True
        toolchain.variables["OPENZL_CPP_INSTALL"] = False
        toolchain.variables["OPENZL_BUILD_PYTHON_DEMO"] = False
        toolchain.variables["OPENZL_BUILD_CPP"] = False
        toolchain.variables["OPENZL_BUILD_CUSTOM_PARSERS"] = False
        toolchain.variables["OPENZL_BUILD_TOOLS"] = False
        toolchain.variables["OPENZL_BUILD_CLI"] = False
        toolchain.variables["OPENZL_BUILD_EXAMPLES"] = False
        if "fPIC" in self.options:
            toolchain.variables["CMAKE_POSITION_INDEPENDENT_CODE"] = bool(
                self.options.fPIC
            )
        toolchain.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(
            self,
            "LICENSE*",
            src=self.source_folder,
            dst=os.path.join(self.package_folder, "licenses"),
        )
        CMake(self).install()

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "openzl")
        component = self.cpp_info.components["openzl"]
        component.set_property("cmake_target_name", "OpenZL::openzl")
        component.libs = ["openzl"]
        component.requires = ["zstd::zstdlib", "lz4::lz4"]
        if self.settings.os in ["Linux", "FreeBSD"]:
            component.system_libs = ["m", "pthread"]
