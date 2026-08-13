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
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import copy, get

required_conan_version = ">=2.0"


class CudfConan(ConanFile):
    name = "cudf"
    description = "The libcudf CUDA/C++ columnar data processing library"
    license = "Apache-2.0"
    url = "https://github.com/bytedance/bolt"
    homepage = "https://github.com/rapidsai/cudf"
    topics = ("cuda", "dataframe", "gpu", "rapids")
    package_type = "library"

    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "cuda_architectures": ["ANY"],
    }
    default_options = {
        "shared": True,
        "fPIC": True,
        "cuda_architectures": "RAPIDS",
    }
    implements = ["auto_shared_fpic"]

    @property
    def _min_cppstd(self):
        return 17

    def layout(self):
        cmake_layout(self, src_folder="src")

    def requirements(self):
        self.requires(
            "zlib/[>=1.3.1 <2]",
            transitive_headers=True,
            transitive_libs=True,
        )

    def build_requirements(self):
        self.tool_requires("cmake/3.31.10")

    def validate(self):
        if self.settings.compiler.get_safe("cppstd"):
            check_min_cppstd(self, self._min_cppstd)

    def source(self):
        get(self, **self.conan_data["sources"][str(self.version)], strip_root=True)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.cache_variables["BUILD_TESTS"] = False
        tc.cache_variables["BUILD_BENCHMARKS"] = False
        tc.cache_variables["BUILD_SHARED_LIBS"] = bool(self.options.shared)
        tc.cache_variables["CMAKE_CUDA_ARCHITECTURES"] = str(
            self.options.cuda_architectures
        )
        tc.cache_variables["CMAKE_FIND_PACKAGE_PREFER_CONFIG"] = True
        tc.cache_variables["CUDF_BUILD_TESTUTIL"] = False
        tc.cache_variables["CUDF_BUILD_STREAMS_TEST_UTIL"] = False
        tc.generate()
        CMakeDeps(self).generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure(build_script_folder=os.path.join(self.source_folder, "cpp"))
        cmake.build()

    def package(self):
        copy(
            self,
            "LICENSE",
            src=self.source_folder,
            dst=os.path.join(self.package_folder, "licenses"),
        )
        CMake(self).install()

    def package_info(self):
        # Keep libcudf's installed CMake package because it reconstructs the
        # RMM, CCCL, CUDA, and RAPIDS Logger interface targets.
        self.cpp_info.set_property("cmake_find_mode", "none")
        self.cpp_info.builddirs = ["."]

        cudf = self.cpp_info.components["cudf"]
        cudf.libs = ["cudf"]
        cudf.includedirs = [
            "include",
            os.path.join("include", "rapids"),
            os.path.join("include", "rapids", "libcudacxx"),
        ]
        cudf.set_property("cmake_target_name", "cudf::cudf")
