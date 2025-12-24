# Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import apply_conandata_patches, export_conandata_patches, get
from conan.tools.env import VirtualBuildEnv
import os


required_conan_version = ">=1.54.0"


class WangleConan(ConanFile):
    name = "wangle"
    description = "Wangle is a library that makes it easy to build services in C++"
    license = "Apache-2.0"
    url = "https://github.com/facebook/wangle"
    homepage = "https://github.com/facebook/wangle"
    topics = ("networking", "fizz", "folly")

    package_type = "library"
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
    }

    def config_options(self):
        if self.settings.os == "Windows":
            self.options.rm_safe("fPIC")

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def layout(self):
        cmake_layout(self, src_folder="src")

    def requirements(self):
        self.requires("fizz/2022.10.31.00", transitive_headers=True, transitive_libs=True)
        self.requires("folly/2022.10.31.00", transitive_headers=True, transitive_libs=True)
        self.requires("fmt/8.0.1", transitive_headers=True, transitive_libs=True)
        self.requires("openssl/1.1.1w")
        self.requires("glog/0.7.1", transitive_headers=True, transitive_libs=True)
        self.requires("gflags/2.2.2")
        self.requires("double-conversion/3.3.0", transitive_headers=True, transitive_libs=True)
        self.requires("libevent/2.1.12", transitive_headers=True, transitive_libs=True)

    def build_requirements(self):
        self.tool_requires("cmake/3.31.3")

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)
        apply_conandata_patches(self)

    def export_sources(self):
        export_conandata_patches(self)

    def generate(self):
        env = VirtualBuildEnv(self)
        env.generate()
        tc = CMakeToolchain(self)
        tc.cache_variables["BUILD_TESTS"] = "OFF"
        tc.cache_variables["CMAKE_FIND_PACKAGE_PREFER_CONFIG"] = "ON"
        tc.cache_variables["CMAKE_IGNORE_PATH"] = "/usr/local/lib;/usr/local/include"
        tc.cache_variables["FOLLY_LIBRARIES"] = "folly::folly"
        tc.cache_variables["FIZZ_LIBRARIES"] = "fizz::fizz"
        tc.cache_variables["GLOG_LIBRARIES"] = "glog::glog"
        tc.generate()
        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmakelists = os.path.join(self.source_folder, "wangle", "CMakeLists.txt")
        cmake = CMake(self)
        cmake.configure(build_script_folder=os.path.join(self.source_folder, "wangle"))
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "wangle")
        self.cpp_info.set_property("cmake_target_name", "wangle::wangle")
        self.cpp_info.libs = ["wangle"]
        if self.settings.os in ["Linux", "FreeBSD"]:
            self.cpp_info.system_libs.append("pthread")
