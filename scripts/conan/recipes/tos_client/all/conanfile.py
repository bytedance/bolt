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
from conan.tools.env import VirtualBuildEnv
from conan.tools.files import copy, get


required_conan_version = ">=1.54.0"


class TOSClientConan(ConanFile):
    name = "tos_client"
    description = "Volcengine TOS SDK for C++"
    license = "Apache-2.0"
    url = "https://github.com/volcengine/ve-tos-cpp-sdk"
    homepage = "https://github.com/volcengine/ve-tos-cpp-sdk"
    topics = ("tos", "object-storage", "volcengine")

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

    def requirements(self):
        self.requires("libcurl/8.12.1")
        self.requires("openssl/1.1.1w")

    def build_requirements(self):
        self.tool_requires("cmake/3.31.10")

    def layout(self):
        cmake_layout(self, src_folder="src", build_folder="_build")

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)

    def generate(self):
        build_env = VirtualBuildEnv(self)
        build_env.generate()

        toolchain = CMakeToolchain(self, generator="Ninja")
        toolchain.cache_variables["BUILD_SHARED_LIB"] = bool(self.options.shared)
        toolchain.cache_variables["BUILD_UNITTEST"] = False
        toolchain.cache_variables["BUILD_DEMO"] = False
        toolchain.cache_variables["BUILD_ASYNC_SDK"] = False
        toolchain.cache_variables["CMAKE_FIND_PACKAGE_PREFER_CONFIG"] = True
        toolchain.generate()

        dependencies = CMakeDeps(self)
        dependencies.generate()

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
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "tos_client")
        self.cpp_info.set_property("cmake_target_name", "tos_client::tos_client")
        self.cpp_info.set_property("cmake_find_mode", "both")
        self.cpp_info.libs = ["ve-tos-cpp-sdk-lib"]
