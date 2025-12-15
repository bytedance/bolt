from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.apple import is_apple_os
from conan.tools.build import check_min_cppstd, cross_building
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import apply_conandata_patches, export_conandata_patches, get, copy, rmdir, replace_in_file, save, rm
from conan.tools.microsoft import is_msvc, is_msvc_static_runtime
from conan.tools.env import Environment, VirtualBuildEnv, VirtualRunEnv
from conan.tools.scm import Version, Git
import os


required_conan_version = ">=1.54.0"


class CelebornCppClientConan(ConanFile):
    name = "celeborn-cpp-client"
    description = "cpp client for Celeborn, a distributed shuffle service"
    topics = ("celeborn", "shuffle", "rss")
    url = "https://github.com/conan-io/conan-center-index"
    homepage = "https://github.com/apache/celeborn"
    license = "Apache-2.0"

    package_type = "library"
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],

        "no_exception_tracer": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,

        "no_exception_tracer": False
    }

    def requirements(self):
        self.tool_requires('cmake/3.31.3')
        self.requires("folly/2022.10.31.00", transitive_headers=True, transitive_libs=True)
        self.requires("fizz/2022.10.31.00", transitive_headers=True, transitive_libs=True)
        self.requires("wangle/2022.10.31.00", transitive_headers=True, transitive_libs=True)
        self.requires("re2/20230301", transitive_headers=True, transitive_libs=True)
        self.requires("xxhash/0.8.3", transitive_headers=True, transitive_libs=True)

    def source(self):
        git = Git(self, folder="..")
        git.clone('https://github.com/apache/celeborn', target='src')
        git = Git(self, folder=self.source_folder)
        git.checkout("0f663d0")
        apply_conandata_patches(self)

    def export_sources(self):
        export_conandata_patches(self)

    def layout(self):
        cmake_layout(self, src_folder="src")

    def build(self):
        root_cmakelists = os.path.join(self.source_folder, "cpp", "CMakeLists.txt")
        replace_in_file(
            self,
            root_cmakelists,
            "find_library(RE2 re2)",
            "find_package(re2 CONFIG REQUIRED)\nset(RE2 re2::re2)",
            strict=False,
        )
        cmake = CMake(self)
        cmake.configure(build_script_folder=os.path.join(self.source_folder, "cpp"))
        cmake.build()
    
    def generate(self):
        build_env = VirtualBuildEnv(self)
        build_env.generate()
        run_env = VirtualRunEnv(self)
        run_env.generate()

        tc = CMakeToolchain(self)

        tc.cache_variables["CELEBORN_BUILD_TESTS"] = "OFF"
        tc.cache_variables["FOLLY_LIBRARIES"] = "folly::folly;fmt::fmt"
        tc.cache_variables["GLOG"] = "glog::glog"
        tc.cache_variables["GFLAGS_LIBRARIES"] = "gflags::gflags"
        tc.cache_variables["LIBSODIUM_LIBRARY"] = "libsodium::libsodium"

        tc.cache_variables["RE2"] = "re2::re2"
        tc.cache_variables["FIZZ"] = "fizz::fizz"
        tc.cache_variables["WANGLE"] = "wangle::wangle"

        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

    def package(self):
        copy(self, "LICENSE*", src=self.source_folder,
             dst=os.path.join(self.package_folder, "licenses"))

        lib_dst = os.path.join(self.package_folder, "lib")
        inc_dst = os.path.join(self.package_folder, "include")

        # libraries
        copy(self, pattern="**/lib*.a", src=self.build_folder, dst=lib_dst, keep_path=False)

        # headers
        copy(self, pattern="**/*.h",
             src=os.path.join(self.source_folder, "cpp"),
             dst=inc_dst,
             keep_path=True,
             excludes=("**/tests/**", "**/test/**", "**/benchmark/**", "**/bench/**"))

        # proto
        copy(self, pattern="*.pb.h",
             src=os.path.join(self.build_folder, "proto"),
             dst=os.path.join(inc_dst, "celeborn", "proto"),
             keep_path=False)

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "celeborn-cpp-client")
        self.cpp_info.set_property("cmake_target_name", "celeborn-cpp-client::celeborn-cpp-client")

        self.cpp_info.libs = [
            "client",
            "network",
            "conf",
            "protocol",
            "proto",
            "memory",
            "utils",
        ]

        self.cpp_info.system_libs = ["pthread", "dl"]

        self.cpp_info.requires = [
            "folly::folly",
            "fizz::fizz",
            "wangle::wangle",
            "re2::re2",
            "xxhash::xxhash",
        ]
