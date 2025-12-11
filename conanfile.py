#
# Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
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

import json
import os
import pydot
import platform
import re
from conan import ConanFile
from conan.tools import files, scm
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.env import Environment, VirtualBuildEnv, VirtualRunEnv
import csv

# Now, set options of third parties
# Note that: in conan 2.0, postfix = "/*""
postfix = "/*"
folly = f"folly{postfix}"
glog = f"glog{postfix}"
boost = f"boost{postfix}"
arrow = f"arrow{postfix}"
orc = f"orc{postfix}"
icu = f"icu{postfix}"
gperftools = f"gperftools{postfix}"
liburing = f"liburing{postfix}"


class TorchOption:
    def __init__(self, value=os.getenv("BOLT_ENABLE_TORCH")):
        upper = str(value).upper()
        if upper in ["TRUE", "T", "1", "ON", "YES", "Y", "CPU"]:
            self._value = "CPU"
        elif upper == "GPU":
            self._value = "GPU"
        elif upper in ["", "NONE", "FALSE", "F", "0", "OFF", "NO", "N"]:
            self._value = None
        else:
            raise ValueError(f"Invalid value for TorchOption: {value}")

    @property
    def value(self):
        return self._value

    @staticmethod
    def all():
        """
        The list of valid TorchOptions
        """
        return [None, "CPU", "GPU"]


class DepLoader:
    def is_bolt(self, name):
        # we doesn't export executable file's dependency
        executable = [
            "bolt_in_10_min_demo",
            "bolt_sparksql_coverage",
            "bolt_prestosql_coverage",
            "bolt_memcpy_meter",
            "bolt_query_replayer",
            "bolt_trace_file_tool",
        ]
        benchmarks = [
            "_benchmarks_",
            "_benchmarks",
            "benchmarks",
            "_benchmark_",
            "_benchmark",
            "benchmark",
            "_bm",
        ]
        if (
            name in executable
            or "example" in name
            or name.endswith("test")
            or name.endswith("benchmark")
            or name.endswith("fuzzer")
        ):
            return False
        for benchmark in benchmarks:
            if benchmark in name:
                return False
        names = ["duckdb", "tpch_extension", "dbgen", "md5"]
        return name in names or name.startswith("bolt")

    def get_edge(self, graph, edge):
        dst, src = edge.get_destination(), edge.get_source()
        dst_name = graph.get_node(dst)[0].get_attributes()["label"]
        src_name = graph.get_node(src)[0].get_attributes()["label"]
        return src_name.replace('"', ""), dst_name.replace('"', "")

    def append_result(self, result, src_name, dst_name):
        if src_name in result:
            if self.is_bolt(dst_name):
                result[src_name][0].append(dst_name)
            else:
                result[src_name][1].append(dst_name)
        else:
            if self.is_bolt(dst_name):
                result[src_name] = [[dst_name], []]
            else:
                result[src_name] = [[], [dst_name]]
        if dst_name not in result and self.is_bolt(dst_name):
            result[dst_name] = [[], []]

    def get_dep(self, path):
        graph = pydot.graph_from_dot_file(path)[0]
        dependencies = {}
        black_list = [
            "bolt_link_libs",
            "bolt_fuzzer_connector",
            "bolt_hive_config",
            "bolt_functions_string",
            "bolt_flag_definitions",
        ]
        for edge in graph.get_edge_list():
            src_name, dst_name = self.get_edge(graph, edge)
            # We will not process target if target's name isn't v with 'bolt'
            if (
                not self.is_bolt(src_name)
                or (dst_name in black_list)
                or (src_name in black_list)
            ):
                continue
            self.append_result(dependencies, src_name, dst_name)
        return dependencies

    def get_bolt_dep(self, path):
        ans = {}
        result = self.get_dep(path)
        for key in result:
            ans[key] = result[key][0]
        return ans

    def get_third_party_dep(self, path):
        ans = {}
        result = self.get_dep(path)
        for key in result:
            ans[key] = result[key][1]
        return ans


class BoltConan(ConanFile):
    description = """ Bolt native engine."""

    name = "bolt"
    settings = "os", "arch", "compiler", "build_type"
    PB_VERSION = "3.21.4"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "python_bind": [True, False],
        "spark_compatible": [True, False],
        "enable_testutil": [True, False],
        "enable_test": [True, False],
        "build_benchmark": ["off", "basic", "on"],
        "enable_coverage": [True, False],
        # format options
        "enable_parquet": [True, False],
        "enable_orc": [True, False],
        "enable_txt": [True, False],
        # file system options
        "enable_hdfs": [True, False],
        "enable_s3": [True, False],
        "enable_asan": [True, False],
        "enable_jit": [True, False],
        "enable_color": [True, False],
        "enable_exception_trace": [True, False],
        "es_build": [True, False],
        "ldb_build": [True, False],
        "enable_arrow_connector": [True, False],
        "enable_crc": [True, False],
        "enable_colocate": [True, False],
        "io_uring_supported": [True, False],
        "enable_torch": TorchOption.all(),
        "enable_perf": [True, False],
        "targets": ["ANY", None],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "python_bind": False,
        "enable_asan": False,
        "enable_color": True,
        # presto cpp worker needs bolt's testutil for ut
        "enable_testutil": True,
        # False by default, to avoid linking
        "enable_test": False,
        "build_benchmark": "off",
        "enable_coverage": False,
        "enable_parquet": True,
        "enable_orc": True,
        "enable_txt": True,
        # file system options
        "enable_hdfs": True,
        "enable_s3": False,
        "targets": None,
        "enable_arrow_connector": False,
        "enable_jit": True,
        "spark_compatible": False,
        "es_build": False,
        "enable_exception_trace": True,
        "ldb_build": False,
        # bytedance presto do not support crc flag
        "enable_crc": False,
        "enable_colocate": False,
        "io_uring_supported": True,
        "enable_torch": TorchOption().value,
        "enable_perf": False,
    }

    FB_VERSION = "2022.10.31.00"

    # global compiler options
    BOLT_GLOABL_FLAGS = "-Werror=return-type"

    build_policy = "missing"

    scm_url = "https://github.com/bytedance/bolt.git"

    def source(self):
        git = scm.Git(self)

        # by default, use master branch
        git.clone(self.scm_url, target=".")

        # if use 'stable" channel, we should use a git relase tag.
        if self.channel and self.channel == "stable":
            if not self.version:
                raise "Do specify a tag for a stable release."
            cmd = f"tags/{self.version} -b tag-{self.version}"
            git.checkout(cmd)
        else:
            scm_branch = self.version
            if scm_branch != "master":
                cmd = f"-b {scm_branch} origin/{scm_branch}"
                git.checkout(cmd)

    def io_uring_supported(self):
        if self.options.io_uring_supported != True:
            return False
        if self.settings.os == "Linux":
            # Try to determine kernel version during package configuration
            kernel_version = platform.release()
            match = re.search(r"^(\d+)\.(\d+)", kernel_version)
            if kernel_version:
                major = int(match.group(1))
                minor = int(match.group(2))
                self.output.info(f"Detected Linux kernel version: {major}.{minor}")
                has_minimum_kernel = (major > 5) or (major == 5 and minor >= 1)

                # Define a variable to be used in the build
                self.output.info(
                    f"Has minimum kernel required for io_uring: {has_minimum_kernel}"
                )
                return has_minimum_kernel
        self.output.info(f"OS is not Linux. io_uring is not supported.")
        return False

    def requirements(self):
        protobuf_version = os.getenv("PROTOBUF_VERSION", "3.21.4")
        self.requires(
            f"folly/{self.FB_VERSION}", transitive_headers=True, transitive_libs=True
        )
        self.requires(
            "arrow/15.0.1-oss", transitive_headers=True, transitive_libs=True
        )
        if self.options.get_safe("enable_jit"):
            self.requires("llvm-core/13.0.0")

        self.requires("simdjson/3.12.3", transitive_headers=True)
        self.requires("sonic-cpp/1.0.2", transitive_headers=True, transitive_libs=True)
        self.requires(
            f"protobuf/{protobuf_version}",
            transitive_headers=True,
            transitive_libs=True,
            force=True,
        )
        self.requires("re2/20230301", transitive_headers=True, transitive_libs=True)
        self.requires("gtest/1.10.0")
        self.requires(
            "icu/74.2", headers=True, transitive_headers=True, transitive_libs=True
        )
        self.requires("xsimd/9.0.1", transitive_headers=True, transitive_libs=True, force=True)
        self.requires(
            "cityhash/cci.20130801", transitive_headers=True, transitive_libs=True
        )
        self.requires("xxhash/0.8.1", transitive_headers=True, transitive_libs=True)
        self.requires(
            "fmt/9.0.0", transitive_headers=True, transitive_libs=True, force=True
        )
        self.requires("ryu/2.0.1", transitive_headers=True, transitive_libs=True)
        self.requires("cpr/1.10.5")
        self.requires("zlib/[>=1.3.1 <2]", force=True)
        self.requires(
            "flex/2.6.4",
            visible=False,
            libs=False,
            headers=True,
            transitive_headers=False,
            transitive_libs=False,
        )
        self.requires("timsort/2.1.0", transitive_headers=True)
        self.requires("duckdb/0.8.1")
        self.requires("snappy/1.2.1", headers=True, force=True)
        self.requires(
            "glog/0.7.1", headers=True, transitive_headers=True, transitive_libs=True
        )
        self.requires("thrift/0.17.0", headers=True, force=True)
        self.requires("roaring/4.3.1", headers=True)
        self.requires("boost/1.85.0", transitive_headers=True, transitive_libs=True)
        self.requires("libxml2/2.13.4", override=True)
        self.requires("double-conversion/3.3.0", override=True)
        self.requires("openssl/1.1.1w")
        if self.options.get_safe("es_build"):
            self.requires("onetbb/2021.12.0")
            self.requires("datasketches-cpp/3.5.1")
        if self.io_uring_supported():
            self.requires("liburing/2.6")
        if self.options.get_safe("python_bind"):
            self.requires("pybind11/2.13.1")
        if self.options.get_safe("enable_colocate"):
            self.requires(f"grpc/1.50.0")
        # upgrade libcurl from 8.11.1 to 8.12.1 to avoid SIGABRT issue in https://github.com/curl/curl/issues/15725
        self.requires("libcurl/8.12.1", override=True)
        if self.options.enable_torch != None:
            self.requires(
                "libtorch/2.6.0", options={"torch": self.options.enable_torch}
            )
        if self.options.get_safe("enable_perf"):
            self.requires("gperftools/2.16")
            self.requires("libunwind/1.8.0", override=True)
        else:
            self.requires("libunwind/1.8.0")
        self.requires("utf8proc/2.11.0", transitive_headers=True, transitive_libs=True)
        self.requires("date/3.0.4-bolt", transitive_headers=True, transitive_libs=True)
        self.requires("libbacktrace/cci.20210118")

    def build_requirements(self):
        self.tool_requires("m4/1.4.19")
        self.tool_requires("bison/3.8.2")
        self.tool_requires("flex/2.6.4")
        self.tool_requires("cmake/3.31.3")
        self.tool_requires("ninja/1.11.1")
        self.tool_requires("protobuf/<host_version>")
        self.tool_requires("thrift/<host_version>")
        if self.options.get_safe("enable_test"):
            self.test_requires("jemalloc/5.3.0")

    def layout(self):
        cmake_layout(self, build_folder="_build")

    def config_options(self):
        if self.options.get_safe("ldb_build"):
            self.options.enable_exception_trace = False

    # Set default options of third parties here
    def configure(self):
        if not self.options.get_safe("enable_test"):
            self.options.enable_coverage = False

        self.options[glog].with_unwind = False
        if self.options.get_safe("es_build"):
            self.options[glog].shared = True

        self.options[boost].without_test = True

        if not self.options.python_bind:
            self.options[boost].without_stacktrace = True

        if not self.options.get_safe("enable_test"):
            self.options.enable_coverage = False

        arrow_simd_level = "default"
        if str(self.settings.arch) in ["x86", "x86_64"]:
            arrow_simd_level = "avx2"
        elif str(self.settings.arch) in ["armv8", "arm", "armv9"]:
            arrow_simd_level = "neon"
        self.options[arrow].parquet = True
        self.options[arrow].filesystem_layer = True
        self.options[arrow].simd_level = arrow_simd_level
        self.options[arrow].with_lz4 = True
        self.options[arrow].with_snappy = True
        self.options[arrow].with_zlib = True
        self.options[arrow].with_json = True
        self.options[arrow].with_zstd = True
        self.options[arrow].with_openssl = True
        self.options[arrow].encryption = True
        self.options[arrow].with_thrift = True
        self.options[arrow].with_hdfs = True
        self.options[arrow].dataset_modules = True
        self.options[arrow].substrait = True
        # substrait depends on protobuf
        self.options[arrow].with_protobuf = True
        self.options[arrow].arrow_acero = True
        self.options[arrow].arrow_bundled_dependencies = True
        if self.options.get_safe("enable_colocate"):
            self.options[arrow].with_boost = True
            self.options[arrow].with_gflags = True
            self.options[arrow].with_protobuf = True
            self.options[arrow].with_grpc = True
            self.options[arrow].with_flight_rpc = True
        self.options[arrow].with_test = True
        self.options[arrow].with_csv = True
        if self.options.get_safe("es_build"):
            self.options[arrow].with_pyarrow = False
        else:
            self.options[arrow].with_pyarrow = True

        # Since we would introduce orc to Bolt through arrow,
        # we should enable orc in arrow.
        self.options[arrow].with_orc = True
        # orc/build_avx512 is True as default, but not supported for AMD CPU
        self.options[orc].build_avx512 = False

        if self.options[arrow].with_pyarrow:
            self.options[arrow].with_re2 = True

        if self.options.get_safe("es_build"):
            onetbb = f"onetbb{postfix}"
            self.options[onetbb].tbbmalloc = False
            # self.options[onetbb].tbbproxy = False

        openssl = f"openssl{postfix}"
        if self.options.get_safe("ldb_build"):
            self.options[folly].no_exception_tracer = True
            self.options[openssl].rand_seed = "devrandom"
            self.options[boost].filesystem_disable_statx = True
            self.options[boost].without_stacktrace = True

        self.options[icu].data_packaging = "static"

        if self.options.get_safe("enable_perf"):
            self.options[gperftools].build_cpu_profiler = True
            self.options[gperftools].build_heap_profiler = True

        if self.io_uring_supported:
            self.options[liburing].with_libc = False

        self.options["date/*"].use_system_tz_db = True

    def generate(self):
        build_env = VirtualBuildEnv(self)
        build_env.generate()

        run_env = VirtualRunEnv(self)
        run_env.generate()

        num_link_job = os.getenv("NUM_LINK_JOB", "4")

        tc = CMakeToolchain(self, generator="Ninja")

        tc.cache_variables["MAX_LINK_JOBS"] = num_link_job

        if str(self.settings.arch) in ["x86", "x86_64"]:
            flags = (
                f"{self.BOLT_GLOABL_FLAGS} -mavx2 -mfma -mavx -mf16c -mlzcnt -mbmi2 "
            )
            tc.cache_variables["CMAKE_CXX_FLAGS"] = flags
            tc.cache_variables["CMAKE_C_FLAGS"] = flags

        if str(self.settings.arch) in ["armv8", "arm"]:
            # Support CRC & NEON on ARMv8
            flags = f"{self.BOLT_GLOABL_FLAGS} -march=armv8.3-a"
            tc.cache_variables["CMAKE_CXX_FLAGS"] = flags
            tc.cache_variables["CMAKE_C_FLAGS"] = flags
        elif str(self.settings.arch) in ["armv9"] and not is_msvc(self):
            # gcc 12+ https://www.phoronix.com/news/GCC-12-ARMv9-march-armv9-a
            flags = f"{self.BOLT_GLOABL_FLAGS} -march=armv9-a"
            tc.variables["CMAKE_C_FLAGS"] = flags
            tc.variables["CMAKE_CXX_FLAGS"] = flags

        if self.options.enable_torch != None:
            tc.cache_variables["BOLT_ENABLE_TORCH"] = "ON"
        else:
            tc.cache_variables["BOLT_ENABLE_TORCH"] = "OFF"

        if self.options.enable_asan:
            tc.cache_variables["CMAKE_CXX_FLAGS"] += (
                " -fsanitize=address -fno-omit-frame-pointer "
            )
            tc.cache_variables["CMAKE_C_FLAGS"] += (
                " -fsanitize=address -fno-omit-frame-pointer "
            )

        tc.cache_variables["TREAT_WARNINGS_AS_ERRORS"] = "OFF"
        tc.cache_variables["ENABLE_ALL_WARNINGS"] = "ON"
        tc.cache_variables["BOLT_ENABLE_PARQUET"] = (
            "ON" if self.options.enable_parquet else "OFF"
        )
        tc.cache_variables["BOLT_ENABLE_ORC"] = (
            "ON" if self.options.enable_orc else "OFF"
        )

        tc.cache_variables["BOLT_ENABLE_TXT"] = (
            "ON" if self.options.enable_txt else "OFF"
        )
        if self.options.get_safe("enable_jit"):
            tc.cache_variables["ENABLE_BOLT_JIT"] = "ON"
            tc.preprocessor_definitions["ENABLE_BOLT_JIT"] = 1

            # TODO: Refactor the IR codegen of expression evaluation
            # Disable it right now
            tc.cache_variables["ENABLE_BOLT_EXPR_JIT"] = "OFF"
        else:
            tc.cache_variables["ENABLE_BOLT_JIT"] = "OFF"
            tc.cache_variables["ENABLE_BOLT_EXPR_JIT"] = "OFF"
            if tc.preprocessor_definitions.get("ENABLE_BOLT_JIT", None) is not None:
                del tc.preprocessor_definitions["ENABLE_BOLT_JIT"]

        tc.cache_variables["ENABLE_META_SORT"] = self.options.get_safe(
            "enable_meta_sort"
        )
        if self.options.get_safe("enable_meta_sort"):
            tc.preprocessor_definitions["ENABLE_META_SORT"] = 1

            # Just for saving compiling time...
            if str(self.settings.build_type) == "Debug":
                # del ENABLE_META_SORT key if exists
                if (
                    tc.preprocessor_definitions.get("ENABLE_META_SORT", None)
                    is not None
                ):
                    del tc.preprocessor_definitions["ENABLE_META_SORT"]
        else:
            if tc.preprocessor_definitions.get("ENABLE_META_SORT", None) is not None:
                del tc.preprocessor_definitions["ENABLE_META_SORT"]

        # enable_exception_trace
        tc.cache_variables["ENABLE_EXCEPTION_TRACE"] = self.options.get_safe(
            "enable_exception_trace"
        )
        if self.options.get_safe("enable_exception_trace"):
            tc.preprocessor_definitions["ENABLE_EXCEPTION_TRACE"] = 1
        else:
            if (
                tc.preprocessor_definitions.get("ENABLE_EXCEPTION_TRACE", None)
                is not None
            ):
                del tc.preprocessor_definitions["ENABLE_EXCEPTION_TRACE"]

        if self.options.es_build:
            tc.cache_variables["BOLT_ENABLE_SIMDJSON"] = "ON"

        if self.options.python_bind:
            tc.cache_variables["BOLT_BUILD_PYTHON_PACKAGE"] = "ON"

        if self.options.enable_arrow_connector:
            tc.cache_variables["BOLT_ENABLE_ARROW_CONNECTOR"] = "ON"

        if self.options.spark_compatible:
            tc.cache_variables["BOLT_ENABLE_SPARK_COMPATIBLE"] = "ON"

        if self.options.get_safe("enable_testutil"):
            tc.cache_variables["BOLT_ENABLE_DUCKDB"] = "ON"
            tc.cache_variables["BOLT_BUILD_TEST_UTILS"] = "ON"
        if self.options.get_safe("enable_test"):
            tc.cache_variables["BOLT_BUILD_TESTING"] = "ON"
        else:
            tc.cache_variables["BOLT_BUILD_TESTING"] = "OFF"
            self.output.info("BOLT_BUILD_TESTING is disabled")

        if self.options.get_safe("build_benchmark") == "on":
            tc.cache_variables["BOLT_BUILD_BENCHMARKS"] = "ON"
            self.output.info("BOLT_BUILD_BENCHMARKS is enabled")
        elif self.options.get_safe("build_benchmark") == "basic":
            tc.cache_variables["BOLT_BUILD_BENCHMARKS_BASIC"] = "ON"
            self.output.info("BOLT_BUILD_BENCHMARKS_BASIC is enabled")
        else:
            self.output.info("BOLT_BUILD_BENCHMARKS is disabled")

        if self.options.get_safe("enable_coverage"):
            tc.cache_variables["BOLT_BUILD_TESTING_WITH_COVERAGE"] = "ON"
        else:
            tc.cache_variables["BOLT_BUILD_TESTING_WITH_COVERAGE"] = "OFF"
            self.output.info("BOLT_BUILD_TESTING_WITH_COVERAGE is disabled")

        tc.cache_variables["BOLT_ENABLE_HDFS"] = "OFF"
        if self.options.get_safe("enable_hdfs"):
            tc.cache_variables["BOLT_ENABLE_HDFS"] = "ON"
        if self.options.get_safe("enable_s3"):
            tc.cache_variables["BOLT_ENABLE_S3"] = "ON"

        if self.options.enable_color:
            tc.cache_variables["BOLT_FORCE_COLORED_OUTPUT"] = "ON"
        if self.options.enable_crc:
            tc.cache_variables["BOLT_ENABLE_CRC"] = "ON"
        if self.options.enable_colocate:
            tc.cache_variables["BOLT_ENABLE_COLOCATE_FUNCTIONS"] = "ON"
        # io_uring
        if self.io_uring_supported():
            tc.cache_variables["KERNEL_SUPPORTS_IO_URING"] = "ON"
        else:
            tc.cache_variables["KERNEL_SUPPORTS_IO_URING"] = "OFF"

        if self.options.get_safe("enable_perf"):
            tc.cache_variables["BOLT_ENABLE_PERF"] = "ON"

        tc.generate()

        # In case there are dependencies listed on build_requirements,
        # VirtualBuildEnv should be used
        tc = VirtualBuildEnv(self)
        tc.generate(scope="build")

        # generate conantoolchain.cmake & xxx-config.cmake
        CMakeDeps(self).generate()

    def build(self):
        num_threads = os.getenv("NUM_THREADS", "4")
        self.output.info(f"Building with num_threads={num_threads}")
        self.conf.define("tools.build:jobs", int(num_threads))

        cmake = CMake(self)
        cmake.configure()
        targets = None
        target_option = self.options.get_safe("targets")
        if target_option != None:  # target is an internal class with __eq__ override
            targets = str(target_option).split(",")
            self.output.info(f"Building targets: {targets}")
        cmake.build(target=targets)
        self.generate_dep_graph()

    def generate_dep_graph(self):
        dot_file = os.path.join(self.build_folder, "graph", "bolt.dot")
        graphviz_command = f"cmake -S {self.source_folder} -B {self.build_folder} --graphviz={dot_file}"
        self.run(graphviz_command, cwd=self.build_folder)
        # generate dependency graph
        dep_path = os.path.join(self.build_folder, "deps")
        deps = {
            "deps": DepLoader().get_bolt_dep(dot_file),
            "thirdparties": DepLoader().get_third_party_dep(dot_file),
        }
        os.makedirs(dep_path, exist_ok=True)
        with open(os.path.join(dep_path, f"dep.json"), "w") as f:
            json.dump(deps, f, indent=4)

    def split_name_version(self, pkg_str):
        parts = pkg_str.split("/", 1)
        return parts

    def package(self):
        files.copy(
            self,
            "LICENSE",
            src=self.source_folder,
            dst=os.path.join(self.package_folder, "licenses"),
        )
        files.copy(
            self,
            "CONTRIBUTING.md",
            src=self.source_folder,
            dst=os.path.join(self.package_folder, "licenses"),
        )
        files.copy(
            self,
            "README.md",
            src=self.source_folder,
            dst=os.path.join(self.package_folder, "licenses"),
        )

        cmake = CMake(self)
        cmake.configure()
        cmake.install()

    def package_info(self):
        self.cpp_info.set_property("cmake_target_name", "bolt::bolt")
        self.cpp_info.set_property(
            "cmake_file_name", "bolt"
        )  # generates config file bolt-config.cmake
        self.cpp_info.set_property(
            "cmake_module_file_name", "bolt"
        )  # generates Findbolt.cmake
        with open(os.path.join(self.package_folder, "deps", "dep.json"), "r") as f:
            deps = json.load(f)
        components_graph_ = deps["deps"]
        bolt_components_third_parties = deps["thirdparties"]

        testutils_components_graph_ = {}
        # if expose test utils for presto cpp worker,
        # if not self.options.spark_compatible:
        if self.options.enable_testutil:
            components_graph_.update(testutils_components_graph_)

        for comp, deps in components_graph_.items():
            # Special handling for interface libraries
            if comp == "bolt_type" or comp == "bolt_type_headers":  
                self.cpp_info.components[comp].libs = []
            else:
                self.cpp_info.components[comp].libs = [comp]
            for dep in deps:
                self.cpp_info.components[comp].requires.append(dep)

            self.cpp_info.components[comp].set_property(
                "cmake_target_name", f"bolt::{comp}"
            )
            self.cpp_info.components[comp].set_property("pkg_config_name", f"lib{comp}")

        for comp, thirdparties in bolt_components_third_parties.items():
            self.cpp_info.components[comp].requires.extend(
                self._cmake_target_to_conan_pkgname(thirdparties)
            )
        self.cpp_info.requires.extend(
            [
                "arrow::arrow",
                "folly::folly",
                "simdjson::simdjson",
                "sonic-cpp::sonic-cpp",
                "protobuf::protobuf",
                "re2::re2",
                "gtest::gtest",
                "icu::icu",
                "xsimd::xsimd",  # Adjust if the package defines a different target name
                "cityhash::cityhash",
                "xxhash::xxhash",
                "fmt::fmt",
                "ryu::ryu",
                "cpr::cpr",
                "timsort::timsort",
                "utf8proc::utf8proc",
                "date::date",
                "openssl::openssl",
                "duckdb::duckdb",
                "libunwind::libunwind",
                "snappy::snappy",
                "glog::glog",
                "thrift::thrift",
                "roaring::roaring",
                "boost::boost",
                "liburing::liburing",
                "zlib::zlib",
                "libbacktrace::libbacktrace",
            ]
        )

    def _cmake_target_to_conan_pkgname(self, deps_list):
        if not isinstance(deps_list, list):
            raise "error in third parties"
        pkg_list = []
        for tgt_name in deps_list:
            # corner case:
            # in the dot files generated by cmake
            # the name of target with alias would look like: "xsimd\\n(xsimd::xsimd)"
            # Note: instead of dot file,
            # it would be better to generate dependency file using a cmake function
            if "(" in tgt_name and ")" in tgt_name:
                l = tgt_name.find("(")
                r = tgt_name.find(")")
                tgt_name = tgt_name[l + 1 : r]

            # here, conan's API seems a bit weird
            pkg = tgt_name.lower()

            # check if is the direct depended 3rd parties
            direct_visible_host = self.dependencies.filter(
                {"build": False, "visible": True, "direct": True}
            )
            prj_requires = [
                str(r).split("/")[0] for r in direct_visible_host.values()
            ]  # openssl/1.1.1

            if pkg == "protobuf::protoc":
                continue

            if pkg.split("::")[0] in prj_requires:
                pkg_list.append(pkg)

        return pkg_list

    def imports(self):
        if self.options.get_safe("es_build"):
            self.copy("*.dll", "bin", "bin")
            self.copy("*.dylib*", "lib", "lib")
            self.copy("*.so*", "lib", "lib")
