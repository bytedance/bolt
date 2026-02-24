#
# Copyright (c) ByteDance Ltd. and/or its affiliates
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
import platform
import re
from conan import ConanFile
from conan.tools import files, scm
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.env import VirtualBuildEnv, VirtualRunEnv

# Now, set options of third parties
postfix = "/*"
folly = f"folly{postfix}"
glog = f"glog{postfix}"
boost = f"boost{postfix}"
arrow = f"arrow{postfix}"
orc = f"orc{postfix}"
icu = f"icu{postfix}"
gperftools = f"gperftools{postfix}"
liburing = f"liburing{postfix}"
llvm_core = f"llvm-core{postfix}"


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


class BoltConan(ConanFile):
    description = """
        Bolt is a C++ acceleration library providing composable, extensible and performant data processing toolkit.
    """

    url = "https://github.com/bytedance/bolt"
    topics = ("vectorized engine", "bytedance")
    homepage = ""
    license = ("Apache-2.0",)
    package_type = "library"

    name = "bolt"
    settings = "os", "arch", "compiler", "build_type"
    PB_VERSION = "3.21.4"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "python_bind": [True, False],
        "spark_compatible": [True, False],
        "enable_testutil": [True, False],
        # format options
        "enable_parquet": [True, False],
        "enable_orc": [True, False],
        "enable_txt": [True, False],
        # file system options
        "enable_hdfs": [True, False],
        "enable_s3": [True, False],
        "use_arrow_hdfs": [True, False],
        "enable_asan": [True, False],
        "enable_jit": [True, False],
        "es_build": [True, False],
        "ldb_build": [True, False],
        "enable_arrow_connector": [True, False],
        "enable_crc": [True, False],
        "enable_colocate": [True, False],
        "io_uring_supported": [True, False],
        "enable_torch": TorchOption.all(),
        "enable_perf": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "python_bind": False,
        "enable_asan": False,
        # presto cpp worker needs bolt's testutil for ut
        "enable_testutil": True,
        "enable_parquet": True,
        "enable_orc": True,
        "enable_txt": True,
        # file system options
        "enable_hdfs": True,
        "enable_s3": False,
        "use_arrow_hdfs": True,
        "enable_arrow_connector": False,
        "enable_jit": True,
        "spark_compatible": False,
        "es_build": False,
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
    BOLT_GLOBAL_FLAGS = "-Werror=return-type"

    build_policy = "missing"

    scm_url = "https://github.com/bytedance/bolt.git"

    def source(self):
        git = scm.Git(self)

        # by default, use main branch
        git.clone(self.scm_url, target=".")

        # if use 'stable" channel, we should use a git release tag.
        # TODO: Remove it since Conan 2.0 is no longer recommending to use
        # variable users and channels
        if self.channel and self.channel == "stable":
            if not self.version:
                raise "Do specify a tag for a stable release."
            cmd = f"tags/{self.version} -b tag-{self.version}"
            git.checkout(cmd)
        else:
            scm_branch = self.version
            if scm_branch != "main":
                cmd = f"-b {scm_branch} origin/{scm_branch}"
                git.checkout(cmd)

    def io_uring_supported(self):
        if not self.options.io_uring_supported:
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
        self.output.info("OS is not Linux. io_uring is not supported.")
        return False

    def requirements(self):
        protobuf_version = os.getenv("PROTOBUF_VERSION", "3.21.4")
        self.requires(
            f"folly/{self.FB_VERSION}", transitive_headers=True, transitive_libs=True
        )
        self.requires("arrow/15.0.1-oss", transitive_headers=True, transitive_libs=True)
        if self.options.get_safe("enable_jit"):
            self.requires("llvm-core/13.0.0")

        if self.options.get_safe("enable_s3"):
            self.requires(
                "aws-sdk-cpp/1.11.692", transitive_headers=True, transitive_libs=True
            )
            self.requires("aws-c-common/0.12.5", force=True)
        self.requires("simdjson/3.12.3", transitive_headers=True)
        self.requires(
            "sonic-cpp/1.0.2-bolt", transitive_headers=True, transitive_libs=True
        )
        self.requires(
            f"protobuf/{protobuf_version}",
            transitive_headers=True,
            transitive_libs=True,
            force=True,
        )
        self.requires("re2/20230301", transitive_headers=True, transitive_libs=True)
        self.requires(
            "icu/74.2", headers=True, transitive_headers=True, transitive_libs=True
        )
        self.requires(
            "xsimd/9.0.1", transitive_headers=True, transitive_libs=True, force=True
        )
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
            self.requires("grpc/1.50.0")
        # upgrade libcurl from 8.11.1 to 8.12.1 to avoid SIGABRT issue in https://github.com/curl/curl/issues/15725
        self.requires("libcurl/8.12.1", override=True)
        if (
            self.options.enable_torch is not None
            and self.options.enable_torch.value is not None
        ):
            self.requires(
                "libtorch/2.6.0", options={"torch": self.options.enable_torch}
            )
        if self.settings.os in ["Linux", "FreeBSD"]:
            if self.options.get_safe("enable_perf"):
                self.requires("gperftools/2.16")
                self.requires("libunwind/1.8.0", override=True)
            else:
                self.requires("libunwind/1.8.0")
        self.requires("utf8proc/2.11.0", transitive_headers=True, transitive_libs=True)
        self.requires("date/3.0.4-bolt", transitive_headers=True, transitive_libs=True)
        self.requires("libbacktrace/cci.20210118")
        if self.options.get_safe("spark_compatible"):
            self.requires("celeborn-cpp-client/main-20251212")
        if self.options.get_safe("enable_testutil"):
            self.requires("gtest/1.17.0", force=True)
            self.requires("duckdb/0.8.1")

    def build_requirements(self):
        self.tool_requires("m4/1.4.19")
        self.tool_requires("bison/3.8.2")
        self.tool_requires("flex/2.6.4")
        self.tool_requires("cmake/3.31.10", override=True)
        self.tool_requires("ninja/1.11.1")
        self.tool_requires("protobuf/<host_version>")
        self.tool_requires("thrift/<host_version>")
        if os.getenv("BOLT_BUILD_TESTING", "OFF") == "ON":
            self.test_requires("jemalloc/5.3.0")

    def layout(self):
        cmake_layout(self, build_folder="_build")

    def config_options(self):
        pass

    # Set default options of third parties here
    def configure(self):
        self.options[glog].with_unwind = False
        if self.options.get_safe("es_build"):
            self.options[glog].shared = True

        self.options[boost].without_test = True

        if not self.options.python_bind:
            self.options[boost].without_stacktrace = True

        if self.options.get_safe("enable_s3"):
            s3_opt = self.options["aws-sdk-cpp/*"]
            setattr(s3_opt, "text-to-speech", False)

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
        if self.options.get_safe("enable_jit"):
            self.options[llvm_core].with_libedit = False
            self.options[llvm_core].with_xml2 = False
            self.options[llvm_core].with_z3 = False
            self.options[llvm_core].with_zstd = False
            self.options[llvm_core].with_ffi = False

        if self.options.get_safe("enable_hdfs") and self.options.get_safe(
            "use_arrow_hdfs"
        ):
            self.options[arrow].with_hdfs = True

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
        build_env.generate(scope="build")

        run_env = VirtualRunEnv(self)
        run_env.generate()

        num_link_job = os.getenv("NUM_LINK_JOB", "4")

        tc = CMakeToolchain(self, generator="Ninja")

        tc.cache_variables["MAX_LINK_JOBS"] = num_link_job

        if str(self.settings.arch) in ["x86", "x86_64"]:
            flags = (
                f"{self.BOLT_GLOBAL_FLAGS} -mavx2 -mfma -mavx -mf16c -mlzcnt -mbmi2 "
            )
            tc.cache_variables["CMAKE_CXX_FLAGS"] = flags
            tc.cache_variables["CMAKE_C_FLAGS"] = flags

        if str(self.settings.arch) in ["armv8", "arm", "armv9"]:
            flags = self._get_arm_cpu_flags()
            tc.cache_variables["CMAKE_CXX_FLAGS"] = flags
            tc.cache_variables["CMAKE_C_FLAGS"] = flags
        if (
            self.options.enable_torch is not None
            and self.options.enable_torch.value is not None
        ):
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

        # hdfs file system, arrow implement as default
        if self.options.get_safe("enable_hdfs"):
            tc.cache_variables["BOLT_ENABLE_HDFS"] = "ON"
            tc.cache_variables["BOLT_USE_ARROW_HDFS"] = "OFF"
            if self.options.get_safe("use_arrow_hdfs"):
                tc.cache_variables["BOLT_USE_ARROW_HDFS"] = "ON"
        else:
            tc.cache_variables["BOLT_ENABLE_HDFS"] = "OFF"
            tc.cache_variables["BOLT_USE_ARROW_HDFS"] = "OFF"

        tc.cache_variables["BOLT_ENABLE_S3"] = "OFF"
        if self.options.get_safe("enable_s3"):
            tc.cache_variables["BOLT_ENABLE_S3"] = "ON"

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

        # for CI / testing / benchmarks
        if os.getenv("BOLT_BUILD_TESTING", "OFF") == "ON":
            tc.cache_variables["BOLT_BUILD_TESTING"] = "ON"
        if os.getenv("BOLT_BUILD_BENCHMARKS_BASIC", "OFF") == "ON":
            tc.cache_variables["BOLT_BUILD_BENCHMARKS_BASIC"] = "ON"
        if os.getenv("BOLT_BUILD_BENCHMARKS", "OFF") == "ON":
            tc.cache_variables["BOLT_BUILD_BENCHMARKS"] = "ON"
        if os.getenv("BOLT_BUILD_TESTING_WITH_COVERAGE", "OFF") == "ON":
            tc.cache_variables["BOLT_BUILD_TESTING"] = "ON"
            tc.cache_variables["BOLT_BUILD_TESTING_WITH_COVERAGE"] = "ON"

        tc.generate()

        # generate conantoolchain.cmake & xxx-config.cmake
        CMakeDeps(self).generate()

    def build(self):
        num_threads = os.getenv("NUM_THREADS", "4")
        self.output.info(f"Building with num_threads={num_threads}")
        self.conf.define("tools.build:jobs", int(num_threads))

        cmake = CMake(self)
        cmake.configure()
        if os.getenv("BOLT_CONAN_CONFIGURE_ONLY") == "1":
            self.output.info(
                f"✓ compile_commands.json at {self.build_folder}/compile_commands.json"
            )
        else:
            cmake.build()

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
        self.cpp_info.set_property("cmake_file_name", "bolt")
        self.cpp_info.set_property("cmake_module_file_name", "bolt")

        self.cpp_info.components["bolt_engine"].libs = ["bolt_engine"]
        self.cpp_info.components["bolt_engine"].set_property(
            "cmake_target_name", "bolt::bolt_engine"
        )
        self.cpp_info.components["bolt_engine"].requires.extend(
            [
                "arrow::arrow",
                "folly::folly",
                "simdjson::simdjson",
                "sonic-cpp::sonic-cpp",
                "protobuf::protobuf",
                "re2::re2",
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
        if self.options.get_safe("enable_jit"):
            self.cpp_info.components["bolt_engine"].requires.append(
                "llvm-core::llvm-core"
            )
            self.cpp_info.components["bolt_engine"].exelinkflags.append(
                "-Wl,--export-dynamic-symbol=jit_*"
            )
        if self.options.get_safe("enable_s3"):
            self.cpp_info.components["bolt_engine"].requires.append(
                "aws-c-common::aws-c-common"
            )
        if self.options.get_safe("spark_compatible"):
            self.cpp_info.components["bolt_engine"].requires.append(
                "celeborn-cpp-client::celeborn-cpp-client"
            )

        if self.options.get_safe("enable_testutil"):
            self.cpp_info.components["bolt_testutils"].libs = ["bolt_testutils"]
            self.cpp_info.components["bolt_testutils"].set_property(
                "cmake_target_name", "bolt::bolt_testutils"
            )
            self.cpp_info.components["bolt_testutils"].requires = [
                "bolt_engine",
                "gtest::gtest",
                "duckdb::duckdb",
            ]

    def _get_arm_cpu_flags(self) -> str:
        """
        Detect specific ARM CPU and return optimal compiler flags.

        Detection:
        Apple Silicon on Darwin -> -mcpu=apple-m1+crc
        Linux ARM64 via MIDR_EL1 -> specific -mcpu flags
        Fallback to generic -march flags
        """
        base_flags = self.BOLT_GLOBAL_FLAGS

        # Apple Silicon detection (macOS)
        # Note: Conan uses "Macos" not "macOS", see https://docs.conan.io/2/reference/config_files/settings.html
        if self.settings.os == "Macos" and platform.machine() == "arm64":
            self.output.info("Detected Apple Silicon, using -mcpu=apple-m1+crc")
            return f"{base_flags} -mcpu=apple-m1+crc"

        # Linux ARM64 detection via MIDR_EL1
        if self.settings.os == "Linux":
            midr_path = "/sys/devices/system/cpu/cpu0/regs/identification/midr_el1"
            try:
                with open(midr_path, "r") as f:
                    midr_value = int(f.read().strip(), 16)

                # Extract PartNum (bits 15:4) and Implementer (bits 31:24)
                part_num = (midr_value >> 4) & 0xFFF
                implementer = (midr_value >> 24) & 0xFF

                # CPU flag mapping based on PartNum
                cpu_flags_map = {
                    0xD0C: "neoverse-n1",  # AWS Graviton2, Ampere Altra
                    0xD49: "neoverse-n2",  # AWS Graviton3
                    0xD40: "neoverse-v1",  # Neoverse V1
                    0xD4F: "neoverse-v2",  # AWS Graviton4, NVIDIA Grace
                }

                if part_num in cpu_flags_map:
                    cpu_name = cpu_flags_map[part_num]
                    mcpu_flag = f"-mcpu={cpu_name}"

                    # NVIDIA Grace (Neoverse V2 with NVIDIA implementer)
                    if part_num == 0xD4F and implementer == 0x4E:
                        mcpu_flag += "+crypto+sha3+sm4+sve2-aes+sve2-sha3+sve2-sm4"
                        self.output.info(
                            f"Detected NVIDIA Grace CPU, using {mcpu_flag}"
                        )
                    else:
                        self.output.info(f"Detected ARM {cpu_name}, using {mcpu_flag}")

                    return f"{base_flags} {mcpu_flag}"
                else:
                    self.output.info(
                        f"Unknown ARM CPU (PartNum: 0x{part_num:x}), using fallback"
                    )
            except (FileNotFoundError, PermissionError, ValueError) as e:
                self.output.warning(f"Could not detect ARM CPU via MIDR_EL1: {e}")

        # Fallback based on arch setting (preserves original behavior)
        if str(self.settings.arch) == "armv9":
            self.output.info("Using fallback -march=armv9-a")
            return f"{base_flags} -march=armv9-a"
        else:
            self.output.info("Using fallback -march=armv8.3-a")
            return f"{base_flags} -march=armv8.3-a"
