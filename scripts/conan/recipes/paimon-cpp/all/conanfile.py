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

from conan import ConanFile
from conan.tools.files import apply_conandata_patches, export_conandata_patches
from conan.tools.scm import Git
import os

from conan.tools.cmake import CMake, CMakeToolchain, CMakeDeps, cmake_layout
from conan.tools.files import copy


class PaimonCppConan(ConanFile):
    """Paimon C++ Conan recipe."""

    name = "paimon-cpp"
    package_type = "library"
    license = "Apache-2.0"
    url = "https://github.com/alibaba/paimon-cpp"
    description = "Paimon C++ core library and optional plugins"
    topics = ("paimon", "lakehouse", "arrow", "parquet", "orc")

    settings = "os", "arch", "compiler", "build_type"

    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "with_orc": [True, False],
        "with_avro": [True, False],
        "with_lance": [True, False],
        "with_jindo": [True, False],
        "with_lumina": [True, False],
        "with_lucene": [True, False],
    }

    default_options = {
        "shared": True,
        "fPIC": True,
        "with_avro": True,
        "with_orc": False,
        "with_lance": False,
        "with_jindo": False,
        "with_lumina": False,
        "with_lucene": False,
    }

    def config_options(self):
        if self.settings.os == "Windows":
            self.options.rm_safe("fPIC")

    def requirements(self):
        """Declare dependencies for the Conan dependency graph."""
        # Core dependencies
        self.requires("arrow/15.0.1-oss", transitive_headers=True, transitive_libs=True)
        self.requires("fmt/9.0.0")
        self.requires("glog/0.7.1")
        self.requires("rapidjson/cci.20250205", force=True)
        self.requires("zlib/1.2.13")
        self.requires("zstd/1.5.7")
        self.requires("lz4/1.9.4")
        self.requires("snappy/1.2.1")

        # Optional dependencies
        if bool(self.options.with_orc):
            # protobuf is only required when ORC is enabled
            self.requires("protobuf/3.21.4")
        if bool(self.options.with_avro):
            pass

    def source(self):
        """Clone paimon-cpp source from GitHub and apply patches."""
        data = self.conan_data["sources"][self.version]
        git_url = data["url"]
        expected_commit = data["revision"]

        git = Git(self)
        git.fetch_commit(git_url, expected_commit)

        apply_conandata_patches(self)

    def export_sources(self):
        export_conandata_patches(self)

    def layout(self):
        cmake_layout(self)

    def _use_cxx11_abi(self) -> bool:
        compiler = str(self.settings.compiler)
        if compiler not in ("gcc", "clang", "apple-clang"):
            return True

        # Conan encodes the libstdc++ ABI choice here.
        libcxx = str(getattr(self.settings.compiler, "libcxx", ""))
        if libcxx == "libstdc++":
            return False
        return True

    def generate(self):
        tc = CMakeToolchain(self)

        flags = "-Wno-error=uninitialized -Wno-error=maybe-uninitialized"
        tc.cache_variables["CMAKE_CXX_FLAGS"] = flags
        tc.cache_variables["CMAKE_C_FLAGS"] = flags

        tc.variables["PAIMON_BUILD_TESTS"] = False
        tc.variables["PAIMON_BUILD_SHARED"] = bool(self.options.shared)
        tc.variables["PAIMON_BUILD_STATIC"] = not bool(self.options.shared)

        tc.variables["PAIMON_ENABLE_ORC"] = bool(self.options.with_orc)
        tc.variables["PAIMON_ENABLE_AVRO"] = bool(self.options.with_avro)
        tc.variables["PAIMON_ENABLE_LANCE"] = bool(self.options.with_lance)
        tc.variables["PAIMON_ENABLE_JINDO"] = bool(self.options.with_jindo)
        tc.variables["PAIMON_ENABLE_LUMINA"] = bool(self.options.with_lumina)
        tc.variables["PAIMON_ENABLE_LUCENE"] = bool(self.options.with_lucene)

        tc.variables["PAIMON_USE_CXX11_ABI"] = self._use_cxx11_abi()

        if "fPIC" in self.options:
            tc.variables["CMAKE_POSITION_INDEPENDENT_CODE"] = bool(self.options.fPIC)
        tc.variables["PAIMON_USE_CONAN_DEPS"] = True
        tc.generate()
        # generate conantoolchain.cmake & xxx-config.cmake
        CMakeDeps(self).generate()

    def configure(self):
        """Adjust transitive dependency options required by recipes."""

        arrow_simd_level = "default"
        if str(self.settings.arch) in ["x86", "x86_64"]:
            arrow_simd_level = "avx2"
        elif str(self.settings.arch) in ["armv8", "arm", "armv9"]:
            arrow_simd_level = "neon"
        arrow = "arrow/*"
        self.options[arrow].parquet = True
        self.options[arrow].dataset_modules = True
        self.options[arrow].acero = True
        self.options[arrow].simd_level = arrow_simd_level
        self.options[arrow].with_re2 = True
        self.options[arrow].with_lz4 = True
        self.options[arrow].with_snappy = True
        self.options[arrow].with_zlib = True
        self.options[arrow].with_json = True
        self.options[arrow].with_zstd = True
        self.options[arrow].with_thrift = True

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(
            self,
            "LICENSE",
            src=self.source_folder,
            dst=os.path.join(self.package_folder, "licenses"),
        )
        copy(
            self,
            "NOTICE",
            src=self.source_folder,
            dst=os.path.join(self.package_folder, "licenses"),
        )

        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        # Expose nice Conan/CMake target names; consumers can also link by lib names.
        self.cpp_info.set_property("cmake_file_name", "Paimon")

        # Core library
        core = self.cpp_info.components["core"]
        core.libs = ["paimon"]
        core.set_property("cmake_target_name", "Paimon::core")
        core.includedirs = ["include"]

        # roaring_bitmap and xxhash are static libs linked into paimon;
        # consumers linking the static paimon lib must also link these.
        core.requires = [
            "tbb",
            "roaring_bitmap",
            "xxhash",
            "arrow::arrow",
            "fmt::fmt",
            "glog::glog",
            "rapidjson::rapidjson",
            "zstd::zstd",
            "lz4::lz4",
            "zlib::zlib",
            "snappy::snappy",
        ]

        # TBB is built from source via ExternalProject (ThirdpartyToolchain)
        tbb_comp = self.cpp_info.components["tbb"]
        tbb_comp.libs = [
            "tbb_debug" if str(self.settings.build_type) == "Debug" else "tbb"
        ]
        tbb_comp.set_property("cmake_target_name", "Paimon::tbb")

        # Internal static libraries (no headers to export — included via paimon's include/)
        rb = self.cpp_info.components["roaring_bitmap"]
        rb.libs = ["roaring_bitmap"]
        rb.set_property("cmake_target_name", "Paimon::roaring_bitmap")

        xh = self.cpp_info.components["xxhash"]
        xh.libs = ["xxhash"]
        xh.set_property("cmake_target_name", "Paimon::xxhash")

        # Many targets link libdl + pthread somewhere in the chain on Linux.
        if str(self.settings.os) == "Linux":
            core.system_libs = ["dl", "pthread"]

        # Always-built plugins in this repo
        fs_local = self.cpp_info.components["fs_local"]
        fs_local.libs = ["paimon_local_file_system"]
        fs_local.requires = ["core"]
        fs_local.set_property("cmake_target_name", "Paimon::fs_local")

        file_index = self.cpp_info.components["file_index"]
        file_index.libs = ["paimon_file_index"]
        file_index.requires = ["core"]
        file_index.set_property("cmake_target_name", "Paimon::file_index")

        global_index = self.cpp_info.components["global_index"]
        global_index.libs = ["paimon_global_index"]
        global_index.requires = ["core", "file_index"]
        global_index.set_property("cmake_target_name", "Paimon::global_index")

        fmt_parquet = self.cpp_info.components["format_parquet"]
        fmt_parquet.libs = ["paimon_parquet_file_format"]
        fmt_parquet.requires = ["core"]
        fmt_parquet.set_property("cmake_target_name", "Paimon::format_parquet")

        fmt_blob = self.cpp_info.components["format_blob"]
        fmt_blob.libs = ["paimon_blob_file_format"]
        fmt_blob.requires = ["core"]
        fmt_blob.set_property("cmake_target_name", "Paimon::format_blob")

        # Optional plugins
        if bool(self.options.with_orc):
            fmt_orc = self.cpp_info.components["format_orc"]
            fmt_orc.libs = ["paimon_orc_file_format"]
            fmt_orc.requires = ["core"]
            fmt_orc.set_property("cmake_target_name", "Paimon::format_orc")

        if bool(self.options.with_avro):
            # Expose internal Avro C++ static library as a component
            avro_comp = self.cpp_info.components["avro"]
            avro_comp.libs = ["avrocpp_s"]
            avro_comp.set_property("cmake_target_name", "Paimon::libavrocpp")
            avro_comp.includedirs = ["include"]

            fmt_avro = self.cpp_info.components["format_avro"]
            fmt_avro.libs = ["paimon_avro_file_format"]
            fmt_avro.requires = ["core", "avro"]
            fmt_avro.set_property("cmake_target_name", "Paimon::format_avro")

        if bool(self.options.with_lance):
            fmt_lance = self.cpp_info.components["format_lance"]
            fmt_lance.libs = ["paimon_lance_file_format"]
            fmt_lance.requires = ["core"]
            fmt_lance.set_property("cmake_target_name", "Paimon::format_lance")

        if bool(self.options.with_jindo):
            fs_jindo = self.cpp_info.components["fs_jindo"]
            fs_jindo.libs = ["paimon_jindo_file_system"]
            fs_jindo.requires = ["core"]
            fs_jindo.set_property("cmake_target_name", "Paimon::fs_jindo")

        if bool(self.options.with_lumina):
            idx_lumina = self.cpp_info.components["index_lumina"]
            idx_lumina.libs = ["paimon_lumina_index"]
            idx_lumina.requires = ["core"]
            idx_lumina.set_property("cmake_target_name", "Paimon::index_lumina")

        if bool(self.options.with_lucene):
            idx_lucene = self.cpp_info.components["index_lucene"]
            idx_lucene.libs = ["paimon_lucene_index"]
            idx_lucene.requires = ["core"]
            idx_lucene.set_property("cmake_target_name", "Paimon::index_lucene")
