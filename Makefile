# Copyright (c) Facebook, Inc. and its affiliates.
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
# --------------------------------------------------------------------------
# Copyright (c) ByteDance Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0
#
# This file has been modified by ByteDance Ltd. and/or its affiliates on
# 2025-11-11.
#
# Original file was released under the Apache License 2.0,
# with the full license text available at:
#     http://www.apache.org/licenses/LICENSE-2.0
#
# This modified file is released under the same license.
# --------------------------------------------------------------------------

# --- 1. Standard Lifecycle Targets ---
# all: Default build; clean: Remove artifacts; help: Show usage; install: Install package
.PHONY: all clean help system_info install

# --- 2. Development Tools & Setup ---
# Includes code formatting, Conan dependency installation/build, and compilation DB generation
.PHONY: clang-format-check conan_install conan_build _compile_db compile_db_all

# --- 3. Conan Package Export ---
# Export the built package to the local Conan cache
.PHONY: export_base export_debug export_release

# --- 4. Build Configurations & Variants ---
# Covers Debug/Release, Spark compatibility, ASAN checks, and builds with test utilities
.PHONY: debug release RelWithDebInfo debug-with-asan
.PHONY: debug_spark release_spark
.PHONY: release_with_test release_with_debug_info_with_test
.PHONY: debug_with_test debug_with_test_spark debug_with_test_cov
.PHONY: debug_spark_with_test release_spark_with_test

# --- 5. Benchmark Build Targets ---
.PHONY: benchmarks-basic-build benchmarks-build
.PHONY: benchmarks-build-spark benchmarks-build-debug

# --- 6. Test Execution & Coverage ---
# Targets for running CTest and generating code coverage reports
.PHONY: unittest unittest_debug unittest_release
.PHONY: unittest_release_spark unittest_debug_spark unittest_coverage

# ---------- Conan variables definition starts ----------
# If built on SCM use date as version
# $(shell date '+%Y.%m.%d.00')
BUILD_VERSION ?= main
BUILD_USER ?=
BUILD_CHANNEL ?=
# Use commas to separate multiple file systems, such as `hdfs,tos`
ENABLE_HDFS ?= True
ENABLE_S3 ?= False
USE_ARROW_HDFS ?= True
ENABLE_ASAN ?= False
LDB_BUILD ?= False
ENABLE_COLOR ?= True
ENABLE_CRC ?= False
ENABLE_PERF ?= False

BUILD_BASE_DIR=_build
BUILD_TYPE=Release
BENCHMARKS_DUMP_DIR=dumps
TREAT_WARNINGS_AS_ERRORS ?= 0
ENABLE_WALL ?= 1
ENABLE_JEMALLOC_PROF ?= False
ENABLE_COLOCATE ?= False
ENABLE_META_SORT ?= False
PROFILE=default

GLUTEN_BOLT_OPTIONS:=" -o *:spark_compatible=True "
GLUTEN_CONAN_OPTIONS:=$(GLUTEN_BOLT_OPTIONS)

# ---------- Conan variables definition ends ----------

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
    # Linux
    MEMORY ?= $(shell free -g | grep 'Mem:' | awk '{print $$2}')
    FREE_MEMORY ?= $(shell free -g | grep 'Mem:' | awk '{print $$4}')
    CPU_CORES ?= $(shell grep -c 'processor' /proc/cpuinfo)
else ifeq ($(UNAME_S),Darwin)
    # macOS
    MEMORY ?= $(shell sysctl -n hw.memsize | awk '{print int($$1/1024/1024/1024)}')
    FREE_MEMORY ?= $(shell vm_stat | grep "Pages free" | awk '{print int($$3*4096/1024/1024/1024)}')
    CPU_CORES ?= $(shell sysctl -n hw.ncpu)
else
    MEMORY ?= 8
    FREE_MEMORY ?= 4
    CPU_CORES ?= 4
endif

# collect system info
ifeq ($(UNAME_S),Darwin)
    OS_DETAILED   := $(shell sw_vers -productName) $(shell sw_vers -productVersion) ($(shell sw_vers -buildVersion))
    CPU_MODEL     := $(shell sysctl -n machdep.cpu.brand_string)
    MEM_TOTAL     := $(shell sysctl -n hw.memsize | awk '{print int($$1/1024/1024/1024) " GB"}')
else
    OS_DETAILED   := $(shell uname -o 2>/dev/null || uname -s) $(shell uname -r)
    DISTRO_NAME   := $(shell grep -E '^(PRETTY_NAME)=' /etc/os-release 2>/dev/null | cut -d '"' -f 2)
    ifneq ($(DISTRO_NAME),)
        OS_DETAILED += [$(DISTRO_NAME)]
    endif
    CPU_MODEL     := $(shell grep "model name" /proc/cpuinfo | head -n1 | cut -d: -f2 | xargs)
    MEM_TOTAL     := $(shell grep MemTotal /proc/meminfo | awk '{print int($$2/1024/1024) " GB"}')
endif
CONAN_EXE := $(shell command -v conan 2> /dev/null)
CMAKE_EXE := $(shell command -v cmake 2> /dev/null)

# Option to make a minimal build. By default set to "OFF"; set to
# "ON" to only build a minimal set of components. This may override
# other build options
BOLT_BUILD_MINIMAL ?= "OFF"

# Control whether to build unit tests. By default set to "ON"; set to
# "OFF" to disable.
BOLT_BUILD_TESTING ?= "OFF"
BOLT_BUILD_BENCHMARKS ?= "OFF"
BOLT_BUILD_BENCHMARKS_BASIC ?= "OFF"
BOLT_BUILD_TESTING_WITH_COVERAGE ?= "OFF"

export GTEST_COLOR=1

OS:=$(shell uname -s)

ifndef CI_NUM_THREADS
# Make sure each core has 4G memory
	NUM_THREADS ?=$(shell echo $$(( $(CPU_CORES) < $(MEMORY) / 4 ? $(CPU_CORES) : $(MEMORY) / 4 )) )
else
	NUM_THREADS ?= $(CI_NUM_THREADS)
endif

ifndef CI_NUM_LINK_JOB
	_NUM_LINK_JOB_CALC := $(shell echo $$(( $(FREE_MEMORY) / 10 )) )
	_NUM_LINK_JOB_FINAL := $(shell echo $$(( $(_NUM_LINK_JOB_CALC) < 4 ? 4 : $(_NUM_LINK_JOB_CALC) )) )
	NUM_LINK_JOB ?= $(_NUM_LINK_JOB_FINAL)
else
	NUM_LINK_JOB ?= $(CI_NUM_LINK_JOB)
endif

ifeq ($(IN_CI), 1)
	export DEPENDENCY_BUILD_TYPE = Release
endif


CPU_TARGET ?= "avx"

ifeq ($(UNAME_S),Darwin)
    FUZZER_SEED ?= $(shell python3 -c 'import random; print(random.randint(1000000000, 5000000000))')
else
    FUZZER_SEED ?= $(shell shuf -i 1000000000-5000000000 -n 1)
endif

FUZZER_MAX_LEVEL_NEST ?= 5
FUZZER_DURATION_SEC ?= 600
FUZZER_EXPRESSION_REPRO_PERSIST_PATH ?= expression_fuzzer
FUZZER_SPARK_EXPRESSION_REPRO_PERSIST_PATH ?= spark_expression_fuzzer
FUZZER_SPARK_AGGREGATION_REPRO_PERSIST_PATH ?= spark_aggregation_fuzzer
FUZZER_JOIN_REPRO_PERSIST_PATH ?= join_fuzzer
FUZZER_SPARK_WINDOW_REPRO_PERSIST_PATH ?= spark_window_fuzzer
FUZZER_SPARK_WINDOW_FUNCTIONS ?= "nth_value,row_number,rank,dense_rank,ntile"
FUZZER_MAX_STRING_LENGTH ?= 100
FUZZER_BATCH_SIZE ?= 100
FUZZER_NUM_BATCH ?= 5
FUZZER_ENABLE_COMPLEX_TYPES ?= true
FUZZER_ENABLE_STRING_VARIABLE_LENGTH ?= true
FUZZER_ENABLE_STRING_INCREMENTAL_GENERATION ?= true
FUZZER_ENABLE_DUPLICATES ?= true
FUZZER_ENABLE_USER_STACKTRACE ?= true
FUZZER_ENABLE_SPILL ?= true
FUZZER_ENABLE_LOG_SIGNATURE_STATS ?= true
FUZZER_ENABLE_HUGEINT_FOR_JOIN ?= false
FUZZER_ENABLE_DUCKDB_VERIFICATION ?= true

PYTHON_EXECUTABLE ?= $(shell which python)

CONAN_OPTIONS=

all: 			#: Build the release version
	$(MAKE) release

clean:					#: Delete all build artifacts
	rm -rf $(BUILD_BASE_DIR)/Rel* && rm -rf $(BUILD_BASE_DIR)/Debug* && rm -rf CMakeUserPresets.json

# only used in CI
clang-format-check:
	find bolt \( -name "*.cpp" -o -name "*.h" \) -type f > files.txt
	cat files.txt | xargs -I{} -P $(CPU_CORES) clang-format -style=file --dry-run {} > log.txt 2>&1
	cat log.txt && echo -e "You can use clang-format -i -style=file path_to_file command to format file"
	if grep -q 'warning' log.txt; then false; fi
	@rm -f files.txt log.txt

conan_install:
	if [ ! -d "_build" ]; then \
		mkdir _build; \
	fi; \
	git rev-parse HEAD && \
	mkdir -p _build/${BUILD_TYPE} && \
	cd _build/${BUILD_TYPE} && \
	echo " -o bolt/*:enable_hdfs=${ENABLE_HDFS} \
	-o bolt/*:use_arrow_hdfs=${USE_ARROW_HDFS} \
	-o bolt/*:enable_s3=${ENABLE_S3} \
	-o bolt/*:enable_asan=${ENABLE_ASAN} \
	-o bolt/*:enable_perf=${ENABLE_PERF} \
	-o bolt/*:enable_color=${ENABLE_COLOR} \
	-o bolt/*:enable_meta_sort=${ENABLE_META_SORT} \
	-o bolt/*:enable_colocate=${ENABLE_COLOCATE} \
	-o bolt/*:ldb_build=${LDB_BUILD} \
	-o bolt/*:enable_crc=${ENABLE_CRC} \
	-pr ${PROFILE} -pr ../../scripts/conan/bolt.profile \
	${CONAN_OPTIONS}" > new_conan.options && \
	set -x && \
	if [ -f conan.options ] && [ -f ../.build_type ] && cmp -s new_conan.options conan.options && [ "`cat ../.build_type`" = "${BUILD_TYPE}" ]; then \
	  echo "Conan options and build type unchanged; preserving CMakeCache.txt"; \
	else \
	  echo "Conan options and build type changed! deleting CMakeCache.txt"; \
	  rm -f CMakeCache.txt; \
	fi && \
	mv new_conan.options conan.options && \
	echo ${BUILD_TYPE} > ../.build_type && \
	read ALL_CONAN_OPTIONS < conan.options && \
	conan graph info ../.. $${ALL_CONAN_OPTIONS} --format=html > bolt.conan.graph.html  && \
	export NUM_LINK_JOB=$(NUM_LINK_JOB) && \
	conan install ../.. --name=bolt --version=${BUILD_VERSION} --user=${BUILD_USER} --channel=${BUILD_CHANNEL} \
	   -s llvm-core/*:build_type=Release \
	   -s "&:build_type=${BUILD_TYPE}" \
	   -s build_type=$${DEPENDENCY_BUILD_TYPE:-${BUILD_TYPE}} \
	$${ALL_CONAN_OPTIONS} --build=missing && \
	cd -

conan_build: conan_install
	cd _build/${BUILD_TYPE} && \
	read ALL_CONAN_OPTIONS < conan.options && \
	NUM_THREADS=$(NUM_THREADS) \
	BOLT_BUILD_TESTING=${BOLT_BUILD_TESTING} \
	BOLT_BUILD_BENCHMARKS=${BOLT_BUILD_BENCHMARKS} \
	BOLT_BUILD_TESTING_WITH_COVERAGE=${BOLT_BUILD_TESTING_WITH_COVERAGE} \
	conan build ../.. --name=bolt --version=${BUILD_VERSION} --user=${BUILD_USER} --channel=${BUILD_CHANNEL} \
	   -s llvm-core/*:build_type=Release \
	   -s "&:build_type=${BUILD_TYPE}" \
	   -s build_type=$${DEPENDENCY_BUILD_TYPE:-${BUILD_TYPE}} \
	   --build=missing $${ALL_CONAN_OPTIONS} && \
	cd -

_compile_db: conan_install
	cd _build/${BUILD_TYPE} && \
	read ALL_CONAN_OPTIONS < conan.options && \
	NUM_THREADS=$(NUM_THREADS) \
	BOLT_BUILD_TESTING=${BOLT_BUILD_TESTING} \
	BOLT_BUILD_BENCHMARKS=${BOLT_BUILD_BENCHMARKS} \
	BOLT_CONAN_CONFIGURE_ONLY=1 \
	conan build ../.. --name=bolt --version=${BUILD_VERSION} --user=${BUILD_USER} --channel=${BUILD_CHANNEL} \
	   -s llvm-core/*:build_type=Release \
	   -s "&:build_type=${BUILD_TYPE}" \
	   -s build_type=$${DEPENDENCY_BUILD_TYPE:-${BUILD_TYPE}} \
	   --build=missing $${ALL_CONAN_OPTIONS} && \
	cd - && \
	cmake --build --preset conan-$$(echo "${BUILD_TYPE}" | tr [A-Z] [a-z]) --target generate_parquet_thrift

compile_db_all:
	$(MAKE) _compile_db \
	BUILD_TYPE=Release \
	BOLT_BUILD_TESTING="ON" \
	BOLT_BUILD_BENCHMARKS="ON" \
	CONAN_OPTIONS=" -o bolt/*:spark_compatible=True -o bolt/*:enable_testutil=True"

export_base:
	cd _build/${BUILD_TYPE} && \
	read ALL_CONAN_OPTIONS < conan.options && \
	conan export-pkg --name=bolt --version=${BUILD_VERSION} --user=${BUILD_USER} --channel=${BUILD_CHANNEL} \
	 $${ALL_CONAN_OPTIONS} \
	 -s llvm-core/*:build_type=Release \
	 -s "&:build_type=${BUILD_TYPE}" \
	 -s build_type=$${DEPENDENCY_BUILD_TYPE:-${BUILD_TYPE}} \
	 ../.. && \
	cd -

export_debug:
	$(MAKE) export_base BUILD_TYPE=Debug

export_release:
	$(MAKE) export_base BUILD_TYPE=Release

install:
	$(MAKE) export_base BUILD_TYPE=$(shell cat _build/.build_type)

debug:      	#: Build with debugging symbols
	$(MAKE) conan_build BUILD_TYPE=Debug CONAN_OPTIONS=" -o bolt/*:spark_compatible=False"

debug-with-asan:  #: Build the debug version with address sanitizer enabled
	$(MAKE) conan_build BUILD_TYPE=Debug CONAN_OPTIONS=" -o enable_asan=True "

release:  	#: Build the release version
	$(MAKE) conan_build BUILD_TYPE=Release CONAN_OPTIONS=" -o bolt/*:spark_compatible=False"

RelWithDebInfo:
	$(MAKE) conan_build BUILD_TYPE=RelWithDebInfo

release_with_test:
	$(MAKE) conan_build BUILD_TYPE=Release BOLT_BUILD_TESTING="ON" CONAN_OPTIONS="-o bolt/*:spark_compatible=False -o bolt/*:enable_testutil=True"

release_with_debug_info_with_test:
	$(MAKE) conan_build BUILD_TYPE=RelWithDebInfo BOLT_BUILD_TESTING="ON" CONAN_OPTIONS="-o bolt/*:spark_compatible=False -o bolt/*:enable_testutil=True"

debug_with_test:
	$(MAKE) conan_build BUILD_TYPE=Debug BOLT_BUILD_TESTING="ON" CONAN_OPTIONS="-o bolt/*:spark_compatible=False -o bolt/*:enable_testutil=True"

debug_with_test_spark:
	$(MAKE) conan_build BUILD_TYPE=Debug BOLT_BUILD_TESTING="ON" CONAN_OPTIONS="-o bolt/*:spark_compatible=True -o bolt/*:enable_testutil=True"

debug_with_test_cov:
	$(MAKE) conan_build BUILD_TYPE=Debug BOLT_BUILD_TESTING="ON" BOLT_BUILD_TESTING_WITH_COVERAGE="ON" CONAN_OPTIONS="-o bolt/*:spark_compatible=False -o bolt/*:enable_testutil=True"

debug_spark:
	$(MAKE) conan_build BUILD_TYPE=Debug CONAN_OPTIONS="-o bolt/*:spark_compatible=True"

release_spark:
	$(MAKE) conan_build BUILD_TYPE=Release CONAN_OPTIONS="-o bolt/*:spark_compatible=True"

release_spark_with_test:
	$(MAKE) conan_build BUILD_TYPE=Release BOLT_BUILD_TESTING="ON" CONAN_OPTIONS="-o bolt/*:spark_compatible=True -o bolt/*:enable_testutil=True"

debug_spark_with_test:
	$(MAKE) conan_build BUILD_TYPE=Debug BOLT_BUILD_TESTING="ON" CONAN_OPTIONS="-o bolt/*:spark_compatible=True -o bolt/*:enable_testutil=True"

benchmarks-basic-build:
	$(MAKE) conan_build BUILD_TYPE=Release  BOLT_BUILD_BENCHMARKS_BASIC="ON" CONAN_OPTIONS="-o bolt/*:spark_compatible=True -o bolt/*:enable_testutil=True -o bolt/*:enable_perf=True"

benchmarks-build:
	$(MAKE) conan_build BUILD_TYPE=Release BOLT_BUILD_BENCHMARKS="ON" CONAN_OPTIONS="-o bolt/*:spark_compatible=False -o bolt/*:enable_testutil=True -o bolt/*:enable_perf=True"

benchmarks-build-spark:
	$(MAKE) conan_build BUILD_TYPE=Release BOLT_BUILD_BENCHMARKS="ON" CONAN_OPTIONS="-o bolt/*:spark_compatible=True -o bolt/*:enable_testutil=True -o bolt/*:enable_perf=True"

benchmarks-build-debug:
	$(MAKE) conan_build BUILD_TYPE=Debug BOLT_BUILD_BENCHMARKS="ON" CONAN_OPTIONS="-o bolt/*:spark_compatible=False -o bolt/*:enable_testutil=True -o bolt/*:enable_perf=True"

unittest_debug: unittest
unittest: debug_with_test
	ctest --test-dir $(BUILD_BASE_DIR)/Debug --timeout 7200 -j $(NUM_THREADS) --output-on-failure

unittest_release: release_with_test
	ctest --test-dir $(BUILD_BASE_DIR)/Release --timeout 7200 -j $(NUM_THREADS) --output-on-failure

unittest_release_spark: release_spark_with_test
	ctest --test-dir $(BUILD_BASE_DIR)/Release --timeout 7200 -j $(NUM_THREADS) --output-on-failure

unittest_debug_spark: debug_spark_with_test
	ctest --test-dir $(BUILD_BASE_DIR)/Debug --timeout 7200 -j $(NUM_THREADS) --output-on-failure

unittest_coverage: debug_with_test_cov		#: Build with debugging and run unit tests
	cd $(BUILD_BASE_DIR)/Debug && \
	lcov --no-external --capture --initial --directory . --output-file coverage_base.info && \
	ctest --timeout 7200 -j $(NUM_THREADS) --output-on-failure && \
	lcov --capture --directory . --output-file coverage_test.info && \
	lcov --add-tracefile coverage_base.info --add-tracefile coverage_test.info --output-file coverage.info && \
	lcov --remove coverage.info '/usr/*' '*/.conan/data/*' '*/_build/*' '*/tests/*' '*/test/*' --output-file coverage_striped.info && \
	genhtml --ignore-errors source coverage_striped.info --output-directory coverage

system_info:
	@echo "----------------------------------------------------------------"
	@echo "## System Configuration Report"
	@echo "*(Auto-generated via 'make report')*"
	@echo ""
	@echo "### 1. Hardware & OS (Performance Context)"
	@echo "- **OS**: $(OS_DETAILED)"
	@echo "- **Arch**: $(shell uname -m)"
	@echo "- **CPU**: $(CPU_MODEL)"
	@echo "- **Cores**: $(CPU_CORES)"
	@echo "- **RAM**: $(MEM_TOTAL)"
	@echo ""
	@echo "### 2. Compiler Toolchain (Build Context)"
	@echo "- **C Compiler**: $(CC)"
	@echo "  - Version: \`$(shell $(CC) --version | head -n 1)\`"
	@echo "- **CXX Compiler**: $(CXX)"
	@echo "  - Version: \`$(shell $(CXX) --version | head -n 1)\`"
ifneq ($(CMAKE_EXE),)
	@echo "- **CMake**: $(shell cmake --version | head -n 1 | awk '{print $$3}')"
	@echo "- **Ninja**: $(shell ninja --version')"
endif
	@echo ""
	@echo "### 3. Conan Dependency Manager"
ifneq ($(CONAN_EXE),)
	@echo "- **Conan Version**: $(shell conan --version)"
	@echo "- **Conan Profile (default)**:"
	@echo "\`\`\`ini"
	@conan profile show default 2>/dev/null || conan profile show 2>/dev/null || echo "Unable to read profile"
	@echo "\`\`\`"
else
	@echo "*Conan not found in PATH*"
endif
	@echo "----------------------------------------------------------------"

help:					#: Show the help messages
	@cat $(firstword $(MAKEFILE_LIST)) | \
	awk '/^[-a-z]+:/' | \
	awk -F: '{ printf("%-20s   %s\n", $$1, $$NF) }'
