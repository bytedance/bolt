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
# Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

.PHONY: all cmake build clean debug release unit submodules

# ---------- Conan variables defination starts ----------
# If built on SCM use date as version
# $(shell date '+%Y.%m.%d.00')
BUILD_VERSION ?= main
BUILD_USER ?= 
BUILD_CHANNEL ?=
# Use commas to separate multiple file systems, such as `hdfs,tos`
FILE_SYSTEM ?= hdfs
ENABLE_ASAN ?= False
LDB_BUILD ?= False
ENABLE_COLOR ?= True
ENABLE_CRC ?= False
ENABLE_EXCEPTION_TRACE ?= True
ENABLE_PERF ?= False

ifeq ($(shell arch), aarch64)
ENABLE_EXCEPTION_TRACE = False
$(info ENABLE_EXCEPTION_TRACE is disabled on ARM platform)
endif

ifeq ($(LDB_BUILD), True)
ENABLE_EXCEPTION_TRACE = False
$(info Turn off ENABLE_EXCEPTION_TRACE when LDB_BUILD is ON)
endif

BUILD_BASE_DIR=_build
BUILD_DIR=release
BUILD_TYPE=Release
BENCHMARKS_BASIC_DIR=$(BUILD_BASE_DIR)/$(BUILD_DIR)/bolt/benchmarks/basic/
BENCHMARKS_DUMP_DIR=dumps
TREAT_WARNINGS_AS_ERRORS ?= 0
ENABLE_WALL ?= 1
ENABLE_JEMALLOC_PROF ?= False
ENABLE_COLOCATE ?= False
ENABLE_META_SORT ?= False
PROFILE=default

GLUTEN_BOLT_OPTIONS:=" -o *:spark_compatible=True "
GLUTEN_CONAN_OPTIONS:=$(GLUTEN_BOLT_OPTIONS)

# ---------- Conan variables defination ends ----------

MEMORY ?=$(shell free -g | grep 'Mem:' | awk '{print $$2}')
FREE_MEMORY ?=$(shell free -g | grep 'Mem:' | awk '{print $$4}')
CPU_CORES ?=$(shell grep -c 'processor' /proc/cpuinfo)


# Option to make a minimal build. By default set to "OFF"; set to
# "ON" to only build a minimal set of components. This may override
# other build options
BOLT_BUILD_MINIMAL ?= "OFF"

# Control whether to build unit tests. By default set to "ON"; set to
# "OFF" to disable.
BOLT_BUILD_TESTING ?= "ON"

CMAKE_FLAGS := -DTREAT_WARNINGS_AS_ERRORS=${TREAT_WARNINGS_AS_ERRORS}
CMAKE_FLAGS += -DENABLE_ALL_WARNINGS=${ENABLE_WALL}

CMAKE_FLAGS += -DBOLT_BUILD_MINIMAL=${BOLT_BUILD_MINIMAL}
CMAKE_FLAGS += -DBOLT_BUILD_TESTING=${BOLT_BUILD_TESTING}

CMAKE_FLAGS += -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)

ifdef AWSSDK_ROOT_DIR
CMAKE_FLAGS += -DAWSSDK_ROOT_DIR=$(AWSSDK_ROOT_DIR)
endif

ifdef GCSSDK_ROOT_DIR
CMAKE_FLAGS += -DGCSSDK_ROOT_DIR=$(GCSSDK_ROOT_DIR)
endif

ifdef AZURESDK_ROOT_DIR
CMAKE_FLAGS += -DAZURESDK_ROOT_DIR=$(AZURESDK_ROOT_DIR)
endif

ifdef BUILD_FOR_GLUTEN
CMAKE_FLAGS += -DBOLT_ENABLE_SPARK_COMPATIBLE=ON
endif

export GTEST_COLOR=1

# Use Ninja if available. If Ninja is used, pass through parallelism control flags.
USE_NINJA ?= 1
ifeq ($(USE_NINJA), 1)
ifneq ($(shell which ninja), )
GENERATOR := -GNinja
GENERATOR += -DMAX_LINK_JOBS=$(MAX_LINK_JOBS)
GENERATOR += -DMAX_HIGH_MEM_JOBS=$(MAX_HIGH_MEM_JOBS)

# Ninja makes compilers disable colored output by default.
GENERATOR += -DBOLT_FORCE_COLORED_OUTPUT=ON
endif
endif

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

CPU_TARGET ?= "avx"

FUZZER_SEED ?= $(shell shuf -i 1000000000-5000000000 -n 1)
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
	rm -rf $(BUILD_BASE_DIR) && rm -rf CMakeUserPresets.json && rm -rf $(BENCHMARKS_BASIC_DIR)

clang-format-check:
	find bolt \( -name "*.cpp" -o -name "*.h" \) -type f > files.txt
	cat files.txt | xargs -I{} -P $(CPU_CORES) clang-format -style=file --dry-run {}  > log.txt 2>&1
	cat log.txt && echo -e "You can use clang-format -i -style=file path_to_file command to format file"
	if grep -q 'warning' log.txt; then false; fi

conan_build:
	if [ ! -d "_build" ]; then \
		mkdir _build; \
	fi; \
	git rev-parse HEAD && \
	mkdir -p _build/${BUILD_TYPE} && \
	rm -f _build/${BUILD_TYPE}/CMakeCache.txt && \
	echo ${BUILD_TYPE} > _build/.build_type && \
	cd _build/${BUILD_TYPE} && \
	echo "-o bolt/*:file_system=${FILE_SYSTEM} \
	-o bolt/*:enable_asan=${ENABLE_ASAN} \
	-o bolt/*:enable_perf=${ENABLE_PERF} \
	-o bolt/*:enable_color=${ENABLE_COLOR} \
	-o bolt/*:enable_meta_sort=${ENABLE_META_SORT} \
	-o bolt/*:enable_colocate=${ENABLE_COLOCATE} \
	-o bolt/*:enable_exception_trace=${ENABLE_EXCEPTION_TRACE} \
	-o bolt/*:ldb_build=${LDB_BUILD} \
	-o bolt/*:enable_crc=${ENABLE_CRC} \
	-pr ${PROFILE} \
	${CONAN_OPTIONS}" > conan.options && \
	read ALL_CONAN_OPTIONS < conan.options && \
	conan graph info ../.. $${ALL_CONAN_OPTIONS} --format=html > bolt.conan.graph.html  && \
	export NUM_LINK_JOB=$(NUM_LINK_JOB) && \
	conan install ../.. --name=bolt --version=${BUILD_VERSION} --user=${BUILD_USER} --channel=${BUILD_CHANNEL} \
	   -s llvm-core/*:build_type=Release -s build_type=${BUILD_TYPE} $${ALL_CONAN_OPTIONS} --build=missing && \
	NUM_THREADS=$(NUM_THREADS) \
	conan build ../.. --name=bolt --version=${BUILD_VERSION} --user=${BUILD_USER} --channel=${BUILD_CHANNEL} \
	   -s llvm-core/*:build_type=Release -s build_type=${BUILD_TYPE} --build=missing $${ALL_CONAN_OPTIONS} && \
	cd -

export_base:
	cd _build/${BUILD_TYPE} && \
	read ALL_CONAN_OPTIONS < conan.options && \
	conan export-pkg --name=bolt --version=${BUILD_VERSION} --user=${BUILD_USER} --channel=${BUILD_CHANNEL} \
	 $${ALL_CONAN_OPTIONS} -s llvm-core/*:build_type=Release -s build_type=${BUILD_TYPE} ../.. && \
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
	$(MAKE) conan_build BUILD_TYPE=Debug CONAN_OPTIONS=" -o bolt/*:enable_test=True -o enable_asan=True"

hdfs-debug-build:			#: Build the debug version with HDFS enabled
	# TODO: Remove after hdfs memory bug is fixed and uncomment the next line
	$(MAKE) debug EXTRA_CMAKE_FLAGS="-DBOLT_ENABLE_HDFS=ON"
	# $(MAKE) debug EXTRA_CMAKE_FLAGS="-DBOLT_ENABLE_ADDRESS_SANITIZER=ON -DBOLT_ENABLE_HDFS=ON"

arrow-vector-debug-build:			#: Build the debug version with HDFS enabled
	$(MAKE) debug EXTRA_CMAKE_FLAGS="-DBOLT_ENABLE_ARROW_VECTORS=ON"

release:  	#: Build the release version
	$(MAKE) conan_build BUILD_TYPE=Release CONAN_OPTIONS=" -o bolt/*:spark_compatible=False"

arrow-bridge-build:      #: Build release version for arrow bridge with arrow enabled
	$(MAKE) release EXTRA_CMAKE_FLAGS="-DBOLT_ENABLE_ARROW=ON -DBOLT_BUILD_TESTING=ON -DTREAT_WARNINGS_AS_ERRORS=OFF"

RelWithDebInfo:
	$(MAKE) conan_build BUILD_TYPE=RelWithDebInfo

release_with_test:
	$(MAKE) conan_build BUILD_TYPE=Release CONAN_OPTIONS=" -o bolt/*:enable_test=True"

release_with_debug_info_with_test:
	$(MAKE) conan_build BUILD_TYPE=RelWithDebInfo CONAN_OPTIONS=" -o bolt/*:enable_test=True"

debug_with_test:
	$(MAKE) conan_build BUILD_TYPE=Debug CONAN_OPTIONS=" -o bolt/*:enable_test=True"

debug_with_test_cov:
	$(MAKE) conan_build BUILD_TYPE=Debug CONAN_OPTIONS=" -o bolt/*:enable_test=True -o bolt/*:enable_coverage=True"

debug_spark:
	$(MAKE) conan_build BUILD_TYPE=Debug CONAN_OPTIONS=$(GLUTEN_CONAN_OPTIONS)

release_spark:
	$(MAKE) conan_build BUILD_TYPE=Release CONAN_OPTIONS=$(GLUTEN_CONAN_OPTIONS)

release_spark_with_test:
	$(MAKE) conan_build BUILD_TYPE=Release CONAN_OPTIONS=$(GLUTEN_CONAN_OPTIONS)" -o bolt/*:enable_test=True"

debug_spark_with_test:
	$(MAKE) conan_build BUILD_TYPE=Debug CONAN_OPTIONS=$(GLUTEN_CONAN_OPTIONS)" -o bolt/*:enable_test=True"

benchmarks-basic-build:
	$(MAKE) conan_build BUILD_TYPE=Release CONAN_OPTIONS=" -o bolt/*:build_benchmark=basic"

benchmarks-build:
	$(MAKE) conan_build BUILD_TYPE=Release CONAN_OPTIONS=" -o bolt/*:build_benchmark=on"

benchmarks-build-spark:
	$(MAKE) conan_build BUILD_TYPE=Release CONAN_OPTIONS=$(GLUTEN_CONAN_OPTIONS)" -o bolt/*:build_benchmark=on"

benchmarks-build-debug:
	$(MAKE) conan_build BUILD_TYPE=Debug CONAN_OPTIONS=" -o bolt/*:build_benchmark=on"

benchmarks-duckdb-build:
	$(MAKE) release EXTRA_CMAKE_FLAGS="-DBOLT_BUILD_DUCKDB_BENCHMARK=ON"

# skip all hdfs test
# skip TimestampWithTimezone register and test
unittest: debug_with_test			#: Build with debugging and run unit tests
	ctest --test-dir $(BUILD_BASE_DIR)/Debug --timeout 7200 -j $(NUM_THREADS) --output-on-failure

unittest_release: release_with_test			#: Build with debugging and run unit tests
	ctest --test-dir $(BUILD_BASE_DIR)/Release --timeout 7200 -j $(NUM_THREADS) --output-on-failure

unittest_release_spark: release_spark_with_test		#: Build with debugging and run unit tests
	ctest --test-dir $(BUILD_BASE_DIR)/Release --timeout 7200 -j $(NUM_THREADS) --output-on-failure

unittest_coverage: debug_with_test_cov		#: Build with debugging and run unit tests
	cd $(BUILD_BASE_DIR)/Debug && \
	lcov --no-external --capture --initial --directory . --output-file coverage_base.info && \
	ctest --timeout 7200 -j $(NUM_THREADS) --output-on-failure && \
	lcov --capture --directory . --output-file coverage_test.info && \
	lcov --add-tracefile coverage_base.info --add-tracefile coverage_test.info --output-file coverage.info && \
	lcov --remove coverage.info '/usr/*' '*/.conan/data/*' '*/_build/*' '*/tests/*' '*/test/*' --output-file coverage_striped.info && \
	genhtml --ignore-errors source coverage_striped.info --output-directory coverage

hdfstest: hdfs-debug-build #: Build with debugging, hdfs enabled and run hdfs tests
	ctest --test-dir $(BUILD_BASE_DIR)/Debug -j ${NUM_THREADS} --output-on-failure -R bolt_hdfs_file_test

help:					#: Show the help messages
	@cat $(firstword $(MAKEFILE_LIST)) | \
	awk '/^[-a-z]+:/' | \
	awk -F: '{ printf("%-20s   %s\n", $$1, $$NF) }'
