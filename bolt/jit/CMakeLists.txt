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

bolt_add_library(
  bolt_thrustjit CompiledModule.cpp
                 ThrustJITv2.cpp
                 aggregation/HashAggrJit.cpp
                 aggregation/ops/CountOps.cpp
                 aggregation/ops/MinMaxOps.cpp
                 aggregation/ops/SumOps.cpp
                 aggregation/ops/AvgOps.cpp
                 aggregation/ops/DecimalSumOps.cpp
                 aggregation/ops/DecimalAvgOps.cpp
                 RowContainer/RowContainerCodeGenerator.cpp
                 RowContainer/RowEqVectorsCodeGenerator.cpp
)

target_link_libraries(bolt_thrustjit PUBLIC llvm-core::llvm-core date::date fmt::fmt Folly::folly)

# Optional: report JIT-generated symbols to Intel VTune via the JIT Profiling
# API (libjitprofiling). Enable with -DBOLT_ENABLE_VTUNE_JIT=ON and point
# VTUNE_SDK_DIR at the VTune SDK (e.g. /opt/intel/oneapi/vtune/<ver>/sdk).
# Runtime reporting is further gated by the BOLT_JIT_VTUNE env var.
if(BOLT_ENABLE_VTUNE_JIT)
  if(NOT VTUNE_SDK_DIR)
    set(VTUNE_SDK_DIR "/opt/intel/oneapi/vtune/2023.2.0/sdk")
  endif()
  find_path(VTUNE_JIT_INCLUDE_DIR jitprofiling.h
    HINTS "${VTUNE_SDK_DIR}/include")
  find_library(VTUNE_JIT_LIBRARY jitprofiling
    HINTS "${VTUNE_SDK_DIR}/lib64" "${VTUNE_SDK_DIR}/lib32")
  if(NOT VTUNE_JIT_INCLUDE_DIR OR NOT VTUNE_JIT_LIBRARY)
    message(FATAL_ERROR
      "BOLT_ENABLE_VTUNE_JIT=ON but jitprofiling.h/libjitprofiling not found "
      "under VTUNE_SDK_DIR=${VTUNE_SDK_DIR}")
  endif()
  target_compile_definitions(bolt_thrustjit PRIVATE BOLT_ENABLE_VTUNE_JIT)
  target_include_directories(bolt_thrustjit PRIVATE "${VTUNE_JIT_INCLUDE_DIR}")
  # libjitprofiling needs dl/pthread at link time.
  target_link_libraries(bolt_thrustjit PRIVATE "${VTUNE_JIT_LIBRARY}" ${CMAKE_DL_LIBS})
  message(STATUS "bolt_thrustjit: VTune JIT profiling enabled (${VTUNE_JIT_LIBRARY})")
endif()

target_compile_options(
  bolt_thrustjit PRIVATE $<$<COMPILE_LANG_AND_ID:CXX,Clang,GNU>:-Werror=return-type>
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
