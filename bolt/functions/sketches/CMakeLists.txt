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

find_package(DataSketches REQUIRED)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
  add_subdirectory(benchmarks)
endif()

# hll.hpp causes deprecated warning and turning the warning off for now
add_compile_options(-Wno-deprecated-declarations)
bolt_add_library(
  bolt_sketch_aggregates
  RegistrationAggregateFunctions.cpp
  RegistrationFunctions.cpp
  SketchCPC.cpp
  SketchCPCSketch.cpp
  SketchFI.cpp
  SketchHLL.cpp
  SketchHLLEstimate.cpp
  SketchHLLSketch.cpp
  SketchHLLUnion.cpp
  SketchKLL.cpp
  SketchKLLSketch.cpp
  SketchQuantiles.cpp
  SketchREQ.cpp
  SketchTheta.cpp
  SketchVarOpt.cpp
)

target_include_directories(
  bolt_sketch_aggregates PRIVATE ${FOLLY_INCLUDE_DIRS} ${DataSketches_INCLUDE_DIRS}
)
target_link_libraries(
  bolt_sketch_aggregates bolt_exec DataSketches::DataSketches Folly::folly
)

set_property(TARGET bolt_sketch_aggregates PROPERTY JOB_POOL_COMPILE high_memory_pool)
