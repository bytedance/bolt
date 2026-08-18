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

# hll.hpp causes deprecated warning and turning the warning off for now
add_compile_options(-Wno-deprecated-declarations)

add_executable(
  bolt_sketches_aggregates_test
  Main.cpp
  SerDeTest.cpp
  SketchCPCSketchTest.cpp
  SketchCPCTest.cpp
  SketchFITest.cpp
  SketchHLLEstimateTest.cpp
  SketchHLLSketchTest.cpp
  SketchHLLTest.cpp
  SketchHLLUnionTest.cpp
  SketchKLLSketchTest.cpp
  SketchKLLTest.cpp
  SketchQuantileTest.cpp
  SketchReqTest.cpp
  SketchTestBase.cpp
  SketchThetaTest.cpp
  SketchVarOptTest.cpp
)

add_test(
  NAME bolt_sketches_aggregates_test
  COMMAND
    bolt_sketches_aggregates_test
  WORKING_DIRECTORY .
)

target_link_libraries(
  bolt_sketches_aggregates_test
  bolt_sketch_aggregates
  bolt_aggregates
  bolt_core
  bolt_exec
  bolt_exec_test_lib
  bolt_functions_test_lib
  bolt_functions_prestosql
  bolt_functions_lib
  DataSketches::DataSketches
  ${gflags_LIBRARIES}
  GTest::gtest
  GTest::gtest_main
)
