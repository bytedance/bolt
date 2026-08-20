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

# Reference implementation of the ColumnarPayload format, described in
# bolt/shuffle/sparksql/ColumnarPayloadFormat.md.
if(NOT ${BOLT_BUILD_TESTING})
  return()
endif()

# Deliberately a plain add_library rather than bolt_add_library: the latter
# builds an OBJECT library and files it under this directory's component,
# which would fold this test only code into bolt_engine.
#
# The target links nothing. It is the oracle the engine's own Writer and
# Reader are tested against, so it must not share code with them and has to
# build without the engine.
add_library(
  bolt_columnar_payload_ref
  STATIC
  Format.cpp
  Generator.cpp
  Validator.cpp
)

# A static archive would still link happily if these files grew a dependency
# on the engine, because the test executable pulls the engine in anyway. This
# executable is what makes the rule real: it links the reference and nothing
# else, so an include of a vector or exec header fails here.
add_executable(bolt_columnar_payload_ref_test Standalone.cpp)

target_link_libraries(
  bolt_columnar_payload_ref_test
  PRIVATE
    bolt_columnar_payload_ref
)

add_test(
  NAME bolt_columnar_payload_ref_test
  COMMAND bolt_columnar_payload_ref_test
)

# Everything needing RowVector lives above the reference, never inside it.
# Vectors.cpp is the only bridge; Conformance.cpp is the seam a real Writer
# and Reader plug into.
add_executable(
  bolt_columnar_payload_test
  Vectors.cpp
  Conformance.cpp
  PayloadTest.cpp
)

target_link_libraries(
  bolt_columnar_payload_test
  PRIVATE
    bolt_columnar_payload_ref
    bolt_exec
    bolt_vector
    bolt_vector_test_lib
    bolt_testutils
    GTest::gtest
    GTest::gtest_main
)

add_test(
  NAME bolt_columnar_payload_test
  COMMAND bolt_columnar_payload_test
)
