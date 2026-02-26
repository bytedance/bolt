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

find_package(simdjson CONFIG REQUIRED)
find_package(sonic-cpp CONFIG REQUIRED)

bolt_add_library(
  bolt_functions_prestosql
  ArithmeticBasicFunctionsRegistration.cpp
  ArithmeticDecimalFunctionsRegistration.cpp
  ArithmeticExponentialFunctionsRegistration.cpp
  ArithmeticFunctionsRegistration.cpp
  ArithmeticMiscFunctionsRegistration.cpp
  ArithmeticProbabilityFunctionsRegistration.cpp
  ArithmeticRandomFunctionsRegistration.cpp
  ArithmeticRoundingFunctionsRegistration.cpp
  ArithmeticTrigonometricFunctionsRegistration.cpp
  ArrayConcatRegistration.cpp
  ArrayFunctionsRegistration.cpp
  ArrayNGramsRegistration.cpp
  BinaryFunctionsRegistration.cpp
  BitwiseFunctionsRegistration.cpp
  CheckedArithmeticRegistration.cpp
  ComparisonFunctionsRegistration.cpp
  DateTimeFunctionsArithmeticRegistration.cpp
  DateTimeFunctionsBasicConversionRegistration.cpp
  DateTimeFunctionsDateComponentExtractionRegistration.cpp
  DateTimeFunctionsRegistration.cpp
  DateTimeFunctionsTimeComponentExtractionRegistration.cpp
  GeneralFunctionsRegistration.cpp
  HyperLogFunctionsRegistration.cpp
  JsonFunctionsRegistration.cpp
  MapFunctionsRegistration.cpp
  MetricsFunctionsRegistration.cpp
  RegistrationFunctions.cpp
  StringFunctionsRegistration.cpp
  URLFunctionsRegistration.cpp
)

# GCC 12 has a bug where it does not respect "pragma ignore" directives and ends up failing
# compilation in an openssl header included by a hash-related function.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(bolt_functions_prestosql PRIVATE -Wno-deprecated-declarations)
endif()

target_link_libraries(
  bolt_functions_prestosql
  bolt_functions_prestosql_impl
  bolt_functions_spark
  bolt_is_null_functions
  simdjson::simdjson
  sonic-cpp::sonic-cpp
  Boost::url
)

set_property(TARGET bolt_functions_prestosql PROPERTY JOB_POOL_COMPILE high_memory_pool)
