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

bolt_add_library(bolt_type_signature TypeSignature.cpp)

target_link_libraries(bolt_type_signature bolt_common_base Boost::headers)

bolt_add_library(
  bolt_expression_functions FunctionSignature.cpp ReverseSignatureBinder.cpp SignatureBinder.cpp
)

target_link_libraries(
  bolt_expression_functions bolt_common_base bolt_type_calculation bolt_type_signature
  bolt_signature_parser
)

bolt_add_library(
  bolt_expression
  BooleanMix.cpp
  CastExpr-tpl.cpp
  CastExpr.cpp
  CoalesceExpr.cpp
  ConjunctExpr.cpp
  ConstantExpr.cpp
  EvalCtx.cpp
  Expr.cpp
  ExprCompiler.cpp
  ExprToSubfieldFilter.cpp
  FieldReference.cpp
  FunctionCallToSpecialForm.cpp
  GenericWriter.cpp
  LambdaExpr.cpp
  PeeledEncoding.cpp
  RegisterSpecialForm.cpp
  RowConstructor.cpp
  SimpleFunctionRegistry.cpp
  SingleSubfieldExtractor.cpp
  SpecialFormRegistry.cpp
  SwitchExpr.cpp
  TryExpr.cpp
  VectorFunction.cpp
)

target_link_libraries(
  bolt_expression
  bolt_common_base
  bolt_core
  bolt_expression_functions
  bolt_filter
  bolt_functions_util
  bolt_vector
  utf8proc::utf8proc
  date::date
)

if(${ENABLE_BOLT_EXPR_JIT})
  find_package(ThrustVector REQUIRED)
  target_link_libraries(bolt_expression PRIVATE ThrustVector::JIT)
endif()

add_subdirectory(type_calculation)
add_subdirectory(signature_parser)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
  add_subdirectory(fuzzer)
endif()
