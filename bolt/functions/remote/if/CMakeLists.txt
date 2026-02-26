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

include(FBThriftCppLibrary)

add_fbthrift_cpp_library(
  bolt_remote_function_thrift RemoteFunction.thrift SERVICES RemoteFunctionService
)

target_compile_options(bolt_remote_function_thrift PRIVATE -Wno-deprecated-declarations)

bolt_add_library(bolt_functions_remote_get_serde GetSerde.cpp)
target_link_libraries(
  bolt_functions_remote_get_serde PUBLIC bolt_remote_function_thrift bolt_presto_serializer
)
