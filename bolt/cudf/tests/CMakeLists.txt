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

bolt_add_library(bolt_cudf_test_util CudfResource.cpp)

target_include_directories(bolt_cudf_test_util PRIVATE ${cudf_INCLUDE_DIRS})

target_link_libraries(
  bolt_cudf_test_util
  PUBLIC cudf::cudf
  PRIVATE bolt_exception
)

# The LIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE macro enables the experimental
# <cuda/memory_resource> header, a feature used by RMM and libcudf. This definition is necessary to
# prevent compilation errors.
#
# References: - RMM Fix: https://github.com/rapidsai/rmm/pull/1852 - libcudacxx Docs:
# https://nvidia.github.io/cccl/libcudacxx/extended_api/memory_resource.html
target_compile_definitions(
  bolt_cudf_test_util PUBLIC LIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE BOLT_HAS_CUDF=1
)
