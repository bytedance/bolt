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

cmake_minimum_required(VERSION 3.15)
project(test_package LANGUAGES CXX)

find_package(Paimon REQUIRED CONFIG)
add_executable(test_package test_package.cpp)
target_link_libraries(test_package PRIVATE Paimon::core)
target_compile_features(test_package PRIVATE cxx_std_17)
if(PAIMON_HAS_ROW_MAPPING)
  target_compile_definitions(test_package PRIVATE PAIMON_HAS_ROW_MAPPING=1)
endif()
