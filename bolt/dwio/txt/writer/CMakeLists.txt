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

if(BOLT_ENABLE_TXT)
  find_package(Arrow CONFIG REQUIRED)
  bolt_add_library(bolt_dwio_txt_writer RegisterTxtWriter.cpp TxtWriter.cpp)
  target_link_libraries(bolt_dwio_txt_writer bolt_dwio_common bolt_arrow_bridge arrow::arrow ${FMT})
else()
  bolt_add_library(bolt_dwio_txt_writer RegisterTxtWriter.cpp)
  # No additional dependencies
endif()
