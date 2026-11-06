/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "bolt/exec/RowContainer.h"
namespace bytedance::bolt::exec {

class ContainerRow2RowSerde {
 public:
  FLATTEN static void serialize(
      char* row,
      char*& current,
      const char* bufferEnd,
      const RowFormatInfo& info,
      bool initializeStringViews = false);

  FLATTEN static int32_t
  deserialize(char*& current, const RowFormatInfo& info, bool advance = true);

  FLATTEN static int32_t rowSize(char* row, const RowFormatInfo& info);

  FLATTEN static void copyRow(
      char*& dest,
      const char* src,
      const size_t length,
      const RowFormatInfo& info);
};

} // namespace bytedance::bolt::exec
