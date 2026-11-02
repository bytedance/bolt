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

#include <string>
namespace bytedance::bolt::functions::flinksql {

/// Registers SQL functions and the explicit Flink compatibility entry points
/// flink_split_index, flink_rand, flink_rand_integer, flink_complex_equal and
/// flink_complex_not_equal. MAP equality supports scalar keys except VARBINARY,
/// with scalar, ARRAY, ROW and MAP values following Flink's generic-data path.
/// Null ARRAY elements compare unequal; null ROW fields compare as values.
void registerFunctions(const std::string& prefix = "");

} // namespace bytedance::bolt::functions::flinksql
