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

namespace bytedance::bolt {

inline constexpr char kTimestampExtensionName[] = "bolt.timestamp";
inline constexpr char kTimestampExtensionMetadata[] = "1";

/// Registers bolt.timestamp with Arrow C++. Call before importing schemas or
/// reading IPC containing seconds/nanos timestamps, including encoded values.
/// Registration is thread-safe and performed once per process. ArrowVectorSerde
/// registers the type automatically. Pure C Data consumers do not need this.
void registerArrowTimestampType();

} // namespace bytedance::bolt
