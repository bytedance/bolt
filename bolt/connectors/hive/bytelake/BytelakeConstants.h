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

#include <stdint.h>

namespace bytedance::bolt::connector::bytelake {

static constexpr uint32_t kMAX_BATCH_SIZE = 4096;

static constexpr const char* kPrimaryKey = "bytelake.recordkey.fields";
static constexpr const char* kPrecombineKey = "bytelake.precombine.fields";

static constexpr const char* kCheckMetaCommitTime = "check_meta_commit_time.enabled";
static constexpr const char* kMetaCommitTime = "_hoodie_commit_time";
static constexpr const char* kHoodieIsDeleted = "_hoodie_is_deleted";

} // namespace bytedance::bolt::connector::bytelake
