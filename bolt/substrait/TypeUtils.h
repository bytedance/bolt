/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "bolt/dwio/common/Options.h"
#include "bolt/substrait/proto/substrait/algebra.pb.h"
#include "bolt/type/Type.h"
namespace bytedance::bolt::substrait {

std::string_view getNameBeforeDelimiter(
    const std::string& compoundName,
    const std::string& delimiter);

// Convert Substrait file format to Bolt FileFormat.
// Extracts the file_format oneof from a Substrait FileOrFiles and maps to
// dwio::common::FileFormat.
dwio::common::FileFormat toBoltFileFormat(
    const ::substrait::ReadRel::LocalFiles::FileOrFiles& file);

// Create a Substrait FileOrFiles message with only file_format set based on
// Bolt FileFormat.
::substrait::ReadRel::LocalFiles::FileOrFiles toSubstraitFileFormat(
    dwio::common::FileFormat fmt);
} // namespace bytedance::bolt::substrait
