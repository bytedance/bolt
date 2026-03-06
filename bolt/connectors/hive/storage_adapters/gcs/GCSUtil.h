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

#pragma once
#include <google/cloud/storage/client.h>
#include "bolt/common/base/Exceptions.h"
namespace bytedance::bolt {

namespace {
constexpr const char* kSep{"/"};
constexpr std::string_view kGcsScheme{"gs://"};

} // namespace

std::string getErrorStringFromGCSError(const google::cloud::StatusCode& error);

inline bool isGCSFile(const std::string_view filename) {
  return (filename.substr(0, kGcsScheme.size()) == kGcsScheme);
}

inline void setBucketAndKeyFromGCSPath(
    const std::string& path,
    std::string& bucket,
    std::string& key) {
  auto firstSep = path.find_first_of(kSep);
  bucket = path.substr(0, firstSep);
  key = path.substr(firstSep + 1);
}
inline std::string gcsURI(const std::string& bucket) {
  return std::string(kGcsScheme) + bucket;
}

inline std::string gcsURI(const std::string& bucket, const std::string& key) {
  return gcsURI(bucket) + kSep + key;
}

inline std::string gcsPath(const std::string_view& path) {
  // Remove the prefix gcs:// from the given path
  return std::string(path.substr(kGcsScheme.length()));
}

} // namespace bytedance::bolt
