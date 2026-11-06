/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#include <TosClientV2.h>
#include <Type.h>
#include "bolt/common/file/File.h"

namespace VolcengineTos {
class TosClientV2;
class AppendObjectV2Input;
} // namespace VolcengineTos

namespace bytedance::bolt {

class TosWriteFile : public WriteFile {
 public:
  TosWriteFile(
      const std::shared_ptr<VolcengineTos::TosClientV2>& tosClient,
      const std::string_view& bucketName,
      const std::string_view& objectName,
      BucketType bucketType,
      bool shouldThrowOnFileAlreadyExists);

  ~TosWriteFile() override = default;

  // Appends data to the end of the file.
  void append(std::string_view data) override;

  void flush() override {}

  // Close the file. Any cleanup (disk flush, etc.) will be done here.
  void close() override;

  // Current file size, i.e. the sum of all previous Appends.
  uint64_t size() const override;

 private:
  // Returns true if the object already exists. Only an explicit 404 (Not Found)
  // is treated as "does not exist"; any other error (403, timeout, 5xx, client
  // error, ...) is thrown so that we never mistake an inaccessible object for a
  // missing one and silently overwrite it.
  bool isObjectExist();

  // Removes an existing object. Used to implement overwrite semantics on
  // append-only (FNS) buckets, where a fresh append from offset 0 requires the
  // target object to be absent first.
  void deleteObject();

  void putObject(std::string_view data);

  void appendObject(std::string_view data);

  std::shared_ptr<VolcengineTos::TosClientV2> tosClient_;

  const std::string bucketName_;

  const std::string objectName_;

  const bool shouldThrowOnFileAlreadyExists_;

  int64_t nextAppendOffset_{0};

  uint64_t preHashCrc64ecma_{0};

  BucketType bucketType_{BucketType::FNS};

  std::unique_ptr<VolcengineTos::AppendObjectV2Input> appendObjectV2Input_;

  bool hasWritten_{false};

  bool isClosed_{false};
};

} // namespace bytedance::bolt
