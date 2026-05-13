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

#include <sys/stat.h>
#include <unistd.h>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>

#include "bolt/common/base/Exceptions.h"
namespace bytedance::bolt::test {

// It manages the lifetime of a temporary file.
class TempFilePath {
 public:
  static std::shared_ptr<TempFilePath> create();

  virtual ~TempFilePath() {
    unlink(path.c_str());
    close(fd);
  }

  const std::string path;

  TempFilePath(const TempFilePath&) = delete;
  TempFilePath& operator=(const TempFilePath&) = delete;

  void append(std::string data) {
    std::ofstream file(path, std::ios_base::app);
    file << data;
    file.flush();
    file.close();
  }

  const int64_t fileSize() {
    struct stat st;
    ::stat(path.data(), &st);
    return st.st_size;
  }

  int64_t fileModifiedTime() {
    struct stat st;
    ::stat(path.data(), &st);
    return st.st_mtime;
  }

  /// If fault injection is enabled, the returned the file path has the faulty
  /// file system prefix scheme. The bolt fs then opens the file through the
  /// faulty file system. The actual file operation might either fails or
  /// delegate to the actual file.
  const std::string& getPath() const {
    return path;
  }

 private:
  int fd;

  TempFilePath() : path(createTempFile(this)) {
    BOLT_CHECK_NE(fd, -1);
  }

  static std::string createTempFile(TempFilePath* tempFilePath) {
    char path[] = "/tmp/bolt_test_XXXXXX";
    tempFilePath->fd = mkstemp(path);
    if (tempFilePath->fd == -1) {
      throw std::logic_error("Cannot open temp file");
    }
    return path;
  }
};

} // namespace bytedance::bolt::test
