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

#include "bolt/common/memory/bm/base/ScopedFd.h"

#include <string>

namespace bytedance::bolt::memory::bm {

class ManagedOpenFile {
 public:
  ManagedOpenFile() = default;
  ManagedOpenFile(std::string path, int fd);
  ~ManagedOpenFile();

  ManagedOpenFile(const ManagedOpenFile&) = delete;
  ManagedOpenFile& operator=(const ManagedOpenFile&) = delete;
  ManagedOpenFile(ManagedOpenFile&& other) noexcept;
  ManagedOpenFile& operator=(ManagedOpenFile&& other) noexcept;

  int fd() const {
    return fd_.get();
  }

  const std::string& path() const {
    return path_;
  }

  bool valid() const {
    return fd_.get() >= 0;
  }

  void Close();
  void CloseAndRemove();

 private:
  std::string path_;
  ScopedFd fd_;
};

} // namespace bytedance::bolt::memory::bm
