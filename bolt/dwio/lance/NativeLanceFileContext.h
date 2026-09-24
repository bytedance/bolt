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

#include "bolt/dwio/common/Options.h"
#include "bolt/dwio/lance/NativeLanceBlobResolver.h"
#include "bolt/dwio/lance/NativeLanceMetadata.h"
#include "bolt/dwio/lance/NativeLanceReadScheduler.h"
#include "bolt/dwio/lance/NativeLanceTypeAdapter.h"

namespace bytedance::bolt::lance::reader {

/// Immutable file-level state shared by row readers.
///
/// The context owns the input used for lazy metadata loading. Each row reader
/// clones a clean input for page reads so scan-local staged I/O is never
/// shared.
class NativeLanceFileContext {
 public:
  NativeLanceFileContext(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options,
      std::shared_ptr<const NativeLanceBlobResolver> blobResolver,
      std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter);

  NativeLanceFileContext(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options,
      std::shared_ptr<const NativeLanceBlobResolver> blobResolver,
      std::unique_ptr<NativeLanceMetadata> metadata);

  memory::MemoryPool& pool() const {
    return pool_;
  }

  const NativeLanceMetadata& metadata() const {
    return *metadata_;
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId() const {
    return typeWithId_;
  }

  const std::shared_ptr<const NativeLanceBlobResolver>& blobResolver() const {
    return blobResolver_;
  }

  NativeLanceReadScheduler::Options readSchedulerOptions() const {
    return readSchedulerOptions_;
  }

  std::unique_ptr<dwio::common::BufferedInput> newInput() const {
    return metadataInput_->clone();
  }

  void validate() const;

 private:
  memory::MemoryPool& pool_;
  std::shared_ptr<dwio::common::BufferedInput> metadataInput_;
  std::unique_ptr<NativeLanceMetadata> metadata_;
  std::shared_ptr<const dwio::common::TypeWithId> typeWithId_;
  std::shared_ptr<const NativeLanceBlobResolver> blobResolver_;
  NativeLanceReadScheduler::Options readSchedulerOptions_;
};

} // namespace bytedance::bolt::lance::reader
