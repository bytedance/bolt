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

#include <cstdint>
#include <exception>

#include "bolt/dwio/lance/NativeLanceFileContext.h"

namespace bytedance::bolt::lance::reader {

enum class NativeLanceFileOpenState : uint8_t {
  kNeedFooter,
  kNeedGlobalBufferIndex,
  kNeedSchema,
  kNeedColumnMetadataIndex,
  kNeedSchemaIndex,
  kNeedValidation,
  kReady,
  kFailed,
};

/// Drives construction of an immutable file context through explicit phases.
class NativeLanceFileOpenTask {
 public:
  NativeLanceFileOpenTask(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options,
      std::shared_ptr<const NativeLanceBlobResolver> blobResolver,
      std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter);

  /// Returns true when another phase remains and false when the context is
  /// ready.
  bool executeStep();

  std::shared_ptr<const NativeLanceFileContext> finish() const;

  NativeLanceFileOpenState state() const {
    return state_;
  }

 private:
  bool executeImpl();
  [[noreturn]] void rethrowFailure() const;

  std::unique_ptr<dwio::common::BufferedInput> input_;
  dwio::common::ReaderOptions options_;
  std::shared_ptr<const NativeLanceBlobResolver> blobResolver_;
  std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter_;
  std::unique_ptr<NativeLanceMetadata> metadata_;
  std::shared_ptr<NativeLanceFileContext> context_;
  std::exception_ptr failure_;
  NativeLanceFileOpenState state_{NativeLanceFileOpenState::kNeedFooter};
};

std::shared_ptr<const NativeLanceFileContext> openNativeLanceFile(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options,
    std::shared_ptr<const NativeLanceBlobResolver> blobResolver,
    std::shared_ptr<const NativeLanceTypeAdapter> typeAdapter);

} // namespace bytedance::bolt::lance::reader
