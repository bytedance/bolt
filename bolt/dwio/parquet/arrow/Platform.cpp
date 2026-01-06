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
 */

/* --------------------------------------------------------------------------
 * Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
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

// Adapted from Apache Arrow.

#include "bolt/dwio/parquet/arrow/Platform.h"

#include <cstdint>
#include <memory>
#include <utility>

#include "arrow/io/memory.h"
#include "bolt/dwio/parquet/arrow/Exception.h"
namespace bytedance::bolt::parquet::arrow {

std::shared_ptr<::arrow::io::BufferOutputStream> CreateOutputStream(
    MemoryPool* pool) {
  PARQUET_ASSIGN_OR_THROW(
      auto stream,
      ::arrow::io::BufferOutputStream::Create(kDefaultOutputStreamSize, pool));
  return stream;
}

std::shared_ptr<ResizableBuffer> AllocateBuffer(
    MemoryPool* pool,
    int64_t size) {
  PARQUET_ASSIGN_OR_THROW(
      auto result, ::arrow::AllocateResizableBuffer(size, pool));
  return std::move(result);
}

} // namespace bytedance::bolt::parquet::arrow
