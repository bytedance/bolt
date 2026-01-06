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

#pragma once

#include <cstdint>

#include "bolt/dwio/parquet/arrow/Platform.h"
#include "bolt/dwio/parquet/arrow/Types.h"
#include "bolt/dwio/parquet/arrow/tests/Hasher.h"
namespace bytedance::bolt::parquet::arrow {

class PARQUET_EXPORT XxHasher : public Hasher {
 public:
  uint64_t Hash(int32_t value) const override;
  uint64_t Hash(int64_t value) const override;
  uint64_t Hash(float value) const override;
  uint64_t Hash(double value) const override;
  uint64_t Hash(const Int96* value) const override;
  uint64_t Hash(const ByteArray* value) const override;
  uint64_t Hash(const FLBA* val, uint32_t len) const override;

  void Hashes(const int32_t* values, int num_values, uint64_t* hashes)
      const override;
  void Hashes(const int64_t* values, int num_values, uint64_t* hashes)
      const override;
  void Hashes(const float* values, int num_values, uint64_t* hashes)
      const override;
  void Hashes(const double* values, int num_values, uint64_t* hashes)
      const override;
  void Hashes(const Int96* values, int num_values, uint64_t* hashes)
      const override;
  void Hashes(const ByteArray* values, int num_values, uint64_t* hashes)
      const override;
  void Hashes(
      const FLBA* values,
      uint32_t type_len,
      int num_values,
      uint64_t* hashes) const override;

  static constexpr int kParquetBloomXxHashSeed = 0;
};

} // namespace bytedance::bolt::parquet::arrow
