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

#include <cstdint>
#include <string>
#include <vector>

#include "bolt/type/StringView.h"
#include "bolt/type/Type.h"
#include "bolt/vector/DecodedVector.h"

namespace bytedance::bolt::connector::hive {

// Append int64 in memcmp-safe form: 8 bytes big-endian with sign bit
// flipped, so byte-wise memcmp matches numeric order (incl. negatives).
void appendBigint(std::string& out, int64_t value);

// Append a string with 4-byte big-endian length prefix + raw bytes.
// Length prefix disambiguates multi-column packs (e.g. ("abc","def") vs
// ("abcdef","")). Note: memcmp orders by (length, content) for differing
// lengths; production fixed-width precombine columns (e.g. 14-char
// commit_time) avoid this corner.
void appendVarchar(std::string& out, StringView value);

// Pack a row's multi-column key into a single string (PK or precombine).
// Result supports equality (hash key) and memcmp ordering (precombine
// compare). Supports BIGINT / VARCHAR; nulls are not allowed by contract.
std::string packCompositeKey(
    const std::vector<const DecodedVector*>& decoders,
    const std::vector<TypeKind>& kinds,
    vector_size_t row);

} // namespace bytedance::bolt::connector::hive
