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

#include "bolt/connectors/hive/bytelake/BytelakeKeyPacker.h"

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::connector::hive {

void appendBigint(std::string& out, int64_t value) {
  // Flip sign bit so two's-complement maps to unsigned with the same order.
  uint64_t u = static_cast<uint64_t>(value) ^ (uint64_t{1} << 63);
  out.push_back(static_cast<char>((u >> 56) & 0xFF));
  out.push_back(static_cast<char>((u >> 48) & 0xFF));
  out.push_back(static_cast<char>((u >> 40) & 0xFF));
  out.push_back(static_cast<char>((u >> 32) & 0xFF));
  out.push_back(static_cast<char>((u >> 24) & 0xFF));
  out.push_back(static_cast<char>((u >> 16) & 0xFF));
  out.push_back(static_cast<char>((u >> 8) & 0xFF));
  out.push_back(static_cast<char>(u & 0xFF));
}

void appendVarchar(std::string& out, StringView value) {
  uint32_t len = static_cast<uint32_t>(value.size());
  out.push_back(static_cast<char>((len >> 24) & 0xFF));
  out.push_back(static_cast<char>((len >> 16) & 0xFF));
  out.push_back(static_cast<char>((len >> 8) & 0xFF));
  out.push_back(static_cast<char>(len & 0xFF));
  out.append(value.data(), value.size());
}

std::string packCompositeKey(
    const std::vector<const DecodedVector*>& decoders,
    const std::vector<TypeKind>& kinds,
    vector_size_t row) {
  BOLT_CHECK_EQ(decoders.size(), kinds.size());
  std::string out;
  out.reserve(decoders.size() * 16);
  for (size_t i = 0; i < decoders.size(); ++i) {
    switch (kinds[i]) {
      case TypeKind::BIGINT:
        appendBigint(out, decoders[i]->valueAt<int64_t>(row));
        break;
      case TypeKind::VARCHAR:
        appendVarchar(out, decoders[i]->valueAt<StringView>(row));
        break;
      default:
        BOLT_FAIL(
            "Unsupported TypeKind in packCompositeKey: {}",
            mapTypeKindToName(kinds[i]));
    }
  }
  return out;
}

} // namespace bytedance::bolt::connector::hive
