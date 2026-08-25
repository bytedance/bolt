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

#include "bolt/common/base/SparkCompatibility.h"

#include <iomanip>
#include <random>
#include <sstream>

#include "bolt/common/base/Uuid.h"
namespace bytedance::bolt {

constexpr int32_t kUuidStringSize = 36;

std::string makeUuid() {
  std::string uuid(kUuidStringSize, '\0');
  makeUuid(uuid.data());
  return uuid;
}
std::string makeUuid(uint64_t seed) {
  std::string uuid(kUuidStringSize, '\0');
  makeUuid(uuid.data(), seed);
  return uuid;
}

std::string makeUuid(std::mt19937_64& rng) {
  std::string uuid(kUuidStringSize, '\0');
  makeUuid(uuid.data(), rng);
  return uuid;
}

std::string makeUuid(uint64_t mostSigBits, uint64_t leastSigBits) {
  std::string uuid(kUuidStringSize, '\0');
  makeUuid(uuid.data(), mostSigBits, leastSigBits);
  return uuid;
}

void makeUuid(char* uuid) {
  static thread_local std::random_device rd{};
  uint64_t seed = rd();
  makeUuid(uuid, seed);
}

void makeUuid(char* uuid, uint64_t seed) {
  std::mt19937_64 rng{seed};
  makeUuid(uuid, rng);
}

void makeUuid(char* uuid, std::mt19937_64& rng) {
  auto next = [&rng]() {
    if (!::bytedance::bolt::kSparkCompatible) {
      return rng();
    }
    uint64_t high = rng() << 32;
    uint64_t low = rng() & 4294967295L;
    return high | low;
  };
  makeUuid(uuid, next(), next());
}

void makeUuid(char* uuid, uint64_t mostSigBits, uint64_t leastSigBits) {
  // UUID v4
  uint64_t ab = (mostSigBits & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL;
  uint64_t cd = (leastSigBits | 0x8000000000000000ULL) & 0xBFFFFFFFFFFFFFFFULL;

  static constexpr char hexChars[] = "0123456789abcdef";
  for (int i = 0; i < 8; ++i) {
    uuid[i] = hexChars[(ab >> (60 - i * 4)) & 0xf];
  }
  uuid[8] = '-';
  for (int i = 0; i < 4; ++i) {
    uuid[9 + i] = hexChars[(ab >> (28 - i * 4)) & 0xf];
  }
  uuid[13] = '-';
  for (int i = 0; i < 4; ++i) {
    uuid[14 + i] = hexChars[(ab >> (12 - i * 4)) & 0xf];
  }
  uuid[18] = '-';
  for (int i = 0; i < 4; ++i) {
    uuid[19 + i] = hexChars[(cd >> (60 - i * 4)) & 0xf];
  }
  uuid[23] = '-';
  for (int i = 0; i < 12; ++i) {
    uuid[24 + i] = hexChars[(cd >> (44 - i * 4)) & 0xf];
  }
}
} // namespace bytedance::bolt
