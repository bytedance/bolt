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
 * This file has been modified by ByteDance Ltd. and/or its affiliates.
 * --------------------------------------------------------------------------
 */

#pragma once

#include <cctype>
#include <cstdlib>

#if defined(__aarch64__) && defined(__linux__)
#include <sys/auxv.h>
#endif

namespace bytedance::bolt::exec {

/// Opt-in via BOLT_HASH_AGG_SVE_NORMALIZED_KEY_PROBE. Unset or empty → off.
/// Enable with 1 / true / yes / on (ASCII, case-insensitive for words).
/// Disable explicitly with 0 / false / no / off. Unknown values → off.
inline bool boltHashAggSveNormalizedKeyProbeEnabledFromEnv() {
  const char* v = std::getenv("BOLT_HASH_AGG_SVE_NORMALIZED_KEY_PROBE");
  if (v == nullptr || *v == '\0') {
    return false;
  }
  if (v[0] == '1' && v[1] == '\0') {
    return true;
  }
  if (v[0] == '0' && v[1] == '\0') {
    return false;
  }
  auto eqNoCase = [](const char* a, const char* b) {
    while (*a && *b) {
      if (std::tolower(static_cast<unsigned char>(*a)) !=
          std::tolower(static_cast<unsigned char>(*b))) {
        return false;
      }
      ++a;
      ++b;
    }
    return *a == *b;
  };
  if (eqNoCase(v, "true") || eqNoCase(v, "yes") || eqNoCase(v, "on")) {
    return true;
  }
  if (eqNoCase(v, "false") || eqNoCase(v, "no") || eqNoCase(v, "off")) {
    return false;
  }
  return false;
}

inline bool linuxAarch64RuntimeHasSve() {
#if defined(__aarch64__) && defined(__linux__)
#ifndef HWCAP_SVE
  constexpr unsigned long kBoltHwcapSve = 1UL << 22;
#else
  constexpr unsigned long kBoltHwcapSve = HWCAP_SVE;
#endif
  return (getauxval(AT_HWCAP) & kBoltHwcapSve) != 0;
#else
  return false;
#endif
}

} // namespace bytedance::bolt::exec
