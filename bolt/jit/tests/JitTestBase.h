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

#ifdef ENABLE_BOLT_JIT

#include <cstdint>
#include <cstring>
#include <string>

namespace bytedance::bolt::jit::test {

// Just for UT
struct StringView {
  explicit StringView(std::string s) {
    len = s.length();
    for (auto i = 0; i < 12 && i < len; ++i) {
      inline_chars[i] = s[i];
    }
  }

  StringView(const StringView& other) {
    len = other.len;
    for (auto i = 0; i < 12 && i < len; ++i) {
      inline_chars[i] = other.inline_chars[i];
    }
  }

  void operator=(const StringView& other) {
    len = other.len;
    for (auto i = 0; i < 12 && i < len; ++i) {
      inline_chars[i] = other.inline_chars[i];
    }
  }

  int32_t len{0};
  char inline_chars[12]{0};
};

struct Timestamp {
  int64_t seconds;
  int64_t nanos;
};

extern "C" {
extern int jit_StringViewCompareWrapper(char* l, char* r);
} // ~ extern

} // namespace bytedance::bolt::jit::test
#endif
