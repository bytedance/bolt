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

#include "bolt/common/base/Exceptions.h"
#include "bolt/functions/Macros.h"
#include "bolt/functions/lib/string/StringCore.h"

#include <cstring>

namespace bytedance::bolt::functions::sparksql {
namespace detail {

static constexpr char kSpace = ' ';

template <bool isAscii, typename TString>
FOLLY_ALWAYS_INLINE int64_t charLength(const TString& input) {
  if constexpr (isAscii) {
    return input.size();
  } else {
    return stringCore::lengthUnicode(input.data(), input.size());
  }
}

template <typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE void copyWithPadding(
    TOutString& output,
    const TInString& input,
    int64_t numPaddingSpaces) {
  output.resize(input.size() + static_cast<size_t>(numPaddingSpaces));
  if (input.size() > 0) {
    std::memcpy(output.data(), input.data(), input.size());
  }
  std::memset(
      output.data() + input.size(),
      kSpace,
      static_cast<size_t>(numPaddingSpaces));
}

template <
    bool isAscii,
    bool padIfShort,
    typename TOutString,
    typename TInString>
FOLLY_ALWAYS_INLINE void
writeSideCheck(TOutString& output, const TInString& input, int32_t limit) {
  const auto numChars = charLength<isAscii>(input);
  if (numChars <= limit) {
    if constexpr (padIfShort) {
      if (numChars < limit) {
        copyWithPadding(output, input, limit - numChars);
        return;
      }
    }
    output.setNoCopy(StringView(input.data(), input.size()));
  } else {
    const auto numTailSpacesToTrim = numChars - limit;
    auto endIdx = static_cast<int64_t>(input.size()) - 1;
    const auto trimTo =
        static_cast<int64_t>(input.size()) - numTailSpacesToTrim;
    int64_t numSpacesTrimmed = 0;

    while (endIdx >= trimTo && input.data()[endIdx] == kSpace) {
      --endIdx;
      ++numSpacesTrimmed;
    }

    if (numChars - numSpacesTrimmed > limit) {
      BOLT_USER_FAIL("Exceeds char/varchar type length limitation: {}", limit);
    }
    output.setNoCopy(StringView(input.data(), static_cast<size_t>(endIdx + 1)));
  }
}

template <bool isAscii, typename TOutString, typename TInString>
FOLLY_ALWAYS_INLINE void
readSidePadding(TOutString& output, const TInString& input, int32_t limit) {
  const auto numChars = charLength<isAscii>(input);
  if (numChars < limit) {
    copyWithPadding(output, input, limit - numChars);
  } else {
    output.setNoCopy(StringView(input.data(), input.size()));
  }
}

} // namespace detail

template <typename T>
struct CharTypeWriteSideCheckFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t limit) {
    detail::writeSideCheck<false, true>(result, input, limit);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t limit) {
    detail::writeSideCheck<true, true>(result, input, limit);
  }
};

template <typename T>
struct VarcharTypeWriteSideCheckFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t limit) {
    detail::writeSideCheck<false, false>(result, input, limit);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t limit) {
    detail::writeSideCheck<true, false>(result, input, limit);
  }
};

template <typename T>
struct ReadSidePaddingFunction {
  BOLT_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t limit) {
    detail::readSidePadding<false>(result, input, limit);
  }

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& input,
      int32_t limit) {
    detail::readSidePadding<true>(result, input, limit);
  }
};

} // namespace bytedance::bolt::functions::sparksql
