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

#include "bolt/vector/fuzzer/VectorFuzzer.h"

#include <boost/random/uniform_int_distribution.hpp>
#include <boost/random/uniform_real_distribution.hpp>
#include <common/base/CheckedArithmetic.h>
#include <fmt/format.h>
#include <sys/types.h>
#include <codecvt>
#include <cstdint>
#include <cstdlib>
#include <iterator>
#include <limits>
#include <locale>
#include <random>
#include <set>
#include <type_traits>

#include "bolt/common/base/Exceptions.h"
#include "bolt/type/StringView.h"
#include "bolt/type/Timestamp.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/NullsBuilder.h"
#include "bolt/vector/VectorTypeUtils.h"
namespace bytedance::bolt {

namespace {

// DWRF requires nano to be in a certain range. Hardcode the value here to avoid
// the dependency on DWRF.
constexpr int64_t MAX_NANOS = 1'000'000'000;

// Structure to help temporary changes to Options. This objects saves the
// current state of the Options object, and restores it when it's destructed.
// For instance, if you would like to temporarily disable nulls for a particular
// recursive call:
//
//  {
//    ScopedOptions scopedOptions(this);
//    opts_.nullRatio = 0;
//    // perhaps change other opts_ values.
//    vector = fuzzFlat(...);
//  }
//  // At this point, opts_ would have the original values again.
//
struct ScopedOptions {
  explicit ScopedOptions(VectorFuzzer* fuzzer)
      : fuzzer(fuzzer), savedOpts(fuzzer->getOptions()) {}

  ~ScopedOptions() {
    fuzzer->setOptions(savedOpts);
  }

  // Stores a copy of Options so we can restore at destruction time.
  VectorFuzzer* fuzzer;
  VectorFuzzer::Options savedOpts;
};

// Maintain an input corpus to generate duplicated inputs if needed.
template <typename T>
std::set<T> inputCorpus;

template <typename T>
void clearInputCorpus(std::set<T>& inputCorpus) {
  inputCorpus.clear();
  BOLT_CHECK_EQ(inputCorpus.size(), 0);
}

template <typename T>
T getInputFromInputCorpus(const std::set<T>& inputCorpus, const size_t& index) {
  BOLT_CHECK_GE(index, 0);
  BOLT_CHECK_LT(index, inputCorpus.size());
  return *std::next(inputCorpus.begin(), index);
}

template <typename T>
void addInputToInputCorpus(std::set<T>& inputCorpus, const T& newInput) {
  inputCorpus.insert(newInput);
}

// Generate random values for the different supported types.
template <typename T>
T rand(FuzzerGenerator& rng) {
  if constexpr (
      std::is_same_v<T, uint8_t> || std::is_same_v<T, int8_t> ||
      std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t> ||
      std::is_same_v<T, int64_t> || std::is_same_v<T, uint32_t> ||
      std::is_same_v<T, uint64_t>) {
    return boost::random::uniform_int_distribution<T>()(rng);
  } else if constexpr (std::is_same_v<T, int128_t>) {
    return HugeInt::build(rand<int64_t>(rng), rand<uint64_t>(rng));
  } else if constexpr (std::is_same_v<T, float> || std::is_same_v<T, double>) {
    return boost::random::uniform_01<T>()(rng);
  } else if constexpr (std::is_same_v<T, bool>) {
    return boost::random::uniform_int_distribution<uint32_t>(0, 1)(rng) != 0;
  } else {
    BOLT_NYI();
  }
}

// Generate special values for the different supported types.
// Special values include NaN, MIN, MAX, 9, 99, 999, etc.
template <typename T>
T generateSpecialValues(FuzzerGenerator& rng) {
  if constexpr (
      std::is_same_v<T, uint8_t> || std::is_same_v<T, int8_t> ||
      std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t> ||
      std::is_same_v<T, int64_t> || std::is_same_v<T, uint32_t> ||
      std::is_same_v<T, uint64_t> || std::is_same_v<T, int128_t> ||
      std::is_same_v<T, float> || std::is_same_v<T, double>) {
    std::vector<T> specialValues;
    specialValues.emplace_back(std::numeric_limits<T>::quiet_NaN());
    specialValues.emplace_back(std::numeric_limits<T>::lowest());
    specialValues.emplace_back(std::numeric_limits<T>::max());

    T all9Values = 9;
    T largestAll9Values = (std::numeric_limits<T>::max() - 9) / 10;
    while (all9Values <= largestAll9Values) {
      specialValues.emplace_back(all9Values);
      all9Values = all9Values * 10 + 9;
    }
    auto index = rand<size_t>(rng) % specialValues.size();
    return specialValues[index];
  } else if constexpr (std::is_same_v<T, bool>) {
    return rand<bool>(rng);
  } else {
    BOLT_NYI();
  }
}

/// Generate a random precision in range [minPrecision, maxPrecision]
/// and a scale in range [0, random precision generated].
/// @param maximum precision.
std::pair<int8_t, int8_t> randPrecisionScale(
    FuzzerGenerator& rng,
    int8_t minPrecision,
    int8_t maxPrecision) {
  struct DecimalSpec {
    std::optional<int8_t> precision;
    std::optional<int8_t> scale;
  };

  using SpecialDecimalSpecs = std::unordered_map<std::string, DecimalSpec>;
  SpecialDecimalSpecs specialDecimalSpecs = {
      {"MAX_REPRESENTABLE", {maxPrecision, 0}},
      {"MIN_REPRESENTABLE", {maxPrecision, 0}},
      {"VERY_HIGH_PRECISION", {maxPrecision, std::nullopt}},
      {"VERY_LONG_FRACTION", {maxPrecision, maxPrecision}},
      {"LARGEST_PRECISION_LARGEST_SCALE", {maxPrecision, maxPrecision}},
      {"LARGEST_PRECISION_SMALLEST_SCALE", {maxPrecision, 0}},
      {"SMALLEST_PRECISION_LARGEST_SCALE", {minPrecision, minPrecision}},
      {"SMALLEST_PRECISION_SMALLEST_SCALE", {minPrecision, 0}}};
  size_t numSpecialDecimalSpecs = specialDecimalSpecs.size();

  auto isSpecial = rand<bool>(rng);
  if (isSpecial) {
    // Randomly select from predefined special precisions and scales.
    auto index = rand<size_t>(rng) % numSpecialDecimalSpecs;
    auto iteratorSpecialDecimalSpecs = specialDecimalSpecs.begin();
    std::advance(iteratorSpecialDecimalSpecs, index);
    const auto& [specialDecimalName, specialDecimalSpec] =
        *iteratorSpecialDecimalSpecs;
    if (specialDecimalSpec.scale.has_value()) {
      return std::make_pair(
          *specialDecimalSpec.precision, *specialDecimalSpec.scale);
    }

    // If the scale is std::nullopt, generate a scale in range [0, precision].
    auto scale = rand<int8_t>(rng) % (*specialDecimalSpec.precision + 1);
    return std::make_pair(*specialDecimalSpec.precision, scale);
  }

  // Purely random generation.
  // Generate precision in range [min precision, max precision].
  auto precision =
      minPrecision + (rand<int8_t>(rng) % (maxPrecision - minPrecision + 1));
  // Generate scale in range [0, precision].
  auto scale = rand<int8_t>(rng) % (precision + 1);
  return {precision, scale};
}

// Generate a short decimal TypePtr with random / special precision and scale.
inline TypePtr randShortDecimalType(FuzzerGenerator& rng) {
  auto [precision, scale] = randPrecisionScale(
      rng,
      ShortDecimalType::kMinPrecision == 0 ? 1
                                           : ShortDecimalType::kMinPrecision,
      ShortDecimalType::kMaxPrecision);
  return DECIMAL(precision, scale);
}

// Generate a long decimal TypePtr with random / special precision and scale.
inline TypePtr randLongDecimalType(FuzzerGenerator& rng) {
  auto [precision, scale] = randPrecisionScale(
      rng, LongDecimalType::kMinPrecision, LongDecimalType::kMaxPrecision);
  return DECIMAL(precision, scale);
}

Timestamp randTimestamp(
    FuzzerGenerator& rng,
    const VectorFuzzer::Options& opts) {
  switch (opts.timestampPrecision) {
    case VectorFuzzer::Options::TimestampPrecision::kNanoSeconds: {
      const auto& format = (opts.timestampFormat.size() == 1)
          ? opts.timestampFormat.front()
          : opts.timestampFormat
                [rand<uint32_t>(rng) % opts.timestampFormat.size()];
      switch (format) {
        case VectorFuzzer::Options::TimestampFormat::kStandard:
          return Timestamp(
              rand<int32_t>(rng), (rand<int64_t>(rng) % MAX_NANOS));
        case VectorFuzzer::Options::TimestampFormat::kSpecial: {
          // Generate timestamps beyond 2262-04-12.
          Timestamp specialTimestamp;

          constexpr int64_t kDaysPerYear = 365;
          constexpr int64_t kMaxInvalidYear = 100'000;
          constexpr int64_t kMinInvalidYear = 2263;

          int64_t year =
              (rand<int64_t>(rng) % (kMaxInvalidYear - kMinInvalidYear + 1)) +
              kMinInvalidYear;
          int64_t seconds;
          try {
            int64_t days = checkedMultiply(year, kDaysPerYear);
            seconds = checkedMultiply(days, Timestamp::kSecondsInDay);
          } catch (...) {
            seconds = rand<int32_t>(rng);
          }
          uint64_t nanos = rand<uint64_t>(rng) % (Timestamp::kMaxNanos + 1);

          std::memcpy(&specialTimestamp, &seconds, sizeof(int64_t));
          std::memcpy(
              reinterpret_cast<char*>(&specialTimestamp) + sizeof(int64_t),
              &nanos,
              sizeof(uint64_t));

          return specialTimestamp;
        }
      }
    }
    case VectorFuzzer::Options::TimestampPrecision::kMicroSeconds:
      return Timestamp::fromMicros(rand<int64_t>(rng));
    case VectorFuzzer::Options::TimestampPrecision::kMilliSeconds:
      return Timestamp::fromMillis(rand<int64_t>(rng));
    case VectorFuzzer::Options::TimestampPrecision::kSeconds:
      return Timestamp(rand<int32_t>(rng), 0);
  }
  return {}; // no-op.
}

size_t getElementsVectorLength(
    const VectorFuzzer::Options& opts,
    vector_size_t size) {
  if (opts.containerVariableLength == false &&
      size * opts.containerLength > opts.complexElementsMaxSize) {
    BOLT_USER_FAIL(
        "Requested fixed opts.containerVariableLength can't be satisfied: "
        "increase opts.complexElementsMaxSize, reduce opts.containerLength"
        " or make opts.containerVariableLength=true");
  }
  return std::min(size * opts.containerLength, opts.complexElementsMaxSize);
}

int64_t randShortDecimal(const TypePtr& type, FuzzerGenerator& rng) {
  auto precision = type->asShortDecimal().precision();
  return rand<int64_t>(rng) % DecimalUtil::getPowersOfTen(precision);
}

int128_t randLongDecimal(const TypePtr& type, FuzzerGenerator& rng) {
  auto precision = type->asLongDecimal().precision();
  return rand<int128_t>(rng) % DecimalUtil::getPowersOfTen(precision);
}

template <typename T>
T randDecimal(
    const TypePtr& type,
    FuzzerGenerator& rng,
    bool isShortDecimal,
    bool isLongDecimal) {
  BOLT_CHECK(
      isShortDecimal != isLongDecimal,
      "Can only geneate either a short decimal or a long decimal.");

  bool generateSpecialValues = rand<bool>(rng);

  if (generateSpecialValues) {
    // Generate special values based on hardcoded rules.
    uint8_t precision = isShortDecimal ? type->asShortDecimal().precision()
                                       : type->asLongDecimal().precision();
    uint8_t scale = isShortDecimal ? type->asShortDecimal().scale()
                                   : type->asLongDecimal().scale();

    using DecimalValueGenerationRule =
        std::function<std::string(int8_t, FuzzerGenerator&)>;

    std::vector<DecimalValueGenerationRule> fractionalDigitsGenerationRules = {
        // All 9s.
        [&](uint8_t length, FuzzerGenerator& /*rng*/) {
          return std::string(length, '9');
        },
        // All 0s.
        [&](uint8_t length, FuzzerGenerator& /*rng*/) {
          return std::string(length, '0');
        },
        // The last digit is 5 to test rounding.
        [&](uint8_t length, FuzzerGenerator& rng) {
          if (length <= 1) {
            return std::string(length, '0');
          }
          std::string digitString;
          digitString.resize(length);
          std::uniform_int_distribution<int8_t> rand0To9(0, 9);
          for (uint8_t i = 0; i < length - 1; ++i) {
            digitString[i] = static_cast<char>('0' + rand0To9(rng));
          }
          digitString[length - 1] = '5';
          BOLT_DCHECK_EQ(
              digitString.size(),
              length,
              "Incorrect decimal string generation.");
          return digitString;
        },
        // .00...01 to simulate very small fractions.
        [&](uint8_t length, FuzzerGenerator& /*rng*/) {
          if (length <= 1) {
            return std::string(length, '0');
          }
          std::string digitString;
          digitString.resize(length);
          digitString.replace(0, length - 1, std::string(length - 1, '0'));
          digitString[length - 1] = '1';
          BOLT_DCHECK_EQ(
              digitString.size(),
              length,
              "Incorrect decimal string generation.");
          return digitString;
        },
        // Leading 0s.
        [&](uint8_t length, FuzzerGenerator& rng) {
          if (length <= 1) {
            return std::string(length, '0');
          }
          std::string digitString;
          digitString.resize(length);
          uint8_t numZeros = (rand<uint8_t>(rng) % (length - 1)) + 1;
          digitString.replace(0, numZeros, std::string(numZeros, '0'));
          std::uniform_int_distribution<int8_t> rand1To9(1, 9);
          for (uint8_t i = numZeros; i < length; ++i) {
            digitString[i] = static_cast<char>('0' + rand1To9(rng));
          }
          BOLT_DCHECK_EQ(
              digitString.size(),
              length,
              "Incorrect decimal string generation.");
          return digitString;
        },
        // Tailing 0s.
        [&](uint8_t length, FuzzerGenerator& rng) {
          if (length <= 1) {
            return std::string(length, '0');
          }
          std::string digitString;
          digitString.resize(length);
          uint8_t numZeros = (rand<uint8_t>(rng) % (length - 1)) + 1;
          uint8_t numRemainingDigits = length - numZeros;
          std::uniform_int_distribution<int8_t> rand1To9(1, 9);
          for (uint8_t i = 0; i < numRemainingDigits; ++i) {
            digitString[i] = static_cast<char>('0' + rand1To9(rng));
          }
          digitString.replace(
              numRemainingDigits, numZeros, std::string(numZeros, '0'));
          BOLT_DCHECK_EQ(
              digitString.size(),
              length,
              "Incorrect decimal string generation.");
          return digitString;
        },
        // Empty wholeDigits or fractionalDigits.
        [&](uint8_t /*length*/, FuzzerGenerator& /*rng*/) {
          return std::string();
        }};

    std::vector<DecimalValueGenerationRule> wholeDigitsGenerationRules(
        fractionalDigitsGenerationRules);
    wholeDigitsGenerationRules.emplace_back(
        // Negative.
        [&](uint8_t length, FuzzerGenerator& rng) {
          if (length <= 1) {
            return std::string(length, '0');
          }
          std::string digitString;
          digitString.resize(length);
          digitString[0] = '-';
          std::uniform_int_distribution<int8_t> rand0To9(0, 9);
          for (uint8_t i = 1; i < length; ++i) {
            digitString[i] = static_cast<char>('0' + rand0To9(rng));
          }
          BOLT_DCHECK_EQ(
              digitString.size(),
              length,
              "Incorrect decimal string generation.");
          return digitString;
        });

    std::string wholeDigitsString = wholeDigitsGenerationRules
        [rand<int8_t>(rng) % wholeDigitsGenerationRules.size()](
            precision - scale, rng);
    std::string fractionalDigitsString = fractionalDigitsGenerationRules
        [rand<int8_t>(rng) % fractionalDigitsGenerationRules.size()](
            scale, rng);

    std::string_view wholeDigits = wholeDigitsString;
    std::string_view fractionalDigits = fractionalDigitsString;

    DecimalUtil::DecimalComponents decimalComponents;
    decimalComponents.wholeDigits = wholeDigits;
    decimalComponents.fractionalDigits = fractionalDigits;

    return DecimalUtil::parseDecimalFromComponents<T>(decimalComponents);
  }

  // Otherwise, apply random generation.
  auto precision = isShortDecimal ? type->asShortDecimal().precision()
                                  : type->asLongDecimal().precision();

  if (isShortDecimal) {
    int64_t generatedValue =
        rand<int64_t>(rng) % DecimalUtil::getPowersOfTen(precision);
    return generatedValue;
  }
  int128_t generatedValue =
      rand<int128_t>(rng) % DecimalUtil::getPowersOfTen(precision);
  return generatedValue;
}

/// Unicode character ranges. Ensure the vector indexes match the UTF8CharList
/// enum values.
///
/// Source: https://jrgraphix.net/research/unicode_blocks.php
const std::vector<std::vector<std::pair<char32_t, char32_t>>> kUTFChatSets{
    // UTF8CharList::ASCII
    {
        {33, 127}, // All ASCII printable chars.
    },
    // UTF8CharList::UNICODE_CASE_SENSITIVE
    {
        {U'\U00000020', U'\U0000007F'}, // Basic Latin.
        {U'\U00000400', U'\U000004FF'}, // Cyrillic.
    },
    // UTF8CharList::EXTENDED_UNICODE
    {
        {U'\U00000370', U'\U000003FF'}, // Greek.
        {U'\U00000100', U'\U0000017F'}, // Latin Extended A.
        {U'\U00000600', U'\U000006FF'}, // Arabic.
        {U'\U00000900', U'\U0000097F'}, // Devanagari.
        {U'\U00000590', U'\U000005FF'}, // Hebrew.
        {U'\U00003040', U'\U0000309F'}, // Hiragana.
        {U'\U00002000', U'\U0000206F'}, // Punctuation.
        {U'\U00002070', U'\U0000209F'}, // Sub/Super Script.
        {U'\U000020A0', U'\U000020CF'}, // Currency.
    },
    // UTF8CharList::MATHEMATICAL_SYMBOLS
    {
        {U'\U00002200', U'\U000022FF'}, // Math Operators.
        {U'\U00002150', U'\U0000218F'}, // Number Forms.
        {U'\U000025A0', U'\U000025FF'}, // Geometric Shapes.
        {U'\U000027C0', U'\U000027EF'}, // Math Symbols.
        {U'\U00002A00', U'\U00002AFF'}, // Supplemental.
    },
    // UTF8CharList::ALL_OTHERS
    // Skip U+D800 to U+DFFF because they are surrogates and do not represent
    // characters on their own.
    {
        {U'\U000000A0', U'\U000000FF'}, {U'\U00000180', U'\U0000024F'},
        {U'\U00000250', U'\U000002AF'}, {U'\U000002B0', U'\U000002FF'},
        {U'\U00000300', U'\U0000036F'}, {U'\U00000370', U'\U000003FF'},
        {U'\U00000500', U'\U0000052F'}, {U'\U00000530', U'\U0000058F'},
        {U'\U00000590', U'\U000005FF'}, {U'\U00000700', U'\U0000074F'},
        {U'\U00000780', U'\U000007BF'}, {U'\U00000980', U'\U000009FF'},
        {U'\U00000A00', U'\U00000A7F'}, {U'\U00000A80', U'\U00000AFF'},
        {U'\U00000B00', U'\U00000B7F'}, {U'\U00000B80', U'\U00000BFF'},
        {U'\U00000C00', U'\U00000C7F'}, {U'\U00000C80', U'\U00000CFF'},
        {U'\U00000D00', U'\U00000D7F'}, {U'\U00000D80', U'\U00000DFF'},
        {U'\U00000E00', U'\U00000E7F'}, {U'\U00000E80', U'\U00000EFF'},
        {U'\U00000F00', U'\U00000FFF'}, {U'\U00001000', U'\U0000109F'},
        {U'\U000010A0', U'\U000010FF'}, {U'\U00001100', U'\U000011FF'},
        {U'\U00001200', U'\U0000137F'}, {U'\U000013A0', U'\U000013FF'},
        {U'\U00001400', U'\U0000167F'}, {U'\U00001680', U'\U0000169F'},
        {U'\U000016A0', U'\U000016FF'}, {U'\U00001700', U'\U0000171F'},
        {U'\U00001720', U'\U0000173F'}, {U'\U00001740', U'\U0000175F'},
        {U'\U00001760', U'\U0000177F'}, {U'\U00001780', U'\U000017FF'},
        {U'\U00001800', U'\U000018AF'}, {U'\U00001900', U'\U0000194F'},
        {U'\U00001950', U'\U0000197F'}, {U'\U000019E0', U'\U000019FF'},
        {U'\U00001D00', U'\U00001D7F'}, {U'\U00001E00', U'\U00001EFF'},
        {U'\U00001F00', U'\U00001FFF'}, {U'\U000020D0', U'\U000020FF'},
        {U'\U00002100', U'\U0000214F'}, {U'\U00002190', U'\U000021FF'},
        {U'\U00002300', U'\U000023FF'}, {U'\U00002400', U'\U0000243F'},
        {U'\U00002440', U'\U0000245F'}, {U'\U00002460', U'\U000024FF'},
        {U'\U00002500', U'\U0000257F'}, {U'\U00002580', U'\U0000259F'},
        {U'\U00002600', U'\U000026FF'}, {U'\U00002700', U'\U000027BF'},
        {U'\U000027F0', U'\U000027FF'}, {U'\U00002800', U'\U000028FF'},
        {U'\U00002900', U'\U0000297F'}, {U'\U00002980', U'\U000029FF'},
        {U'\U00002B00', U'\U00002BFF'}, {U'\U00002E80', U'\U00002EFF'},
        {U'\U00002F00', U'\U00002FDF'}, {U'\U00002FF0', U'\U00002FFF'},
        {U'\U00003000', U'\U0000303F'}, {U'\U000030A0', U'\U000030FF'},
        {U'\U00003100', U'\U0000312F'}, {U'\U00003130', U'\U0000318F'},
        {U'\U00003190', U'\U0000319F'}, {U'\U000031A0', U'\U000031BF'},
        {U'\U000031F0', U'\U000031FF'}, {U'\U00003200', U'\U000032FF'},
        {U'\U00003300', U'\U000033FF'}, {U'\U00003400', U'\U00004DBF'},
        {U'\U00004DC0', U'\U00004DFF'}, {U'\U00004E00', U'\U00009FFF'},
        {U'\U0000A000', U'\U0000A48F'}, {U'\U0000A490', U'\U0000A4CF'},
        {U'\U0000AC00', U'\U0000D7AF'}, {U'\U0000E000', U'\U0000F8FF'},
        {U'\U0000F900', U'\U0000FAFF'}, {U'\U0000FB00', U'\U0000FB4F'},
        {U'\U0000FB50', U'\U0000FDFF'}, {U'\U0000FE00', U'\U0000FE0F'},
        {U'\U0000FE20', U'\U0000FE2F'}, {U'\U0000FE30', U'\U0000FE4F'},
        {U'\U0000FE50', U'\U0000FE6F'}, {U'\U0000FE70', U'\U0000FEFF'},
        {U'\U0000FF00', U'\U0000FFEF'}, {U'\U0000FFF0', U'\U0000FFFF'},
        {U'\U00010000', U'\U0001007F'}, {U'\U00010080', U'\U000100FF'},
        {U'\U00010100', U'\U0001013F'}, {U'\U00010300', U'\U0001032F'},
        {U'\U00010330', U'\U0001034F'}, {U'\U00010380', U'\U0001039F'},
        {U'\U00010400', U'\U0001044F'}, {U'\U00010450', U'\U0001047F'},
        {U'\U00010480', U'\U000104AF'}, {U'\U00010800', U'\U0001083F'},
        {U'\U0001D000', U'\U0001D0FF'}, {U'\U0001D100', U'\U0001D1FF'},
        {U'\U0001D300', U'\U0001D35F'}, {U'\U0001D400', U'\U0001D7FF'},
        {U'\U00020000', U'\U0002A6DF'}, {U'\U0002F800', U'\U0002FA1F'},
        {U'\U000E0000', U'\U000E007F'},
    }};

FOLLY_ALWAYS_INLINE char32_t getRandomChar(
    FuzzerGenerator& rng,
    const std::vector<std::pair<char32_t, char32_t>>& charSet) {
  const auto& chars = charSet.size() == 1
      ? charSet.front()
      : charSet[rand<uint32_t>(rng) % charSet.size()];
  auto size = chars.second - chars.first;
  auto inc = (rand<uint32_t>(rng) % size);
  char32_t res = chars.first + inc;
  return res;
}

template <typename T>
void FOLLY_ALWAYS_INLINE fillBuffer(
    T& buffer,
    size_t start,
    size_t length,
    FuzzerGenerator& rng,
    const VectorFuzzer::Options& opts) {
  for (size_t i = 0; i < length; ++i) {
    const auto& encoding = (opts.charEncodings.size() == 1)
        ? opts.charEncodings.front()
        : opts.charEncodings[rand<uint32_t>(rng) % opts.charEncodings.size()];
    buffer[start + i] = getRandomChar(rng, kUTFChatSets[encoding]);
  }
}

/// Generates a random string (string size and encoding are passed through
/// Options). Returns a StringView which uses `buf` as the underlying buffer.
template <typename T>
StringView randString(
    FuzzerGenerator& rng,
    const VectorFuzzer::Options& opts,
    std::string& buf,
    std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t>& converter,
    const bool doStringIncrementalGeneration,
    T& reusablePartBuffer,
    const size_t reusablePartLength) {
  buf.clear();

  std::u32string wbuf;
  const size_t stringLength = opts.stringVariableLength
      ? rand<uint32_t>(rng) % opts.stringLength
      : opts.stringLength;
  wbuf.resize(stringLength);

  if (doStringIncrementalGeneration && reusablePartLength != 0) {
    for (size_t i = 0; i < stringLength;) {
      if (rand<bool>(rng)) {
        // Reuse the reusable part.
        size_t j;
        for (j = 0; j < reusablePartLength && i + j < stringLength; ++j) {
          wbuf[i + j] = reusablePartBuffer[j];
        }
        i += j;
      } else {
        // Randomly generate a character at the current index.
        fillBuffer(wbuf, i, 1, rng, opts);
        ++i;
      }
    }
  } else {
    // Randomly generate the entire string.
    fillBuffer(wbuf, 0, stringLength, rng, opts);
  }
  buf.append(converter.to_bytes(wbuf));
  return StringView(buf);
}

template <TypeKind kind>
VectorPtr fuzzConstantPrimitiveImpl(
    memory::MemoryPool* pool,
    const TypePtr& type,
    vector_size_t size,
    FuzzerGenerator& rng,
    const VectorFuzzer::Options& opts) {
  using TCpp = typename TypeTraits<kind>::NativeType;
  if constexpr (std::is_same_v<TCpp, StringView>) {
    std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> converter;
    std::string buf;
    auto stringView = randString(
        rng,
        opts,
        buf,
        converter,
        opts.enableStringIncrementalGeneration,
        "",
        0);

    return std::make_shared<ConstantVector<TCpp>>(
        pool, size, false, type, std::move(stringView));
  }
  if constexpr (std::is_same_v<TCpp, Timestamp>) {
    return std::make_shared<ConstantVector<TCpp>>(
        pool, size, false, type, randTimestamp(rng, opts));
  } else if (type->isShortDecimal()) {
    return std::make_shared<ConstantVector<int64_t>>(
        pool, size, false, type, randDecimal<int64_t>(type, rng, true, false));
  } else if (type->isLongDecimal()) {
    return std::make_shared<ConstantVector<int128_t>>(
        pool, size, false, type, randDecimal<int128_t>(type, rng, false, true));
  } else {
    return std::make_shared<ConstantVector<TCpp>>(
        pool, size, false, type, rand<TCpp>(rng));
  }
}

template <TypeKind kind>
void fuzzFlatPrimitiveImpl(
    const VectorPtr& vector,
    FuzzerGenerator& rng,
    const VectorFuzzer::Options& opts) {
  using TFlat = typename KindToFlatVector<kind>::type;
  using TCpp = typename TypeTraits<kind>::NativeType;

  auto flatVector = vector->as<TFlat>();
  std::string strBuf;

  std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> converter;

  // Randomly generate a reusable part (substring) to prepare for incremental
  // string generation.
  size_t reusablePartLength = opts.enableStringIncrementalGeneration
      ? rand<uint32_t>(rng) % opts.stringLength
      : 0;
  std::u32string reusablePartBuffer;
  reusablePartBuffer.resize(reusablePartLength);
  fillBuffer(reusablePartBuffer, 0, reusablePartLength, rng, opts);

  for (size_t i = 0; i < vector->size(); ++i) {
    if constexpr (std::is_same_v<TCpp, StringView>) {
      if (opts.genTimestampString) {
        auto str =
            std::to_string(randTimestamp(rng, opts).toNanos()).substr(0, 10);
        for (int j = 0; j < rand<uint>(rng) % 24; ++j) {
          str += "," +
              std::to_string(randTimestamp(rng, opts).toNanos()).substr(0, 10);
        }
        flatVector->set(i, StringView(str));
      } else {
        if (i == 0) {
          clearInputCorpus(inputCorpus<std::string>);
        }

        bool selectFromInputCorpus = rand<bool>(rng);
        size_t stringInputCorpusSize = inputCorpus<std::string>.size();
        if (opts.enableDuplicates && selectFromInputCorpus &&
            stringInputCorpusSize > 0) {
          // Reuse an existing string from the corpus as the next input.
          auto index = rand<size_t>(rng) % stringInputCorpusSize;
          auto selectedStringFromCorpus =
              getInputFromInputCorpus(inputCorpus<std::string>, index);
          flatVector->set(i, StringView(selectedStringFromCorpus));
        } else {
          // Generate a new string as the next input.
          bool doStringIncrementalGeneration =
              opts.enableStringIncrementalGeneration ? rand<bool>(rng) : false;

          auto newString = randString(
              rng,
              opts,
              strBuf,
              converter,
              doStringIncrementalGeneration,
              reusablePartBuffer,
              reusablePartLength);

          std::string newStringCopy(newString.data(), newString.size());
          flatVector->set(i, StringView(newStringCopy));
          addInputToInputCorpus(inputCorpus<std::string>, newStringCopy);
        }
      }
    } else if constexpr (std::is_same_v<TCpp, Timestamp>) {
      if (i == 0) {
        clearInputCorpus(inputCorpus<Timestamp>);
      }

      auto selectFromInputCorpus = rand<bool>(rng);
      size_t timestampInputCorpusSize = inputCorpus<Timestamp>.size();
      if (opts.enableDuplicates && selectFromInputCorpus &&
          timestampInputCorpusSize > 0) {
        // Reuse an existing timestamp from the corpus as the next input.
        auto index = rand<size_t>(rng) % timestampInputCorpusSize;
        auto selectedTimestampFromInputCorpus =
            getInputFromInputCorpus(inputCorpus<Timestamp>, index);
        flatVector->set(i, selectedTimestampFromInputCorpus);
      } else {
        // Generate a new timestamp as the next input.
        auto generatedTimestamp = randTimestamp(rng, opts);
        flatVector->set(i, generatedTimestamp);
        addInputToInputCorpus(inputCorpus<Timestamp>, generatedTimestamp);
      }
    } else if constexpr (std::is_same_v<TCpp, int64_t>) {
      if (vector->type()->isShortDecimal()) {
        if (i == 0) {
          clearInputCorpus(inputCorpus<int64_t>);
        }

        auto selectFromInputCorpus = rand<bool>(rng);
        size_t shortDecimalInputCorpusSize = inputCorpus<int64_t>.size();
        if (opts.enableDuplicates && selectFromInputCorpus &&
            shortDecimalInputCorpusSize > 0) {
          // Reuse an existing short decimal from the corpus as the next
          // input.
          auto index = rand<size_t>(rng) % shortDecimalInputCorpusSize;
          auto selectedShortDecimalFromInputCorpus =
              getInputFromInputCorpus(inputCorpus<int64_t>, index);
          flatVector->set(i, selectedShortDecimalFromInputCorpus);
        } else {
          // Generate a new short decimal as the next input.
          auto generatedShortDecimal =
              randDecimal<int64_t>(vector->type(), rng, true, false);
          flatVector->set(i, generatedShortDecimal);
          addInputToInputCorpus(inputCorpus<int64_t>, generatedShortDecimal);
        }
      } else {
        if (i == 0) {
          clearInputCorpus(inputCorpus<int64_t>);
        }

        bool selectFromInputCorpus = rand<bool>(rng);
        size_t bigIntInputCorpusSize = inputCorpus<int64_t>.size();
        if (opts.enableDuplicates && selectFromInputCorpus &&
            bigIntInputCorpusSize > 0) {
          // Reuse an existing bigint from the corpus as the next input.
          auto index = rand<size_t>(rng) % bigIntInputCorpusSize;
          auto selectedBigIntFromInputCorpus =
              getInputFromInputCorpus(inputCorpus<int64_t>, index);
          flatVector->set(i, selectedBigIntFromInputCorpus);
        } else {
          bool isSpecialValue = rand<bool>(rng);
          if (isSpecialValue && opts.enableDuplicates) {
            // Use a special int64_t value as the next input.
            auto selectedSpecialValue = generateSpecialValues<TCpp>(rng);
            flatVector->set(i, selectedSpecialValue);
            addInputToInputCorpus(inputCorpus<int64_t>, selectedSpecialValue);
          } else {
            // Generate a random int64_t value as the next input.
            auto generatedBigInt = rand<TCpp>(rng);
            flatVector->set(i, generatedBigInt);
            addInputToInputCorpus(inputCorpus<int64_t>, generatedBigInt);
          }
        }
      }
    } else if constexpr (std::is_same_v<TCpp, int128_t>) {
      if (vector->type()->isLongDecimal()) {
        if (i == 0) {
          clearInputCorpus(inputCorpus<int128_t>);
        }

        auto selectFromInputCorpus = rand<bool>(rng);
        size_t longDecimalInputCorpusSize = inputCorpus<int128_t>.size();
        if (opts.enableDuplicates && selectFromInputCorpus &&
            longDecimalInputCorpusSize > 0) {
          // Reuse an existing long decimal from the corpus as the next input.
          auto index = rand<size_t>(rng) % longDecimalInputCorpusSize;
          auto selectedLongDecimalFromInputCorpus =
              getInputFromInputCorpus(inputCorpus<int128_t>, index);
          flatVector->set(i, selectedLongDecimalFromInputCorpus);
        } else {
          // Generate a new long decimal as the next input.
          auto generatedLongDecimal =
              randDecimal<int128_t>(vector->type(), rng, false, true);
          flatVector->set(i, generatedLongDecimal);
          addInputToInputCorpus(inputCorpus<int128_t>, generatedLongDecimal);
        }
      } else if (vector->type()->isHugeint()) {
        if (i == 0) {
          clearInputCorpus(inputCorpus<int128_t>);
        }

        bool selectFromInputCorpus = rand<bool>(rng);
        size_t hugeIntInputCorpusSize = inputCorpus<int128_t>.size();
        if (opts.enableDuplicates && selectFromInputCorpus &&
            hugeIntInputCorpusSize > 0) {
          // Reuse an existing hugeint (int128_t) from the corpus as the next
          // input.
          auto index = rand<size_t>(rng) % hugeIntInputCorpusSize;
          auto selectedHugeIntFromInputCorpus =
              getInputFromInputCorpus(inputCorpus<int128_t>, index);
          flatVector->set(i, selectedHugeIntFromInputCorpus);
        } else {
          bool isSpecialValue = rand<bool>(rng);
          if (isSpecialValue && opts.enableDuplicates) {
            // Use a special int128_t value as the next input.
            auto selectedSpecialValue = generateSpecialValues<TCpp>(rng);
            flatVector->set(i, selectedSpecialValue);
            addInputToInputCorpus(inputCorpus<int128_t>, selectedSpecialValue);
          } else {
            // Generate a random int128_t value as the next input.
            auto generatedBigInt = rand<TCpp>(rng);
            flatVector->set(i, generatedBigInt);
            addInputToInputCorpus(inputCorpus<int128_t>, generatedBigInt);
          }
        }
      } else {
        BOLT_NYI();
      }
    } else {
      if (i == 0) {
        clearInputCorpus(inputCorpus<TCpp>);
      }

      bool selectFromInputCorpus = rand<bool>(rng);
      size_t inputCorpusSize = inputCorpus<TCpp>.size();
      if (opts.enableDuplicates && selectFromInputCorpus &&
          inputCorpusSize > 0) {
        // Reuse an existing input as the new input.
        auto index = rand<size_t>(rng) % inputCorpusSize;
        auto selectedInputFromInputCorpus =
            getInputFromInputCorpus(inputCorpus<TCpp>, index);
        flatVector->set(i, selectedInputFromInputCorpus);
      } else {
        bool isSpecialValue = rand<bool>(rng);
        if (isSpecialValue && opts.enableDuplicates) {
          // Use a special value as the next input.
          auto selectedSpecialValue = generateSpecialValues<TCpp>(rng);
          flatVector->set(i, selectedSpecialValue);
          addInputToInputCorpus(inputCorpus<TCpp>, selectedSpecialValue);
        } else {
          // Generate a random new input.
          auto generatedValue = rand<TCpp>(rng);
          flatVector->set(i, generatedValue);
          addInputToInputCorpus(inputCorpus<TCpp>, generatedValue);
        }
      }
    }
  }
}

// Servers as a wrapper around a vector that will be used to load a
// lazyVector. Ensures that the loaded vector will only contain valid rows for
// the row set that it was loaded for. NOTE: If the vector is a multi-level
// dictionary, the indices from all the dictionaries are combined.
class VectorLoaderWrap : public VectorLoader {
 public:
  explicit VectorLoaderWrap(VectorPtr vector) : vector_(vector) {}

  void loadInternal(
      RowSet rowSet,
      ValueHook* hook,
      vector_size_t resultSize,
      VectorPtr* result) override {
    BOLT_CHECK(!hook, "VectorLoaderWrap doesn't support ValueHook");
    SelectivityVector rows(rowSet.back() + 1, false);
    for (auto row : rowSet) {
      rows.setValid(row, true);
    }
    rows.updateBounds();
    *result = makeEncodingPreservedCopy(rows, resultSize);
  }

 private:
  // Returns a copy of 'vector_' while retaining dictionary encoding if
  // present. Multiple dictionary layers are collapsed into one.
  VectorPtr makeEncodingPreservedCopy(
      SelectivityVector& rows,
      vector_size_t vectorSize);
  VectorPtr vector_;
};

bool hasNestedDictionaryLayers(const VectorPtr& baseVector) {
  return baseVector && VectorEncoding::isDictionary(baseVector->encoding()) &&
      VectorEncoding::isDictionary(baseVector->valueVector()->encoding());
}

} // namespace

VectorPtr VectorFuzzer::fuzzNotNull(const TypePtr& type) {
  return fuzzNotNull(type, opts_.vectorSize);
}

VectorPtr VectorFuzzer::fuzzNotNull(const TypePtr& type, vector_size_t size) {
  ScopedOptions restorer(this);
  opts_.nullRatio = 0;
  return fuzz(type, size);
}

VectorPtr VectorFuzzer::fuzz(const TypePtr& type) {
  return fuzz(type, opts_.vectorSize);
}

VectorPtr VectorFuzzer::fuzz(const TypePtr& type, vector_size_t size) {
  VectorPtr vector;
  vector_size_t vectorSize = size;

  bool usingLazyVector = opts_.allowLazyVector && coinToss(0.1);
  // Lazy Vectors cannot be sliced, so we skip this if using lazy wrapping.
  if (!usingLazyVector && coinToss(0.1)) {
    // Extend the underlying vector to allow slicing later.
    vectorSize += rand<uint32_t>(rng_) % 8;
  }

  // 20% chance of adding a constant vector.
  if ((!opts_.fuzzForArrowFlatten || type->isPrimitiveType()) &&
      coinToss(0.2)) {
    vector = fuzzConstant(type, vectorSize);
  } else {
    vector = type->isPrimitiveType() ? fuzzFlatPrimitive(type, vectorSize)
                                     : fuzzComplex(type, vectorSize);
  }

  if (vectorSize > size) {
    auto offset = rand<uint32_t>(rng_) % (vectorSize - size + 1);
    vector = vector->slice(offset, size);
  }

  if (usingLazyVector) {
    vector = wrapInLazyVector(vector);
  }

  // Toss a coin and add dictionary indirections.
  while (coinToss(0.5) && opts_.enableDictionary) {
    if (opts_.fuzzForArrowFlatten &&
        !vector->wrappedVector()->isFlatEncoding()) {
      break;
    }
    vectorSize = size;
    if (!usingLazyVector && vectorSize > 0 && coinToss(0.05)) {
      vectorSize += rand<uint32_t>(rng_) % 8;
    }
    vector = fuzzDictionary(vector, vectorSize);
    if (vectorSize > size) {
      auto offset = rand<uint32_t>(rng_) % (vectorSize - size + 1);
      vector = vector->slice(offset, size);
    }
  }
  BOLT_CHECK_EQ(vector->size(), size);
  return vector;
}

VectorPtr VectorFuzzer::fuzz(const GeneratorSpec& generatorSpec) {
  return generatorSpec.generateData(rng_, pool_, opts_.vectorSize);
}

VectorPtr VectorFuzzer::fuzzConstant(const TypePtr& type) {
  return fuzzConstant(type, opts_.vectorSize);
}

VectorPtr VectorFuzzer::fuzzConstant(const TypePtr& type, vector_size_t size) {
  // For constants, there are two possible cases:
  // - generate a regular constant vector (only for primitive types).
  // - generate a random vector and wrap it using a constant vector.
  if (type->isPrimitiveType() && coinToss(0.5)) {
    // For regular constant vectors, toss a coin to determine its nullability.
    if (coinToss(opts_.nullRatio)) {
      return BaseVector::createNullConstant(type, size, pool_);
    }
    if (type->isUnKnown()) {
      return BaseVector::createNullConstant(type, size, pool_);
    } else {
      return BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          fuzzConstantPrimitiveImpl,
          type->kind(),
          pool_,
          type,
          size,
          rng_,
          opts_);
    }
  }

  // Otherwise, create constant by wrapping around another vector. This will
  // return a null constant if the element being wrapped is null in the
  // generated inner vector.

  // Inner vector size can't be zero.
  auto innerVectorSize = rand<uint32_t>(rng_) % opts_.vectorSize + 1;
  auto constantIndex = rand<vector_size_t>(rng_) % innerVectorSize;

  ScopedOptions restorer(this);
  opts_.allowLazyVector = false;
  // Have a lower cap on repeated sizes inside constant. Otherwise will OOM when
  // flattening.
  if (opts_.maxConstantContainerSize.has_value()) {
    opts_.containerLength = std::min<int32_t>(
        opts_.maxConstantContainerSize.value(), opts_.containerLength);
    opts_.complexElementsMaxSize = std::min<int32_t>(
        opts_.maxConstantContainerSize.value(), opts_.complexElementsMaxSize);
  }
  return BaseVector::wrapInConstant(
      size, constantIndex, fuzz(type, innerVectorSize));
}

VectorPtr VectorFuzzer::fuzzFlat(const TypePtr& type) {
  return fuzzFlat(type, opts_.vectorSize);
}

VectorPtr VectorFuzzer::fuzzFlatNotNull(const TypePtr& type) {
  return fuzzFlatNotNull(type, opts_.vectorSize);
}

VectorPtr VectorFuzzer::fuzzFlatNotNull(
    const TypePtr& type,
    vector_size_t size) {
  ScopedOptions restorer(this);
  opts_.nullRatio = 0;
  return fuzzFlat(type, size);
}

VectorPtr VectorFuzzer::fuzzFlat(const TypePtr& type, vector_size_t size) {
  // Primitive types.
  if (type->isPrimitiveType()) {
    return fuzzFlatPrimitive(type, size);
  }
  // Arrays.
  else if (type->isArray()) {
    const auto& elementType = type->asArray().elementType();
    auto elementsLength = getElementsVectorLength(opts_, size);

    auto elements = opts_.containerHasNulls
        ? fuzzFlat(elementType, elementsLength)
        : fuzzFlatNotNull(elementType, elementsLength);
    return fuzzArray(elements, size);
  }
  // Maps.
  else if (type->isMap()) {
    // Do not initialize keys and values inline in the fuzzMap call as C++ does
    // not specify the order they'll be called in, leading to inconsistent
    // results across platforms.
    const auto& keyType = type->asMap().keyType();
    const auto& valueType = type->asMap().valueType();
    auto length = getElementsVectorLength(opts_, size);

    auto keys = opts_.normalizeMapKeys || !opts_.containerHasNulls
        ? fuzzFlatNotNull(keyType, length)
        : fuzzFlat(keyType, length);
    auto values = opts_.containerHasNulls ? fuzzFlat(valueType, length)
                                          : fuzzFlatNotNull(valueType, length);
    return fuzzMap(keys, values, size);
  }
  // Rows.
  else if (type->isRow()) {
    const auto& rowType = type->asRow();
    std::vector<VectorPtr> childrenVectors;
    childrenVectors.reserve(rowType.children().size());

    for (const auto& childType : rowType.children()) {
      childrenVectors.emplace_back(
          opts_.containerHasNulls ? fuzzFlat(childType, size)
                                  : fuzzFlatNotNull(childType, size));
    }

    return fuzzRow(std::move(childrenVectors), rowType.names(), size);
  } else {
    BOLT_UNREACHABLE();
  }
}

VectorPtr VectorFuzzer::fuzzFlatPrimitive(
    const TypePtr& type,
    vector_size_t size) {
  BOLT_CHECK(type->isPrimitiveType());
  auto vector = BaseVector::create(type, size, pool_);

  if (type->isUnKnown()) {
    auto* rawNulls = vector->mutableRawNulls();
    bits::fillBits(rawNulls, 0, size, bits::kNull);
  } else {
    // First, fill it with random values.
    // TODO: We should bias towards edge cases (min, max, Nan, etc).
    auto kind = vector->typeKind();
    BOLT_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
        fuzzFlatPrimitiveImpl, kind, vector, rng_, opts_);

    // Second, generate a random null vector.
    for (size_t i = 0; i < vector->size(); ++i) {
      if (coinToss(opts_.nullRatio)) {
        vector->setNull(i, true);
      }
    }
  }
  return vector;
}

VectorPtr VectorFuzzer::fuzzComplex(const TypePtr& type, vector_size_t size) {
  ScopedOptions restorer(this);
  opts_.allowLazyVector = false;

  switch (type->kind()) {
    case TypeKind::ROW:
      return fuzzRow(std::dynamic_pointer_cast<const RowType>(type), size);

    case TypeKind::ARRAY: {
      const auto& elementType = type->asArray().elementType();
      auto elementsLength = getElementsVectorLength(opts_, size);

      auto elements = opts_.containerHasNulls
          ? fuzz(elementType, elementsLength)
          : fuzzNotNull(elementType, elementsLength);
      return fuzzArray(elements, size);
    }

    case TypeKind::MAP: {
      // Do not initialize keys and values inline in the fuzzMap call as C++
      // does not specify the order they'll be called in, leading to
      // inconsistent results across platforms.
      const auto& keyType = type->asMap().keyType();
      const auto& valueType = type->asMap().valueType();
      auto length = getElementsVectorLength(opts_, size);

      auto keys = opts_.normalizeMapKeys || !opts_.containerHasNulls
          ? fuzzNotNull(keyType, length)
          : fuzz(keyType, length);
      auto values = opts_.containerHasNulls ? fuzz(valueType, length)
                                            : fuzzNotNull(valueType, length);
      return fuzzMap(keys, values, size);
    }

    default:
      BOLT_UNREACHABLE("Unexpected type: {}", type->toString());
  }
  return nullptr; // no-op.
}

VectorPtr VectorFuzzer::fuzzDictionary(const VectorPtr& vector) {
  return fuzzDictionary(vector, vector->size());
}

VectorPtr VectorFuzzer::fuzzDictionary(
    const VectorPtr& vector,
    vector_size_t size) {
  const size_t vectorSize = vector->size();
  BOLT_CHECK(
      vectorSize > 0 || size == 0,
      "Cannot build a non-empty dictionary on an empty underlying vector");
  BufferPtr indices = fuzzIndices(size, vectorSize);

  auto nulls = opts_.dictionaryHasNulls ? fuzzNulls(size) : nullptr;
  return BaseVector::wrapInDictionary(nulls, indices, size, vector);
}

void VectorFuzzer::fuzzOffsetsAndSizes(
    BufferPtr& offsets,
    BufferPtr& sizes,
    size_t elementsSize,
    size_t size) {
  offsets = allocateOffsets(size, pool_);
  sizes = allocateSizes(size, pool_);
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  auto rawSizes = sizes->asMutable<vector_size_t>();

  size_t containerAvgLength = std::max(elementsSize / size, 1UL);
  size_t childSize = 0;
  size_t length = 0;

  for (auto i = 0; i < size; ++i) {
    rawOffsets[i] = childSize;

    // If variable length, generate a random number between zero and 2 *
    // containerAvgLength (so that the average of generated containers size is
    // equal to number of input elements).
    if (opts_.containerVariableLength) {
      length = rand<uint32_t>(rng_) % (containerAvgLength * 2);
    } else {
      length = containerAvgLength;
    }

    // If we exhausted the available elements, add empty arrays.
    if ((childSize + length) > elementsSize) {
      length = 0;
    }
    rawSizes[i] = length;
    childSize += length;
  }
}

ArrayVectorPtr VectorFuzzer::fuzzArray(
    const VectorPtr& elements,
    vector_size_t size) {
  BufferPtr offsets, sizes;
  fuzzOffsetsAndSizes(offsets, sizes, elements->size(), size);
  return std::make_shared<ArrayVector>(
      pool_,
      ARRAY(elements->type()),
      fuzzNulls(size),
      size,
      offsets,
      sizes,
      elements);
}

VectorPtr VectorFuzzer::normalizeMapKeys(
    const VectorPtr& keys,
    size_t mapSize,
    BufferPtr& offsets,
    BufferPtr& sizes) {
  // Map keys cannot be null.
  const auto& nulls = keys->nulls();
  if (nulls) {
    BOLT_CHECK_EQ(
        BaseVector::countNulls(nulls, 0, keys->size()),
        0,
        "Map keys cannot be null when opt.normalizeMapKeys is true");
  }

  auto rawOffsets = offsets->as<vector_size_t>();
  auto rawSizes = sizes->asMutable<vector_size_t>();

  // Looks for duplicate key values.
  std::unordered_set<uint64_t> set;
  for (size_t i = 0; i < mapSize; ++i) {
    set.clear();

    for (size_t j = 0; j < rawSizes[i]; ++j) {
      vector_size_t idx = rawOffsets[i] + j;
      uint64_t hash = keys->hashValueAt(idx);

      // If we find the same hash (either same key value or hash colision), we
      // cut it short by reducing this element's map size. This should not
      // happen frequently.
      auto it = set.find(hash);
      if (it != set.end()) {
        rawSizes[i] = j;
        break;
      }
      set.insert(hash);
    }
  }
  return keys;
}

MapVectorPtr VectorFuzzer::fuzzMap(
    const VectorPtr& keys,
    const VectorPtr& values,
    vector_size_t size) {
  size_t elementsSize = std::min(keys->size(), values->size());
  BufferPtr offsets, sizes;
  fuzzOffsetsAndSizes(offsets, sizes, elementsSize, size);
  return std::make_shared<MapVector>(
      pool_,
      MAP(keys->type(), values->type()),
      fuzzNulls(size),
      size,
      offsets,
      sizes,
      opts_.normalizeMapKeys ? normalizeMapKeys(keys, size, offsets, sizes)
                             : keys,
      values);
}

RowVectorPtr VectorFuzzer::fuzzInputRow(const RowTypePtr& rowType) {
  return fuzzRow(rowType, opts_.vectorSize, false);
}

RowVectorPtr VectorFuzzer::fuzzInputFlatRow(const RowTypePtr& rowType) {
  std::vector<VectorPtr> children;
  auto size = static_cast<vector_size_t>(opts_.vectorSize);
  children.reserve(rowType->size());
  for (auto i = 0; i < rowType->size(); ++i) {
    children.emplace_back(fuzzFlat(rowType->childAt(i), size));
  }

  return std::make_shared<RowVector>(
      pool_, rowType, nullptr, size, std::move(children));
}

RowVectorPtr VectorFuzzer::fuzzRow(
    std::vector<VectorPtr>&& children,
    std::vector<std::string> childrenNames,
    vector_size_t size) {
  std::vector<TypePtr> types;
  types.reserve(children.size());

  for (const auto& child : children) {
    types.emplace_back(child->type());
  }

  return std::make_shared<RowVector>(
      pool_,
      ROW(std::move(childrenNames), std::move(types)),
      fuzzNulls(size),
      size,
      std::move(children));
}

RowVectorPtr VectorFuzzer::fuzzRow(
    std::vector<VectorPtr>&& children,
    vector_size_t size) {
  std::vector<TypePtr> types;
  types.reserve(children.size());

  for (const auto& child : children) {
    types.emplace_back(child->type());
  }

  return std::make_shared<RowVector>(
      pool_, ROW(std::move(types)), fuzzNulls(size), size, std::move(children));
}

RowVectorPtr VectorFuzzer::fuzzRow(const RowTypePtr& rowType) {
  ScopedOptions restorer(this);
  opts_.allowLazyVector = false;
  return fuzzRow(rowType, opts_.vectorSize);
}

RowVectorPtr VectorFuzzer::fuzzRow(
    const RowTypePtr& rowType,
    vector_size_t size,
    bool allowTopLevelNulls) {
  std::vector<VectorPtr> children;
  children.reserve(rowType->size());

  for (auto i = 0; i < rowType->size(); ++i) {
    children.push_back(
        opts_.containerHasNulls ? fuzz(rowType->childAt(i), size)
                                : fuzzNotNull(rowType->childAt(i), size));
  }

  return std::make_shared<RowVector>(
      pool_,
      rowType,
      allowTopLevelNulls ? fuzzNulls(size) : nullptr,
      size,
      std::move(children));
}

BufferPtr VectorFuzzer::fuzzNulls(vector_size_t size) {
  NullsBuilder builder{size, pool_};
  for (size_t i = 0; i < size; ++i) {
    if (coinToss(opts_.nullRatio)) {
      builder.setNull(i);
    }
  }
  return builder.build();
}

BufferPtr VectorFuzzer::fuzzIndices(
    vector_size_t size,
    vector_size_t baseVectorSize) {
  BOLT_CHECK_GE(size, 0);
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(size, pool_);
  auto rawIndices = indices->asMutable<vector_size_t>();

  for (size_t i = 0; i < size; ++i) {
    rawIndices[i] = rand<vector_size_t>(rng_) % baseVectorSize;
  }
  return indices;
}

TypePtr VectorFuzzer::randShortDecimalType() {
  return bolt::randShortDecimalType(rng_);
}

TypePtr VectorFuzzer::randLongDecimalType() {
  return bolt::randLongDecimalType(rng_);
}

TypePtr VectorFuzzer::randType(int maxDepth, bool withDecimal) {
  return bolt::randType(rng_, maxDepth, withDecimal, opts_.enableHugeint);
}

TypePtr VectorFuzzer::randOrderableType(int maxDepth) {
  return bolt::randOrderableType(rng_, maxDepth);
}

TypePtr VectorFuzzer::randType(
    const std::vector<TypePtr>& scalarTypes,
    int maxDepth) {
  return bolt::randType(rng_, scalarTypes, maxDepth);
}

RowTypePtr VectorFuzzer::randRowType(int maxDepth) {
  return bolt::randRowType(rng_, maxDepth);
}

RowTypePtr VectorFuzzer::randRowType(
    const std::vector<TypePtr>& scalarTypes,
    int maxDepth) {
  return bolt::randRowType(rng_, scalarTypes, maxDepth);
}

VectorPtr VectorFuzzer::wrapInLazyVector(VectorPtr baseVector) {
  if (hasNestedDictionaryLayers(baseVector)) {
    auto indices = baseVector->wrapInfo();
    auto values = baseVector->valueVector();
    auto nulls = baseVector->nulls();

    auto copiedNulls = AlignedBuffer::copy(baseVector->pool(), nulls);

    return BaseVector::wrapInDictionary(
        copiedNulls, indices, baseVector->size(), wrapInLazyVector(values));
  }
  return std::make_shared<LazyVector>(
      baseVector->pool(),
      baseVector->type(),
      baseVector->size(),
      std::make_unique<VectorLoaderWrap>(baseVector));
}

RowVectorPtr VectorFuzzer::fuzzRowChildrenToLazy(RowVectorPtr rowVector) {
  std::vector<VectorPtr> children;
  BOLT_CHECK_NULL(rowVector->nulls());
  for (auto child : rowVector->children()) {
    BOLT_CHECK_NOT_NULL(child);
    BOLT_CHECK(!child->isLazy());
    // TODO: If child has dictionary wrappings, add ability to insert lazy wrap
    // between those layers.
    children.push_back(coinToss(0.5) ? wrapInLazyVector(child) : child);
  }
  return std::make_shared<RowVector>(
      pool_,
      rowVector->type(),
      nullptr,
      rowVector->size(),
      std::move(children));
}

// Utility function to check if a RowVector can have nested lazy children.
// This is only possible if the rows for its children map 1-1 with the top level
// rows (Rows of 'baseRowVector''s parent). For this to be true, the row vector
// cannot have any encoding layers over it, and it cannot have nulls.
bool canWrapRowChildInLazy(const VectorPtr& baseRowVector) {
  if (baseRowVector->typeKind() == TypeKind::ROW) {
    RowVector* rowVector = baseRowVector->as<RowVector>();
    if (rowVector) {
      return rowVector->nulls() == nullptr;
    }
  }
  return false;
}

// Utility function That only takes a row vector that passes the check in
// canWrapChildrenInLazy() and either picks the first child to be wrapped in
// lazy OR picks the first row vector child that passes canWrapChildrenInLazy()
// and traverses its children recursively.
VectorPtr wrapRowChildInLazyRecursive(VectorPtr& baseVector) {
  RowVector* rowVector = baseVector->as<RowVector>();
  BOLT_CHECK_NOT_NULL(rowVector);
  std::vector<VectorPtr> children;
  children.reserve(rowVector->childrenSize());
  bool foundChildVectorToWrap = false;
  for (column_index_t i = 0; i < rowVector->childrenSize(); i++) {
    auto child = rowVector->childAt(i);
    if (!foundChildVectorToWrap && canWrapRowChildInLazy(child)) {
      child = wrapRowChildInLazyRecursive(child);
      foundChildVectorToWrap = true;
    }
    children.push_back(child);
  }
  if (!foundChildVectorToWrap && !children.empty()) {
    children[0] = VectorFuzzer::wrapInLazyVector(children[0]);
  }

  BufferPtr newNulls = nullptr;
  if (rowVector->nulls()) {
    newNulls = AlignedBuffer::copy(rowVector->pool(), rowVector->nulls());
  }

  return std::make_shared<RowVector>(
      rowVector->pool(),
      rowVector->type(),
      std::move(newNulls),
      rowVector->size(),
      std::move(children));
}

RowVectorPtr VectorFuzzer::fuzzRowChildrenToLazy(
    RowVectorPtr rowVector,
    const std::vector<int>& columnsToWrapInLazy) {
  if (columnsToWrapInLazy.empty()) {
    return rowVector;
  }
  std::vector<VectorPtr> children;
  int listIndex = 0;
  for (column_index_t i = 0; i < rowVector->childrenSize(); i++) {
    auto child = rowVector->childAt(i);
    BOLT_USER_CHECK_NOT_NULL(child);
    BOLT_USER_CHECK(!child->isLazy());
    if (listIndex < columnsToWrapInLazy.size() &&
        i == (column_index_t)std::abs(columnsToWrapInLazy[listIndex])) {
      child = canWrapRowChildInLazy(child)
          ? wrapRowChildInLazyRecursive(child)
          : VectorFuzzer::wrapInLazyVector(child);
      if (columnsToWrapInLazy[listIndex] < 0) {
        // Negative index represents a lazy vector that is loaded.
        child->loadedVector();
      }
      listIndex++;
    }
    children.push_back(child);
  }

  BufferPtr newNulls = nullptr;
  if (rowVector->nulls()) {
    newNulls = AlignedBuffer::copy(rowVector->pool(), rowVector->nulls());
  }

  return std::make_shared<RowVector>(
      rowVector->pool(),
      rowVector->type(),
      std::move(newNulls),
      rowVector->size(),
      std::move(children));
}

VectorPtr VectorLoaderWrap::makeEncodingPreservedCopy(
    SelectivityVector& rows,
    vector_size_t vectorSize) {
  DecodedVector decoded;
  decoded.decode(*vector_, rows, false);

  if (decoded.isConstantMapping() || decoded.isIdentityMapping()) {
    VectorPtr result;
    BaseVector::ensureWritable(rows, vector_->type(), vector_->pool(), result);
    result->resize(vectorSize);
    result->copy(vector_.get(), rows, nullptr, false);
    return result;
  }

  SelectivityVector baseRows;
  auto baseVector = decoded.base();

  baseRows.resize(baseVector->size(), false);
  rows.applyToSelected([&](auto row) {
    if (!decoded.isNullAt(row)) {
      baseRows.setValid(decoded.index(row), true);
    }
  });
  baseRows.updateBounds();

  VectorPtr baseResult;
  BaseVector::ensureWritable(
      baseRows, baseVector->type(), vector_->pool(), baseResult);
  baseResult->copy(baseVector, baseRows, nullptr, false);

  BufferPtr indices = allocateIndices(vectorSize, vector_->pool());
  auto rawIndices = indices->asMutable<vector_size_t>();
  auto decodedIndices = decoded.indices();
  rows.applyToSelected(
      [&](auto row) { rawIndices[row] = decodedIndices[row]; });

  BufferPtr nulls = nullptr;
  if (decoded.nulls(&rows) || vectorSize > rows.end()) {
    // We fill [rows.end(), vectorSize) with nulls then copy nulls for selected
    // baseRows.
    nulls = allocateNulls(vectorSize, vector_->pool(), bits::kNull);
    if (baseRows.hasSelections()) {
      if (decoded.nulls(&rows)) {
        std::memcpy(
            nulls->asMutable<uint64_t>(),
            decoded.nulls(&rows),
            bits::nbytes(rows.end()));
      } else {
        bits::fillBits(
            nulls->asMutable<uint64_t>(), 0, rows.end(), bits::kNotNull);
      }
    }
  }

  return BaseVector::wrapInDictionary(
      std::move(nulls), std::move(indices), vectorSize, baseResult);
}

namespace {

const std::vector<TypePtr> defaultScalarTypes(
    bool withDecimal = true,
    bool withHugeint = true) {
  // @TODO Add decimal TypeKinds to randType.
  static std::vector<TypePtr> kScalarTypes{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
      DATE(),
      INTERVAL_DAY_TIME(),
      DECIMAL(18, 5),
      DECIMAL(38, 18)};

  // There is a very limited decimal support, for example
  // protobuf generation
  static std::vector<TypePtr> kScalarTypesWithoutDecimal{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
      DATE(),
      INTERVAL_DAY_TIME()};

  // dwrf::BaseColumnWriter does not support HUGEINT yet, which can cause
  // failures when running JoinFuzzerTest.
  static std::vector<TypePtr> kScalarTypesWithoutHugeint{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
      DATE(),
      INTERVAL_DAY_TIME(),
      DECIMAL(18, 5)};

  return withDecimal ? (withHugeint ? kScalarTypes : kScalarTypesWithoutHugeint)
                     : kScalarTypesWithoutDecimal;
}
} // namespace

TypePtr randType(
    FuzzerGenerator& rng,
    int maxDepth,
    bool withDecimal,
    bool withHugeint) {
  return randType(rng, defaultScalarTypes(withDecimal, withHugeint), maxDepth);
}

TypePtr randOrderableType(FuzzerGenerator& rng, int maxDepth) {
  // Should we generate a scalar type?
  if (maxDepth <= 1 || rand<bool>(rng)) {
    return randType(rng, 0);
  }

  // ARRAY or ROW?
  if (rand<bool>(rng)) {
    return ARRAY(randOrderableType(rng, maxDepth - 1));
  }

  auto numFields = 1 + rand<uint32_t>(rng) % 7;
  std::vector<std::string> names;
  std::vector<TypePtr> fields;
  for (int i = 0; i < numFields; ++i) {
    names.push_back(fmt::format("f{}", i));
    fields.push_back(randOrderableType(rng, maxDepth - 1));
  }
  return ROW(std::move(names), std::move(fields));
}

TypePtr randType(
    FuzzerGenerator& rng,
    const std::vector<TypePtr>& scalarTypes,
    int maxDepth) {
  const int numScalarTypes = scalarTypes.size();
  // Should we generate a scalar type?
  if (maxDepth <= 1 || rand<bool>(rng)) {
    auto type = scalarTypes[rand<uint32_t>(rng) % numScalarTypes];
    if (type->isShortDecimal()) {
      return randShortDecimalType(rng);
    }
    if (type->isLongDecimal()) {
      return randLongDecimalType(rng);
    }
    return type;
  }
  switch (rand<uint32_t>(rng) % 3) {
    case 0:
      return MAP(
          randType(rng, scalarTypes, 0),
          randType(rng, scalarTypes, maxDepth - 1));
    case 1:
      return ARRAY(randType(rng, scalarTypes, maxDepth - 1));
    default:
      return randRowType(rng, scalarTypes, maxDepth - 1);
  }
}

RowTypePtr randRowType(FuzzerGenerator& rng, int maxDepth) {
  return randRowType(rng, defaultScalarTypes(), maxDepth);
}

RowTypePtr randRowType(
    FuzzerGenerator& rng,
    const std::vector<TypePtr>& scalarTypes,
    int maxDepth) {
  int numFields = 1 + rand<uint32_t>(rng) % 7;
  std::vector<std::string> names;
  std::vector<TypePtr> fields;
  for (int i = 0; i < numFields; ++i) {
    names.push_back(fmt::format("f{}", i));
    fields.push_back(randType(rng, scalarTypes, maxDepth));
  }
  return ROW(std::move(names), std::move(fields));
}

} // namespace bytedance::bolt