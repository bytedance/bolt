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

#include "bolt/functions/sparksql/EncodeDecode.h"

#include <unicode/ucnv.h>
#include <unicode/ustring.h>
#include <unicode/utypes.h>

#include "bolt/expression/DecodedArgs.h"
#include "bolt/expression/VectorWriters.h"
#include "bolt/vector/ConstantVector.h"
#include "bolt/vector/FlatVector.h"

namespace bytedance::bolt::functions::sparksql {
namespace {

/// Length of the well-formed UTF-8 sequence starting at 'p', by lead byte.
size_t utf8SequenceLength(unsigned char lead) {
  if (lead < 0x80) {
    return 1;
  }
  if ((lead >> 5) == 0x06) {
    return 2;
  }
  if ((lead >> 4) == 0x0E) {
    return 3;
  }
  return 4;
}

/// Length of the maximal subpart of the ill-formed UTF-8 sequence at
/// 'data[index]', or 0 when the sequence there is well-formed.
///
/// Java replaces each *maximal subpart* of an ill-formed sequence with a single
/// U+FFFD (Unicode 16.0 section 3.9, "U+FFFD Substitution of Maximal
/// Subparts"), while ICU's UTF-8 converter emits one U+FFFD per byte. The two
/// disagree whenever a malformed sequence is longer than one byte: for
/// X'EDA080' Java produces one U+FFFD and ICU produces three.
///
/// The rules below mirror OpenJDK sun.nio.cs.UTF_8's malformedN() and
/// isMalformed3/isMalformed4 helpers so the boundaries match exactly.
size_t
malformedUtf8Length(const unsigned char* data, size_t size, size_t index) {
  const auto isContinuation = [](unsigned char b) {
    return (b & 0xC0) == 0x80;
  };
  const unsigned char b1 = data[index];
  const size_t remaining = size - index;

  if (b1 < 0x80) {
    return 0;
  }
  if ((b1 >> 5) == 0x06) {
    // 110xxxxx: C0 and C1 are overlong, so only C2..DF can start a sequence.
    if ((b1 & 0x1E) == 0) {
      return 1;
    }
    return (remaining < 2 || !isContinuation(data[index + 1])) ? 1 : 0;
  }
  if ((b1 >> 4) == 0x0E) {
    // 1110xxxx
    if (remaining < 2) {
      return 1;
    }
    const unsigned char b2 = data[index + 1];
    if ((b1 == 0xE0 && (b2 & 0xE0) == 0x80) || !isContinuation(b2)) {
      return 1;
    }
    if (remaining < 3 || !isContinuation(data[index + 2])) {
      return 2;
    }
    const uint32_t codePoint =
        ((b1 & 0x0Fu) << 12) | ((b2 & 0x3Fu) << 6) | (data[index + 2] & 0x3Fu);
    // A surrogate is a single ill-formed 3-byte subpart, not three subparts.
    return (codePoint >= 0xD800 && codePoint <= 0xDFFF) ? 3 : 0;
  }
  if ((b1 >> 3) == 0x1E) {
    // 11110xxx
    if (remaining < 2) {
      return 1;
    }
    const unsigned char b2 = data[index + 1];
    if (b1 > 0xF4 || (b1 == 0xF0 && (b2 < 0x90 || b2 > 0xBF)) ||
        (b1 == 0xF4 && (b2 & 0xF0) != 0x80) || !isContinuation(b2)) {
      return 1;
    }
    if (remaining < 3 || !isContinuation(data[index + 2])) {
      return 2;
    }
    return (remaining < 4 || !isContinuation(data[index + 3])) ? 3 : 0;
  }
  // A stray continuation byte, or F5..FF.
  return 1;
}

/// Decodes UTF-8 the way Java's CharsetDecoder does, replacing each maximal
/// ill-formed subpart with one U+FFFD. Well-formed spans are copied verbatim,
/// so valid input costs a scan and a memcpy.
void decodeUtf8WithJavaReplacement(StringView input, std::string& out) {
  static constexpr char kReplacement[] = "\xEF\xBF\xBD";
  const auto* data = reinterpret_cast<const unsigned char*>(input.data());
  const size_t size = input.size();

  out.clear();
  size_t index = 0;
  size_t validFrom = 0;
  while (index < size) {
    const size_t malformed = malformedUtf8Length(data, size, index);
    if (malformed == 0) {
      index += utf8SequenceLength(data[index]);
      continue;
    }
    if (index > validFrom) {
      out.append(input.data() + validFrom, index - validFrom);
    }
    out.append(kReplacement, 3);
    index += malformed;
    validFrom = index;
  }
  if (size > validFrom) {
    out.append(input.data() + validFrom, size - validFrom);
  }
}

/// Rejects names that Java's Charset.forName would reject outright.
///
/// ICU is far more permissive than Java: it accepts " UTF-8", "UTF-8 " and
/// spellings such as "ISO8859_1" that Java does not resolve, so without this
/// check Bolt would offload queries that Spark itself fails.
///
/// The rules mirror OpenJDK's Charset.checkName: the name must be non-empty,
/// may contain only A-Z, a-z, 0-9 and - + : _ . and must start with a letter
/// or a digit. Note '_' *is* legal inside a Java charset name, so "UTF_8"
/// fails in Java at alias lookup rather than at validation; either way it must
/// not be silently accepted here.
void validateJavaCharsetName(const std::string& charset) {
  bool legal = !charset.empty();
  for (size_t i = 0; legal && i < charset.size(); ++i) {
    const char c = charset[i];
    if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9')) {
      continue;
    }
    // Legal only after the first character.
    if (i != 0 && (c == '-' || c == '+' || c == ':' || c == '_' || c == '.')) {
      continue;
    }
    legal = false;
  }
  if (!legal) {
    BOLT_USER_FAIL("Illegal charset name '{}'", charset);
  }
}

/// Maps a charset name to the ICU converter that encodes the way Java does.
///
/// Java's "UTF-16" writes a big-endian BOM (FE FF) followed by big-endian code
/// units. ICU's "UTF-16" converter instead writes FF FE and little-endian units
/// on a little-endian host, so using it directly produces bytes Spark would
/// not. ICU's "UnicodeBig" is the fixed big-endian-with-BOM converter that
/// matches.
///
/// This substitution applies to encoding only. On decode, ICU's "UTF-16"
/// correctly honours either BOM and defaults to big-endian when none is
/// present, which is exactly Java's contract; "UnicodeBig" would mis-decode
/// input that carries a little-endian BOM.
std::string resolveCharsetForEncode(const std::string& charset) {
  UErrorCode status = U_ZERO_ERROR;
  UConverter* converter = ucnv_open(charset.c_str(), &status);
  if (U_FAILURE(status)) {
    // Leave the name alone; opening it again below produces the user error.
    return charset;
  }
  const char* canonical = ucnv_getName(converter, &status);
  const bool isUtf16 = U_SUCCESS(status) && canonical != nullptr &&
      std::string(canonical) == "UTF-16";
  ucnv_close(converter);
  return isUtf16 ? "UnicodeBig" : charset;
}

/// Owns a UConverter for one charset name.
///
/// Spark's encode/decode take the charset as an argument, so when that argument
/// is constant the converter is opened once at plan time instead of per row.
///
/// 'forEncode' selects the direction, which matters for one charset: see
/// resolveCharsetForEncode.
class Converter {
 public:
  Converter(const std::string& charset, bool forEncode)
      : charset_(charset),
        icuCharset_(forEncode ? resolveCharsetForEncode(charset) : charset) {
    validateJavaCharsetName(charset);
    UErrorCode status = U_ZERO_ERROR;
    converter_ = ucnv_open(icuCharset_.c_str(), &status);
    if (U_FAILURE(status)) {
      // Spark throws for an unsupported charset rather than returning NULL;
      // an unknown name is a query-authoring error, not a data value.
      BOLT_USER_FAIL(
          "Unsupported charset '{}': {}", charset_, u_errorName(status));
    }
    UErrorCode nameStatus = U_ZERO_ERROR;
    const char* canonical = ucnv_getName(converter_, &nameStatus);
    isUtf8_ = U_SUCCESS(nameStatus) && canonical != nullptr &&
        std::string(canonical) == "UTF-8";
    // Java's String.getBytes substitutes '?' for characters the target charset
    // cannot represent. ICU's own default is 0x1A, which would silently produce
    // bytes that differ from Spark, so set it explicitly.
    //
    // ucnv_setSubstString is used rather than ucnv_setSubstChars because the
    // latter takes raw bytes and rejects a single-byte '?' for UTF-16BE/LE/
    // UTF-16, where a valid substitute is two bytes wide. Passing the character
    // and letting ICU encode it works for every charset.
    const UChar substitute[1] = {u'?'};
    ucnv_setSubstString(converter_, substitute, 1, &status);
    if (U_FAILURE(status)) {
      ucnv_close(converter_);
      BOLT_USER_FAIL(
          "Failed to configure charset '{}': {}",
          charset_,
          u_errorName(status));
    }
  }

  ~Converter() {
    if (converter_ != nullptr) {
      ucnv_close(converter_);
    }
  }

  Converter(const Converter&) = delete;
  Converter& operator=(const Converter&) = delete;

  /// Encodes UTF-8 'input' into this charset, appending to 'out'.
  void encode(StringView input, std::string& out) const {
    // UTF-8 -> UTF-16, which is the pivot ICU converts from.
    UErrorCode status = U_ZERO_ERROR;
    int32_t utf16Length = 0;
    u_strFromUTF8(
        nullptr, 0, &utf16Length, input.data(), input.size(), &status);
    // Pre-flighting always reports a buffer overflow; only other codes matter.
    if (status != U_BUFFER_OVERFLOW_ERROR && U_FAILURE(status)) {
      BOLT_USER_FAIL("Invalid UTF-8 input: {}", u_errorName(status));
    }
    status = U_ZERO_ERROR;
    std::vector<UChar> utf16(utf16Length + 1);
    u_strFromUTF8(
        utf16.data(),
        utf16.size(),
        &utf16Length,
        input.data(),
        input.size(),
        &status);
    if (U_FAILURE(status)) {
      BOLT_USER_FAIL("Invalid UTF-8 input: {}", u_errorName(status));
    }

    const int32_t capacity = UCNV_GET_MAX_BYTES_FOR_STRING(
        utf16Length, ucnv_getMaxCharSize(converter_));
    out.resize(capacity);
    status = U_ZERO_ERROR;
    // Reset so a previous row's trailing state cannot leak into this one.
    ucnv_resetFromUnicode(converter_);
    const int32_t size = ucnv_fromUChars(
        converter_, out.data(), capacity, utf16.data(), utf16Length, &status);
    if (U_FAILURE(status)) {
      BOLT_USER_FAIL(
          "Failed to encode to charset '{}': {}",
          charset_,
          u_errorName(status));
    }
    out.resize(size);
  }

  /// Decodes 'input' from this charset into UTF-8, appending to 'out'.
  void decode(StringView input, std::string& out) const {
    if (isUtf8_) {
      // ICU would emit one U+FFFD per bad byte; Java emits one per maximal
      // ill-formed subpart. Handle UTF-8 directly to match Spark.
      decodeUtf8WithJavaReplacement(input, out);
      return;
    }
    UErrorCode status = U_ZERO_ERROR;
    // One UChar per input byte is always enough: no charset produces more
    // UTF-16 code units than it consumes bytes.
    std::vector<UChar> utf16(input.size() + 1);
    ucnv_resetToUnicode(converter_);
    int32_t utf16Length = ucnv_toUChars(
        converter_,
        utf16.data(),
        utf16.size(),
        input.data(),
        input.size(),
        &status);
    if (status == U_BUFFER_OVERFLOW_ERROR) {
      // One UTF-16 code unit per input byte covers the charsets Spark
      // documents, but an ICU mapping table may expand one byte into several
      // code units. ICU reports the required length, so grow and retry rather
      // than failing the row.
      status = U_ZERO_ERROR;
      utf16.resize(utf16Length + 1);
      ucnv_resetToUnicode(converter_);
      utf16Length = ucnv_toUChars(
          converter_,
          utf16.data(),
          utf16.size(),
          input.data(),
          input.size(),
          &status);
    }
    if (U_FAILURE(status)) {
      BOLT_USER_FAIL(
          "Failed to decode from charset '{}': {}",
          charset_,
          u_errorName(status));
    }

    int32_t utf8Length = 0;
    status = U_ZERO_ERROR;
    u_strToUTF8(nullptr, 0, &utf8Length, utf16.data(), utf16Length, &status);
    if (status != U_BUFFER_OVERFLOW_ERROR && U_FAILURE(status)) {
      BOLT_USER_FAIL("Failed to produce UTF-8: {}", u_errorName(status));
    }
    status = U_ZERO_ERROR;
    out.resize(utf8Length);
    u_strToUTF8(
        out.data(),
        out.size(),
        &utf8Length,
        utf16.data(),
        utf16Length,
        &status);
    if (U_FAILURE(status)) {
      BOLT_USER_FAIL("Failed to produce UTF-8: {}", u_errorName(status));
    }
    out.resize(utf8Length);
  }

 private:
  const std::string charset_;
  // The ICU converter name actually opened, which differs from charset_ only
  // for UTF-16 on the encode side.
  const std::string icuCharset_;
  // Decoding UTF-8 does not go through ICU: see decode(). Set from ICU's
  // canonical name so aliases such as "utf8" take the same path.
  bool isUtf8_{false};
  UConverter* converter_{nullptr};
};

/// True when the charset name denotes UTF-8, whatever spelling is used.
/// ICU canonicalises aliases, so this also accepts 'utf8', 'UTF_8' and so on.
bool isUtf8(const std::string& charset) {
  UErrorCode status = U_ZERO_ERROR;
  UConverter* converter = ucnv_open(charset.c_str(), &status);
  if (U_FAILURE(status)) {
    return false;
  }
  const char* canonical = ucnv_getName(converter, &status);
  const bool utf8 = U_SUCCESS(status) && canonical != nullptr &&
      std::string(canonical) == "UTF-8";
  ucnv_close(converter);
  return utf8;
}

template <bool kEncode>
class EncodeDecodeFunction : public exec::VectorFunction {
 public:
  /// 'constantCharset' is empty when the charset argument is not constant, in
  /// which case a converter is built per row.
  explicit EncodeDecodeFunction(std::optional<std::string> constantCharset)
      : constantCharset_(std::move(constantCharset)) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto* input = decodedArgs.at(0);
    auto* charsetArg = decodedArgs.at(1);

    BaseVector::ensureWritable(rows, outputType, context.pool(), result);
    auto* flatResult = result->as<FlatVector<StringView>>();

    // A constant charset is still resolved here rather than in the constructor.
    // The constructor runs at plan time, outside applyToSelectedNoThrow, so an
    // invalid literal charset would escape TRY() and abort the whole query
    // instead of failing the rows that use it.
    std::shared_ptr<Converter> constantConverter;
    bool passThrough = false;

    std::string buffer;
    // NoThrow so that a bad charset or an unconvertible value fails only the
    // rows it affects, and honours the TRY() semantics the engine expects,
    // rather than aborting the whole batch.
    context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      const auto value = input->valueAt<StringView>(row);

      const Converter* converter = nullptr;
      std::shared_ptr<Converter> perRow;
      if (constantCharset_.has_value()) {
        if (constantConverter == nullptr && !passThrough) {
          // For UTF-8 encoding the output bytes equal the input bytes, so the
          // conversion can be skipped and only the type changes. Decoding must
          // still go through ICU so invalid sequences become U+FFFD instead of
          // being copied unchanged into a VARCHAR result.
          validateJavaCharsetName(constantCharset_.value());
          passThrough = kEncode && isUtf8(constantCharset_.value());
          if (!passThrough) {
            constantConverter =
                std::make_shared<Converter>(constantCharset_.value(), kEncode);
          }
        }
        converter = constantConverter.get();
      } else {
        perRow = std::make_shared<Converter>(
            charsetArg->valueAt<StringView>(row).str(), kEncode);
        converter = perRow.get();
      }

      if (converter == nullptr) {
        // UTF-8 encode fast path: bytes are unchanged.
        flatResult->set(row, value);
        return;
      }

      if constexpr (kEncode) {
        converter->encode(value, buffer);
      } else {
        converter->decode(value, buffer);
      }
      flatResult->set(row, StringView(buffer));
    });
  }

  bool isDefaultNullBehavior() const override {
    return true;
  }

 private:
  // Held as a name, not a Converter: construction must happen during execution
  // so that an invalid charset is reportable through TRY().
  const std::optional<std::string> constantCharset_;
};

/// Reads the charset argument if it is a constant, so the converter can be
/// created once rather than per row.
std::optional<std::string> constantCharset(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  BOLT_CHECK_EQ(inputArgs.size(), 2);
  const auto& arg = inputArgs[1].constantValue;
  if (arg == nullptr || arg->isNullAt(0)) {
    return std::nullopt;
  }
  return arg->as<ConstantVector<StringView>>()->valueAt(0).str();
}

} // namespace

std::shared_ptr<exec::VectorFunction> makeEncode(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /* config */) {
  return std::make_shared<EncodeDecodeFunction<true>>(
      constantCharset(inputArgs));
}

std::vector<std::shared_ptr<exec::FunctionSignature>> encodeSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("varbinary")
              .argumentType("varchar")
              .argumentType("varchar")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeDecode(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /* config */) {
  return std::make_shared<EncodeDecodeFunction<false>>(
      constantCharset(inputArgs));
}

std::vector<std::shared_ptr<exec::FunctionSignature>> decodeSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("varchar")
              .argumentType("varbinary")
              .argumentType("varchar")
              .build()};
}

} // namespace bytedance::bolt::functions::sparksql
