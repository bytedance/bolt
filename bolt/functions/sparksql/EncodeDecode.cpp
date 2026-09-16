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

/// Owns a UConverter for one charset name.
///
/// Spark's encode/decode take the charset as an argument, so when that argument
/// is constant the converter is opened once at plan time instead of per row.
class Converter {
 public:
  explicit Converter(const std::string& charset) : charset_(charset) {
    UErrorCode status = U_ZERO_ERROR;
    converter_ = ucnv_open(charset_.c_str(), &status);
    if (U_FAILURE(status)) {
      // Spark throws for an unsupported charset rather than returning NULL;
      // an unknown name is a query-authoring error, not a data value.
      BOLT_USER_FAIL(
          "Unsupported charset '{}': {}", charset_, u_errorName(status));
    }
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
    UErrorCode status = U_ZERO_ERROR;
    // One UChar per input byte is always enough: no charset produces more
    // UTF-16 code units than it consumes bytes.
    std::vector<UChar> utf16(input.size() + 1);
    ucnv_resetToUnicode(converter_);
    const int32_t utf16Length = ucnv_toUChars(
        converter_,
        utf16.data(),
        utf16.size(),
        input.data(),
        input.size(),
        &status);
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
  explicit EncodeDecodeFunction(std::optional<std::string> constantCharset) {
    if (constantCharset.has_value()) {
      // For UTF-8 the output bytes equal the input bytes, so conversion can be
      // skipped entirely; only the type changes.
      passThrough_ = isUtf8(constantCharset.value());
      if (!passThrough_) {
        converter_ = std::make_shared<Converter>(constantCharset.value());
      }
    }
  }

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

    std::string buffer;
    rows.applyToSelected([&](vector_size_t row) {
      const auto value = input->valueAt<StringView>(row);

      if (passThrough_) {
        // Bytes are unchanged; copy straight through.
        flatResult->set(row, value);
        return;
      }

      const Converter* converter = converter_.get();
      std::shared_ptr<Converter> perRow;
      if (converter == nullptr) {
        perRow = std::make_shared<Converter>(
            charsetArg->valueAt<StringView>(row).str());
        converter = perRow.get();
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
  std::shared_ptr<Converter> converter_;
  bool passThrough_{false};
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
