/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "bolt/common/memory/ByteStream.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/VariantVector.h"

namespace bytedance::bolt::exec::variant_serde {

using DeserializeStringFunction =
    void (*)(ByteInputStream&, vector_size_t, BaseVector&, bool);

using ReadStringViewFunction = StringView (*)(ByteInputStream&, std::string&);

/// Serialize a VARIANT value (value + metadata) to a stream.
/// Works with any stream type that supports appendOne<T> and appendStringView.
template <typename StreamT>
void serializeVariant(
    const BaseVector& vector,
    vector_size_t index,
    StreamT& stream) {
  const auto* wrapped = vector.wrappedVector();
  BOLT_CHECK_EQ(
      wrapped->encoding(),
      VectorEncoding::Simple::VARIANT,
      "Unexpected encoding for VARIANT vector: {}",
      wrapped->encoding());

  auto variantVector = wrapped->asUnchecked<VariantVector>();
  const auto wrappedIndex = vector.wrappedIndex(index);
  const auto variant = variantVector->valueAt(wrappedIndex);
  stream.template appendOne<int32_t>(variant.value.size());
  stream.appendStringView(variant.value);
  stream.template appendOne<int32_t>(variant.metadata.size());
  stream.appendStringView(variant.metadata);
}

/// Deserialize a VARIANT value from a ByteInputStream into a vector.
inline void deserializeVariant(
    ByteInputStream& in,
    vector_size_t index,
    BaseVector& result,
    bool exactSize,
    // Pointer to the deserializeString function (different signatures in each
    // serde implementation, but same behavior for our purposes).
    DeserializeStringFunction deserializeString) {
  if (result.encoding() == VectorEncoding::Simple::VARIANT) {
    auto variantVector = result.asUnchecked<VariantVector>();
    deserializeString(in, index, *variantVector->valueChildVector(), exactSize);
    deserializeString(
        in, index, *variantVector->metadataChildVector(), exactSize);
  } else {
    BOLT_CHECK_EQ(result.encoding(), VectorEncoding::Simple::FLAT);
    auto values = result.asUnchecked<FlatVector<VariantValue>>();

    // For inline-sized strings (<=12 bytes), StringView copies bytes into
    // its internal inline storage, so it's safe to read into a stack buffer.
    // For larger strings, we allocate a buffer that outlives the StringView.
    auto readPart = [&](StringView& part) {
      auto size = in.read<int32_t>();
      if (StringView::isInline(size)) {
        char buf[StringView::kInlineSize];
        in.readBytes(buf, size);
        part = StringView(buf, size);
      } else {
        auto buffer = AlignedBuffer::allocate<char>(size, result.pool());
        in.readBytes(buffer->asMutable<char>(), size);
        part = StringView(buffer->as<char>(), size);
        values->addStringBuffer(buffer);
      }
    };

    VariantValue variant;
    readPart(variant.value);
    readPart(variant.metadata);
    values->set(index, variant);
  }
}

/// Read a length-prefixed StringView from a ByteInputStream.
/// Requires a `readStringView(stream, storage)` function in scope.
inline StringView readVariantStringView(
    ByteInputStream& stream,
    std::string& storage) {
  auto size = stream.read<int32_t>();
  storage.resize(size);
  stream.readBytes(storage.data(), size);
  return StringView(storage);
}

/// Compare a serialized VARIANT value against a vector value.
inline std::optional<int32_t> compareVariantStreamVsVector(
    ByteInputStream& left,
    const BaseVector& right,
    vector_size_t index,
    CompareFlags flags,
    ReadStringViewFunction readSV) {
  const auto* wrapped = right.wrappedVector();
  BOLT_CHECK_EQ(
      wrapped->encoding(),
      VectorEncoding::Simple::VARIANT,
      "Unexpected encoding for VARIANT vector: {}",
      wrapped->encoding());

  auto variantVector = wrapped->asUnchecked<VariantVector>();
  const auto wrappedIndex = right.wrappedIndex(index);
  const auto rightValue = variantVector->valueAt(wrappedIndex);

  std::string storage;
  auto leftValue = readSV(left, storage);
  auto result = leftValue.compare(rightValue.value);
  if (result == 0) {
    auto leftMetadata = readSV(left, storage);
    result = leftMetadata.compare(rightValue.metadata);
  }
  return flags.ascending ? result : result * -1;
}

/// Compare two serialized VARIANT values from streams.
inline std::optional<int32_t> compareVariantStreamVsStream(
    ByteInputStream& left,
    ByteInputStream& right,
    CompareFlags flags,
    ReadStringViewFunction readSV) {
  std::string leftStorage;
  std::string rightStorage;
  StringView leftValue = readSV(left, leftStorage);
  StringView rightValue = readSV(right, rightStorage);
  auto result = leftValue.compare(rightValue);
  if (result == 0) {
    auto leftMetadata = readSV(left, leftStorage);
    auto rightMetadata = readSV(right, rightStorage);
    result = leftMetadata.compare(rightMetadata);
  }
  return flags.ascending ? result : result * -1;
}

/// Hash a serialized VARIANT value from a stream.
inline uint64_t hashVariant(
    ByteInputStream& stream,
    ReadStringViewFunction readSV) {
  std::string valueStorage;
  std::string metadataStorage;
  auto value = readSV(stream, valueStorage);
  auto metadata = readSV(stream, metadataStorage);
  return folly::hasher<VariantValue>()({value, metadata});
}

} // namespace bytedance::bolt::exec::variant_serde
