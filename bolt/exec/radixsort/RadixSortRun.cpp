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

#include "bolt/exec/radixsort/RadixSortRun.h"

#include <array>
#include <cstring>
#include <limits>

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/radixsort/PayloadRowReader.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"

namespace bytedance::bolt::exec::radixsort {
namespace {

void validateRowVector(
    const RowType& expected,
    const RowVector& input,
    const char* name) {
  BOLT_CHECK(
      input.type()->equivalent(expected) &&
          input.childrenSize() == expected.size(),
      "{} type does not match RadixSortRun",
      name);
  bool validChildren = true;
  bool validSizes = true;
  for (uint32_t column = 0; column < expected.size(); ++column) {
    const auto& child = input.childAt(column);
    validChildren &= child != nullptr;
    if (child != nullptr) {
      validSizes &= child->size() >= input.size();
    }
  }
  BOLT_CHECK(validChildren, "{} child is missing", name);
  BOLT_CHECK(validSizes, "{} child is too short", name);
}

void validateProjectionInput(const RowType& expected, const RowVector& input) {
  BOLT_CHECK(
      input.type()->equivalent(expected),
      "Sort projection input type does not match output type");
  BOLT_CHECK_EQ(
      input.childrenSize(),
      expected.size(),
      "Sort projection input column count does not match");
  bool validChildren = true;
  bool validSizes = true;
  for (uint32_t column = 0; column < expected.size(); ++column) {
    const auto& child = input.childAt(column);
    validChildren &= child != nullptr;
    if (child != nullptr) {
      validSizes &= child->size() >= input.size();
    }
  }
  BOLT_CHECK(validChildren, "Sort projection input child is missing");
  BOLT_CHECK(validSizes, "Sort projection input child is too short");
}

template <RadixSortKeyLayoutKind KIND>
void materializeVariableKeyViews(
    const RadixSortRunStorage& arena,
    uint64_t begin,
    vector_size_t count,
    RadixSortInlineKeyBuffer* inlineBuffers,
    EncodedKeyView* views) {
  using Traits = RadixSortKeyTraits<KIND>;
  static_assert(Traits::kVariable);
  vector_size_t outputRow = 0;
  while (outputRow < count) {
    const auto range = arena.keyRangeAt(begin + outputRow, count - outputRow);
    for (vector_size_t row = 0; row < range.count; ++row) {
      const auto* key =
          range.data + static_cast<uint64_t>(row) * Traits::kWidth;
      const auto size = loadUnaligned<uint64_t>(key + Traits::kSizeOffset);
      if (size > Traits::kInlineCapacity) {
        const auto* data =
            loadUnaligned<const char*>(key + Traits::kDataOffset);
        views[outputRow + row] = {std::string_view(data, size), false};
        continue;
      }
      auto& output = inlineBuffers[outputRow + row];
      for (uint32_t word = 0; word < Traits::kInlineWords; ++word) {
        auto value = loadUnaligned<uint64_t>(key + word * sizeof(uint64_t));
        if constexpr (std::endian::native == std::endian::little) {
          value = byteSwap(value);
        }
        storeUnaligned<uint64_t>(
            output.data() + word * sizeof(uint64_t), value);
      }
      views[outputRow + row] = {
          std::string_view(output.data(), Traits::kInlineCapacity), true};
    }
    outputRow += range.count;
  }
}

template <RadixSortKeyLayoutKind KIND>
bool materializeExternalVariableKeyPointerViews(
    std::span<const char* const> keys,
    EncodedKeyView* views) {
  using Traits = RadixSortKeyTraits<KIND>;
  static_assert(Traits::kVariable);
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    const auto* key = keys[row];
    const auto size = loadUnaligned<uint64_t>(key + Traits::kSizeOffset);
    if (size <= Traits::kInlineCapacity) {
      return false;
    }
    const auto* data = loadUnaligned<const char*>(key + Traits::kDataOffset);
    views[row] = {std::string_view(data, size), false};
  }
  return true;
}

template <RadixSortKeyLayoutKind KIND>
void materializeVariableKeyPointerViews(
    std::span<const char* const> keys,
    RadixSortInlineKeyBuffer* inlineBuffers,
    EncodedKeyView* views) {
  using Traits = RadixSortKeyTraits<KIND>;
  static_assert(Traits::kVariable);
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    const auto* key = keys[row];
    const auto size = loadUnaligned<uint64_t>(key + Traits::kSizeOffset);
    if (size > Traits::kInlineCapacity) {
      const auto* data = loadUnaligned<const char*>(key + Traits::kDataOffset);
      views[row] = {std::string_view(data, size), false};
      continue;
    }
    auto& output = inlineBuffers[row];
    for (uint32_t word = 0; word < Traits::kInlineWords; ++word) {
      auto value = loadUnaligned<uint64_t>(key + word * sizeof(uint64_t));
      if constexpr (std::endian::native == std::endian::little) {
        value = byteSwap(value);
      }
      storeUnaligned<uint64_t>(output.data() + word * sizeof(uint64_t), value);
    }
    views[row] = {
        std::string_view(output.data(), Traits::kInlineCapacity), true};
  }
}

} // namespace

std::unique_ptr<RadixSortOutputProjection> RadixSortOutputProjection::create(
    const RowTypePtr& outputType,
    const RowTypePtr& keyType,
    const std::vector<column_index_t>& directKeyChannels,
    const std::vector<bool>& bitExactRequired) {
  BOLT_CHECK_NOT_NULL(
      outputType, "Sort projection output type must not be null");
  BOLT_CHECK_NOT_NULL(keyType, "Sort projection key type must not be null");
  BOLT_CHECK_GT(
      keyType->size(), 0, "Sort projection requires at least one key");
  BOLT_CHECK_EQ(
      directKeyChannels.size(),
      keyType->size(),
      "Sort projection direct key channel count does not match");
  BOLT_CHECK(
      bitExactRequired.empty() || bitExactRequired.size() == outputType->size(),
      "Sort projection bit-exact column count does not match");

  std::vector<int32_t> decodedKeyByOutput(outputType->size(), -1);
  std::vector<uint32_t> directOccurrences(outputType->size(), 0);
  bool validDirectChannels = true;
  bool validDirectTypes = true;
  for (uint32_t key = 0; key < keyType->size(); ++key) {
    const auto channel = directKeyChannels[key];
    if (channel >= outputType->size()) {
      validDirectChannels = false;
      continue;
    }
    validDirectTypes &=
        keyType->childAt(key)->equivalent(*outputType->childAt(channel));
    ++directOccurrences[channel];
    if (decodedKeyByOutput[channel] < 0) {
      decodedKeyByOutput[channel] = key;
    }
  }
  BOLT_CHECK(
      validDirectChannels,
      "Sort projection direct key channel is out of range");
  BOLT_CHECK(
      validDirectTypes,
      "Sort projection direct key type does not match output");

  std::vector<RadixSortOutputColumn> columns(outputType->size());
  std::vector<column_index_t> payloadChannels;
  std::vector<std::string> payloadNames;
  std::vector<TypePtr> payloadTypes;
  payloadChannels.reserve(outputType->size());
  payloadNames.reserve(outputType->size());
  payloadTypes.reserve(outputType->size());
  for (uint32_t output = 0; output < outputType->size(); ++output) {
    const bool preserveBits =
        !bitExactRequired.empty() && bitExactRequired[output];
    const auto decodedKey = decodedKeyByOutput[output];
    if (decodedKeyByOutput[output] >= 0 && directOccurrences[output] == 1 &&
        !preserveBits) {
      columns[output] = {
          RadixSortOutputSource::kDecodedKey,
          static_cast<uint32_t>(decodedKey)};
      continue;
    }
    const auto payload = static_cast<uint32_t>(payloadChannels.size());
    columns[output] = {RadixSortOutputSource::kPayload, payload};
    payloadChannels.push_back(output);
    payloadNames.push_back(outputType->nameOf(output));
    payloadTypes.push_back(outputType->childAt(output));
  }

  RowTypePtr payloadType;
  if (!payloadChannels.empty()) {
    payloadType = ROW(std::move(payloadNames), std::move(payloadTypes));
  }
  return std::unique_ptr<RadixSortOutputProjection>(
      new RadixSortOutputProjection(
          outputType,
          keyType,
          std::move(payloadType),
          std::move(columns),
          std::move(payloadChannels),
          directKeyChannels));
}

void RadixSortOutputProjection::projectKeys(
    const RowVector& input,
    std::vector<VectorPtr>& children) const {
  children.clear();
  children.reserve(directKeyChannels_.size());
  for (const auto channel : directKeyChannels_) {
    children.push_back(input.childAt(channel));
  }
}

void RadixSortOutputProjection::projectPayload(
    const RowVector& input,
    std::vector<VectorPtr>& children) const {
  children.clear();
  children.reserve(payloadChannels_.size());
  for (const auto channel : payloadChannels_) {
    children.push_back(input.childAt(channel));
  }
}

RowVectorPtr RadixSortOutputProjection::reconstruct(
    const RowVectorPtr& decodedKeys,
    const RowVectorPtr& payload,
    memory::MemoryPool* pool) const {
  BOLT_CHECK_NOT_NULL(pool, "Sort projection output pool must not be null");
  if (needsDecodedKeys_) {
    BOLT_CHECK(
        decodedKeys != nullptr && decodedKeys->type()->equivalent(*keyType_) &&
            decodedKeys->childrenSize() == keyType_->size(),
        "Decoded sort key type does not match projection");
  } else if (decodedKeys != nullptr) {
    BOLT_CHECK_NULL(
        decodedKeys, "Payload-only sort projection received decoded keys");
  }
  if (hasPayload()) {
    BOLT_CHECK(
        payload != nullptr && payload->type()->equivalent(*payloadType_) &&
            (decodedKeys == nullptr || payload->size() == decodedKeys->size()),
        "Payload row output does not match projection");
  } else if (payload != nullptr) {
    BOLT_CHECK_NULL(
        payload, "Key-only sort projection received payload output");
  }

  std::vector<VectorPtr> children(outputType_->size());
  bool validChildren = true;
  bool validChildTypes = true;
  for (uint32_t output = 0; output < columns_.size(); ++output) {
    const auto& column = columns_[output];
    if (column.source == RadixSortOutputSource::kDecodedKey) {
      children[output] = decodedKeys->childAt(column.sourceIndex);
    } else {
      children[output] = payload->childAt(column.sourceIndex);
    }
    validChildren &= children[output] != nullptr;
    if (children[output] != nullptr) {
      validChildTypes &=
          children[output]->type()->equivalent(*outputType_->childAt(output));
    }
  }
  BOLT_CHECK(validChildren, "Sort projection output child must not be null");
  BOLT_CHECK(
      validChildTypes, "Sort projection output child type does not match");
  const auto size =
      decodedKeys != nullptr ? decodedKeys->size() : payload->size();
  return std::make_shared<RowVector>(
      pool, outputType_, nullptr, size, std::move(children));
}

std::unique_ptr<RadixSortRun> RadixSortRun::create(
    memory::MemoryPool* pool,
    const RowTypePtr& outputType,
    const RowTypePtr& keyType,
    const std::vector<CompareFlags>& keyFlags,
    const std::vector<column_index_t>& directKeyChannels,
    const std::vector<bool>& bitExactRequired,
    RadixSortRunOptions options) {
  BOLT_CHECK_NOT_NULL(pool, "RadixSortRun memory pool must not be null");
  BOLT_CHECK_NOT_NULL(outputType, "RadixSortRun output type must not be null");
  BOLT_CHECK_NOT_NULL(keyType, "RadixSortRun key type must not be null");
  BOLT_CHECK_EQ(
      keyType->size(),
      keyFlags.size(),
      "RadixSortRun key type and flag counts do not match");
  BOLT_CHECK(
      options.knownNonNullKeys.empty() ||
          options.knownNonNullKeys.size() == keyType->size(),
      "RadixSortRun non-null key statistics count does not match");
  BOLT_CHECK(
      options.initialKeyMayHaveNulls.empty() ||
          options.initialKeyMayHaveNulls.size() == keyType->size(),
      "RadixSortRun initial key nullability count does not match");
  BOLT_CHECK_GT(options.keysPerBlock, 0);
  BOLT_CHECK_GT(options.preferredKeyHeapGroupBytes, 0);
  BOLT_CHECK_GT(options.payloadRowsPerBlock, 0);
  BOLT_CHECK_GT(options.preferredPayloadHeapGroupBytes, 0);

  auto projection = RadixSortOutputProjection::create(
      outputType, keyType, directKeyChannels, bitExactRequired);

  std::unique_ptr<RadixSortKeyCodec> keyCodec;
  RadixSortKeyCodec::bind(
      keyType->children(), keyFlags, options.knownNonNullKeys, keyCodec);
  BOLT_CHECK(
      keyCodec->canEncodeDecode(),
      "RadixSortRun key codec does not support decode");

  std::shared_ptr<const PayloadRowLayout> payloadLayout;
  if (projection->hasPayload()) {
    payloadLayout = PayloadRowLayout::create(projection->payloadType());
    BOLT_CHECK_NOT_NULL(
        payloadLayout, "RadixSortRun payload layout must not be empty");
    BOLT_CHECK(
        options.initialPayloadMayHaveNulls.empty() ||
            options.initialPayloadMayHaveNulls.size() ==
                projection->payloadType()->size(),
        "RadixSortRun initial payload nullability count does not match");
  } else {
    BOLT_CHECK(
        options.initialPayloadMayHaveNulls.empty(),
        "Key-only RadixSortRun cannot have payload nullability");
  }

  auto keyMayHaveNulls = std::move(options.initialKeyMayHaveNulls);
  if (keyMayHaveNulls.empty()) {
    keyMayHaveNulls.resize(keyType->size(), 0);
  }
  auto payloadMayHaveNulls = std::move(options.initialPayloadMayHaveNulls);
  if (projection->hasPayload() && payloadMayHaveNulls.empty()) {
    payloadMayHaveNulls.resize(projection->payloadType()->size(), 0);
  }

  auto keyLayout = RadixSortKeyLayout::select(
      keyCodec->maximumEncodedSize(),
      projection->hasPayload(),
      keyType->size());
  auto arena = std::make_unique<RadixSortRunStorage>(
      pool,
      keyLayout,
      options.keysPerBlock,
      options.preferredKeyHeapGroupBytes,
      payloadLayout,
      options.payloadRowsPerBlock,
      options.preferredPayloadHeapGroupBytes);
  return std::unique_ptr<RadixSortRun>(new RadixSortRun(
      pool,
      std::move(projection),
      std::move(keyCodec),
      std::move(payloadLayout),
      keyLayout,
      std::move(arena),
      std::move(keyMayHaveNulls),
      std::move(payloadMayHaveNulls)));
}

void RadixSortRun::append(const RowVector& input) {
  BOLT_CHECK(
      state_ == RadixSortRunState::kBuilding,
      "RadixSortRun accepts input only while building");
  validateRowVector(*projection_->outputType(), input, "RadixSortRun input");
  if (input.size() == 0) {
    return;
  }
  const auto nextInputRows =
      checkedAdd<uint64_t>(metrics_.inputRows, input.size());
  BOLT_CHECK(
      nextInputRows.has_value(), "RadixSortRun input row count overflows");

  std::vector<VectorPtr> keyInputChildren;
  projection_->projectKeys(input, keyInputChildren);
  RowVector keys(
      pool_,
      projection_->keyType(),
      nullptr,
      input.size(),
      std::move(keyInputChildren));

  VectorPtr directFixedKey;
  for (uint32_t column = 0; column < keys.childrenSize(); ++column) {
    const auto mayHaveNulls = keys.childAt(column)->mayHaveNulls();
    keyMayHaveNulls_[column] |= mayHaveNulls;
    currentRunKeyMayHaveNulls_[column] |= mayHaveNulls;
  }
  EncodedKeyBatch encodedKeys;
  const auto encodeBegin = std::chrono::steady_clock::now();
  if (keys.childrenSize() == 1 &&
      keyCodec_->canEncodeSingleFixedFlat(*keys.childAt(0), *storage_)) {
    directFixedKey = keys.childAt(0);
  } else {
    keyCodec_->encode(keys, pool_, encodedKeys);
  }
  const auto encodeTimeUs = elapsedUs(encodeBegin);

  std::vector<uint8_t> payloadMayHaveNulls;
  if (projection_->hasPayload()) {
    std::vector<VectorPtr> payloadInputChildren;
    projection_->projectPayload(input, payloadInputChildren);
    RowVector payloadInput(
        pool_,
        projection_->payloadType(),
        nullptr,
        input.size(),
        std::move(payloadInputChildren));
    payloadMayHaveNulls.resize(payloadInput.childrenSize(), 0);
    for (uint32_t column = 0; column < payloadInput.childrenSize(); ++column) {
      payloadMayHaveNulls[column] =
          payloadInput.childAt(column)->mayHaveNulls();
    }
    const auto appendBegin = std::chrono::steady_clock::now();
    PayloadRowBatch payloadBatch;
    PayloadRowWriter::append(payloadInput, *storage_, payloadBatch);
    BOLT_CHECK_EQ(
        payloadBatch.size(),
        input.size(),
        "RadixSortRun payload row count does not match input");
    const auto payloads =
        std::span<char* const>(payloadBatch.rows()->as<char*>(), input.size());
    if (directFixedKey == nullptr) {
      storage_->appendBatch(encodedKeys, payloads);
    } else {
      keyCodec_->appendSingleFixedFlat(
          *directFixedKey, input.size(), *storage_, payloads);
    }
    metrics_.appendTimeUs += elapsedUs(appendBegin);
  } else {
    const auto appendBegin = std::chrono::steady_clock::now();
    if (directFixedKey == nullptr) {
      storage_->appendBatch(encodedKeys);
    } else {
      keyCodec_->appendSingleFixedFlat(
          *directFixedKey, input.size(), *storage_, {});
    }
    metrics_.appendTimeUs += elapsedUs(appendBegin);
  }
  if (projection_->hasPayload()) {
    BOLT_CHECK_EQ(
        payloadMayHaveNulls.size(),
        payloadMayHaveNulls_.size(),
        "RadixSortRun payload nullability does not match");
    for (uint32_t column = 0; column < payloadMayHaveNulls_.size(); ++column) {
      payloadMayHaveNulls_[column] |= payloadMayHaveNulls[column];
    }
  }
  metrics_.encodeTimeUs += encodeTimeUs;
  metrics_.inputRows = *nextInputRows;
  BOLT_CHECK(
      storage_->size() == metrics_.inputRows &&
          storage_->payloadSize() ==
              (projection_->hasPayload() ? metrics_.inputRows : 0),
      "RadixSortRun arena row counts are inconsistent");
}

void RadixSortRun::finalize() {
  BOLT_CHECK(
      state_ == RadixSortRunState::kBuilding,
      "RadixSortRun can be finalized only once");
  state_ = RadixSortRunState::kFinalizing;
  const auto begin = std::chrono::steady_clock::now();
  try {
    RadixSortRunSorter sorter(*storage_);
    const auto skippableValidityOffsets =
        keyCodec_->leadingSkippableValidityOffsets(currentRunKeyMayHaveNulls_);
    sorter.sort(skippableValidityOffsets);
  } catch (...) {
    metrics_.sortTimeUs += elapsedUs(begin);
    throw;
  }
  metrics_.sortTimeUs += elapsedUs(begin);
  state_ = RadixSortRunState::kSortedInMemory;
}

RowVectorPtr RadixSortRun::getOutput(
    vector_size_t maxRows,
    memory::MemoryPool* outputPool) {
  if (state_ == RadixSortRunState::kConsumed) {
    return nullptr;
  }
  BOLT_CHECK(
      state_ == RadixSortRunState::kSortedInMemory,
      "RadixSortRun output requires a finalized run");
  BOLT_CHECK_GT(maxRows, 0, "RadixSortRun output batch size must be positive");
  BOLT_CHECK_NOT_NULL(
      outputPool, "RadixSortRun output memory pool must not be null");

  if (outputPosition_ == storage_->size()) {
    clear();
    return nullptr;
  }
  const auto remaining = storage_->size() - outputPosition_;
  const auto count = static_cast<vector_size_t>(
      std::min<uint64_t>(remaining, static_cast<uint64_t>(maxRows)));
  const auto nextOutputRows = checkedAdd<uint64_t>(metrics_.outputRows, count);
  BOLT_CHECK(
      nextOutputRows.has_value(), "RadixSortRun output row count overflows");
  const auto begin = std::chrono::steady_clock::now();
  RowVectorPtr decodedKeys;
  if (projection_->needsDecodedKeys()) {
    decodedKeys = decodeKeys(outputPosition_, count, outputPool);
  }
  auto payload = gatherPayload(outputPosition_, count, outputPool);
  auto result = projection_->reconstruct(
      projection_->needsDecodedKeys() ? decodedKeys : nullptr,
      projection_->hasPayload() ? payload : nullptr,
      outputPool);

  outputPosition_ += count;
  metrics_.outputRows = *nextOutputRows;
  metrics_.outputTimeUs += elapsedUs(begin);
  if (outputPosition_ == storage_->size()) {
    clear();
  }
  return result;
}

RowVectorPtr RadixSortRun::getOutput(
    std::span<const char* const> keys,
    std::span<char* const> payloads,
    memory::MemoryPool* outputPool) {
  BOLT_CHECK_NOT_NULL(
      outputPool, "RadixSortRun output memory pool must not be null");
  BOLT_CHECK(
      !projection_->hasPayload() || payloads.size() == keys.size(),
      "RadixSortRun merge output key and payload counts do not match");
  const auto count = static_cast<vector_size_t>(keys.size());
  if (count == 0) {
    return nullptr;
  }
  RowVectorPtr decodedKeys;
  if (projection_->needsDecodedKeys()) {
    decodedKeys = decodeKeyPointers(keys, outputPool);
  }
  auto payload = gatherPayloadPointers(payloads, outputPool);
  auto result = projection_->reconstruct(
      projection_->needsDecodedKeys() ? decodedKeys : nullptr,
      projection_->hasPayload() ? payload : nullptr,
      outputPool);
  metrics_.outputRows += count;
  return result;
}

vector_size_t RadixSortRun::collectRemainingRows(
    vector_size_t maxRows,
    const char** keys,
    char** payloads) {
  if (state_ == RadixSortRunState::kConsumed) {
    return 0;
  }
  BOLT_CHECK(
      state_ == RadixSortRunState::kSortedInMemory,
      "RadixSortRun remaining rows require a finalized run");
  BOLT_CHECK_GT(maxRows, 0);
  BOLT_CHECK_NOT_NULL(keys);
  BOLT_CHECK(!projection_->hasPayload() || payloads != nullptr);
  if (outputPosition_ == storage_->size()) {
    return 0;
  }
  const auto count = static_cast<vector_size_t>(std::min<uint64_t>(
      storage_->size() - outputPosition_, static_cast<uint64_t>(maxRows)));
  const auto keyWidth = keyLayout_.width();
  vector_size_t outputRow = 0;
  if (projection_->hasPayload()) {
    const auto payloadOffset = *keyLayout_.payloadOffset();
    while (outputRow < count) {
      const auto range =
          storage_->keyRangeAt(outputPosition_ + outputRow, count - outputRow);
      for (vector_size_t row = 0; row < range.count; ++row) {
        auto* key = range.data + static_cast<uint64_t>(row) * keyWidth;
        keys[outputRow + row] = key;
        payloads[outputRow + row] = loadUnaligned<char*>(key + payloadOffset);
      }
      outputRow += range.count;
    }
  } else {
    while (outputRow < count) {
      const auto range =
          storage_->keyRangeAt(outputPosition_ + outputRow, count - outputRow);
      for (vector_size_t row = 0; row < range.count; ++row) {
        keys[outputRow + row] =
            range.data + static_cast<uint64_t>(row) * keyWidth;
      }
      outputRow += range.count;
    }
  }
  outputPosition_ += count;
  return count;
}

void RadixSortRun::clear() {
  decodedKeysOutput_.reset();
  payloadOutput_.reset();
  decodeCursorOutput_.reset();
  decodeInlineOutput_.reset();
  decodeViewsOutput_.reset();
  payloadRowsOutput_.reset();
  if (storage_ != nullptr) {
    storage_->clear();
    storage_.reset();
  }
  state_ = RadixSortRunState::kConsumed;
}

RowVectorPtr RadixSortRun::decodeKeys(
    uint64_t begin,
    vector_size_t count,
    memory::MemoryPool* outputPool) {
  if (keyCodec_->canDecodeSingleFixedColumn() && !keyLayout_.isVariable()) {
    keyCodec_->decodeSingleFixedColumn(
        *storage_,
        begin,
        count,
        keyMayHaveNulls_[0] != 0,
        outputPool,
        decodedKeysOutput_);
    return decodedKeysOutput_;
  }

  const auto prepareScratch =
      [&](BufferPtr& buffer, uint64_t elementSize, auto allocate) {
        const auto bytes = static_cast<uint64_t>(count) * elementSize;
        if (buffer == nullptr || buffer->pool() != outputPool) {
          buffer = allocate();
        } else if (buffer->capacity() < bytes) {
          return false;
        } else {
          buffer->setSize(bytes);
        }
        return true;
      };
  if (!prepareScratch(
          decodeInlineOutput_, sizeof(RadixSortInlineKeyBuffer), [&]() {
            return AlignedBuffer::allocate<RadixSortInlineKeyBuffer>(
                count, outputPool);
          })) {
    AlignedBuffer::reallocate<RadixSortInlineKeyBuffer>(
        &decodeInlineOutput_, count);
  }
  if (!prepareScratch(decodeViewsOutput_, sizeof(EncodedKeyView), [&]() {
        return AlignedBuffer::allocate<EncodedKeyView>(count, outputPool);
      })) {
    AlignedBuffer::reallocate<EncodedKeyView>(&decodeViewsOutput_, count);
  }
  auto* rawInlineBuffers =
      decodeInlineOutput_->asMutable<RadixSortInlineKeyBuffer>();
  auto* rawViews = decodeViewsOutput_->asMutable<EncodedKeyView>();
  if (keyLayout_.kind() == RadixSortKeyLayoutKind::kKeyOnlyVariable32) {
    materializeVariableKeyViews<RadixSortKeyLayoutKind::kKeyOnlyVariable32>(
        *storage_, begin, count, rawInlineBuffers, rawViews);
    keyCodec_->decode(
        std::span<const EncodedKeyView>(rawViews, count),
        projection_->decodedKeyMask(),
        keyMayHaveNulls_,
        outputPool,
        decodeCursorOutput_,
        decodedKeysOutput_);
    return decodedKeysOutput_;
  }
  if (keyLayout_.kind() == RadixSortKeyLayoutKind::kKeyWithPayloadVariable32) {
    materializeVariableKeyViews<
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable32>(
        *storage_, begin, count, rawInlineBuffers, rawViews);
    keyCodec_->decode(
        std::span<const EncodedKeyView>(rawViews, count),
        projection_->decodedKeyMask(),
        keyMayHaveNulls_,
        outputPool,
        decodeCursorOutput_,
        decodedKeysOutput_);
    return decodedKeysOutput_;
  }
  if (keyLayout_.kind() == RadixSortKeyLayoutKind::kKeyWithPayloadVariable56) {
    materializeVariableKeyViews<
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable56>(
        *storage_, begin, count, rawInlineBuffers, rawViews);
    keyCodec_->decode(
        std::span<const EncodedKeyView>(rawViews, count),
        projection_->decodedKeyMask(),
        keyMayHaveNulls_,
        outputPool,
        decodeCursorOutput_,
        decodedKeysOutput_);
    return decodedKeysOutput_;
  }
  if (keyLayout_.kind() == RadixSortKeyLayoutKind::kKeyWithPayloadVariable64) {
    materializeVariableKeyViews<
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable64>(
        *storage_, begin, count, rawInlineBuffers, rawViews);
    keyCodec_->decode(
        std::span<const EncodedKeyView>(rawViews, count),
        projection_->decodedKeyMask(),
        keyMayHaveNulls_,
        outputPool,
        decodeCursorOutput_,
        decodedKeysOutput_);
    return decodedKeysOutput_;
  }
  vector_size_t outputRow = 0;
  while (outputRow < count) {
    const auto range =
        storage_->keyRangeAt(begin + outputRow, count - outputRow);
    for (vector_size_t row = 0; row < range.count; ++row) {
      const auto* key =
          range.data + static_cast<uint64_t>(row) * keyLayout_.width();
      RadixSortKey(keyLayout_, key)
          .deconstruct(
              rawInlineBuffers[outputRow + row], rawViews[outputRow + row]);
    }
    outputRow += range.count;
  }
  keyCodec_->decode(
      std::span<const EncodedKeyView>(rawViews, count),
      projection_->decodedKeyMask(),
      keyMayHaveNulls_,
      outputPool,
      decodeCursorOutput_,
      decodedKeysOutput_);
  return decodedKeysOutput_;
}

RowVectorPtr RadixSortRun::decodeKeyPointers(
    std::span<const char* const> keys,
    memory::MemoryPool* outputPool) {
  const auto count = static_cast<vector_size_t>(keys.size());
  if (keyCodec_->canDecodeSingleFixedColumn() && !keyLayout_.isVariable()) {
    keyCodec_->decodeSingleFixedColumn(
        keys,
        keyLayout_.kind(),
        keyMayHaveNulls_[0] != 0,
        outputPool,
        decodedKeysOutput_);
    return decodedKeysOutput_;
  }
  const auto prepareScratch =
      [&](BufferPtr& buffer, uint64_t elementSize, auto allocate) {
        const auto bytes = static_cast<uint64_t>(count) * elementSize;
        if (buffer == nullptr || buffer->pool() != outputPool) {
          buffer = allocate();
        } else if (buffer->capacity() < bytes) {
          return false;
        } else {
          buffer->setSize(bytes);
        }
        return true;
      };
  if (!prepareScratch(decodeViewsOutput_, sizeof(EncodedKeyView), [&]() {
        return AlignedBuffer::allocate<EncodedKeyView>(count, outputPool);
      })) {
    AlignedBuffer::reallocate<EncodedKeyView>(&decodeViewsOutput_, count);
  }
  auto* rawViews = decodeViewsOutput_->asMutable<EncodedKeyView>();
  if (keyLayout_.kind() == RadixSortKeyLayoutKind::kKeyWithPayloadVariable32) {
    if (materializeExternalVariableKeyPointerViews<
            RadixSortKeyLayoutKind::kKeyWithPayloadVariable32>(
            keys, rawViews)) {
      keyCodec_->decode(
          std::span<const EncodedKeyView>(rawViews, count),
          projection_->decodedKeyMask(),
          keyMayHaveNulls_,
          outputPool,
          decodeCursorOutput_,
          decodedKeysOutput_);
      return decodedKeysOutput_;
    }
  } else if (
      keyLayout_.kind() == RadixSortKeyLayoutKind::kKeyWithPayloadVariable56) {
    if (materializeExternalVariableKeyPointerViews<
            RadixSortKeyLayoutKind::kKeyWithPayloadVariable56>(
            keys, rawViews)) {
      keyCodec_->decode(
          std::span<const EncodedKeyView>(rawViews, count),
          projection_->decodedKeyMask(),
          keyMayHaveNulls_,
          outputPool,
          decodeCursorOutput_,
          decodedKeysOutput_);
      return decodedKeysOutput_;
    }
  } else if (
      keyLayout_.kind() == RadixSortKeyLayoutKind::kKeyWithPayloadVariable64) {
    if (materializeExternalVariableKeyPointerViews<
            RadixSortKeyLayoutKind::kKeyWithPayloadVariable64>(
            keys, rawViews)) {
      keyCodec_->decode(
          std::span<const EncodedKeyView>(rawViews, count),
          projection_->decodedKeyMask(),
          keyMayHaveNulls_,
          outputPool,
          decodeCursorOutput_,
          decodedKeysOutput_);
      return decodedKeysOutput_;
    }
  } else if (keyLayout_.kind() == RadixSortKeyLayoutKind::kKeyOnlyVariable32) {
    if (materializeExternalVariableKeyPointerViews<
            RadixSortKeyLayoutKind::kKeyOnlyVariable32>(keys, rawViews)) {
      keyCodec_->decode(
          std::span<const EncodedKeyView>(rawViews, count),
          projection_->decodedKeyMask(),
          keyMayHaveNulls_,
          outputPool,
          decodeCursorOutput_,
          decodedKeysOutput_);
      return decodedKeysOutput_;
    }
  }
  if (!prepareScratch(
          decodeInlineOutput_, sizeof(RadixSortInlineKeyBuffer), [&]() {
            return AlignedBuffer::allocate<RadixSortInlineKeyBuffer>(
                count, outputPool);
          })) {
    AlignedBuffer::reallocate<RadixSortInlineKeyBuffer>(
        &decodeInlineOutput_, count);
  }
  auto* rawInlineBuffers =
      decodeInlineOutput_->asMutable<RadixSortInlineKeyBuffer>();
  if (keyLayout_.kind() == RadixSortKeyLayoutKind::kKeyWithPayloadVariable32) {
    materializeVariableKeyPointerViews<
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable32>(
        keys, rawInlineBuffers, rawViews);
  } else if (
      keyLayout_.kind() == RadixSortKeyLayoutKind::kKeyWithPayloadVariable56) {
    materializeVariableKeyPointerViews<
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable56>(
        keys, rawInlineBuffers, rawViews);
  } else if (
      keyLayout_.kind() == RadixSortKeyLayoutKind::kKeyWithPayloadVariable64) {
    materializeVariableKeyPointerViews<
        RadixSortKeyLayoutKind::kKeyWithPayloadVariable64>(
        keys, rawInlineBuffers, rawViews);
  } else if (keyLayout_.kind() == RadixSortKeyLayoutKind::kKeyOnlyVariable32) {
    materializeVariableKeyPointerViews<
        RadixSortKeyLayoutKind::kKeyOnlyVariable32>(
        keys, rawInlineBuffers, rawViews);
  } else {
    for (vector_size_t row = 0; row < count; ++row) {
      RadixSortKey(keyLayout_, keys[row])
          .deconstruct(rawInlineBuffers[row], rawViews[row]);
    }
  }
  keyCodec_->decode(
      std::span<const EncodedKeyView>(rawViews, count),
      projection_->decodedKeyMask(),
      keyMayHaveNulls_,
      outputPool,
      decodeCursorOutput_,
      decodedKeysOutput_);
  return decodedKeysOutput_;
}

RowVectorPtr RadixSortRun::gatherPayload(
    uint64_t begin,
    vector_size_t count,
    memory::MemoryPool* outputPool) {
  if (!projection_->hasPayload()) {
    payloadOutput_.reset();
    return nullptr;
  }
  if (payloadRowsOutput_ == nullptr ||
      payloadRowsOutput_->pool() != outputPool) {
    payloadRowsOutput_ =
        AlignedBuffer::allocate<char*>(count, outputPool, nullptr);
  } else if (
      payloadRowsOutput_->capacity() <
      static_cast<uint64_t>(count) * sizeof(char*)) {
    AlignedBuffer::reallocate<char*>(
        &payloadRowsOutput_, count, static_cast<char*>(nullptr));
  } else {
    payloadRowsOutput_->setSize(static_cast<uint64_t>(count) * sizeof(char*));
  }
  auto** rawRows = payloadRowsOutput_->asMutable<char*>();
  vector_size_t outputRow = 0;
  while (outputRow < count) {
    const auto range =
        storage_->keyRangeAt(begin + outputRow, count - outputRow);
    for (vector_size_t row = 0; row < range.count; ++row) {
      const auto* key =
          range.data + static_cast<uint64_t>(row) * keyLayout_.width();
      rawRows[outputRow + row] =
          loadUnaligned<char*>(key + *keyLayout_.payloadOffset());
    }
    outputRow += range.count;
  }
  PayloadRowReader::gather(
      *payloadLayout_,
      std::span<char* const>(rawRows, count),
      outputPool,
      payloadOutput_,
      payloadMayHaveNulls_);
  return payloadOutput_;
}

RowVectorPtr RadixSortRun::gatherPayloadPointers(
    std::span<char* const> payloads,
    memory::MemoryPool* outputPool) {
  if (!projection_->hasPayload()) {
    payloadOutput_.reset();
    return nullptr;
  }
  PayloadRowReader::gather(
      *payloadLayout_,
      payloads,
      outputPool,
      payloadOutput_,
      payloadMayHaveNulls_);
  return payloadOutput_;
}

uint64_t RadixSortRun::elapsedUs(
    const std::chrono::steady_clock::time_point& begin) {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - begin)
      .count();
}

} // namespace bytedance::bolt::exec::radixsort
