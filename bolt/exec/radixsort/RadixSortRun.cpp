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

#include "bolt/common/base/Exceptions.h"
#include "bolt/exec/radixsort/PayloadRow.h"
#include "bolt/exec/radixsort/RadixSortUtils.h"

namespace bytedance::bolt::exec::radixsort {
namespace {

template <bool Inline>
EncodedKeyView materializeVariableKeyView(
    const RadixSortKeyLayout& layout,
    const char* key) {
  const auto size = loadUnaligned<uint64_t>(key + *layout.sizeOffset());
  const auto* data = Inline ? key + layout.heapKeyOffset()
                            : loadCompactPointer(key + *layout.dataOffset());
  return {std::string_view(data, size - layout.heapKeyOffset())};
}

template <bool Inline>
void materializeVariableKeyPointerViews(
    const RadixSortKeyLayout& layout,
    std::span<const char* const> keys,
    EncodedKeyView* views) {
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    views[row] = materializeVariableKeyView<Inline>(layout, keys[row]);
  }
}

template <bool Inline>
void materializeVariableKeyViews(
    const RadixSortRunStorage& storage,
    uint64_t begin,
    vector_size_t count,
    EncodedKeyView* views) {
  const auto& layout = storage.layout();
  vector_size_t outputRow = 0;
  while (outputRow < count) {
    const auto range = storage.keyRangeAt(begin + outputRow, count - outputRow);
    for (vector_size_t row = 0; row < range.count; ++row) {
      const auto* keyDataAtRow =
          range.data + static_cast<uint64_t>(row) * layout.width();
      views[outputRow + row] =
          materializeVariableKeyView<Inline>(layout, keyDataAtRow);
    }
    outputRow += range.count;
  }
}

void materializeKeyPointerViews(
    const RadixSortKeyLayout& layout,
    std::span<const char* const> keys,
    RadixSortInlineKeyBuffer* buffers,
    EncodedKeyView* views) {
  for (vector_size_t row = 0; row < keys.size(); ++row) {
    RadixSortKey(layout, keys[row]).deconstruct(buffers[row], views[row]);
  }
}

void materializeKeyViews(
    const RadixSortRunStorage& storage,
    uint64_t begin,
    vector_size_t count,
    RadixSortInlineKeyBuffer* buffers,
    EncodedKeyView* views) {
  vector_size_t outputRow = 0;
  while (outputRow < count) {
    const auto range = storage.keyRangeAt(begin + outputRow, count - outputRow);
    for (vector_size_t row = 0; row < range.count; ++row) {
      const auto* key =
          range.data + static_cast<uint64_t>(row) * storage.layout().width();
      RadixSortKey(storage.layout(), key)
          .deconstruct(buffers[outputRow + row], views[outputRow + row]);
    }
    outputRow += range.count;
  }
}

template <typename T>
void prepareReusableDecodeBuffer(
    BufferPtr& buffer,
    vector_size_t count,
    memory::MemoryPool* pool) {
  const auto bytes = checkedMultiply<uint64_t>(count, sizeof(T));
  BOLT_CHECK(bytes.has_value(), "Radix sort decode scratch size overflows");
  if (buffer == nullptr || buffer->pool() != pool) {
    buffer.reset();
    buffer = AlignedBuffer::allocate<T>(count, pool);
  } else if (buffer->capacity() < *bytes) {
    buffer.reset();
    buffer = AlignedBuffer::allocate<T>(count, pool);
  } else {
    buffer->setSize(*bytes);
  }
}

} // namespace

std::unique_ptr<RadixSortOutputProjection> RadixSortOutputProjection::create(
    const RowTypePtr& outputType,
    const RowTypePtr& keyType,
    const std::vector<column_index_t>& directKeyChannels) {
  std::vector<int32_t> decodedKeyByOutput(outputType->size(), -1);
  std::vector<uint32_t> directOccurrences(outputType->size(), 0);
  for (uint32_t key = 0; key < keyType->size(); ++key) {
    const auto channel = directKeyChannels[key];
    ++directOccurrences[channel];
    if (decodedKeyByOutput[channel] < 0) {
      decodedKeyByOutput[channel] = key;
    }
  }

  std::vector<RadixSortOutputColumn> columns(outputType->size());
  std::vector<column_index_t> payloadChannels;
  std::vector<std::string> payloadNames;
  std::vector<TypePtr> payloadTypes;
  payloadChannels.reserve(outputType->size());
  payloadNames.reserve(outputType->size());
  payloadTypes.reserve(outputType->size());
  for (uint32_t output = 0; output < outputType->size(); ++output) {
    const auto decodedKey = decodedKeyByOutput[output];
    if (decodedKeyByOutput[output] >= 0 && directOccurrences[output] == 1) {
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
    memory::MemoryPool* pool,
    RowVectorPtr& output) const {
  const auto size =
      decodedKeys != nullptr ? decodedKeys->size() : payload->size();
  if (output == nullptr) {
    output = std::make_shared<RowVector>(
        pool,
        outputType_,
        nullptr,
        size,
        std::vector<VectorPtr>(outputType_->size()));
  }
  for (uint32_t outputChannel = 0; outputChannel < columns_.size();
       ++outputChannel) {
    const auto& column = columns_[outputChannel];
    if (column.source == RadixSortOutputSource::kDecodedKey) {
      output->childAt(outputChannel) = decodedKeys->childAt(column.sourceIndex);
    } else {
      output->childAt(outputChannel) = payload->childAt(column.sourceIndex);
    }
  }
  return output;
}

std::unique_ptr<RadixSortRun> RadixSortRun::create(
    memory::MemoryPool* pool,
    const RowTypePtr& outputType,
    const RowTypePtr& keyType,
    const std::vector<CompareFlags>& keyFlags,
    const std::vector<column_index_t>& directKeyChannels,
    RadixSortRunOptions options) {
  BOLT_CHECK_NOT_NULL(pool, "RadixSortRun memory pool must not be null");
  BOLT_CHECK_NOT_NULL(keyType, "RadixSortRun key type must not be null");
  BOLT_CHECK_EQ(
      keyType->size(),
      keyFlags.size(),
      "RadixSortRun key type and flag counts do not match");

  auto projection =
      RadixSortOutputProjection::create(outputType, keyType, directKeyChannels);

  std::unique_ptr<RadixSortKeyCodec> keyCodec;
  RadixSortKeyCodec::bind(keyType->children(), keyFlags, keyCodec);

  std::shared_ptr<const PayloadRowLayout> payloadLayout;
  if (projection->hasPayload()) {
    payloadLayout = PayloadRowLayout::create(projection->payloadType());
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
      keyCodec->maximumEncodedSize(), projection->hasPayload());
  if (keyLayout.isVariable()) {
    keyLayout = RadixSortKeyLayout::select(
        keyCodec->maximumEncodedSize(),
        projection->hasPayload(),
        keyCodec->heapKeyOffsetForVariableLayout(keyLayout.inlineCapacity()));
  }
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
      std::move(payloadMayHaveNulls),
      options.initialVariableKeysFitRadixPrefix));
}

void RadixSortRun::append(const RowVector& input) {
  BOLT_CHECK(
      state_ == RadixSortRunState::kBuilding,
      "RadixSortRun accepts input only while building");
  if (input.size() == 0) {
    return;
  }
  const auto nextInputRows =
      checkedAdd<uint64_t>(metrics_.inputRows, input.size());

  std::vector<VectorPtr> keyInputChildren;
  projection_->projectKeys(input, keyInputChildren);
  RowVector keys(
      pool_,
      projection_->keyType(),
      nullptr,
      input.size(),
      std::move(keyInputChildren));

  for (uint32_t column = 0; column < keys.childrenSize(); ++column) {
    const auto mayHaveNulls = keys.childAt(column)->mayHaveNulls();
    keyMayHaveNulls_[column] |= mayHaveNulls;
    currentRunKeyMayHaveNulls_[column] |= mayHaveNulls;
  }

  const bool directVariableKey = keyLayout_.isVariable();
  const auto appendKeys = [&](std::span<char* const> payloads) {
    if (directVariableKey) {
      const auto batchMaximumEncodedKeySize =
          keyCodec_->encodeAndAppendVariable(
              keys, *storage_, payloads, firstSuffixColumn_, keySizeScratch_);
      currentRunMaximumEncodedKeySize_ = std::max(
          currentRunMaximumEncodedKeySize_, batchMaximumEncodedKeySize);
      variableKeysFitRadixPrefix_ &=
          batchMaximumEncodedKeySize <= keyLayout_.inlineCapacity();
      return;
    }
    keyCodec_->encodeAndAppendInline(keys, *storage_, payloads);
  };

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
    payloadWriter_.append(payloadInput, *storage_, payloadBatch_);
    const auto payloads =
        std::span<char* const>(payloadBatch_.rows()->as<char*>(), input.size());
    appendKeys(payloads);
    metrics_.appendTimeUs += elapsedUs(appendBegin);
  } else {
    const auto appendBegin = std::chrono::steady_clock::now();
    appendKeys({});
    metrics_.appendTimeUs += elapsedUs(appendBegin);
  }
  if (projection_->hasPayload()) {
    for (uint32_t column = 0; column < payloadMayHaveNulls_.size(); ++column) {
      payloadMayHaveNulls_[column] |= payloadMayHaveNulls[column];
    }
  }
  metrics_.inputRows = *nextInputRows;
}

void RadixSortRun::finalize() {
  BOLT_CHECK(
      state_ == RadixSortRunState::kBuilding,
      "RadixSortRun can be finalized only once");
  keySizeScratch_.reset();
  payloadBatch_ = PayloadRowBatch{};
  payloadWriter_.clear();
  state_ = RadixSortRunState::kFinalizing;
  decodeVariableKeysFromInline_ = variableKeysFitRadixPrefix_;
  const auto begin = std::chrono::steady_clock::now();
  try {
    RadixSortRunSorter sorter(*storage_);
    auto skippableValidityOffsets = keyCodec_->leadingSkippableValidityOffsets(
        currentRunKeyMayHaveNulls_, keyLayout_.radixWidth());
    if (keyLayout_.isVariable()) {
      for (auto offset = static_cast<uint32_t>(std::min<uint64_t>(
               currentRunMaximumEncodedKeySize_, keyLayout_.radixWidth()));
           offset < keyLayout_.radixWidth();
           ++offset) {
        skippableValidityOffsets.push_back(offset);
      }
    }
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
  RowVectorPtr output;
  return getOutput(maxRows, outputPool, output);
}

RowVectorPtr RadixSortRun::getOutput(
    vector_size_t maxRows,
    memory::MemoryPool* outputPool,
    RowVectorPtr& output) {
  if (state_ == RadixSortRunState::kConsumed) {
    return nullptr;
  }
  BOLT_CHECK(
      state_ == RadixSortRunState::kSortedInMemory,
      "RadixSortRun output requires a finalized run");

  if (outputPosition_ == storage_->size()) {
    clear();
    return nullptr;
  }
  const auto remaining = storage_->size() - outputPosition_;
  const auto count = static_cast<vector_size_t>(
      std::min<uint64_t>(remaining, static_cast<uint64_t>(maxRows)));
  const auto nextOutputRows = checkedAdd<uint64_t>(metrics_.outputRows, count);
  const auto begin = std::chrono::steady_clock::now();
  RowVectorPtr decodedKeys;
  if (projection_->needsDecodedKeys()) {
    decodedKeys = decodeKeys(outputPosition_, count, outputPool);
  }
  auto payload = gatherPayload(outputPosition_, count, outputPool);
  auto result = projection_->reconstruct(
      projection_->needsDecodedKeys() ? decodedKeys : nullptr,
      projection_->hasPayload() ? payload : nullptr,
      outputPool,
      output);

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
  RowVectorPtr output;
  return getOutput(keys, payloads, outputPool, output);
}

RowVectorPtr RadixSortRun::getOutput(
    std::span<const char* const> keys,
    std::span<char* const> payloads,
    memory::MemoryPool* outputPool,
    RowVectorPtr& output) {
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
      outputPool,
      output);
  metrics_.outputRows += count;
  return result;
}

void RadixSortRun::writeMergeOutput(
    std::span<const char* const> keys,
    std::span<char* const> payloads,
    vector_size_t outputOffset,
    RowVector& output) {
  BOLT_DCHECK_GE(outputOffset, 0);
  BOLT_DCHECK_LE(outputOffset, output.size());
  BOLT_DCHECK_LE(
      keys.size(), static_cast<size_t>(output.size() - outputOffset));
  BOLT_DCHECK_EQ(payloads.size(), projection_->hasPayload() ? keys.size() : 0);

  if (keys.empty()) {
    return;
  }
  if (projection_->needsDecodedKeys()) {
    decodeKeysAt(keys, outputOffset, output.pool(), output);
  }
  if (projection_->hasPayload()) {
    gatherPayloadAt(payloads, outputOffset, output);
  }
}

void RadixSortRun::prepareMergeOutput(RowVector& output) {
  BOLT_DCHECK_NOT_NULL(output.pool());
  if (projection_->needsDecodedKeys()) {
    if (!mergeDecodePlan_.has_value()) {
      MergeDecodePlan plan;
      plan.singleFixedWordBytes =
          keyCodec_->singleFixedWordBytes(keyLayout_.kind());
      if (!plan.singleFixedWordBytes.has_value()) {
        const auto firstSuffix =
            keyLayout_.isVariable() ? firstSuffixColumn_ : 0;
        plan.scratchWords = keyCodec_->decodeScratchWordsPerRowWithMask(
            projection_->decodedKeyMask(), keyMayHaveNulls_, firstSuffix);
      }
      mergeDecodePlan_.emplace(std::move(plan));
    }
  }
  if (projection_->hasPayload()) {
    if (!mergePayloadGatherPlan_.has_value()) {
      mergePayloadGatherPlan_.emplace(PayloadRowReader::makePlan(
          *payloadLayout_,
          projection_->payloadChannels(),
          payloadMayHaveNulls_));
    }
    PayloadRowReader::bind(*mergePayloadGatherPlan_, output);
  }
}

void RadixSortRun::finishMergeOutput(
    RowVector& output,
    vector_size_t writtenRows) {
  BOLT_DCHECK_GE(writtenRows, 0);
  BOLT_DCHECK_LE(writtenRows, output.size());
  if (projection_->needsDecodedKeys()) {
    keyCodec_->finishDecode(
        projection_->decodedKeyMask(),
        output,
        projection_->directKeyChannels());
  }
  if (mergePayloadGatherPlan_.has_value()) {
    PayloadRowReader::finish(*mergePayloadGatherPlan_);
  }
  const auto nextOutputRows =
      checkedAdd<uint64_t>(metrics_.outputRows, writtenRows);
  BOLT_CHECK(nextOutputRows.has_value(), "Radix sort output rows overflow");
  metrics_.outputRows = *nextOutputRows;
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
        payloads[outputRow + row] = loadCompactPointer(key + payloadOffset);
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
  mergePayloadGatherPlan_.reset();
  keySizeScratch_.reset();
  payloadBatch_ = PayloadRowBatch{};
  payloadWriter_.clear();
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
  if (keyCodec_->tryDecodeSingleFixedColumn(
          *storage_,
          begin,
          count,
          keyMayHaveNulls_[0] != 0,
          outputPool,
          decodedKeysOutput_)) {
    return decodedKeysOutput_;
  }

  prepareReusableDecodeBuffer<EncodedKeyView>(
      decodeViewsOutput_, count, outputPool);
  auto* rawViews = decodeViewsOutput_->asMutable<EncodedKeyView>();
  if (keyLayout_.isVariable()) {
    if (decodeVariableKeysFromInline_) {
      materializeVariableKeyViews<true>(*storage_, begin, count, rawViews);
    } else {
      materializeVariableKeyViews<false>(*storage_, begin, count, rawViews);
    }
  } else {
    prepareReusableDecodeBuffer<RadixSortInlineKeyBuffer>(
        decodeInlineOutput_, count, outputPool);
    materializeKeyViews(
        *storage_,
        begin,
        count,
        decodeInlineOutput_->asMutable<RadixSortInlineKeyBuffer>(),
        rawViews);
  }
  keyCodec_->decode(
      std::span<const EncodedKeyView>(rawViews, count),
      projection_->decodedKeyMask(),
      keyMayHaveNulls_,
      outputPool,
      decodeCursorOutput_,
      decodedKeysOutput_,
      keyLayout_.isVariable() ? firstSuffixColumn_ : 0);
  if (keyLayout_.isVariable()) {
    keyCodec_->decodeFixedPrefix(
        *storage_,
        begin,
        count,
        projection_->decodedKeyMask(),
        keyMayHaveNulls_,
        decodedKeysOutput_,
        firstSuffixColumn_);
  }
  return decodedKeysOutput_;
}

RowVectorPtr RadixSortRun::decodeKeyPointers(
    std::span<const char* const> keys,
    memory::MemoryPool* outputPool) {
  const auto count = static_cast<vector_size_t>(keys.size());
  if (keyCodec_->tryDecodeSingleFixedColumn(
          keys,
          keyLayout_.kind(),
          keyMayHaveNulls_[0] != 0,
          outputPool,
          decodedKeysOutput_)) {
    return decodedKeysOutput_;
  }
  prepareReusableDecodeBuffer<EncodedKeyView>(
      decodeViewsOutput_, count, outputPool);
  auto* rawViews = decodeViewsOutput_->asMutable<EncodedKeyView>();
  if (keyLayout_.isVariable()) {
    if (decodeVariableKeysFromInline_) {
      materializeVariableKeyPointerViews<true>(keyLayout_, keys, rawViews);
    } else {
      materializeVariableKeyPointerViews<false>(keyLayout_, keys, rawViews);
    }
  } else {
    prepareReusableDecodeBuffer<RadixSortInlineKeyBuffer>(
        decodeInlineOutput_, count, outputPool);
    materializeKeyPointerViews(
        keyLayout_,
        keys,
        decodeInlineOutput_->asMutable<RadixSortInlineKeyBuffer>(),
        rawViews);
  }
  keyCodec_->decode(
      std::span<const EncodedKeyView>(rawViews, count),
      projection_->decodedKeyMask(),
      keyMayHaveNulls_,
      outputPool,
      decodeCursorOutput_,
      decodedKeysOutput_,
      keyLayout_.isVariable() ? firstSuffixColumn_ : 0);
  if (keyLayout_.isVariable()) {
    keyCodec_->decodeFixedPrefix(
        keys,
        projection_->decodedKeyMask(),
        keyMayHaveNulls_,
        decodedKeysOutput_,
        firstSuffixColumn_);
  }
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
          loadCompactPointer(key + *keyLayout_.payloadOffset());
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

void RadixSortRun::decodeKeysAt(
    std::span<const char* const> keys,
    vector_size_t outputOffset,
    memory::MemoryPool* outputPool,
    RowVector& output) {
  const auto count = static_cast<vector_size_t>(keys.size());
  BOLT_DCHECK(mergeDecodePlan_.has_value());
  const auto& plan = *mergeDecodePlan_;
  if (plan.singleFixedWordBytes.has_value()) {
    keyCodec_->decodeSingleFixedAt(
        keys,
        keyMayHaveNulls_[0] != 0,
        outputOffset,
        *plan.singleFixedWordBytes,
        output.childAt(projection_->directKeyChannels()[0]));
    return;
  }

  prepareReusableDecodeBuffer<EncodedKeyView>(
      decodeViewsOutput_, count, outputPool);
  auto* rawViews = decodeViewsOutput_->asMutable<EncodedKeyView>();
  if (keyLayout_.isVariable()) {
    if (decodeVariableKeysFromInline_) {
      materializeVariableKeyPointerViews<true>(keyLayout_, keys, rawViews);
    } else {
      materializeVariableKeyPointerViews<false>(keyLayout_, keys, rawViews);
    }
  } else {
    prepareReusableDecodeBuffer<RadixSortInlineKeyBuffer>(
        decodeInlineOutput_, count, outputPool);
    materializeKeyPointerViews(
        keyLayout_,
        keys,
        decodeInlineOutput_->asMutable<RadixSortInlineKeyBuffer>(),
        rawViews);
  }

  keyCodec_->decodeSuffixAt(
      std::span<const EncodedKeyView>(rawViews, count),
      projection_->decodedKeyMask(),
      keyMayHaveNulls_,
      outputOffset,
      outputPool,
      decodeCursorOutput_,
      output,
      projection_->directKeyChannels(),
      plan.scratchWords,
      keyLayout_.isVariable() ? firstSuffixColumn_ : 0);
  if (keyLayout_.isVariable()) {
    keyCodec_->decodePrefixAt(
        keys,
        projection_->decodedKeyMask(),
        keyMayHaveNulls_,
        outputOffset,
        output,
        projection_->directKeyChannels(),
        firstSuffixColumn_);
  }
}

void RadixSortRun::gatherPayloadAt(
    std::span<char* const> payloads,
    vector_size_t outputOffset,
    RowVector& output) {
  BOLT_DCHECK(mergePayloadGatherPlan_.has_value());
  PayloadRowReader::gather(*mergePayloadGatherPlan_, payloads, outputOffset);
}

uint64_t RadixSortRun::elapsedUs(
    const std::chrono::steady_clock::time_point& begin) {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - begin)
      .count();
}

} // namespace bytedance::bolt::exec::radixsort
