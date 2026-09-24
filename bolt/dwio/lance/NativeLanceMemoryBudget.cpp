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

#include "bolt/dwio/lance/NativeLanceMemoryBudget.h"

#include <algorithm>
#include <utility>

#include "bolt/common/base/Exceptions.h"

namespace bytedance::bolt::lance::reader {

NativeLanceMemoryBudget::Reservation::Reservation(
    NativeLanceMemoryBudget* budget,
    NativeLanceMemoryClass memoryClass,
    uint64_t bytes,
    bool oversized)
    : budget_(budget),
      memoryClass_(memoryClass),
      bytes_(bytes),
      oversized_(oversized) {}

NativeLanceMemoryBudget::Reservation::Reservation(Reservation&& other) noexcept
    : budget_(std::exchange(other.budget_, nullptr)),
      memoryClass_(other.memoryClass_),
      bytes_(std::exchange(other.bytes_, 0)),
      oversized_(std::exchange(other.oversized_, false)) {}

NativeLanceMemoryBudget::Reservation&
NativeLanceMemoryBudget::Reservation::operator=(Reservation&& other) noexcept {
  if (this != &other) {
    release();
    budget_ = std::exchange(other.budget_, nullptr);
    memoryClass_ = other.memoryClass_;
    bytes_ = std::exchange(other.bytes_, 0);
    oversized_ = std::exchange(other.oversized_, false);
  }
  return *this;
}

NativeLanceMemoryBudget::Reservation::~Reservation() {
  release();
}

void NativeLanceMemoryBudget::Reservation::release() {
  if (budget_ == nullptr) {
    return;
  }
  budget_->release(memoryClass_, bytes_, oversized_);
  budget_ = nullptr;
  bytes_ = 0;
  oversized_ = false;
}

NativeLanceMemoryBudget::NativeLanceMemoryBudget(uint64_t limitBytes)
    : limitBytes_(limitBytes) {
  BOLT_CHECK_GT(limitBytes_, 0);
}

std::optional<NativeLanceMemoryBudget::Reservation>
NativeLanceMemoryBudget::tryReserve(
    NativeLanceMemoryClass memoryClass,
    uint64_t bytes) {
  BOLT_CHECK_LT(
      static_cast<size_t>(memoryClass),
      static_cast<size_t>(NativeLanceMemoryClass::kNumClasses));
  const auto index = classIndex(memoryClass);
  std::lock_guard<std::mutex> lock(mutex_);
  if (oversizedOutputActive_ || bytes > limitBytes_ - usedBytes_) {
    return std::nullopt;
  }
  usedBytes_ += bytes;
  usedByClass_[index] += bytes;
  peakBytes_ = std::max(peakBytes_, usedBytes_);
  return Reservation(this, memoryClass, bytes, false);
}

NativeLanceMemoryBudget::Reservation
NativeLanceMemoryBudget::reserveOversizedOutput(uint64_t bytes) {
  std::lock_guard<std::mutex> lock(mutex_);
  BOLT_CHECK_GT(bytes, limitBytes_);
  BOLT_CHECK_EQ(
      usedBytes_,
      0,
      "An oversized Lance output requires an otherwise empty budget");
  BOLT_CHECK(!oversizedOutputActive_);
  oversizedOutputActive_ = true;
  usedBytes_ = bytes;
  usedByClass_[classIndex(NativeLanceMemoryClass::kOutput)] = bytes;
  peakBytes_ = std::max(peakBytes_, usedBytes_);
  return Reservation(this, NativeLanceMemoryClass::kOutput, bytes, true);
}

uint64_t NativeLanceMemoryBudget::usedBytes() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return usedBytes_;
}

uint64_t NativeLanceMemoryBudget::peakBytes() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return peakBytes_;
}

uint64_t NativeLanceMemoryBudget::usedBytes(
    NativeLanceMemoryClass memoryClass) const {
  BOLT_CHECK_LT(
      static_cast<size_t>(memoryClass),
      static_cast<size_t>(NativeLanceMemoryClass::kNumClasses));
  std::lock_guard<std::mutex> lock(mutex_);
  return usedByClass_[classIndex(memoryClass)];
}

void NativeLanceMemoryBudget::release(
    NativeLanceMemoryClass memoryClass,
    uint64_t bytes,
    bool oversized) noexcept {
  const auto index = classIndex(memoryClass);
  std::lock_guard<std::mutex> lock(mutex_);
  if (usedBytes_ < bytes || usedByClass_[index] < bytes ||
      (oversized && !oversizedOutputActive_)) {
    std::terminate();
  }
  usedBytes_ -= bytes;
  usedByClass_[index] -= bytes;
  if (oversized) {
    if (usedBytes_ != 0) {
      std::terminate();
    }
    oversizedOutputActive_ = false;
  }
}

size_t NativeLanceMemoryBudget::classIndex(
    NativeLanceMemoryClass memoryClass) noexcept {
  return static_cast<size_t>(memoryClass);
}

} // namespace bytedance::bolt::lance::reader
