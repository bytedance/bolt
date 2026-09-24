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

#pragma once

#include <array>
#include <cstdint>
#include <mutex>
#include <optional>

namespace bytedance::bolt::lance::reader {

enum class NativeLanceMemoryClass : uint8_t {
  kOutput,
  kCompressedInput,
  kCodecContext,
  kControl,
  kPayload,
  kDecodeScratch,
  kCompatibilityVector,
  kNumClasses,
};

/// Enforces a hard bound on scan-local transient allocations.
class NativeLanceMemoryBudget {
 public:
  class Reservation {
   public:
    Reservation() = default;
    Reservation(const Reservation&) = delete;
    Reservation& operator=(const Reservation&) = delete;
    Reservation(Reservation&& other) noexcept;
    Reservation& operator=(Reservation&& other) noexcept;
    ~Reservation();

    uint64_t bytes() const {
      return bytes_;
    }

    NativeLanceMemoryClass memoryClass() const {
      return memoryClass_;
    }

    explicit operator bool() const {
      return budget_ != nullptr;
    }

   private:
    friend class NativeLanceMemoryBudget;

    Reservation(
        NativeLanceMemoryBudget* budget,
        NativeLanceMemoryClass memoryClass,
        uint64_t bytes,
        bool oversized);

    void release();

    NativeLanceMemoryBudget* budget_{nullptr};
    NativeLanceMemoryClass memoryClass_{NativeLanceMemoryClass::kOutput};
    uint64_t bytes_{0};
    bool oversized_{false};
  };

  explicit NativeLanceMemoryBudget(uint64_t limitBytes);

  std::optional<Reservation> tryReserve(
      NativeLanceMemoryClass memoryClass,
      uint64_t bytes);

  /// Admits one output larger than the limit only when no other bytes are held.
  Reservation reserveOversizedOutput(uint64_t bytes);

  uint64_t usedBytes() const;
  uint64_t peakBytes() const;
  uint64_t limitBytes() const {
    return limitBytes_;
  }
  uint64_t usedBytes(NativeLanceMemoryClass memoryClass) const;

 private:
  void release(
      NativeLanceMemoryClass memoryClass,
      uint64_t bytes,
      bool oversized) noexcept;

  static size_t classIndex(NativeLanceMemoryClass memoryClass) noexcept;

  const uint64_t limitBytes_;
  mutable std::mutex mutex_;
  uint64_t usedBytes_{0};
  uint64_t peakBytes_{0};
  bool oversizedOutputActive_{false};
  std::array<uint64_t, static_cast<size_t>(NativeLanceMemoryClass::kNumClasses)>
      usedByClass_{};
};

} // namespace bytedance::bolt::lance::reader
