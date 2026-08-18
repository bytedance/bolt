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
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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

#pragma once

#include <glog/logging.h>
#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
namespace bytedance::bolt {

// Circular double linked list with 6 byte pointers. Used for free
// list in HashStringAllocator so that we get a minimum allocation
// payload size of 16 bytes. (12 bits for the links, 4 for trailer).
class CompactDoubleList {
 public:
  CompactDoubleList() {
    setNext(this);
    setPrevious(this);
  }

  CompactDoubleList(const CompactDoubleList& other) = delete;
  CompactDoubleList(CompactDoubleList&& other) = delete;
  CompactDoubleList& operator=(const CompactDoubleList& other) = default;
  void operator=(CompactDoubleList&& other) = delete;

  // Return true if 'this' is the only element.
  bool empty() const {
    return next() == this;
  }

  // inserts 'entry' after 'this'
  void insert(CompactDoubleList* entry) {
    auto* nextEntry = next();
    entry->setNext(nextEntry);
    entry->setPrevious(this);
    nextEntry->setPrevious(entry);
    setNext(entry);
  }

  // Removes and returns the first element. Sets 'listIsEmpty' if the list is
  // empty after the removal. Returns nullptr if the list was already empty.
  CompactDoubleList* popFront(bool& listIsEmpty) {
    auto* entry = next();
    if (entry == this) {
      listIsEmpty = true;
      return nullptr;
    }
    auto* nextEntry = entry->next();
    setNext(nextEntry);
    nextEntry->setPrevious(this);
    listIsEmpty = nextEntry == this;
    return entry;
  }

  // Unlinks 'this' from its list. Returns true if the list is empty after the
  // removal. Throws if 'this' is not linked to a list.
  bool remove() {
    auto* nextEntry = next();
    BOLT_CHECK(nextEntry != this);
    auto* previousEntry = previous();
    previousEntry->setNext(nextEntry);
    nextEntry->setPrevious(previousEntry);
    return previousEntry == nextEntry;
  }

  CompactDoubleList* next() const {
    return loadPointer(nextLow_, nextHigh_);
  }

  CompactDoubleList* previous() const {
    return loadPointer(previousLow_, previousHigh_);
  }

  /// Updates links after the next() of 'this' has been moved to 'newNext'. Sets
  /// the next link of this, the previous link of 'newNext' and the previous
  /// link of the next() of the moved 'newNext'. The use case is taking the
  // head of a free list block without a full remove of block plus reinsert of
  // the remainder of the block.
  void nextMoved(CompactDoubleList* newNext) {
    setNext(newNext);
    BOLT_CHECK(newNext->previous() == this);
    newNext->next()->setPrevious(newNext);
  }

 private:
  static constexpr uint8_t kPointerSignificantBits = 48;

  void setNext(CompactDoubleList* next) {
    storePointer(next, nextLow_, nextHigh_);
  }

  void setPrevious(CompactDoubleList* previous) {
    storePointer(previous, previousLow_, previousHigh_);
  }

  CompactDoubleList* loadPointer(uint32_t low, uint16_t high) const {
    return reinterpret_cast<CompactDoubleList*>(
        low | (static_cast<uint64_t>(high) << 32));
  }

  void storePointer(CompactDoubleList* pointer, uint32_t& low, uint16_t& high) {
    DCHECK_EQ(
        reinterpret_cast<uint64_t>(pointer) &
            ~bits::lowMask(kPointerSignificantBits),
        0);
    uint64_t data = reinterpret_cast<uint64_t>(pointer);
    low = static_cast<uint32_t>(data);
    high = static_cast<uint16_t>(data >> 32);
  }

  // 12 bytes. Stores 2 48 bit pointers.
  uint32_t nextLow_;
  uint32_t previousLow_;
  uint16_t nextHigh_;
  uint16_t previousHigh_;
};

} // namespace bytedance::bolt
