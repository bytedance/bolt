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

#include <cstdint>
#include <vector>

#include "bolt/common/base/BitUtil.h"
#include "bolt/common/base/Exceptions.h"
#include "bolt/common/base/IOUtils.h"
#include "bolt/type/StringView.h"
#define XXH_INLINE_ALL
#include <xxhash.h>
namespace bytedance::bolt {
// BloomFilter filter with groups of 64 bits, of which 4 are set. The hash
// number has 4 6 bit fields that select the bits in the word and the
// remaining bits select the word in the filter. With 8 bits per
// expected entry, we get ~2% false positives. 'hashInput' determines
// if the value added or checked needs to be hashed. If this is false,
// we assume that the input is already a 64 bit hash number.
template <typename Allocator = std::allocator<uint64_t>>
class BloomFilter {
 public:
  explicit BloomFilter() : bits_{Allocator()} {}
  explicit BloomFilter(const Allocator& allocator) : bits_{allocator} {}

  // Prepares 'this' for use with an expected 'capacity'
  // entries. Drops any prior content.
  void reset(int32_t capacity) {
    bits_.clear();
    // 2 bytes per value.
    bits_.resize(std::max<int32_t>(4, bits::nextPowerOfTwo(capacity) / 4));
  }

  bool isSet() const {
    return bits_.size() > 0;
  }

  // Adds 'value'.
  // Input is hashed uint64_t value, optional hash function is
  // folly::hasher<InputType>()(value).
  void insert(uint64_t value) {
    set(bits_.data(), bits_.size(), value);
  }

  // Input is hashed uint64_t value, optional hash function is
  // folly::hasher<InputType>()(value).
  bool mayContain(uint64_t value) const {
    return test(bits_.data(), bits_.size(), value);
  }

  void merge(const char* serialized) {
    common::InputByteStream stream(serialized);
    auto version = stream.read<int8_t>();
    BOLT_USER_CHECK_EQ(kBloomFilterV1, version);
    auto size = stream.read<int32_t>();
    bits_.resize(size);
    auto bitsdata =
        reinterpret_cast<const uint64_t*>(serialized + stream.offset());
    if (bits_.size() == 0) {
      for (auto i = 0; i < size; i++) {
        bits_[i] = bitsdata[i];
      }
      return;
    } else if (size == 0) {
      return;
    }
    BOLT_DCHECK_EQ(bits_.size(), size);
    bits::orBits(bits_.data(), bitsdata, 0, 64 * size);
  }

  uint32_t serializedSize() const {
    return 1 /* version */
        + 4 /* number of bits */
        + bits_.size() * 8;
  }

  void serialize(char* output) const {
    common::OutputByteStream stream(output);
    stream.appendOne(kBloomFilterV1);
    stream.appendOne((int32_t)bits_.size());
    for (auto bit : bits_) {
      stream.appendOne(bit);
    }
  }

 private:
  // We use 4 independent hash functions by taking 24 bits of
  // the hash code and breaking these up into 4 groups of 6 bits. Each group
  // represents a number between 0 and 63 (2^6-1) and maps to one bit in a
  // 64-bit number. We combine these to get a 64-bit number with up to 4 bits
  // set.
  inline static uint64_t bloomMask(uint64_t hashCode) {
    return (1L << (hashCode & 63)) | (1L << ((hashCode >> 6) & 63)) |
        (1L << ((hashCode >> 12) & 63)) | (1L << ((hashCode >> 18) & 63));
  }

  // Skip 24 bits used for bloomMask and use the next N bits of the hash code
  // as index. N = log2(bloomSize). bloomSize must be a power of 2.
  inline static uint32_t bloomIndex(uint32_t bloomSize, uint64_t hashCode) {
    return ((hashCode >> 24) & (bloomSize - 1));
  }

  inline static void
  set(uint64_t* FOLLY_NONNULL bloom, int32_t bloomSize, uint64_t hashCode) {
    auto mask = bloomMask(hashCode);
    auto index = bloomIndex(bloomSize, hashCode);
    bloom[index] |= mask;
  }

  inline static bool test(
      const uint64_t* FOLLY_NONNULL bloom,
      int32_t bloomSize,
      uint64_t hashCode) {
    auto mask = bloomMask(hashCode);
    auto index = bloomIndex(bloomSize, hashCode);
    return mask == (bloom[index] & mask);
  }

  const int8_t kBloomFilterV1 = 1;
  std::vector<uint64_t, Allocator> bits_;
};

enum class BloomFilterKind {
  kParquet,
  kNGram,
};

class MetaBloomFilter {
 protected:
  MetaBloomFilter(BloomFilterKind kind) : kind_(kind) {}

 public:
  virtual ~MetaBloomFilter() {}
  virtual bool mayContain(uint64_t hash) const {
    BOLT_UNSUPPORTED("{}: mayContain(uint64_t) is not supported.", toString());
  }
  virtual bool mayContain(const char* data, size_t len) const {
    BOLT_UNSUPPORTED(
        "{}: mayContain(const char*, size_t) is not supported.", toString());
  }

  virtual bool mayContain(const std::string& data) const {
    BOLT_UNSUPPORTED(
        "{}: mayContain(const std::string&) is not supported.", toString());
  }

  virtual void insert(uint64_t hash) {
    BOLT_UNSUPPORTED("{}: insert(uint64_t) is not supported.", toString());
  }
  virtual void insert(const char* data, size_t len) {
    BOLT_UNSUPPORTED(
        "{}: insert(const char*, size_t) is not supported.", toString());
  }
  virtual void insert(const std::string& data) {
    if (data.size() > 0) {
      return insert(data.c_str(), data.size());
    }
  }
  virtual int64_t GetBitsetSize() const = 0;
  virtual std::string toString() const;

  virtual uint64_t Hash(int32_t value) const {
    BOLT_UNSUPPORTED("{}: Hash(int32_t) is not supported.", toString());
  }
  virtual uint64_t Hash(int64_t value) const {
    BOLT_UNSUPPORTED("{}: Hash(int64_t) is not supported.", toString());
  }
  virtual uint64_t Hash(float value) const {
    BOLT_UNSUPPORTED("{}: Hash(float) is not supported.", toString());
  }
  virtual uint64_t Hash(double value) const {
    BOLT_UNSUPPORTED("{}: Hash(double) is not supported.", toString());
  }
  virtual uint64_t Hash(const std::string value) const {
    BOLT_UNSUPPORTED("{}: Hash(std::string) is not supported.", toString());
  }

 private:
  BloomFilterKind kind_;
};

namespace {
template <typename T>
uint64_t XxHashHelper(T value, uint32_t seed) {
  return XXH64(reinterpret_cast<const void*>(&value), sizeof(T), seed);
}
} // namespace

// used for parquet bloom filter
class BlockSplitBloomFilter : public MetaBloomFilter {
 public:
  BlockSplitBloomFilter(int32_t num_bytes);
  BlockSplitBloomFilter(const uint8_t* bitset, int32_t num_bytes);

  bool mayContain(uint64_t hash) const override;
  void insert(uint64_t hash) override;

  uint64_t Hash(int32_t value) const override {
    return XxHashHelper(value, kParquetBloomXxHashSeed);
  }
  uint64_t Hash(int64_t value) const override {
    return XxHashHelper(value, kParquetBloomXxHashSeed);
  }
  uint64_t Hash(float value) const override {
    return XxHashHelper(value, kParquetBloomXxHashSeed);
  }
  uint64_t Hash(double value) const override {
    return XxHashHelper(value, kParquetBloomXxHashSeed);
  }
  uint64_t Hash(const std::string value) const override {
    return XXH64(
        reinterpret_cast<const void*>(value.c_str()),
        value.length(),
        kParquetBloomXxHashSeed);
  }

  int64_t GetBitsetSize() const override {
    return num_bytes_;
  }

  const std::vector<uint8_t>& getFilter() const {
    return data_;
  }
  std::vector<uint8_t>& getFilter() {
    return data_;
  }

 private:
  static constexpr uint32_t kParquetBloomXxHashSeed = 0;

  // Minimum Bloom filter size, it sets to 32 bytes to fit a tiny Bloom filter.
  static constexpr uint32_t kMinimumBloomFilterBytes = 32;

  // Maximum Bloom filter size, it sets to HDFS default block size 128MB
  // This value will be reconsidered when implementing Bloom filter producer.
  static constexpr uint32_t kMaximumBloomFilterBytes = 128 * 1024 * 1024;

  // Bytes in a tiny Bloom filter block.
  static constexpr int32_t kBytesPerFilterBlock = 32;

  // The number of bits to be set in each tiny Bloom filter
  static constexpr int32_t kBitsSetPerBlock = 8;

  // A mask structure used to set bits in each tiny Bloom filter.
  struct BlockMask {
    uint32_t item[kBitsSetPerBlock];
  };

  // The block-based algorithm needs eight odd SALT values to calculate eight
  // indexes of bit to set, one bit in each 32-bit word.
  static constexpr uint32_t SALT[kBitsSetPerBlock] = {
      0x47b6137bU,
      0x44974d91U,
      0x8824ad5bU,
      0xa2b7289dU,
      0x705495c7U,
      0x2df1424bU,
      0x9efc4947U,
      0x5c6bfb31U};

  /// Set bits in mask array according to input key.
  /// @param key the value to calculate mask values.
  /// @param mask the mask array is used to set inside a block
  void SetMask(uint32_t key, BlockMask& mask) const;

  // The underlying buffer of bitset.
  std::vector<uint8_t> data_;

  // The number of bytes of Bloom filter bitset.
  int32_t num_bytes_;
};

class NGramBloomFilter : public MetaBloomFilter {
 public:
  static constexpr int64_t SEED_GEN_A = 845897321;
  static constexpr int64_t SEED_GEN_B = 217728422;
  static constexpr int64_t MAX_BLOOM_FILTER_SIZE = 1 << 30;

  NGramBloomFilter(int64_t size, int32_t hashes, int32_t seed);
  NGramBloomFilter(
      int64_t size,
      int32_t hashes,
      int32_t seed,
      const std::vector<uint64_t>& bits);
  NGramBloomFilter(
      int64_t size,
      int32_t hashes,
      int32_t seed,
      const char* bits);

  bool mayContain(const char* data, size_t len) const override;
  bool mayContain(const std::string& data) const override {
    if (data.size() > 0) {
      return mayContain(data.c_str(), data.size());
    } else {
      return false;
    }
    return true;
  }
  void insert(const char* data, size_t len) override;
  void insert(const std::string& data) override {
    if (data.size() > 0) {
      return insert(data.c_str(), data.size());
    }
  }
  int64_t GetBitsetSize() const override {
    return size_;
  }
  void clear();

  /// Checks if this contains everything from another bloom filter.
  /// Bloom filters must have equal size and seed.
  bool contains(const NGramBloomFilter& bf);

  const std::vector<uint64_t>& getFilter() const {
    return filter_;
  }
  std::vector<uint64_t>& getFilter() {
    return filter_;
  }

  // For debug.
  bool isEmpty() const;

  bool operator==(const NGramBloomFilter& bf) const {
    if (this->size_ != bf.size_)
      return false;
    if (this->hashes_ != bf.hashes_)
      return false;
    if (this->seed_ != bf.seed_)
      return false;
    if (this->words_ != bf.words_)
      return false;
    if (this->filter_.size() != bf.filter_.size())
      return false;
    for (int i = 0; i < this->filter_.size(); ++i) {
      if (this->filter_[i] != bf.filter_[i])
        return false;
    }
    return true;
  }

  // Combines the two bloomFilter bits_ using bitwise OR.
  void merge(NGramBloomFilter& bloomFilter) {
    BOLT_CHECK_EQ(words_, bloomFilter.words_);
    for (auto i = 0; i < words_; i++) {
      filter_[i] |= bloomFilter.filter_[i];
    }
  }

  // Combines the two bloomFilter bits_ using bitwise OR.
  void merge(const std::vector<uint64_t>& bloomFilter) {
    BOLT_CHECK_EQ(words_, bloomFilter.size());
    for (auto i = 0; i < words_; i++) {
      filter_[i] |= bloomFilter[i];
    }
  }

  int64_t size() const {
    return size_;
  }
  int32_t hashes() const {
    return hashes_;
  }
  int32_t seed() const {
    return seed_;
  }
  int64_t words() const {
    return words_;
  }

 private:
  int64_t size_;
  int32_t hashes_;
  int32_t seed_;
  int64_t words_;
  std::vector<uint64_t> filter_;
};

} // namespace bytedance::bolt
