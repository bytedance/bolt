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

// Partially inspired and adapted from Apache Arrow.

#pragma once
#include <cstring>
#include "bolt/common/base/Exceptions.h"
#include "bolt/dwio/parquet/encryption/EncryptionType.h"

namespace bytedance::bolt::parquet::encryption {
constexpr int kGcmTagLength = 16;
constexpr int kNonceLength = 12;

// Module types
constexpr int8_t kFooter = 0;
constexpr int8_t kColumnMetaData = 1;
constexpr int8_t kDataPage = 2;
constexpr int8_t kDictionaryPage = 3;
constexpr int8_t kDataPageHeader = 4;
constexpr int8_t kDictionaryPageHeader = 5;
constexpr int8_t kColumnIndex = 6;
constexpr int8_t kOffsetIndex = 7;

/// Performs AES decryption operations with GCM or CTR ciphers.
class AesDecryptor {
 public:
  /// Can serve one key length only. Possible values: 16, 24, 32 bytes.
  /// If contains_length is true, expect ciphertext length prepended to the
  /// ciphertext
  explicit AesDecryptor(
      parquet::ParquetCipher::type algorithm,
      int keyLength,
      bool metadata,
      int32_t maxEncryptedSize,
      bool containsLength = true);

  /// \brief Factory function to create an AesDecryptor
  ///
  /// \param alg_id the encryption algorithm to use
  /// \param metadata if true then this is a metadata decryptor
  /// \param all_decryptors A weak reference to all decryptors that need to be
  /// wiped out when decryption is finished.
  /// \return shared pointer to a new
  /// AesDecryptor
  static std::shared_ptr<AesDecryptor> make(
      parquet::ParquetCipher::type algorithm,
      int keyLength,
      bool metadata,
      int32_t maxEncryptedSize,
      std::vector<std::weak_ptr<AesDecryptor>>* allDecryptors);

  ~AesDecryptor();
  void wipeOut();

  /// Size difference between plaintext and ciphertext, for this cipher.
  int ciphertextSizeDelta();

  /// Decrypts ciphertext with the key and aad. Key length is passed only for
  /// validation. If different from value in constructor, exception will be
  /// thrown.
  int decrypt(
      const uint8_t* ciphertext,
      int ciphertextLength,
      const uint8_t* key,
      int keyLength,
      const uint8_t* aad,
      int aadLength,
      uint8_t* plaintext,
      int plaintextLength);

 private:
  // PIMPL Idiom
  class AesDecryptorImpl;
  std::unique_ptr<AesDecryptorImpl> impl_;
};

std::string createModuleAad(
    const std::string& fileAad,
    int8_t moduleType,
    int16_t rowGroupOrdinal,
    int16_t columnOrdinal,
    int32_t pageOrdinal);

std::string createFooterAad(const std::string& aadPrefixBytes);

// Update last two bytes of page (or page header) module AAD
void quickUpdatePageAad(int32_t newPageOrdinal, std::string* pageAad);

// Wraps OpenSSL RAND_bytes function
void RandBytes(unsigned char* buf, int num);

} // namespace bytedance::bolt::parquet::encryption
