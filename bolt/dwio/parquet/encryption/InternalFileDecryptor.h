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
#include <parquet/encryption/encryption.h>
#include "bolt/common/memory/MemoryPool.h"
#include "bolt/dwio/parquet/encryption/EncryptionType.h"

namespace bytedance::bolt::parquet {

namespace encryption {
class AesDecryptor;
}

class Decryptor {
 public:
  Decryptor(
      std::shared_ptr<encryption::AesDecryptor> decryptor,
      const std::string& key,
      const std::string& fileAad,
      const std::string& aad,
      memory::MemoryPool* pool);

  const std::string& fileAad() const {
    return fileAad_;
  }
  void UpdateAad(const std::string& aad) {
    aad_ = aad;
  }
  memory::MemoryPool* pool() {
    return pool_;
  }

  int CiphertextSizeDelta();
  int Decrypt(
      const uint8_t* ciphertext,
      int ciphertextLength,
      uint8_t* plaintext,
      int plaintextLength);

 private:
  std::shared_ptr<encryption::AesDecryptor> aesDecryptor_;
  std::string key_;
  std::string fileAad_;
  std::string aad_;
  memory::MemoryPool* pool_;
};

class InternalFileDecryptor {
 public:
  explicit InternalFileDecryptor(
      ::parquet::FileDecryptionProperties* properties,
      const std::string& fileAad,
      ParquetCipher::type algorithm,
      const std::string& footerKeyMetadata,
      memory::MemoryPool* pool,
      int32_t maxEncryptedSize = 0);

  std::string& fileAad() {
    return fileAad_;
  }

  std::string GetFooterKey();

  bytedance::bolt::parquet::ParquetCipher::type algorithm() {
    return algorithm_;
  }

  std::string& footerKeyMetadata() {
    return footerKeyMetadata_;
  }

  ::parquet::FileDecryptionProperties* properties() {
    return properties_;
  }

  void WipeOutDecryptionKeys();

  memory::MemoryPool* pool() {
    return pool_;
  }

  std::shared_ptr<Decryptor> GetFooterDecryptor();
  std::shared_ptr<Decryptor> GetFooterDecryptorForColumnMeta(
      const std::string& aad = "");
  std::shared_ptr<Decryptor> GetFooterDecryptorForColumnData(
      const std::string& aad = "");
  std::shared_ptr<Decryptor> GetColumnMetaDecryptor(
      const std::string& columnPath,
      const std::string& columnKeyMetadata,
      const std::string& aad = "");
  std::shared_ptr<Decryptor> GetColumnDataDecryptor(
      const std::string& columnPath,
      const std::string& columnKeyMetadata,
      const std::string& aad = "");

 private:
  ::parquet::FileDecryptionProperties* properties_;
  // Concatenation of aad_prefix (if exists) and aad_file_unique
  std::string fileAad_;
  std::map<std::string, std::shared_ptr<Decryptor>> columnDataMap_;
  std::map<std::string, std::shared_ptr<Decryptor>> columnMetadataMap_;

  std::shared_ptr<Decryptor> footerMetadataDecryptor_;
  std::shared_ptr<Decryptor> footerDataDecryptor_;
  bytedance::bolt::parquet::ParquetCipher::type algorithm_;
  std::string footerKeyMetadata_;
  // A weak reference to all decryptors that need to be wiped out when
  // decryption is finished
  std::vector<std::weak_ptr<encryption::AesDecryptor>> allDecryptors_;

  memory::MemoryPool* pool_;
  int32_t maxEncryptedSize_;

  std::shared_ptr<Decryptor> GetFooterDecryptor(
      const std::string& aad,
      bool metadata);
  std::shared_ptr<Decryptor> GetColumnDecryptor(
      const std::string& columnPath,
      const std::string& columnKeyMetadata,
      const std::string& aad,
      bool metadata = false);
};

} // namespace bytedance::bolt::parquet
