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

#include "bolt/dwio/parquet/encryption/InternalFileDecryptor.h"
#include "bolt/dwio/parquet/encryption/EncryptionInternal.h"

namespace bytedance::bolt::parquet {

// Thin wrapper around AesDecryptor that owns key / AAD context for one stream.
Decryptor::Decryptor(
    std::shared_ptr<encryption::AesDecryptor> aesDecryptor,
    const std::string& key,
    const std::string& fileAad,
    const std::string& aad,
    memory::MemoryPool* pool)
    : aesDecryptor_(aesDecryptor),
      key_(key),
      fileAad_(fileAad),
      aad_(aad),
      pool_(pool) {}

int Decryptor::CiphertextSizeDelta() {
  return aesDecryptor_->ciphertextSizeDelta();
}

int Decryptor::Decrypt(
    const uint8_t* ciphertext,
    int ciphertextLength,
    uint8_t* plaintext,
    int plaintextLength) {
  return aesDecryptor_->decrypt(
      ciphertext,
      ciphertextLength,
      reinterpret_cast<const uint8_t*>(key_.c_str()),
      static_cast<int>(key_.size()),
      reinterpret_cast<const uint8_t*>(aad_.c_str()),
      static_cast<int>(aad_.size()),
      plaintext,
      plaintextLength);
}

// Manages all decryptors needed to read a single encrypted Parquet file.
InternalFileDecryptor::InternalFileDecryptor(
    ::parquet::FileDecryptionProperties* properties,
    const std::string& fileAad,
    ParquetCipher::type algorithm,
    const std::string& footerKeyMetadata,
    memory::MemoryPool* pool,
    int32_t maxEncryptedSize)
    : properties_(properties),
      fileAad_(fileAad),
      algorithm_(algorithm),
      footerKeyMetadata_(footerKeyMetadata),
      pool_(pool),
      maxEncryptedSize_(maxEncryptedSize) {
  if (properties_->is_utilized()) {
    BOLT_FAIL(
        "Reusing decryption properties with explicit keys for another file");
  }
  properties_->set_utilized();
}

// Returns decryptor for footer (metadata) using the file-level AAD.
std::shared_ptr<Decryptor> InternalFileDecryptor::GetFooterDecryptor() {
  std::string aad = encryption::createFooterAad(fileAad_);
  return GetFooterDecryptor(aad, true);
}

// Lazily create and cache decryptors for footer metadata / data.
std::shared_ptr<Decryptor> InternalFileDecryptor::GetFooterDecryptor(
    const std::string& aad,
    bool metadata) {
  if (metadata) {
    if (footerMetadataDecryptor_ != nullptr)
      return footerMetadataDecryptor_;
  } else {
    if (footerDataDecryptor_ != nullptr)
      return footerDataDecryptor_;
  }

  std::string footerKey = properties_->footer_key();
  if (footerKey.empty()) {
    if (footerKeyMetadata_.empty())
      BOLT_FAIL("No footer key, and no key metadata.");
    if (properties_->key_retriever() == nullptr)
      BOLT_FAIL("No footer key, and no key retriever.");
    try {
      footerKey = properties_->key_retriever()->GetKey(footerKeyMetadata_);
    } catch (::parquet::KeyAccessDeniedException& e) {
      BOLT_FAIL("Footer key: access denied {}", e.what());
    }
  }
  if (footerKey.empty()) {
    BOLT_FAIL("Invalid footer encryption key. Could not parse footer metadata");
  }

  // Create both data and metadata decryptors to avoid redundant retrieval of
  // the same key from key_retriever.
  int keyLength = static_cast<int>(footerKey.size());
  auto aesMetadataDecryptor = encryption::AesDecryptor::make(
      algorithm_,
      keyLength,
      /*metadata=*/true,
      maxEncryptedSize_,
      &allDecryptors_);
  auto aesDataDecryptor = encryption::AesDecryptor::make(
      algorithm_,
      keyLength,
      /*metadata=*/false,
      maxEncryptedSize_,
      &allDecryptors_);

  footerMetadataDecryptor_ = std::make_shared<Decryptor>(
      aesMetadataDecryptor, footerKey, fileAad_, aad, pool_);
  footerDataDecryptor_ = std::make_shared<Decryptor>(
      aesDataDecryptor, footerKey, fileAad_, aad, pool_);

  if (metadata)
    return footerMetadataDecryptor_;
  return footerDataDecryptor_;
}

std::shared_ptr<Decryptor>
InternalFileDecryptor::GetFooterDecryptorForColumnData(const std::string& aad) {
  return GetFooterDecryptor(aad, false);
}

std::shared_ptr<Decryptor>
InternalFileDecryptor::GetFooterDecryptorForColumnMeta(const std::string& aad) {
  return GetFooterDecryptor(aad, true);
}

std::shared_ptr<Decryptor> InternalFileDecryptor::GetColumnMetaDecryptor(
    const std::string& columnPath,
    const std::string& columnKeyMetadata,
    const std::string& aad) {
  return GetColumnDecryptor(columnPath, columnKeyMetadata, aad, true);
}

std::shared_ptr<Decryptor> InternalFileDecryptor::GetColumnDataDecryptor(
    const std::string& columnPath,
    const std::string& columnKeyMetadata,
    const std::string& aad) {
  return GetColumnDecryptor(columnPath, columnKeyMetadata, aad, false);
}

// Returns decryptor for a particular column, creating it on first use.
std::shared_ptr<Decryptor> InternalFileDecryptor::GetColumnDecryptor(
    const std::string& columnPath,
    const std::string& columnKeyMetadata,
    const std::string& aad,
    bool metadata) {
  std::string columnKey;
  // Reuse existing decryptor if this column has already been initialized.
  if (metadata) {
    if (columnMetadataMap_.find(columnPath) != columnMetadataMap_.end()) {
      auto res(columnMetadataMap_.at(columnPath));
      res->UpdateAad(aad);
      return res;
    }
  } else {
    if (columnDataMap_.find(columnPath) != columnDataMap_.end()) {
      auto res(columnDataMap_.at(columnPath));
      res->UpdateAad(aad);
      return res;
    }
  }

  columnKey = properties_->column_key(columnPath);
  // No explicit column key given via API. Retrieve via key metadata.
  if (columnKey.empty() && !columnKeyMetadata.empty() &&
      properties_->key_retriever() != nullptr) {
    try {
      columnKey = properties_->key_retriever()->GetKey(columnKeyMetadata);
    } catch (::parquet::KeyAccessDeniedException& e) {
      BOLT_FAIL("HiddenColumnException, path = {}, {}", columnPath, e.what());
    }
  }
  if (columnKey.empty()) {
    BOLT_FAIL("HiddenColumnException, path = {}", columnPath);
  }

  // Create both data and metadata decryptors to avoid redundant retrieval of
  // key using the key_retriever.
  int keyLength = static_cast<int>(columnKey.size());
  auto aesMetadataDecryptor = encryption::AesDecryptor::make(
      algorithm_,
      keyLength,
      /*metadata=*/true,
      maxEncryptedSize_,
      &allDecryptors_);
  auto aesDataDecryptor = encryption::AesDecryptor::make(
      algorithm_,
      keyLength,
      /*metadata=*/false,
      maxEncryptedSize_,
      &allDecryptors_);

  columnMetadataMap_[columnPath] = std::make_shared<Decryptor>(
      aesMetadataDecryptor, columnKey, fileAad_, aad, pool_);
  columnDataMap_[columnPath] = std::make_shared<Decryptor>(
      aesDataDecryptor, columnKey, fileAad_, aad, pool_);

  if (metadata) {
    return columnMetadataMap_[columnPath];
  }
  return columnDataMap_[columnPath];
}

} // namespace bytedance::bolt::parquet
