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

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "bolt/common/base/Exceptions.h"
#include "bolt/common/memory/Memory.h"
#include "bolt/dwio/parquet/arrow/EncryptionInternal.h"
#include "bolt/dwio/parquet/encryption/EncryptionInternal.h"

using namespace bytedance::bolt;

using bytedance::bolt::parquet::arrow::ParquetCipher;

namespace parquet_arrow_encryption =
    bytedance::bolt::parquet::arrow::encryption;
namespace parquet_encryption = bytedance::bolt::parquet::encryption;

namespace {

const uint8_t* bytesOrNull(const std::string& s) {
  return s.empty() ? nullptr : reinterpret_cast<const uint8_t*>(s.data());
}

std::vector<uint8_t> toBytes(const std::string& s) {
  return std::vector<uint8_t>(s.begin(), s.end());
}

std::vector<uint8_t> encryptWithArrowAesEncryptor(
    ParquetCipher::type algorithm,
    bool metadata,
    bool writeLength,
    const std::vector<uint8_t>& plaintext,
    const std::string& key,
    const std::string& moduleAad) {
  parquet_arrow_encryption::AesEncryptor encryptor(
      algorithm, static_cast<int>(key.size()), metadata, writeLength);

  std::vector<uint8_t> ciphertext(
      plaintext.size() + encryptor.CiphertextSizeDelta());
  int writtenLen = encryptor.Encrypt(
      plaintext.data(),
      static_cast<int>(plaintext.size()),
      bytesOrNull(key),
      static_cast<int>(key.size()),
      bytesOrNull(moduleAad),
      static_cast<int>(moduleAad.size()),
      ciphertext.data());

  ciphertext.resize(writtenLen);
  return ciphertext;
}

} // namespace

TEST(EncryptionAesDecryptorTest, GcmRoundTripUsesModuleAad) {
  const std::string key(16, 'k');
  const std::string fileAad(8, 'f');
  const std::string moduleAad = parquet_encryption::createFooterAad(fileAad);

  const auto plaintext = toBytes(std::string("hello\0world", 11));
  const auto ciphertext = encryptWithArrowAesEncryptor(
      ParquetCipher::AES_GCM_V1, true, true, plaintext, key, moduleAad);

  parquet_encryption::AesDecryptor decryptor(
      ::parquet::ParquetCipher::AES_GCM_V1,
      static_cast<int>(key.size()),
      true,
      0,
      true);

  std::vector<uint8_t> decrypted(plaintext.size());
  const int decryptedLen = decryptor.decrypt(
      ciphertext.data(),
      static_cast<int>(ciphertext.size()),
      bytesOrNull(key),
      static_cast<int>(key.size()),
      bytesOrNull(moduleAad),
      static_cast<int>(moduleAad.size()),
      decrypted.data(),
      static_cast<int>(decrypted.size()));

  ASSERT_EQ(decryptedLen, static_cast<int>(plaintext.size()));
  EXPECT_EQ(decrypted, plaintext);

  const std::string wrongAad = moduleAad + "x";
  EXPECT_THROW(
      decryptor.decrypt(
          ciphertext.data(),
          static_cast<int>(ciphertext.size()),
          bytesOrNull(key),
          static_cast<int>(key.size()),
          bytesOrNull(wrongAad),
          static_cast<int>(wrongAad.size()),
          decrypted.data(),
          static_cast<int>(decrypted.size())),
      BoltRuntimeError);

  auto tampered = ciphertext;
  tampered.back() ^= 0x01;
  EXPECT_THROW(
      decryptor.decrypt(
          tampered.data(),
          static_cast<int>(tampered.size()),
          bytesOrNull(key),
          static_cast<int>(key.size()),
          bytesOrNull(moduleAad),
          static_cast<int>(moduleAad.size()),
          decrypted.data(),
          static_cast<int>(decrypted.size())),
      BoltRuntimeError);
}

TEST(EncryptionAesDecryptorTest, CtrRoundTrip) {
  const std::string key(16, 'k');
  const std::string fileAad(8, 'f');
  const std::string moduleAad = parquet_encryption::createFooterAad(fileAad);

  const auto plaintext = toBytes("payload");
  const auto ciphertext = encryptWithArrowAesEncryptor(
      ParquetCipher::AES_GCM_CTR_V1, false, true, plaintext, key, moduleAad);

  parquet_encryption::AesDecryptor decryptor(
      ::parquet::ParquetCipher::AES_GCM_CTR_V1,
      static_cast<int>(key.size()),
      false,
      0,
      true);

  std::vector<uint8_t> decrypted(plaintext.size());
  const int decryptedLen = decryptor.decrypt(
      ciphertext.data(),
      static_cast<int>(ciphertext.size()),
      bytesOrNull(key),
      static_cast<int>(key.size()),
      bytesOrNull(moduleAad),
      static_cast<int>(moduleAad.size()),
      decrypted.data(),
      static_cast<int>(decrypted.size()));

  ASSERT_EQ(decryptedLen, static_cast<int>(plaintext.size()));
  EXPECT_EQ(decrypted, plaintext);
}

TEST(EncryptionAesDecryptorTest, InvalidKeyLengthThrows) {
  const std::string key(15, 'k');

  EXPECT_THROW(
      parquet_encryption::AesDecryptor(
          ::parquet::ParquetCipher::AES_GCM_V1,
          static_cast<int>(key.size()),
          true,
          0),
      BoltRuntimeError);
}
