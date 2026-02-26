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

#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "bolt/dwio/parquet/encryption/EncryptionInternal.h"
namespace bytedance::bolt::parquet::encryption {
constexpr int kGcmMode = 0;
constexpr int kCtrMode = 1;
constexpr int kCtrIvLength = 16;
constexpr int kBufferSizeLength = 4;

#define DECRYPT_INIT(CTX, ALG)                                        \
  if (1 != EVP_DecryptInit_ex(CTX, ALG, nullptr, nullptr, nullptr)) { \
    BOLT_FAIL("Couldn't init ALG decryption");                        \
  }

static void checkPageOrdinal(int32_t pageOrdinal) {
  if (pageOrdinal > std::numeric_limits<int16_t>::max()) {
    BOLT_FAIL(
        "Encrypted Parquet files can't have more than {}  pages per chunk: got {}",
        std::numeric_limits<int16_t>::max(),
        pageOrdinal);
  }
}

static std::string shortToBytesLe(int16_t input) {
  int8_t output[2];
  memset(output, 0, 2);
  output[1] = static_cast<int8_t>(0xff & (input >> 8));
  output[0] = static_cast<int8_t>(0xff & (input));

  return std::string(reinterpret_cast<char const*>(output), 2);
}

std::string createFooterAad(const std::string& aadPrefixBytes) {
  return createModuleAad(
      aadPrefixBytes,
      kFooter,
      static_cast<int16_t>(-1),
      static_cast<int16_t>(-1),
      static_cast<int16_t>(-1));
}

// Update last two bytes with new page ordinal (instead of creating new page AAD
// from scratch)
void quickUpdatePageAad(int32_t newPageOrdinal, std::string* pageAad) {
  checkPageOrdinal(newPageOrdinal);
  const std::string pageOrdinalBytes =
      shortToBytesLe(static_cast<int16_t>(newPageOrdinal));
  std::memcpy(
      pageAad->data() + pageAad->length() - 2, pageOrdinalBytes.data(), 2);
}

class AesDecryptor::AesDecryptorImpl {
 public:
  explicit AesDecryptorImpl(
      parquet::ParquetCipher::type algorithm,
      int keyLength,
      bool metadata,
      int32_t maxEncryptedSize,
      bool containsLength);

  ~AesDecryptorImpl() {
    if (nullptr != ctx_) {
      EVP_CIPHER_CTX_free(ctx_);
      ctx_ = nullptr;
    }
  }

  int Decrypt(
      const uint8_t* ciphertext,
      int ciphertextLength,
      const uint8_t* key,
      int keyLength,
      const uint8_t* aad,
      int aadLength,
      uint8_t* plaintext,
      int plaintextLength);

  void WipeOut() {
    if (nullptr != ctx_) {
      EVP_CIPHER_CTX_free(ctx_);
      ctx_ = nullptr;
    }
  }

  int CiphertextSizeDelta() const {
    return ciphertextSizeDelta_;
  }

 private:
  EVP_CIPHER_CTX* ctx_;
  int aes_mode_;
  int keyLength_;
  int ciphertextSizeDelta_;
  int lengthBufferLength_;
  int32_t maxEncryptedSize_;
  int GcmDecrypt(
      const uint8_t* ciphertext,
      int ciphertextLength,
      const uint8_t* key,
      int keyLength,
      const uint8_t* aad,
      int aadLength,
      uint8_t* plaintext,
      int plaintextLength);

  int CtrDecrypt(
      const uint8_t* ciphertext,
      int ciphertextLength,
      const uint8_t* key,
      int keyLength,
      uint8_t* plaintext,
      int plaintextLength);
};

int AesDecryptor::decrypt(
    const uint8_t* ciphertext,
    int ciphertextLength,
    const uint8_t* key,
    int keyLength,
    const uint8_t* aad,
    int aadLength,
    uint8_t* plaintext,
    int plaintextLength) {
  return impl_->Decrypt(
      ciphertext,
      ciphertextLength,
      key,
      keyLength,
      aad,
      aadLength,
      plaintext,
      plaintextLength);
}

void AesDecryptor::wipeOut() {
  impl_->WipeOut();
}

AesDecryptor::~AesDecryptor() {}

AesDecryptor::AesDecryptorImpl::AesDecryptorImpl(
    parquet::ParquetCipher::type algorithm,
    int keyLength,
    bool metadata,
    int32_t maxEncryptedSize,
    bool containsLength) {
  ctx_ = nullptr;
  lengthBufferLength_ = containsLength ? kBufferSizeLength : 0;
  ciphertextSizeDelta_ = lengthBufferLength_ + kNonceLength;
  if (metadata || (parquet::ParquetCipher::AES_GCM_V1 == algorithm)) {
    aes_mode_ = kGcmMode;
    ciphertextSizeDelta_ += kGcmTagLength;
  } else {
    aes_mode_ = kCtrMode;
  }

  if (16 != keyLength && 24 != keyLength && 32 != keyLength) {
    BOLT_FAIL("Wrong key length: {}", keyLength);
  }

  keyLength_ = keyLength;
  maxEncryptedSize_ = maxEncryptedSize;

  ctx_ = EVP_CIPHER_CTX_new();
  if (nullptr == ctx_) {
    BOLT_FAIL("Couldn't init cipher context");
  }

  if (kGcmMode == aes_mode_) {
    if (16 == keyLength) {
      DECRYPT_INIT(ctx_, EVP_aes_128_gcm());
    } else if (24 == keyLength) {
      DECRYPT_INIT(ctx_, EVP_aes_192_gcm());
    } else if (32 == keyLength) {
      DECRYPT_INIT(ctx_, EVP_aes_256_gcm());
    }
  } else {
    if (16 == keyLength) {
      DECRYPT_INIT(ctx_, EVP_aes_128_ctr());
    } else if (24 == keyLength) {
      DECRYPT_INIT(ctx_, EVP_aes_192_ctr());
    } else if (32 == keyLength) {
      DECRYPT_INIT(ctx_, EVP_aes_256_ctr());
    }
  }
}

AesDecryptor::AesDecryptor(
    parquet::ParquetCipher::type algorithm,
    int keyLength,
    bool metadata,
    int32_t maxEncryptedSize,
    bool containsLength)
    : impl_{std::unique_ptr<AesDecryptorImpl>(new AesDecryptorImpl(
          algorithm,
          keyLength,
          metadata,
          maxEncryptedSize,
          containsLength))} {}

std::shared_ptr<AesDecryptor> AesDecryptor::make(
    parquet::ParquetCipher::type algorithm,
    int keyLength,
    bool metadata,
    int32_t maxEncryptedSize,
    std::vector<std::weak_ptr<AesDecryptor>>* allDecryptors) {
  if (parquet::ParquetCipher::AES_GCM_V1 != algorithm &&
      parquet::ParquetCipher::AES_GCM_CTR_V1 != algorithm) {
    BOLT_FAIL("Crypto algorithm {} is not supported", algorithm);
  }

  auto decryptor = std::make_shared<AesDecryptor>(
      algorithm, keyLength, metadata, maxEncryptedSize);
  if (allDecryptors != nullptr) {
    allDecryptors->push_back(decryptor);
  }
  return decryptor;
}

int AesDecryptor::ciphertextSizeDelta() {
  return impl_->CiphertextSizeDelta();
}

int AesDecryptor::AesDecryptorImpl::GcmDecrypt(
    const uint8_t* ciphertext,
    int ciphertextLength,
    const uint8_t* key,
    int keyLength,
    const uint8_t* aad,
    int aadLength,
    uint8_t* plaintext,
    int plaintextLength) {
  int len;
  int writePlaintextLength;

  uint8_t tag[kGcmTagLength];
  memset(tag, 0, kGcmTagLength);
  uint8_t nonce[kNonceLength];
  memset(nonce, 0, kNonceLength);

  if (lengthBufferLength_ > 0) {
    // Extract ciphertext length
    int writtenCiphertextLength = ((ciphertext[3] & 0xff) << 24) |
        ((ciphertext[2] & 0xff) << 16) | ((ciphertext[1] & 0xff) << 8) |
        ((ciphertext[0] & 0xff));

    if (ciphertextLength > 0 &&
        ciphertextLength != (writtenCiphertextLength + lengthBufferLength_)) {
      BOLT_FAIL("Wrong ciphertext length");
    }
    ciphertextLength = writtenCiphertextLength + lengthBufferLength_;
  } else {
    if (ciphertextLength == 0) {
      BOLT_FAIL("Zero ciphertext length");
    }
  }

  auto decryptLength =
      ciphertextLength - lengthBufferLength_ - kNonceLength - kGcmTagLength;

  BOLT_CHECK(decryptLength <= plaintextLength, "plain text buffer too short");

  // Extracting IV and tag
  std::copy(
      ciphertext + lengthBufferLength_,
      ciphertext + lengthBufferLength_ + kNonceLength,
      nonce);
  std::copy(
      ciphertext + ciphertextLength - kGcmTagLength,
      ciphertext + ciphertextLength,
      tag);

  // Setting key and IV
  if (1 != EVP_DecryptInit_ex(ctx_, nullptr, nullptr, key, nonce)) {
    BOLT_FAIL("Couldn't set key and IV");
  }

  // Setting additional authenticated data
  if ((nullptr != aad) &&
      (1 != EVP_DecryptUpdate(ctx_, nullptr, &len, aad, aadLength))) {
    BOLT_FAIL("Couldn't set AAD");
  }

  // Decryption
  if (!EVP_DecryptUpdate(
          ctx_,
          plaintext,
          &len,
          ciphertext + lengthBufferLength_ + kNonceLength,
          decryptLength)) {
    BOLT_FAIL("Failed decryption update");
  }

  writePlaintextLength = len;

  // Checking the tag (authentication)
  if (!EVP_CIPHER_CTX_ctrl(ctx_, EVP_CTRL_GCM_SET_TAG, kGcmTagLength, tag)) {
    BOLT_FAIL("Failed authentication");
  }

  // Finalization
  if (1 != EVP_DecryptFinal_ex(ctx_, plaintext + len, &len)) {
    BOLT_FAIL("Failed decryption finalization");
  }

  writePlaintextLength += len;
  return writePlaintextLength;
}

int AesDecryptor::AesDecryptorImpl::CtrDecrypt(
    const uint8_t* ciphertext,
    int ciphertextLength,
    const uint8_t* key,
    int keyLength,
    uint8_t* plaintext,
    int plaintextLength) {
  int len;
  int writePlaintextLength;

  uint8_t iv[kCtrIvLength];
  memset(iv, 0, kCtrIvLength);

  if (lengthBufferLength_ > 0) {
    // Extract ciphertext length
    int writtenCiphertextLength = ((ciphertext[3] & 0xff) << 24) |
        ((ciphertext[2] & 0xff) << 16) | ((ciphertext[1] & 0xff) << 8) |
        ((ciphertext[0] & 0xff));

    if (ciphertextLength > 0 &&
        ciphertextLength != (writtenCiphertextLength + lengthBufferLength_)) {
      BOLT_FAIL("Wrong ciphertext length");
    }
    ciphertextLength = writtenCiphertextLength;
  } else {
    if (ciphertextLength == 0) {
      BOLT_FAIL("Zero ciphertext length");
    }
  }

  auto decryptLength = ciphertextLength - kNonceLength;
  BOLT_CHECK(decryptLength <= plaintextLength, "plain text buffer too short");
  bool left = false;
  if (maxEncryptedSize_ != 0 && decryptLength >= maxEncryptedSize_) {
    left = true;
    decryptLength = maxEncryptedSize_;
  }

  // Extracting nonce
  std::copy(
      ciphertext + lengthBufferLength_,
      ciphertext + lengthBufferLength_ + kNonceLength,
      iv);
  // Parquet CTR IVs are comprised of a 12-byte nonce and a 4-byte initial
  // counter field.
  // The first 31 bits of the initial counter field are set to 0, the last bit
  // is set to 1.
  iv[kCtrIvLength - 1] = 1;

  // Setting key and IV
  if (1 != EVP_DecryptInit_ex(ctx_, nullptr, nullptr, key, iv)) {
    BOLT_FAIL("Couldn't set key and IV");
  }

  // Decryption
  if (!EVP_DecryptUpdate(
          ctx_,
          plaintext,
          &len,
          ciphertext + lengthBufferLength_ + kNonceLength,
          decryptLength)) {
    BOLT_FAIL("Failed decryption update");
  }

  writePlaintextLength = len;

  // Finalization
  if (1 != EVP_DecryptFinal_ex(ctx_, plaintext + len, &len)) {
    BOLT_FAIL("Failed decryption finalization");
  }

  writePlaintextLength += len;
  if (left) {
    std::copy(
        ciphertext + lengthBufferLength_ + kNonceLength + decryptLength,
        ciphertext + ciphertextLength + lengthBufferLength_,
        plaintext + writePlaintextLength);
    writePlaintextLength += ciphertextLength - kNonceLength - decryptLength;
  }
  return writePlaintextLength;
}

int AesDecryptor::AesDecryptorImpl::Decrypt(
    const uint8_t* ciphertext,
    int ciphertextLength,
    const uint8_t* key,
    int keyLength,
    const uint8_t* aad,
    int aadLength,
    uint8_t* plaintext,
    int plaintextLength) {
  if (keyLength_ != keyLength) {
    BOLT_FAIL("Wrong key length {}. Should be {}", keyLength, keyLength_);
  }

  if (kGcmMode == aes_mode_) {
    return GcmDecrypt(
        ciphertext,
        ciphertextLength,
        key,
        keyLength,
        aad,
        aadLength,
        plaintext,
        plaintextLength);
  }

  return CtrDecrypt(
      ciphertext, ciphertextLength, key, keyLength, plaintext, plaintextLength);
}

std::string createModuleAad(
    const std::string& fileAad,
    int8_t moduleType,
    int16_t rowGroupOrdinal,
    int16_t columnOrdinal,
    int32_t pageOrdinal) {
  checkPageOrdinal(pageOrdinal);
  const int16_t pageOrdinalShort = static_cast<int16_t>(pageOrdinal);
  int8_t typeOrdinalBytes[1];
  typeOrdinalBytes[0] = moduleType;
  std::string typeOrdinalBytes_str(
      reinterpret_cast<char const*>(typeOrdinalBytes), 1);
  if (kFooter == moduleType) {
    std::string result = fileAad + typeOrdinalBytes_str;
    return result;
  }
  std::string rowGroupOrdinalBytes = shortToBytesLe(rowGroupOrdinal);
  std::string columnOrdinalBytes = shortToBytesLe(columnOrdinal);
  if (kDataPage != moduleType && kDataPageHeader != moduleType) {
    std::ostringstream out;
    out << fileAad << typeOrdinalBytes_str << rowGroupOrdinalBytes
        << columnOrdinalBytes;
    return out.str();
  }
  std::string pageOrdinalBytes = shortToBytesLe(pageOrdinalShort);
  std::ostringstream out;
  out << fileAad << typeOrdinalBytes_str << rowGroupOrdinalBytes
      << columnOrdinalBytes << pageOrdinalBytes;
  return out.str();
}

} // namespace bytedance::bolt::parquet::encryption
