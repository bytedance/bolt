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

// Partially inspired and adapted from Apache Arrow.

#pragma once

#include <folly/ScopeGuard.h>

#include <thrift/protocol/TCompactProtocol.h> //@manual

#include "bolt/dwio/parquet/encryption/EncryptionInternal.h"
#include "bolt/dwio/parquet/encryption/InternalFileDecryptor.h"
#include "bolt/dwio/parquet/thrift/ThriftTransport.h"

namespace bytedance::bolt::parquet::thrift {

// Helper for reading Thrift messages, optionally decrypting them first.
class ThriftDeserializer {
 public:
  ThriftDeserializer() = default;

  template <class T>
  bool DeserializeMessage(
      const uint8_t* buf,
      uint32_t* len,
      T* deserializedMsg,
      const std::shared_ptr<Decryptor>& decryptor) {
    uint32_t clen = *len;
    int64_t allocateSize = clen - decryptor->CiphertextSizeDelta();
    uint8_t* decryptedBuffer =
        reinterpret_cast<uint8_t*>(decryptor->pool()->allocate(allocateSize));
    auto decryptBufferGuard = folly::makeGuard(
        [&]() { decryptor->pool()->free(decryptedBuffer, allocateSize); });
    const uint8_t* cipherBuf = buf;
    uint32_t decryptedBufferLen =
        decryptor->Decrypt(cipherBuf, 0, decryptedBuffer, allocateSize);
    if (decryptedBufferLen <= 0) {
      return false;
    }
    *len = decryptedBufferLen + decryptor->CiphertextSizeDelta();
    DeserializeUnencryptedMessage(
        decryptedBuffer, &decryptedBufferLen, deserializedMsg);
    return true;
  }

  template <class T>
  void DeserializeUnencryptedMessage(
      const uint8_t* buf,
      uint32_t* len,
      T* deserializedMsg) {
    auto thriftTransport = std::make_shared<ThriftBufferedTransport>(buf, *len);
    auto thriftProtocol = std::make_unique<
        apache::thrift::protocol::TCompactProtocolT<ThriftTransport>>(
        thriftTransport);

    *len = deserializedMsg->read(thriftProtocol.get());
  }
};

} // namespace bytedance::bolt::parquet::thrift
