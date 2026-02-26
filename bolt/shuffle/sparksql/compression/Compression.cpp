/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

#include "bolt/shuffle/sparksql/compression/Compression.h"

#include "bolt/common/base/Exceptions.h"

#ifdef GLUTEN_ENABLE_QAT
#include "bolt/shuffle/sparksql/compression/qat/QatCodec.h"
#endif

#ifdef GLUTEN_ENABLE_IAA
#include "bolt/shuffle/sparksql/compression/qpl/qpl_codec.h"
#endif
namespace bytedance::bolt::shuffle::sparksql {

CodecBackend getCodecBackend(const std::string& codecBackend) {
  if (codecBackend == "none") {
    return CodecBackend::NONE;
  } else if (codecBackend == "qat") {
    return CodecBackend::QAT;
  } else if (codecBackend == "iaa") {
    return CodecBackend::IAA;
  } else {
    BOLT_FAIL("Not support this codec backend " + codecBackend);
  }
}

std::string getCodecBackendName(CodecBackend backend) {
  switch (backend) {
    case CodecBackend::NONE:
      return "none";
    case CodecBackend::QAT:
      return "qat";
    case CodecBackend::IAA:
      return "iaa";
    default:
      BOLT_FAIL("Not support this codec backend");
  }
}

CompressionMode getCompressionMode(const std::string& compressionMode) {
  if (compressionMode == "buffer") {
    return CompressionMode::BUFFER;
  } else if (compressionMode == "rowvector") {
    return CompressionMode::ROWVECTOR;
  } else {
    BOLT_FAIL("Not support this compression mode " + compressionMode);
  }
}

std::unique_ptr<arrow::util::Codec> createArrowIpcCodec(
    arrow::Compression::type compressedType,
    CodecBackend codecBackend,
    int32_t compressionLevel) {
  std::unique_ptr<arrow::util::Codec> codec;
  switch (compressedType) {
    case arrow::Compression::LZ4_FRAME: {
      auto result = arrow::util::Codec::Create(compressedType);
      BOLT_CHECK(
          result.ok(),
          "Failed to create LZ4 codec: {}",
          result.status().ToString());
      codec = result.MoveValueUnsafe();
    } break;
    case arrow::Compression::ZSTD: {
      if (codecBackend == CodecBackend::NONE) {
        auto result =
            arrow::util::Codec::Create(compressedType, compressionLevel);
        BOLT_CHECK(
            result.ok(),
            "Failed to create ZSTD codec: {}",
            result.status().ToString());
        codec = result.MoveValueUnsafe();
      } else if (codecBackend == CodecBackend::QAT) {
#if defined(GLUTEN_ENABLE_QAT)
        codec = qat::makeDefaultQatZstdCodec();
#else
        BOLT_FAIL("Backend QAT but not compile with option GLUTEN_ENABLE_QAT");
#endif
      } else {
        BOLT_FAIL("Backend IAA not support zstd compression");
      }
    } break;
    case arrow::Compression::GZIP: {
      if (codecBackend == CodecBackend::NONE) {
        return nullptr;
      } else if (codecBackend == CodecBackend::QAT) {
#if defined(GLUTEN_ENABLE_QAT)
        codec = qat::makeDefaultQatGZipCodec();
#else
        BOLT_FAIL("Backend QAT but not compile with option GLUTEN_ENABLE_QAT");
#endif
      } else {
#if defined(GLUTEN_ENABLE_IAA)
        codec = qpl::MakeDefaultQplGZipCodec();
#else
        BOLT_FAIL("Backend IAA but not compile with option GLUTEN_ENABLE_IAA");
#endif
      }
    } break;
    default:
      return nullptr;
  }
  return codec;
}
} // namespace bytedance::bolt::shuffle::sparksql
