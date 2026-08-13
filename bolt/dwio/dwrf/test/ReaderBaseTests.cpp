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

#include <gtest/gtest.h>
#include <cstring>
#include "bolt/dwio/common/InputStream.h"
#include "bolt/dwio/common/encryption/TestProvider.h"
#include "bolt/dwio/common/exception/Exception.h"
#include "bolt/dwio/dwrf/reader/StripeReaderBase.h"
#include "bolt/dwio/dwrf/test/OrcTest.h"
#include "bolt/dwio/dwrf/utils/ProtoUtils.h"
#include "bolt/type/fbhive/HiveTypeParser.h"

using namespace ::testing;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::dwio::common::encryption;
using namespace bytedance::bolt::dwio::common::encryption::test;
using namespace bytedance::bolt::dwrf;
using namespace bytedance::bolt::dwrf::encryption;
using namespace bytedance::bolt::memory;
using namespace bytedance::bolt::type::fbhive;

namespace {

class NamedBadProtoStream : public SeekableInputStream {
 public:
  explicit NamedBadProtoStream(std::string name) : name_{std::move(name)} {}

  bool Next(const void** buffer, int32_t* size) override {
    if (returned_) {
      return false;
    }
    returned_ = true;
    *buffer = badProto_;
    *size = sizeof(badProto_);
    return true;
  }

  void BackUp(int32_t /*count*/) override {}

  int64_t ByteCount() const override {
    return returned_ ? sizeof(badProto_) : 0;
  }

  void seekToPosition(PositionProvider& /*position*/) override {}

  std::string getName() const override {
    return name_;
  }

  size_t positionSize() override {
    return 1;
  }

  bool SkipInt64(int64_t /*count*/) override {
    return false;
  }

 private:
  const std::string name_;
  bool returned_{false};
  const char badProto_[1]{static_cast<char>(0xff)};
};

} // namespace

void addStats(
    ProtoWriter& writer,
    const Encrypter& encrypter,
    proto::EncryptionGroup& group,
    const std::vector<uint32_t>& nodes) {
  proto::FileStatistics stats;
  for (auto& node : nodes) {
    auto stat = stats.add_statistics();
    stat->set_numberofvalues(node * 100);
  }
  writer.writeProto(*group.add_statistics(), stats, encrypter);
}

class EncryptedStatsTest : public Test {
 protected:
  static void SetUpTestCase() {
    MemoryManager::testingSetInstance(MemoryManager::Options{});
  }

  void SetUp() override {
    pool_ = memoryManager()->addRootPool("EncryptedStatsTest");
    sinkPool_ = pool_->addLeafChild("sink");
    ProtoWriter writer{pool_, *sinkPool_};
    auto& context = const_cast<const ProtoWriter&>(writer).getContext();

    // fake post script
    proto::PostScript ps;
    ps.set_compression(
        static_cast<proto::CompressionKind>(context.compression()));
    if (context.compression() !=
        bytedance::bolt::common::CompressionKind::CompressionKind_NONE) {
      ps.set_compressionblocksize(context.compressionBlockSize());
    }

    // fake footer
    TestEncrypter encrypter;
    HiveTypeParser parser;
    auto type = parser.parse("struct<a:int,b:struct<a:int,b:int>,c:int,d:int>");
    auto footer =
        google::protobuf::Arena::CreateMessage<proto::Footer>(&arena_);
    // add empty stats to the file
    for (size_t i = 0; i < 7; ++i) {
      footer->add_statistics()->set_numberofvalues(i);
    }
    ProtoUtils::writeType(*type, *footer);
    auto enc = footer->mutable_encryption();
    enc->set_keyprovider(proto::Encryption_KeyProvider_UNKNOWN);
    auto group = enc->add_encryptiongroups();
    group->add_nodes(1);
    group->set_keymetadata("footer1");
    encrypter.setKey("footer1");
    addStats(writer, encrypter, *group, {1});

    auto group2 = enc->add_encryptiongroups();
    group2->set_keymetadata("footer2");

    group2->add_nodes(6);
    group2->add_nodes(2);
    encrypter.setKey("footer2");
    addStats(writer, encrypter, *group2, {6});
    addStats(writer, encrypter, *group2, {2, 3, 4});

    TestDecrypterFactory factory;
    auto handler = DecryptionHandler::create(*footer, &factory);
    handler_ = handler.get();

    auto readFile =
        std::make_shared<bytedance::bolt::InMemoryReadFile>(std::string());
    readerPool_ = pool_->addLeafChild("reader");
    reader_ = std::make_unique<ReaderBase>(
        *readerPool_,
        std::make_unique<BufferedInput>(readFile, *readerPool_),
        std::make_unique<PostScript>(std::move(ps)),
        footer,
        nullptr,
        std::move(handler));
  }

  void clearKey(uint32_t groupIdx) {
    auto& decrypter =
        const_cast<TestEncryption&>(dynamic_cast<const TestEncryption&>(
            handler_->getEncryptionProviderByIndex(groupIdx)));
    decrypter.setKey("");
  }

  google::protobuf::Arena arena_;
  std::unique_ptr<ReaderBase> reader_;
  DecryptionHandler* handler_;
  std::shared_ptr<MemoryPool> pool_;
  std::shared_ptr<MemoryPool> sinkPool_;
  std::shared_ptr<MemoryPool> readerPool_;
};

TEST_F(EncryptedStatsTest, getStatistics) {
  auto stats = reader_->getStatistics();
  for (size_t i = 1; i < 7; ++i) {
    auto& stat = stats->getColumnStatistics(i);
    if (i != 5) {
      ASSERT_EQ(stat.getNumberOfValues(), i * 100);
    } else {
      ASSERT_EQ(stat.getNumberOfValues(), i);
    }
  }
}

TEST_F(EncryptedStatsTest, getStatisticsKeyNotLoaded) {
  clearKey(0);
  auto stats = reader_->getStatistics();
  for (size_t i = 2; i < 7; ++i) {
    auto& stat = stats->getColumnStatistics(i);
    if (i != 5) {
      ASSERT_EQ(stat.getNumberOfValues(), i * 100);
    } else {
      ASSERT_EQ(stat.getNumberOfValues(), i);
    }
  }
}

TEST_F(EncryptedStatsTest, getColumnStatistics) {
  for (size_t i = 1; i < 7; ++i) {
    auto stats = reader_->getColumnStatistics(i);
    if (i != 5) {
      ASSERT_EQ(stats->getNumberOfValues(), i * 100);
    } else {
      ASSERT_EQ(stats->getNumberOfValues(), i);
    }
  }
}

TEST_F(EncryptedStatsTest, getColumnStatisticsKeyNotLoaded) {
  clearKey(0);
  for (size_t i = 2; i < 7; ++i) {
    auto stats = reader_->getColumnStatistics(i);
    if (i != 5) {
      ASSERT_EQ(stats->getNumberOfValues(), i * 100);
    } else {
      ASSERT_EQ(stats->getNumberOfValues(), i);
    }
  }
}

std::unique_ptr<ReaderBase> createCorruptedFileReader(
    uint64_t footerLen,
    uint32_t cacheLen) {
  auto pool = bytedance::bolt::memory::memoryManager()->addLeafPool();
  MemorySink sink{1024, {.pool = pool.get()}};
  DataBufferHolder holder{*pool, 1024, 0, DEFAULT_PAGE_GROW_RATIO, &sink};
  BufferedOutputStream output{holder};

  DataBuffer<char> header{*pool, 3};
  std::memcpy(header.data(), "ORC", 3);
  sink.write(std::move(header));

  proto::Footer footer;
  footer.set_numberofrows(0);
  auto type = footer.add_types();
  type->set_kind(proto::Type_Kind::Type_Kind_STRUCT);

  footer.SerializeToZeroCopyStream(&output);
  output.flush();
  auto actualFooterLen = sink.size();

  proto::PostScript ps;
  // Ignore the real footer length and set a random footer length.
  ps.set_footerlength(footerLen);
  ps.set_cachesize(cacheLen);
  ps.set_compression(proto::CompressionKind::NONE);

  ps.SerializeToZeroCopyStream(&output);
  output.flush();
  auto psLen = static_cast<uint8_t>(sink.size() - actualFooterLen);

  DataBuffer<char> buf{*pool, 1};
  buf.data()[0] = psLen;

  sink.write(std::move(buf));
  auto readFile = std::make_shared<bytedance::bolt::InMemoryReadFile>(
      std::string_view(sink.data(), sink.size()));
  return std::make_unique<ReaderBase>(
      *pool, std::make_unique<BufferedInput>(readFile, *pool));
}

class ReaderBaseTest : public Test {
 protected:
  static void SetUpTestCase() {
    MemoryManager::testingSetInstance(MemoryManager::Options{});
  }
};

TEST_F(ReaderBaseTest, InvalidPostScriptThrows) {
  EXPECT_THROW(
      { createCorruptedFileReader(1'000'000, 0); }, exception::LoggedException);
  EXPECT_THROW(
      { createCorruptedFileReader(0, 1'000'000); }, exception::LoggedException);
}

TEST(ProtoUtilsTest, DirectInputStreamParseErrorPrefix) {
  try {
    ProtoUtils::readProto<proto::PostScript>(
        std::make_unique<NamedBadProtoStream>(
            "DirectInputStream 49 of 49"));
    FAIL() << "Expected readProto to throw";
  } catch (const std::exception& e) {
    const std::string message = e.what();
    EXPECT_NE(
        message.find(kDwrfDirectInputStreamProtoParseErrorPrefix),
        std::string::npos);
  }

  try {
    ProtoUtils::readProto<proto::PostScript>(
        std::make_unique<NamedBadProtoStream>(
            "SeekableArrayInputStream 49 of 49"));
    FAIL() << "Expected readProto to throw";
  } catch (const std::exception& e) {
    const std::string message = e.what();
    EXPECT_NE(message.find("Failed to parse proto from "), std::string::npos);
    EXPECT_EQ(
        message.find(kDwrfDirectInputStreamProtoParseErrorPrefix),
        std::string::npos);
  }
}
