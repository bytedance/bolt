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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/common/encode/Coding.h"
#include "bolt/dwio/common/Statistics.h"
#include "bolt/dwio/common/exception/Exceptions.h"
#include "bolt/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "bolt/dwio/dwrf/reader/DwrfData.h"
#include "bolt/dwio/dwrf/reader/SelectiveStringDictionaryColumnReader.h"
#include "bolt/dwio/dwrf/reader/StreamLabels.h"
#include "bolt/dwio/dwrf/test/OrcTest.h"
#include "bolt/type/fbhive/HiveTypeParser.h"
#include "bolt/vector/FlatVector.h"
#include "folly/Random.h"
#include "folly/String.h"

using namespace ::testing;
using namespace bytedance::bolt;
using namespace bytedance::bolt::dwio::common;
using namespace bytedance::bolt::dwrf;
using bytedance::bolt::type::fbhive::HiveTypeParser;

namespace {

// Simple stride index provider used in tests. It always returns a fixed stride
// index so we can deterministically exercise loadStrideDictionary.
class StaticStrideIndexProvider : public StrideIndexProvider {
 public:
  explicit StaticStrideIndexProvider(uint64_t stride) noexcept
      : strideIndex_(stride) {}

  ~StaticStrideIndexProvider() noexcept override = default;

  uint64_t getStrideIndex() const override {
    return strideIndex_;
  }

 private:
  uint64_t strideIndex_;
};

class SelectiveStringDictionaryColumnReaderTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

// Helper to build a TypeWithId for a single string column "myString".
std::shared_ptr<const dwio::common::TypeWithId> makeFileType() {
  auto rowType = HiveTypeParser().parse("struct<myString:string>");
  auto root = dwio::common::TypeWithId::create(rowType);
  return root->childAt(0);
}

// Builds a SelectiveStringDictionaryColumnReader wired to the provided
// MockStripeStreams and params/scanSpec.
std::unique_ptr<SelectiveStringDictionaryColumnReader> makeReader(
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    DwrfParams& params,
    common::ScanSpec& scanSpec) {
  return std::make_unique<SelectiveStringDictionaryColumnReader>(
      fileType, params, scanSpec);
}

} // namespace

// 1) Happy path: stride dictionary streams (data + length) are present and
// loadStrideDictionary populates scanState_.dictionary2.
TEST_F(
    SelectiveStringDictionaryColumnReaderTest,
    StrideDictionaryPresentPopulatesSecondDictionary) {
  MockStripeStreams streams;
  memory::AllocationPool pool{&streams.getMemoryPool()};
  StreamLabels labels{pool};
  ColumnReaderStatistics columnStats;
  DwrfParams params(streams, labels, columnStats);

  // Encoding for the string column: dictionary with 50 base entries.
  proto::ColumnEncoding dictEncoding;
  dictEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictEncoding.set_dictionarysize(50);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&dictEncoding));

  // No present stream -> no nulls.
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  const int32_t rowIndexStride = 10;
  proto::RowIndex index;

  // Build dictionary and stride dictionary streams following the existing
  // TestColumnReader::testStringDictSkipNoNulls pattern, so that the
  // underlying decoders see a valid layout.
  char data[1024];
  data[0] = 0x9c; // -100
  size_t dataLen = 1;
  size_t strideDictSize = 0;
  size_t strideDictSizeTotal = 0;
  char dict[2048];
  char strideDict[2048];
  size_t dictLen = 0;
  size_t strideDictLen = 0;
  std::vector<std::string> dictVals;
  std::vector<std::string> strideDictVals;
  proto::RowIndexEntry* entry = nullptr;

  for (uint32_t i = 0; i < 100; ++i) {
    if (i % rowIndexStride == 0) {
      // Record previous stride dictionary size for the last row group.
      if (entry) {
        entry->add_positions(strideDictSize);
      }
      strideDictSizeTotal += strideDictSize;
      strideDictSize = 0;

      // New row-group entry. Layout of positions matches the writer tests:
      //   [strideDictLen, 0, strideDictSizeTotal, strideDictSize].
      entry = index.add_entry();
      entry->add_positions(strideDictLen);
      entry->add_positions(0);
      entry->add_positions(strideDictSizeTotal);
    }

    std::string val = folly::to<std::string>(folly::Random::rand32());
    auto valLen = val.length();
    if (i % 2 == 0) {
      dataLen = writeVuLong(data, dataLen, i / 2);
      dictVals.push_back(val);
      memcpy(dict + dictLen, val.c_str(), valLen);
      dictLen += valLen;
    } else {
      dataLen = writeVuLong(data, dataLen, strideDictSize++);
      strideDictVals.push_back(val);
      memcpy(strideDict + strideDictLen, val.c_str(), valLen);
      strideDictLen += valLen;
    }
  }
  // Final stride size for last row group.
  entry->add_positions(strideDictSize);

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(data, dataLen)));
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA, false))
      .WillRepeatedly(Return(new SeekableArrayInputStream(dict, dictLen)));
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY, true))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(strideDict, strideDictLen)));

  // LENGTH stream for base and stride dictionaries.
  char dictLength[1024];
  char strideDictLength[1024];
  dictLength[0] = 0xce; // -50
  strideDictLength[0] = 0xce;
  size_t dictLengthLen = 1;
  size_t strideDictLengthLen = 1;
  for (uint32_t i = 0; i < 50; ++i) {
    dictLengthLen =
        writeVuLong(dictLength, dictLengthLen, dictVals[i].length());
    strideDictLengthLen = writeVuLong(
        strideDictLength, strideDictLengthLen, strideDictVals[i].length());
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, false))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(dictLength, dictLengthLen)));
  EXPECT_CALL(
      streams,
      getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH, true))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(strideDictLength, strideDictLengthLen)));

  // IN_DICTIONARY flags: every other row uses the stride dictionary.
  const unsigned char inDictBits[] = {0x0a, 0xaa};
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_IN_DICTIONARY, false))
      .WillRepeatedly(Return(new SeekableArrayInputStream(
          inDictBits, BOLT_ARRAY_SIZE(inDictBits))));

  // Row index for the column.
  auto indexData = index.SerializePartialAsString();
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, _))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(indexData.data(), indexData.size())));

  // Stride index provider always reports stride 0 so that
  // loadStrideDictionary() reads the first row-group dictionary.
  StaticStrideIndexProvider provider(0);
  EXPECT_CALL(streams, getStrideIndexProviderProxy())
      .WillRepeatedly(Return(&provider));

  auto fileType = makeFileType();

  common::ScanSpec scanSpec("myString");
  scanSpec.setProjectOut(true);
  scanSpec.setExtractValues(true);

  auto reader = makeReader(fileType, params, scanSpec);

  std::vector<vector_size_t> rowNumbers(100);
  for (vector_size_t i = 0; i < 100; ++i) {
    rowNumbers[i] = i;
  }
  RowSet rowSet(rowNumbers.data(), rowNumbers.data() + rowNumbers.size());

  // This exercises ensureInitialized() and loadStrideDictionary().
  reader->read(0, rowSet, nullptr);
  VectorPtr result;
  reader->getValues(rowSet, &result);

  auto& scanState = reader->scanState();
  EXPECT_GT(scanState.dictionary2.numValues, 0);
}

// 2) Missing stride dictionary streams: only IN_DICTIONARY is present and
// STRIDE_DICTIONARY / STRIDE_DICTIONARY_LENGTH streams are both absent.
// Reader should safely degrade to the top-level dictionary, not throwing,
// and dictionary2.numValues must stay 0.
TEST_F(
    SelectiveStringDictionaryColumnReaderTest,
    MissingStrideDictionaryDegradesToBaseDictionary) {
  MockStripeStreams streams;
  memory::AllocationPool pool{&streams.getMemoryPool()};
  StreamLabels labels{pool};
  ColumnReaderStatistics columnStats;
  DwrfParams params(streams, labels, columnStats);

  proto::ColumnEncoding dictEncoding;
  dictEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
  dictEncoding.set_dictionarysize(50);
  EXPECT_CALL(streams, getEncodingProxy(_))
      .WillRepeatedly(Return(&dictEncoding));

  // No present stream -> no nulls.
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
      .WillRepeatedly(Return(nullptr));

  // Build a simple base dictionary (reuse part of the previous pattern).
  char data[1024];
  data[0] = 0x9c;
  size_t dataLen = 1;
  char dict[2048];
  size_t dictLen = 0;
  std::vector<std::string> dictVals;

  for (uint32_t i = 0; i < 50; ++i) {
    std::string val = folly::to<std::string>(folly::Random::rand32());
    auto valLen = val.length();
    dataLen = writeVuLong(data, dataLen, i);
    dictVals.push_back(val);
    memcpy(dict + dictLen, val.c_str(), valLen);
    dictLen += valLen;
  }

  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
      .WillRepeatedly(Return(new SeekableArrayInputStream(data, dataLen)));
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA, false))
      .WillRepeatedly(Return(new SeekableArrayInputStream(dict, dictLen)));

  // Base dictionary LENGTH stream.
  char dictLength[1024];
  dictLength[0] = 0xce;
  size_t dictLengthLen = 1;
  for (uint32_t i = 0; i < dictVals.size(); ++i) {
    dictLengthLen =
        writeVuLong(dictLength, dictLengthLen, dictVals[i].length());
  }
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, false))
      .WillRepeatedly(
          Return(new SeekableArrayInputStream(dictLength, dictLengthLen)));

  // IN_DICTIONARY stream exists so SelectiveStringDictionaryColumnReader
  // attempts to initialize stride dictionary related state, but the
  // stride dictionary streams themselves are legitimately missing.
  const unsigned char inDictBits[] = {0x0a, 0xaa};
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_IN_DICTIONARY, false))
      .WillRepeatedly(Return(new SeekableArrayInputStream(
          inDictBits, BOLT_ARRAY_SIZE(inDictBits))));

  // STRIDE_DICTIONARY / STRIDE_DICTIONARY_LENGTH streams are absent.
  EXPECT_CALL(
      streams, getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY, true))
      .WillRepeatedly(Return(nullptr));
  EXPECT_CALL(
      streams,
      getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH, true))
      .WillRepeatedly(Return(nullptr));

  // Minimal row index: we only need one entry and four positions so that
  // IntDecoder::loadIndices() (used for size offset) stays in bounds. We set
  // the stride dictionary size (4th position) to 0.
  proto::RowIndex index;
  auto* entry = index.add_entry();
  entry->add_positions(0); // dict len stream offset
  entry->add_positions(0); // dict len skip
  entry->add_positions(0); // accumulated stride dict size
  entry->add_positions(0); // stride dict size for this row group

  auto indexData = index.SerializePartialAsString();
  EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, _))
      .WillRepeatedly(Return(
          new SeekableArrayInputStream(indexData.data(), indexData.size())));

  StaticStrideIndexProvider provider(0);
  EXPECT_CALL(streams, getStrideIndexProviderProxy())
      .WillRepeatedly(Return(&provider));

  auto fileType = makeFileType();

  common::ScanSpec scanSpec("myString");
  scanSpec.setProjectOut(true);
  scanSpec.setExtractValues(true);

  auto reader = makeReader(fileType, params, scanSpec);

  std::vector<vector_size_t> rowNumbers(50);
  for (vector_size_t i = 0; i < 50; ++i) {
    rowNumbers[i] = i;
  }
  RowSet rowSet(rowNumbers.data(), rowNumbers.data() + rowNumbers.size());

  EXPECT_NO_THROW({
    reader->read(0, rowSet, nullptr);
    VectorPtr result;
    reader->getValues(rowSet, &result);
  });

  auto& scanState = reader->scanState();
  EXPECT_EQ(scanState.dictionary2.numValues, 0);
}

// 3) Inconsistent stride dictionary streams: only one of the two stride
// streams (data or length) is present. Constructor is expected to fail with a
// LoggedException carrying the "Inconsistent stride dictionary streams" message
// from DWIO_ENSURE.
TEST_F(
    SelectiveStringDictionaryColumnReaderTest,
    InconsistentStrideDictionaryStreamsThrow) {
  // Common setup for encoding and base dictionary streams.
  auto fileType = makeFileType();

  auto makeParamsAndScanSpec = [](MockStripeStreams& streams,
                                  ColumnReaderStatistics& columnStats,
                                  DwrfParams*& outParams,
                                  std::unique_ptr<DwrfParams>& holder,
                                  std::unique_ptr<common::ScanSpec>& scanSpec) {
    memory::AllocationPool pool{&streams.getMemoryPool()};
    auto labels = std::make_unique<StreamLabels>(pool);

    holder = std::make_unique<DwrfParams>(streams, *labels, columnStats);
    outParams = holder.get();

    scanSpec = std::make_unique<common::ScanSpec>("myString");
    scanSpec->setProjectOut(true);
    scanSpec->setExtractValues(true);
  };

  // Helper to prepare minimal base dictionary and IN_DICTIONARY streams.
  auto prepareBaseStreams = [](MockStripeStreams& streams) {
    proto::ColumnEncoding dictEncoding;
    dictEncoding.set_kind(proto::ColumnEncoding_Kind_DICTIONARY);
    dictEncoding.set_dictionarysize(1);
    EXPECT_CALL(streams, getEncodingProxy(_))
        .WillRepeatedly(Return(&dictEncoding));

    EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_PRESENT, false))
        .WillRepeatedly(Return(nullptr));
    EXPECT_CALL(streams, getStreamProxy(_, proto::Stream_Kind_ROW_INDEX, false))
        .WillRepeatedly(Return(nullptr));

    const unsigned char data[] = {0x00};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_DATA, true))
        .WillRepeatedly(Return(new SeekableArrayInputStream(data, 1)));

    const char dictBytes[] = {'x'};
    EXPECT_CALL(
        streams, getStreamProxy(1, proto::Stream_Kind_DICTIONARY_DATA, false))
        .WillRepeatedly(Return(new SeekableArrayInputStream(dictBytes, 1)));

    const unsigned char lenBytes[] = {0x01};
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_LENGTH, false))
        .WillRepeatedly(
            Return(new SeekableArrayInputStream(lenBytes, sizeof(lenBytes))));

    const unsigned char inDictBits[] = {0xff};
    EXPECT_CALL(
        streams, getStreamProxy(1, proto::Stream_Kind_IN_DICTIONARY, false))
        .WillRepeatedly(Return(new SeekableArrayInputStream(
            inDictBits, BOLT_ARRAY_SIZE(inDictBits))));

    // Minimal row index with four positions as before.
    proto::RowIndex index;
    auto* entry = index.add_entry();
    entry->add_positions(0);
    entry->add_positions(0);
    entry->add_positions(0);
    entry->add_positions(0);
    auto indexData = index.SerializePartialAsString();
    EXPECT_CALL(streams, getStreamProxy(1, proto::Stream_Kind_ROW_INDEX, _))
        .WillRepeatedly(Return(
            new SeekableArrayInputStream(indexData.data(), indexData.size())));

    StaticStrideIndexProvider provider(0);
    EXPECT_CALL(streams, getStrideIndexProviderProxy())
        .WillRepeatedly(Return(&provider));
  };

  // Case A: data stream exists, length stream missing.
  {
    MockStripeStreams streams;
    ColumnReaderStatistics columnStats;
    DwrfParams* paramsPtr = nullptr;
    std::unique_ptr<DwrfParams> paramsHolder;
    std::unique_ptr<common::ScanSpec> scanSpec;

    prepareBaseStreams(streams);

    // Stride dictionary data stream only.
    const char strideDictBytes[] = {'y'};
    EXPECT_CALL(
        streams, getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY, true))
        .WillRepeatedly(
            Return(new SeekableArrayInputStream(strideDictBytes, 1)));
    EXPECT_CALL(
        streams,
        getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH, true))
        .WillRepeatedly(Return(nullptr));

    makeParamsAndScanSpec(
        streams, columnStats, paramsPtr, paramsHolder, scanSpec);

    EXPECT_THROW(
        {
          auto reader = makeReader(fileType, *paramsPtr, *scanSpec);
          (void)reader;
        },
        dwio::common::exception::LoggedException);
  }

  // Case B: length stream exists, data stream missing.
  {
    MockStripeStreams streams;
    ColumnReaderStatistics columnStats;
    DwrfParams* paramsPtr = nullptr;
    std::unique_ptr<DwrfParams> paramsHolder;
    std::unique_ptr<common::ScanSpec> scanSpec;

    prepareBaseStreams(streams);

    EXPECT_CALL(
        streams, getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY, true))
        .WillRepeatedly(Return(nullptr));
    const unsigned char strideLenBytes[] = {0x01};
    EXPECT_CALL(
        streams,
        getStreamProxy(1, proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH, true))
        .WillRepeatedly(Return(new SeekableArrayInputStream(
            strideLenBytes, sizeof(strideLenBytes))));

    makeParamsAndScanSpec(
        streams, columnStats, paramsPtr, paramsHolder, scanSpec);

    EXPECT_THROW(
        {
          auto reader = makeReader(fileType, *paramsPtr, *scanSpec);
          (void)reader;
        },
        dwio::common::exception::LoggedException);
  }
}
