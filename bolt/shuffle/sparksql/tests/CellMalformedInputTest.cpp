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

#include <folly/lang/Bits.h>
#include <gtest/gtest.h>

#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <string>
#include <vector>

#include "bolt/shuffle/sparksql/cell/CellEncoding.h"
#include "bolt/shuffle/sparksql/cell/CellFormat.h"
#include "bolt/shuffle/sparksql/cell/CellNestedLayout.h"
#include "bolt/shuffle/sparksql/tests/CellTestUtils.h"
#include "bolt/vector/tests/utils/VectorTestBase.h"

/// What CellShuffleReader makes of bytes no writer wrote: each malformed
/// case fails the read with an error naming its defect, at any batch size,
/// and the edge cases that are well formed read back. The bytes are a
/// writer's output with one edit where the writer makes such a payload,
/// else written field by field as CellFormat.h lays the format out.
namespace bytedance::bolt::shuffle::sparksql::cell {
namespace {

/// Where the fields of a payload's header lie, its kind byte first.
constexpr size_t kRowCountAt = 1;
constexpr size_t kRunCountAt = 5;
constexpr size_t kVariableBytesAt = 9;
constexpr size_t kNullStoredSizeAt = 17;
constexpr size_t kNullBodyAt = 1 + kPayloadFixedHeaderBytes;

constexpr auto kStored = RunLayout::kCombinedUncompressed;

template <typename T>
void put(std::string& out, T value) {
  out.append(reinterpret_cast<const char*>(&value), sizeof(value));
}

/// `bytes` with `value` written at `at`.
template <typename T>
std::string with(std::string bytes, size_t at, T value) {
  folly::storeUnaligned<T>(bytes.data() + at, value);
  return bytes;
}

/// The bytes of `values`, as a raw stream or a block's body holds them.
template <typename T>
std::string bytesOf(const std::vector<T>& values) {
  return std::string(
      reinterpret_cast<const char*>(values.data()), values.size() * sizeof(T));
}

/// `values` as an encoded stream, as the writer encodes them.
template <typename T>
std::string encoded(const std::vector<T>& values) {
  std::string out;
  encodeStream<T>(values.data(), values.size(), out);
  return out;
}

/// One Encoding Block: its EncodingByte, then `body`.
std::string block(EncodingKind kind, uint32_t param, const std::string& body) {
  return std::string(1, static_cast<char>(makeEncodingByte(kind, param))) +
      body;
}

/// A null body: one tag per wire column, then `records`.
std::string nullBody(
    const std::vector<NullTag>& tags,
    const std::string& records = {}) {
  std::string out(nullTagBytes(tags.size()), '\0');
  for (uint32_t col = 0; col < tags.size(); ++col) {
    setNullTag(reinterpret_cast<uint8_t*>(out.data()), col, tags[col]);
  }
  return out + records;
}

/// A null record: a run of `run` rows, plus kNullRecordRunNull when they
/// are null, then `fill` rows of `bitmap`.
struct NullRecord {
  uint32_t run;
  uint32_t fill;
  uint64_t bitmap;
};

/// A kRawNull column's `records` behind their count, `count`.
std::string records(uint32_t count, const std::vector<NullRecord>& records) {
  std::string out;
  put(out, count);
  for (const auto& record : records) {
    put(out, static_cast<uint16_t>(record.run));
    put(out, static_cast<uint8_t>(record.fill));
    std::string bitmap(kNullRecordBitmapBytes, '\0');
    ::memcpy(bitmap.data(), &record.bitmap, sizeof(record.bitmap));
    out += bitmap;
  }
  return out;
}

/// A dictionary of a string column's data stream: each entry its length
/// byte and its bytes, then `marker` and the count of rows it matches.
std::string dictionary(
    const std::vector<std::string>& entries,
    uint8_t marker,
    uint32_t matched) {
  std::string out;
  for (const auto& entry : entries) {
    out += static_cast<char>(entry.size());
    out += entry;
  }
  out += static_cast<char>(marker);
  put(out, matched);
  return out;
}

/// One run of `layout`: its stored sizes, decoded sizes and body.
std::string run(
    RunLayout layout,
    const std::vector<uint64_t>& stored,
    const std::vector<uint64_t>& decoded,
    const std::string& body) {
  std::string out(1, static_cast<char>(layout));
  for (const auto size : stored) {
    put(out, size);
  }
  for (const auto size : decoded) {
    put(out, size);
  }
  return out + body;
}

/// `streams` in one run stored as they are.
std::string storedRun(const std::vector<std::string>& streams) {
  std::vector<uint64_t> sizes;
  std::string body;
  for (const auto& stream : streams) {
    sizes.push_back(stream.size());
    body += stream;
  }
  return run(kStored, {body.size()}, sizes, body);
}

/// `bytes` as one LZ4 frame.
std::string lz4(const std::string& bytes) {
  const auto codec = test::lz4Codec();
  std::string out(codec->maxCompressedLen(bytes.size()), '\0');
  out.resize(codec->compress(
      reinterpret_cast<const uint8_t*>(bytes.data()),
      bytes.size(),
      reinterpret_cast<uint8_t*>(out.data()),
      out.size()));
  return out;
}

/// `streams` in one run compressed whole, which the reader views in place,
/// back to back; the last stream's decoded size is `extra` bytes longer
/// than it is.
std::string compressedRun(
    const std::vector<std::string>& streams,
    uint64_t extra = 0) {
  std::vector<uint64_t> sizes;
  std::string body;
  for (const auto& stream : streams) {
    sizes.push_back(stream.size());
    body += stream;
  }
  sizes.back() += extra;
  const auto stored = lz4(body);
  return run(RunLayout::kCombined, {stored.size()}, sizes, stored);
}

/// One payload, its kind byte first: `rows` rows, `nullBody` and
/// `encodingTags` as they are, then `runs`. A compressed null body
/// declares `nullDecodedSize`.
std::string payload(
    uint32_t rows,
    const std::string& nullBody,
    const std::string& encodingTags,
    const std::vector<std::string>& runs,
    uint32_t nullDecodedSize = 0) {
  std::string out(1, static_cast<char>(CellBlockKind::kPayload));
  put(out, rows);
  put(out, static_cast<uint32_t>(runs.size()));
  put(out, uint64_t{0});
  put(out, static_cast<uint32_t>(nullBody.size()));
  put(out, nullDecodedSize);
  out += nullBody + encodingTags;
  for (const auto& bytes : runs) {
    out += bytes;
  }
  return out;
}

/// One case: a partition's bytes, read as `type`, and the error they fail
/// with.
struct Case {
  const char* name;
  RowTypePtr type;
  std::string bytes;
  const char* error;
  /// The bytes hold LZ4 frames.
  bool lz4{false};
  /// The writer's nativeNestedTypes.
  bool nativeNestedTypes{true};
};

class CellMalformedInputTest : public testing::Test,
                               public bolt::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    char pathTemplate[] = "/tmp/bolt_cell_malformed_test_XXXXXX";
    ASSERT_NE(::mkdtemp(pathTemplate), nullptr);
    dir_ = pathTemplate;
  }

  void TearDown() override {
    std::filesystem::remove_all(dir_);
  }

  /// The one partition of `data` as an uncompressed writer writes it.
  std::string written(const RowVectorPtr& data) {
    const auto dataFile = dir_ + "/data";
    test::removeShuffleFiles(dataFile);
    return test::writeShuffle(
               test::shuffleOptions(
                   dataFile, 1, arrow::Compression::UNCOMPRESSED, false),
               test::roundRobin({data}, 1),
               pool())
        .partitions[0];
  }

  /// Every batch `bytes` read as `type` holds, `batchSize` rows a batch.
  std::vector<RowVectorPtr> read(
      const std::string& bytes,
      const RowTypePtr& type,
      bool lz4,
      int32_t batchSize,
      bool nativeNestedTypes = true) {
    const auto codec = lz4 ? test::lz4Codec() : nullptr;
    const auto reader = test::makeReader(
        bytes,
        type,
        codec.get(),
        pool(),
        batchSize,
        1 << 20,
        nativeNestedTypes);
    std::vector<RowVectorPtr> batches;
    while (auto batch = reader->next()) {
      batches.push_back(std::move(batch));
    }
    return batches;
  }

  std::string dir_;
};

TEST_F(CellMalformedInputTest, malformedPayloadsAreRejected) {
  const auto bigint = ROW({"a"}, {BIGINT()});
  const auto integer = ROW({"a"}, {INTEGER()});
  const auto twoBigints = ROW({"a", "b"}, {BIGINT(), BIGINT()});
  const auto varchar = ROW({"a"}, {VARCHAR()});
  const auto timestamp = ROW({"a"}, {TIMESTAMP()});
  // Lengths alone on the wire: the elements are UNKNOWN.
  const auto lengthsOnly = ROW({"a"}, {ARRAY(UNKNOWN())});
  const auto arrays = ROW({"a"}, {ARRAY(BIGINT())});
  const std::string noTags(1, '\0');
  const auto noNulls = nullBody({NullTag::kNoNull});
  const auto twoNoNulls = nullBody({NullTag::kNoNull, NullTag::kNoNull});
  const auto threeBigints = encoded<int64_t>({1, 2, 3});

  // A writer's payload of 100 BIGINT rows, nothing null: header, a tag
  // byte, an encoding tags byte and one stored run.
  const auto bigints = written(makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row * 7; })}));
  ASSERT_EQ(bigints[0], static_cast<char>(CellBlockKind::kPayload));
  ASSERT_EQ(bigints.substr(kRowCountAt, 4), std::string("\x64\0\0\0", 4));
  ASSERT_EQ(bigints.substr(kRunCountAt, 4), std::string("\x01\0\0\0", 4));
  ASSERT_EQ(bigints.substr(kNullStoredSizeAt, 4), std::string("\x01\0\0\0", 4));
  ASSERT_EQ(bigints.substr(kNullBodyAt, 2), noNulls + noTags);
  // `bigints` with byte `at` set to `value`.
  const auto withByte = [&](size_t at, char value) {
    auto bytes = bigints;
    bytes[at] = value;
    return bytes;
  };
  // A null body one byte longer than its tags.
  auto longNullBody = with(bigints, kNullStoredSizeAt, uint32_t{2});
  longNullBody.insert(kNullBodyAt + 1, 1, '\0');
  // A writer's header-only payload of 10 UNKNOWN rows.
  const auto unknowns = written(
      makeRowVector({BaseVector::createNullConstant(UNKNOWN(), 10, pool())}));
  ASSERT_EQ(unknowns.size(), 1 + kPayloadFixedHeaderBytes);

  // `rows` rows of one wire column, none null, its `streams` stored.
  const auto plain = [&](uint32_t rows,
                         const std::vector<std::string>& streams) {
    return payload(rows, noNulls, noTags, {storedRun(streams)});
  };
  // 10 INTEGER rows: rows 0, 1 and 3 null, as the one record says.
  const auto someNulls = [&](const std::vector<NullRecord>& recs,
                             uint32_t count = 1) {
    return payload(
        10,
        nullBody({NullTag::kRawNull}, records(count, recs)),
        noTags,
        {storedRun({encoded<int32_t>({1, 2, 3, 4, 5, 6, 7})})});
  };
  constexpr uint32_t kNull = kNullRecordRunNull;
  // A string column's lengths cut short, its data behind them in one
  // compressed run: a decoder reading past the lengths reads the data.
  const auto cutLengths = [&](uint32_t rows, const std::string& lengths) {
    return payload(
        rows,
        noNulls,
        noTags,
        {compressedRun({lengths, std::string(5 * rows, 'z')})});
  };
  // A string column in dictionary form: `indexes` its length stream.
  const auto dictionaryForm = [&](uint32_t rows,
                                  const std::string& indexes,
                                  const std::string& data) {
    return payload(
        rows, noNulls, std::string(1, '\x01'), {storedRun({indexes, data})});
  };
  constexpr auto kMaxPositions =
      static_cast<int32_t>(CellNestedLayout::kMaxPositions);
  constexpr auto kInt32Max = std::numeric_limits<int32_t>::max();

  const std::vector<Case> cases{
      // The stream and the header.
      {"cut after the kind byte",
       bigint,
       bigints.substr(0, 1),
       "truncated payload header"},
      {"cut inside the header",
       bigint,
       bigints.substr(0, 5),
       "truncated payload header"},
      {"cut after the header",
       bigint,
       bigints.substr(0, kNullBodyAt),
       "truncated null body"},
      {"one byte short",
       bigint,
       bigints.substr(0, bigints.size() - 1),
       "truncated run buffer"},
      {"an unknown block kind",
       bigint,
       '\x07' + bigints.substr(1),
       "unknown cell shuffle block kind"},
      {"no rows, but values",
       bigint,
       with(bigints, kRowCountAt, uint32_t{0}),
       "malformed value stream"},
      {"rows past kMaxPayloadRows",
       bigint,
       with(bigints, kRowCountAt, kMaxPayloadRows + 1),
       "row count beyond the configured limit"},
      {"runs past 65536",
       bigint,
       with(bigints, kRunCountAt, uint32_t{(1 << 16) + 1}),
       "run count beyond the configured limit"},
      {"no null body",
       bigint,
       with(bigints, kNullStoredSizeAt, uint32_t{0}),
       "null stored size must be at least 1"},
      {"bytes of a payload without wire columns",
       ROW({"u"}, {UNKNOWN()}),
       with(unknowns, kVariableBytesAt, uint64_t{1}),
       "payload without wire columns must contain only its header"},
      // The null body.
      {"a null body one byte long",
       bigint,
       longNullBody,
       "null body length mismatch"},
      {"a reserved null tag",
       bigint,
       withByte(kNullBodyAt, '\x03'),
       "reserved null tag"},
      {"a set unused null tag slot",
       bigint,
       withByte(kNullBodyAt, '\x05'),
       "non-zero unused null tag bits"},
      {"a record's fill past its bitmap",
       integer,
       someNulls({{kNull | 2, kNullRecordBits + 1, 0b101}}),
       "null record fill beyond its bitmap"},
      {"a bit set past a record's fill",
       integer,
       someNulls({{kNull | 2, 3, 0b1101}}),
       "non-zero bits past a null record's fill"},
      {"an empty record", integer, someNulls({{0, 0, 0}}), "empty null record"},
      {"a record past the rows",
       integer,
       someNulls({{kNull | 11, 0, 0}}),
       "null records past the payload's rows"},
      {"more records than the body holds",
       integer,
       someNulls({{kNull | 2, 3, 0b101}}, 2),
       "truncated null records"},
      {"a compressed null body that decompresses short",
       integer,
       payload(
           3,
           lz4(noNulls),
           noTags,
           {storedRun({encoded<int32_t>({1, 2, 3})})},
           static_cast<uint32_t>(noNulls.size() + 1)),
       "null body decompression failed",
       true},
      // The encoding tags.
      {"a dictionary bit on a BIGINT column",
       bigint,
       withByte(kNullBodyAt + 1, '\x01'),
       "encoding tag on a non-string column"},
      {"a set unused encoding tag bit",
       bigint,
       withByte(kNullBodyAt + 1, '\x02'),
       "non-zero unused encoding tag bits"},
      // The runs.
      {"a run past the end of the input",
       bigint,
       payload(3, noNulls, noTags, {run(kStored, {100}, {100}, threeBigints)}),
       "truncated run buffer"},
      {"decoded sizes that overflow",
       twoBigints,
       payload(
           3,
           twoNoNulls,
           noTags,
           {run(
               kStored,
               {5},
               {uint64_t{1} << 63, (uint64_t{1} << 63) + 5},
               std::string(5, '\0'))}),
       "run decoded sizes overflow"},
      {"a stream stored empty but decoded",
       twoBigints,
       payload(
           3,
           nullBody({NullTag::kNoNull, NullTag::kAllNull}),
           noTags,
           {run(
               RunLayout::kSeparate,
               {threeBigints.size(), 0},
               {0, 5},
               threeBigints)}),
       "empty stored buffer with decoded size"},
      {"a stored run whose size is not its streams'",
       bigint,
       payload(
           3,
           noNulls,
           noTags,
           {run(
               kStored,
               {threeBigints.size() + 1},
               {threeBigints.size()},
               threeBigints)}),
       "combined-uncompressed size mismatch"},
      {"a compressed run that decompresses short",
       bigint,
       payload(3, noNulls, noTags, {compressedRun({threeBigints}, 1)}),
       "combined buffer decompression failed",
       true},
      // The streams.
      {"a raw stream one value long",
       ROW({"a"}, {DOUBLE()}),
       plain(3, {bytesOf<double>({1, 2, 3, 4})}),
       "raw stream length mismatch"},
      {"an INTEGER stream a block too long",
       integer,
       plain(
           20, {encoded(std::vector<int32_t>(20, 9)) + encoded<int32_t>({7})}),
       "malformed value stream"},
      {"a value stream without its blocks",
       integer,
       plain(3, {""}),
       "malformed value stream"},
      {"a malformed block in the middle of a stream",
       integer,
       plain(
           48,
           {encoded(std::vector<int32_t>(16, 1)) +
            block(EncodingKind::kBitPack, 0, std::string(64, '\0')) +
            encoded(std::vector<int32_t>(16, 2))}),
       "malformed value stream"},
      {"bytes after a stream's last block",
       integer,
       plain(3, {encoded<int32_t>({1, 2, 3}) + '\0'}),
       "malformed value stream"},
      {"kPlain with a parameter",
       integer,
       plain(
           16,
           {block(
               EncodingKind::kPlain, 1, bytesOf(std::vector<int32_t>(16, 3)))}),
       "malformed value stream"},
      {"kBitPack of 33 bits for an INTEGER",
       integer,
       plain(16, {block(EncodingKind::kBitPack, 33, std::string(66, '\0'))}),
       "malformed value stream"},
      {"a kBitPack block a byte short",
       varchar,
       cutLengths(3, block(EncodingKind::kBitPack, 8, "\x05\x05")),
       "malformed string length stream",
       true},
      {"a kConstNarrow block cut inside its value",
       varchar,
       cutLengths(3, block(EncodingKind::kConstNarrow, 2, "\x05")),
       "malformed string length stream",
       true},
      {"a FOR block cut inside its base",
       varchar,
       cutLengths(
           3, block(EncodingKind::kForBitPack, 0, std::string("\x05\0\0", 3))),
       "malformed string length stream",
       true},
      {"a FOR block cut inside its deltas",
       varchar,
       cutLengths(
           3,
           block(
               EncodingKind::kForBitPack,
               8,
               bytesOf<int64_t>({5}) + std::string(2, '\0'))),
       "malformed string length stream",
       true},
      {"a full FOR block cut inside its base",
       varchar,
       cutLengths(
           8, block(EncodingKind::kForBitPack, 8, std::string("\x05\0\0", 3))),
       "malformed string length stream",
       true},
      // The values.
      {"a boolean byte of 2 at the end of an append",
       ROW({"a"}, {BOOLEAN()}),
       plain(7, {std::string("\x01\x00\x01\x00\x01\x00\x02", 7)}),
       "invalid boolean byte"},
      {"timestamp nanos of 1e9",
       timestamp,
       plain(1, {encoded<int64_t>({0}), encoded<int64_t>({1'000'000'000})}),
       "timestamp out of range"},
      {"timestamp nanos of -1",
       timestamp,
       plain(1, {encoded<int64_t>({0}), encoded<int64_t>({-1})}),
       "timestamp out of range"},
      {"timestamp seconds past kMaxSeconds",
       timestamp,
       plain(
           1,
           {encoded<int64_t>({Timestamp::kMaxSeconds + 1}),
            encoded<int64_t>({0})}),
       "timestamp out of range"},
      {"string data a byte long",
       varchar,
       plain(3, {encoded<int64_t>({2, 2, 2}), "aabbccd"}),
       "string data length mismatch"},
      {"an all-null string column with lengths",
       varchar,
       payload(
           3,
           nullBody({NullTag::kAllNull}),
           noTags,
           {storedRun({encoded<int64_t>({1}), ""})}),
       "streams of an all-null column are not empty"},
      {"a dictionary index past its dictionary",
       varchar,
       dictionaryForm(
           3,
           std::string("\x00\x02\x01", 3),
           dictionary({"a", "b"}, kDictMoreMarker, 2) +
               dictionary({"c", "d", "e"}, kDictLastMarker, 1)),
       "dictionary index out of range"},
      {"a dictionary of 64 bytes",
       varchar,
       dictionaryForm(
           1,
           std::string(1, '\0'),
           dictionary(
               {std::string(kDictEntryMaxLen, 'w')}, kDictLastMarker, 1)),
       "dictionary over its serialized budget"},
      // Nested layouts, and the adapter's binary column.
      {"a negative container length",
       lengthsOnly,
       plain(3, {encoded<int32_t>({2, -1, 1})}),
       "negative container length"},
      {"container lengths past kMaxPositions",
       lengthsOnly,
       plain(1, {encoded<int32_t>({kMaxPositions + 1})}),
       "container lengths beyond the position limit"},
      {"container lengths past 32 bits",
       lengthsOnly,
       plain(3, {encoded<int32_t>({kInt32Max, kInt32Max, 2})}),
       "container lengths beyond the position limit"},
      {"a length stream past its lengths",
       lengthsOnly,
       plain(3, {encoded<int32_t>({1, 2, 3}) + encoded<int32_t>({4})}),
       "malformed container length stream"},
      {"empty containers whose elements have values",
       arrays,
       payload(
           2,
           twoNoNulls,
           noTags,
           {storedRun({encoded<int32_t>({0, 0}), encoded<int64_t>({10})})}),
       "malformed value stream"},
      {"a null binary of the adapter",
       arrays,
       payload(1, nullBody({NullTag::kAllNull}), noTags, {storedRun({"", ""})}),
       "Cell complex payload must be non-null",
       false,
       false},
      {"a truncated binary of the adapter",
       arrays,
       plain(1, {encoded<int64_t>({1}), std::string(1, '\0')}),
       "Invalid CompactRow",
       false,
       false},
  };
  for (const auto& c : cases) {
    for (const int32_t batchSize : {4096, 7}) {
      SCOPED_TRACE(fmt::format("{}, batches of {}", c.name, batchSize));
      try {
        read(c.bytes, c.type, c.lz4, batchSize, c.nativeNestedTypes);
        ADD_FAILURE() << "read without an error";
      } catch (const BoltException& e) {
        EXPECT_NE(e.message().find(c.error), std::string::npos) << e.message();
      }
    }
  }
}

TEST_F(CellMalformedInputTest, malformedBlocksTellWhereTheReaderWas) {
  // The message of the error reading `streams` as `type` fails with.
  const auto errorOf = [&](const std::vector<std::string>& streams,
                           const RowTypePtr& type) -> std::string {
    try {
      const auto reader =
          test::makeReader(streams, type, nullptr, pool(), 4096, 1 << 20);
      while (reader->next() != nullptr) {
      }
    } catch (const BoltException& e) {
      return e.message();
    }
    ADD_FAILURE() << "read without an error";
    return "";
  };
  const auto expectHas = [](const std::string& message,
                            const std::string& part) {
    EXPECT_NE(message.find(part), std::string::npos)
        << "'" << part << "' not in: " << message;
  };
  // `bytes` as the error shows them.
  const auto hex = [](const std::string& bytes) {
    std::string out;
    for (const char c : bytes) {
      out += fmt::format(
          "{}{:02x}", out.empty() ? "" : " ", static_cast<uint8_t>(c));
    }
    return out;
  };
  // What the error says of `payload`, a writer's payload read whole from
  // offset 0 of the stream.
  const auto previous = [](const std::string& payload) {
    const auto field = [&](size_t at) {
      return folly::loadUnaligned<uint32_t>(payload.data() + 1 + at);
    };
    return fmt::format(
        "previous payload at offset 0 to {}: rows={} runCount={} "
        "nullStoredSize={} nullDecodedSize={}",
        payload.size(),
        field(0),
        field(4),
        field(16),
        field(20));
  };
  // After a payload, a payload header of 5 rows, 2^17 runs and a null body
  // of one byte, then the bytes `tail` the error shows the first 32 of.
  std::string tooManyRuns(1, static_cast<char>(CellBlockKind::kPayload));
  put(tooManyRuns, uint32_t{5});
  put(tooManyRuns, uint32_t{1} << 17);
  put(tooManyRuns, uint64_t{0});
  put(tooManyRuns, uint32_t{1});
  put(tooManyRuns, uint32_t{0});
  const std::string tail = "abcdefghijklmnopqrstuvwxyz0123456789";

  const auto bigint = ROW({"a"}, {BIGINT()});
  const auto bigints = written(makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row * 7; })}));
  auto message = errorOf({bigints + tooManyRuns + tail}, bigint);
  expectHas(
      message,
      fmt::format(
          "malformed cell shuffle payload: run count beyond the configured "
          "limit [stream 0 (1 opened), block 1 at offset {}, read to offset {}",
          bigints.size(),
          bigints.size() + tooManyRuns.size()));
  expectHas(message, previous(bigints));
  expectHas(message, "block header bytes: " + hex(tooManyRuns) + ";");
  expectHas(message, "next 32 bytes: " + hex(tail.substr(0, 32)) + "]");

  // The same in a nested layout's payload.
  const auto arrays = ROW({"a"}, {ARRAY(BIGINT())});
  const auto nested = written(makeRowVector({makeArrayVector<int64_t>(
      50,
      [](auto row) { return row % 3; },
      [](auto index) { return index; })}));
  message = errorOf({nested + tooManyRuns + tail}, arrays);
  expectHas(
      message,
      fmt::format(
          "malformed cell shuffle payload: run count beyond the configured "
          "limit [stream 0 (1 opened), block 1 at offset {}, read to offset {}",
          nested.size(),
          nested.size() + tooManyRuns.size()));
  expectHas(message, previous(nested));
  expectHas(message, "block header bytes: " + hex(tooManyRuns) + ";");

  // A header cut short: only its bytes read are shown.
  const auto cut = tooManyRuns.substr(0, 11);
  message = errorOf({bigints + cut}, bigint);
  expectHas(
      message,
      fmt::format(
          "truncated payload header [stream 0 (1 opened), block 1 at offset "
          "{}, read to offset {}",
          bigints.size(),
          bigints.size() + cut.size()));
  expectHas(
      message, "block header bytes: " + hex(cut) + "; next 0 bytes: none]");

  // A composite block's header cut short.
  const auto composite =
      std::string(1, static_cast<char>(CellBlockKind::kCompositeRows)) +
      bytesOf<uint32_t>({3});
  message = errorOf({bigints + composite}, bigint);
  expectHas(
      message,
      fmt::format(
          "malformed composite shuffle block: truncated composite block header "
          "[stream 0 (1 opened), block 1 at offset {}, read to offset {}",
          bigints.size(),
          bigints.size() + composite.size()));
  expectHas(message, "block header bytes: " + hex(composite) + ";");

  // An unknown kind opening a second stream: no payload before it there.
  message = errorOf({bigints, '\x07' + tail}, bigint);
  expectHas(
      message,
      "unknown cell shuffle block kind [stream 1 (2 opened), block 0 at "
      "offset 0, read to offset 1; no previous payload in this stream; "
      "block header bytes: 07; next 32 bytes: " +
          hex(tail.substr(0, 32)) + "]");
}

TEST_F(CellMalformedInputTest, edgesOfTheFormatAreRead) {
  // A FOR block of delta width 0 is its base, at every value, though the
  // writer never chooses one.
  constexpr int32_t kBase = -123'456;
  const auto integer = ROW({"a"}, {INTEGER()});
  const auto forZero = payload(
      16,
      nullBody({NullTag::kNoNull}),
      std::string(1, '\0'),
      {storedRun({block(
          EncodingKind::kForBitPack,
          0,
          bytesOf(std::vector<int32_t>{kBase}))})});
  for (const int32_t batchSize : {4096, 7}) {
    SCOPED_TRACE(batchSize);
    test::expectPartitionRows(
        test::rowsIn({makeRowVector(
            {makeFlatVector<int32_t>(std::vector<int32_t>(16, kBase))})}),
        read(forZero, integer, false, batchSize),
        0,
        batchSize);
  }

  // A payload of kMaxPayloadRows rows, the most a decoder takes, every one
  // a null array: its first batch reads back.
  const auto arrays = ROW({"a"}, {ARRAY(BIGINT())});
  const auto mostRows = payload(
      kMaxPayloadRows,
      nullBody({NullTag::kAllNull, NullTag::kAllNull}),
      std::string(1, '\0'),
      {});
  const auto reader =
      test::makeReader(mostRows, arrays, nullptr, pool(), 4096, 1 << 20);
  const auto batch = reader->next();
  ASSERT_NE(batch, nullptr);
  ASSERT_EQ(batch->size(), 4096);
  for (vector_size_t row = 0; row < batch->size(); ++row) {
    ASSERT_TRUE(batch->childAt(0)->isNullAt(row));
  }
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::cell
