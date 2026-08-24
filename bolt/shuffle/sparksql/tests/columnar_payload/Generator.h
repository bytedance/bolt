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

/// Reference Writer for the Shuffle Payload format. It produces conforming
/// payloads from logical data so that a real Reader can be tested before a
/// real Writer exists, and so that a real Writer's output can be compared
/// against an independent implementation.
///
/// Normative reference: bolt/shuffle/sparksql/ColumnarPayloadFormat.md.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "bolt/shuffle/sparksql/tests/columnar_payload/Format.h"

namespace bytedance::bolt::shuffle::sparksql::test {

/// Which Encoding Block encoding to emit. Anything other than kAuto is for
/// tests that need a specific decode path exercised; the generator falls back
/// to kAuto whenever the forced encoding cannot represent a block.
enum class EncodingPolicy : uint8_t {
  kAuto = 0,
  kForcePlain,
  kForceConstNarrow,
  kForceBitPack,
  kForceForBitPack,
  /// Cycle through the kinds block by block, skipping any that cannot
  /// represent the block at hand. One payload then exercises every decode
  /// path, including the transitions between them.
  kRotate,
};

struct GeneratorOptions {
  CompressionLayout layout{CompressionLayout::kSeparate};

  /// When false every buffer takes the uncompressed form, which for SEPARATE
  /// means decoded_sizes[s] == 0 and for COMBINED means COMBINED_STORED. The
  /// codec is then never invoked.
  bool compress{false};

  /// Number of Runs to split the streams across. Split points are moved to
  /// the nearest legal boundary, so the emitted count may be lower when a
  /// stream has fewer legal boundaries than requested.
  size_t runCount{1};

  EncodingPolicy encodingPolicy{EncodingPolicy::kAuto};

  /// When false, emit legal but deliberately non-minimal parameters: a full
  /// width narrow_bytes, the widest legal bit_width, a padded
  /// delta_bit_width. The RFC requires a Reader to accept any legal encoding,
  /// so this catches Readers that assume the Writer always picks the smallest
  /// body.
  bool minimalEncodingWidth{true};

  /// Default string encoding, used for every kString column that
  /// `stringEncodings` does not override.
  bool useDictionary{false};

  /// Per column string encoding. EncodingTag is a column level property, so a
  /// payload may mix RAW and Dictionary string columns. Entries for non
  /// string columns are ignored; a short vector leaves the remaining columns
  /// on `useDictionary`.
  std::vector<StringEncoding> stringEncodings;

  /// Vary compression_layout from Run to Run rather than using `layout`
  /// throughout. The RFC allows this and a Reader must not latch the layout
  /// of the first Run.
  bool rotateLayoutPerRun{false};

  /// Under SEPARATE, decide compression independently per stream so that one
  /// Run mixes compressed and stored buffers. Needs `compress`.
  bool compressPerStream{false};

  /// Seed for the per Run and per stream choices above, so that a failing
  /// payload is reproducible from the options alone.
  uint32_t variationSeed{0};

  /// Dictionaries opened before the writer gives up and falls back to RAW.
  size_t maxDictionaries{4};

  /// Emit ALL_NULL / NO_NULL instead of a degenerate RAW_NULL bitmap. Setting
  /// this false exercises the Reader's RAW_NULL path on uniform columns.
  bool degenerateNullTags{true};

  /// Compress the Null body. Independent of `compress`, which covers Runs.
  bool compressNullBody{false};

  /// Value written to the variable_size field. Negative means exact.
  int64_t variableSizeOverride{-1};
};

/// Byte offsets of the top level regions, so that tests can corrupt a
/// specific field without re-deriving the layout.
struct PayloadLayout {
  size_t nullBodyOffset{0};
  size_t encodingTagsOffset{0};
  size_t runsOffset{0};
  std::vector<size_t> runOffsets;
};

/// What the generator actually emitted, as opposed to what the options asked
/// for. Random data does not guarantee that every encoding path is reached:
/// CONST_NARROW needs a whole block of equal values, an ALL_NULL tag needs a
/// wholly null column, and a tail block needs a value count that is not a
/// multiple of the block size. Aggregating these counters across a test
/// matrix turns "probably covered" into an assertion.
struct GenerationStats {
  /// Blocks emitted per EncodingKind, indexed by its enumerator value.
  size_t encodingKindBlocks[4]{};

  /// Runs emitted per CompressionLayout, indexed by its enumerator value.
  size_t runLayouts[3]{};

  /// Columns per NullTag, indexed by its enumerator value. Index 3 stays 0.
  size_t nullTags[4]{};

  size_t fullBlocks{0};
  size_t tailBlocks{0};
  size_t dictionaries{0};
  size_t dictionaryFallbackValues{0};
  size_t compressedBuffers{0};
  size_t storedBuffers{0};
  size_t emptyStreams{0};

  void merge(const GenerationStats& other);

  /// Names the first dimension with no coverage at all, or nullptr when every
  /// counter this struct tracks has been hit at least once.
  const char* firstGap() const;
};

struct GeneratedPayload {
  std::vector<uint8_t> bytes;
  PayloadLayout layout;
  GenerationStats stats;

  /// Streams in RFC section 1.4 order, before Run splitting and before the
  /// compression envelope. Useful for asserting on encoding decisions.
  std::vector<std::vector<uint8_t>> streams;
};

class ColumnarPayloadGenerator {
 public:
  /// `codec` may be null when options.compress and options.compressNullBody
  /// are both false.
  ColumnarPayloadGenerator(Codec* codec, GeneratorOptions options)
      : codec_(codec), options_(std::move(options)) {}

  /// Returns false and fills `error` when the table violates a constraint the
  /// format cannot express, for example a value outside its type's range or a
  /// column whose value count disagrees with its null bitmap.
  bool
  generate(const FlatTable& table, GeneratedPayload& out, std::string& error);

 private:
  struct StreamSet;

  bool buildStreams(const FlatTable& table, StreamSet& out, std::string& error);

  bool buildEncodingLoopStream(
      const std::vector<int64_t>& values,
      PhysicalType type,
      std::vector<uint8_t>& out,
      std::vector<size_t>& blockBoundaries,
      std::string& error);

  void encodeBlock(
      const std::vector<int64_t>& values,
      size_t begin,
      size_t count,
      PhysicalType type,
      std::vector<uint8_t>& out);

  StringEncoding stringEncodingFor(size_t column) const;

  bool buildStringStreams(
      const FlatColumn& column,
      StringEncoding encoding,
      std::vector<uint8_t>& lengthStream,
      std::vector<size_t>& lengthBoundaries,
      std::vector<uint8_t>& dataStream,
      std::vector<size_t>& dataBoundaries,
      std::string& error);

  Codec* codec_;
  GeneratorOptions options_;

  /// Counts Encoding Blocks within one generate() call, driving kRotate.
  size_t blockIndex_{0};

  /// Reset at the start of every generate() call.
  GenerationStats stats_;
};

/// Builders for the staging form. The corpus is written with them, and tests
/// that need a one-off shape should be too rather than assembling the arrays
/// by hand.
FlatColumn intColumn(PhysicalType type, std::vector<int64_t> values);
FlatColumn doubleColumn(PhysicalType type, std::vector<double> values);
FlatColumn stringColumn(std::vector<std::string> values);

/// A column with nulls: `isNull` sizes the column, `values` holds only the
/// rows it marks non-null.
FlatColumn nullableIntColumn(
    PhysicalType type,
    std::vector<bool> isNull,
    std::vector<int64_t> values);

/// A column whose rows are all null. The value arrays stay empty, which is
/// what the format expects of a column that carries nothing.
FlatColumn nullColumn(PhysicalType type, size_t rowCount);

/// Wraps one column into a table, taking the row count from it.
FlatTable oneColumn(FlatColumn column);

/// Builds a table from columns that must already agree on their row count.
FlatTable tableOf(std::vector<FlatColumn> columns);

/// A table built to exercise one specific structure, named so that a failure
/// reports the shape rather than an opaque index.
struct NamedTable {
  const char* name;
  FlatTable table;
};

/// Deterministic tables covering the boundaries of the format: the value
/// patterns each Encoding Block kind is chosen for, the block counts either
/// side of the 64 byte boundary, every NullTag, and the dictionary limits.
///
/// Every case here is constructed rather than sampled. A block of equal
/// values, a wholly null column, a string of exactly the maximum dictionary
/// entry length, or a dictionary filled to the byte are all shapes that
/// random data reaches either never or so rarely that a defect on the path
/// would go unnoticed.
///
/// ColumnarPayloadVectors.h exposes the same corpus as RowVectors.
std::vector<NamedTable> boundaryCorpus();

} // namespace bytedance::bolt::shuffle::sparksql::test
