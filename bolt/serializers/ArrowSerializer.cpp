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
#include "ArrowSerializer.h"
#include "bolt/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "bolt/vector/BaseVector.h"
#include "bolt/vector/FlatVector.h"
#include "bolt/vector/LazyVector.h"
#include "bolt/vector/VectorStream.h"
#include "bolt/vector/arrow/Bridge.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/c/bridge.h>
#include <arrow/io/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/table.h>

#include <folly/compression/Zstd.h>
#include <folly/io/IOBuf.h>
#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <memory>
#include <sstream>
#include <utility>
#include <vector>

#include "PrestoSerializer.h"
#include "bolt/type/Type.h"
#include "bolt/vector/LazyVector.h"

namespace bytedance::bolt::serializer::arrowserde {
namespace { // for ArrowVectorSerializer
// Same Header in PrestoSerializer.cpp
constexpr int8_t kCompressedBitMask = 1;
constexpr int8_t kEncryptedBitMask = 2;
constexpr int8_t kCheckSumBitMask = 4;
// uncompressed size comes after the number of rows and the codec
constexpr int32_t kSizeInBytesOffset{4 + 1};
// There header for a page is:
// + number of rows (4 bytes)
// + codec (1 byte)
// + uncompressed size (4 bytes)
// + size (4 bytes) (this is the compressed size if the data is compressed,
//                   otherwise it's uncompressed size again)
// + checksum (8 bytes)
//
// See https://prestodb.io/docs/current/develop/serialized-page.html for a
// detailed specification of the format.
#ifndef BOLT_ENABLE_CRC
constexpr int32_t kHeaderSize{kSizeInBytesOffset + 4 + 4};
#else
constexpr int32_t kHeaderSize{kSizeInBytesOffset + 4 + 4 + 8};
#endif

constexpr int kHeadN = 2;

inline void writeInt32(OutputStream* out, int32_t v) {
  out->write(reinterpret_cast<char*>(&v), sizeof(v));
}
inline void writeInt64(OutputStream* out, int64_t v) {
  out->write(reinterpret_cast<char*>(&v), sizeof(v));
}

struct ArrowArrayReleaser {
  ArrowArray* array;
  explicit ArrowArrayReleaser(ArrowArray* a) : array(a) {}
  ~ArrowArrayReleaser() {
    if (array && array->release)
      array->release(array);
  }
  void disarm() {
    array = nullptr;
  }
};

struct ArrowSchemaReleaser {
  ArrowSchema* schema;
  explicit ArrowSchemaReleaser(ArrowSchema* s) : schema(s) {}
  ~ArrowSchemaReleaser() {
    if (schema && schema->release)
      schema->release(schema);
  }
  void disarm() {
    schema = nullptr;
  }
};

// Arrow -> Bolt OutputStream
class BoltArrowOutputStream : public ::arrow::io::OutputStream {
 public:
  explicit BoltArrowOutputStream(bytedance::bolt::OutputStream* out)
      : out_(out) {}

  ::arrow::Status Close() override {
    closed_ = true;
    return ::arrow::Status::OK();
  }
  bool closed() const override {
    return closed_;
  }

  ::arrow::Result<int64_t> Tell() const override {
    return static_cast<int64_t>(out_->tellp());
  }

  ::arrow::Status Write(const void* data, int64_t nbytes) override {
    if (closed_)
      return ::arrow::Status::IOError("write on closed");
    if (nbytes <= 0)
      return ::arrow::Status::OK();
    out_->write(
        reinterpret_cast<const char*>(data),
        static_cast<std::streamsize>(nbytes));
    return ::arrow::Status::OK();
  }

  ::arrow::Status Write(const std::shared_ptr<::arrow::Buffer>& buf) override {
    return Write(buf->data(), buf->size());
  }

 private:
  bytedance::bolt::OutputStream* out_;
  bool closed_{false};
};

class BoltArrowInputStream : public ::arrow::io::InputStream {
 public:
  BoltArrowInputStream(
      bytedance::bolt::ByteInputStream* src,
      int64_t limit,
      memory::MemoryPool* pool)
      : src_(src), remaining_(limit), pool_(pool) {}

  ::arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    if (closed_)
      return ::arrow::Status::IOError("read on closed");
    if (nbytes <= 0 || remaining_ == 0)
      return int64_t{0};

    int64_t toRead = std::min<int64_t>(nbytes, remaining_);
    auto* p = static_cast<uint8_t*>(out);
    int64_t copied = 0;

    while (toRead > 0) {
      auto view = src_->nextView(static_cast<int32_t>(toRead));
      if (view.size() > 0) {
        std::memcpy(p, view.data(), view.size());
        p += view.size();
        copied += view.size();
        toRead -= view.size();
        pos_ += view.size();
        remaining_ -= view.size();
        continue;
      }

      if (src_->atEnd()) {
        break;
      }

      int64_t chunk = std::min<int64_t>(toRead, 64 * 1024);
      src_->readBytes(p, static_cast<int32_t>(chunk));
      p += chunk;
      copied += chunk;
      toRead -= chunk;
      pos_ += chunk;
      remaining_ -= chunk;
    }

    return copied;
  }

  ::arrow::Result<std::shared_ptr<::arrow::Buffer>> Read(
      int64_t nbytes) override {
    if (nbytes <= 0 || remaining_ == 0) {
      return ::arrow::AllocateBuffer(0);
    }
    int64_t want = std::min<int64_t>(nbytes, remaining_);
    auto buffer = AlignedBuffer::allocate<char>(want, pool_);
    ARROW_ASSIGN_OR_RAISE(auto got, Read(want, buffer->asMutable<char>()));
    auto rawBuffer = buffer->as<uint8_t>();
    auto arrowBuffer = std::shared_ptr<::arrow::Buffer>(
        new ::arrow::Buffer(rawBuffer, static_cast<int64_t>(got)),
        [buf = std::move(buffer)](::arrow::Buffer* p) {
          delete p;
          // buf will be released when go out of scope
        });
    return arrowBuffer;
  }

  ::arrow::Result<int64_t> Tell() const override {
    return pos_;
  }
  bool closed() const override {
    return closed_;
  }
  ::arrow::Status Close() override {
    closed_ = true;
    return ::arrow::Status::OK();
  }

 private:
  bytedance::bolt::ByteInputStream* src_;
  int64_t remaining_;
  int64_t pos_{0};
  bool closed_{false};
  memory::MemoryPool* pool_;
};

ArrowVectorSerde::ArrowSerdeOptions toArrowSerdeOptions(
    const VectorSerde::Options* options) {
  if (options == nullptr) {
    return ArrowVectorSerde::ArrowSerdeOptions();
  }
  return ArrowVectorSerde::ArrowSerdeOptions(
      options->useLosslessTimestamp, options->compressionKind);
}

class ArrowVectorSerializer : public VectorSerializer {
 public:
  ArrowVectorSerializer(
      const RowTypePtr& rowType,
      std::vector<VectorEncoding::Simple> encodings,
      int32_t numRows,
      StreamArena* streamArena,
      bool useLosslessTimestamp,
      common::CompressionKind compressionKind,
      const ArrowOptions* arrowBridgeOptions = nullptr)
      : rowType_(rowType),
        numRows_(numRows),
        streamArena_(streamArena),
        codec_(common::compressionKindToCodec(compressionKind)) {
    if (arrowBridgeOptions) {
      arrowBridgeOptions_ = *arrowBridgeOptions;
    } else {
      arrowBridgeOptions_ = toArrowSerdeOptions(nullptr).arrowOptions;
    }
    arrowBridgeOptions_.timestampUnit =
        useLosslessTimestamp ? TimestampUnit::kNano : TimestampUnit::kMicro;
    VLOG(1) << "[ArrowVectorSerializer.ctor] type=" << rowType_->toString()
            << " numRows=" << numRows_
            << " compression=" << static_cast<int>(compressionKind)
            << " losslessTs=" << (useLosslessTimestamp ? 1 : 0)
            << " opts{ flattenDict="
            << (arrowBridgeOptions_.flattenDictionary ? 1 : 0)
            << ", flattenConst="
            << (arrowBridgeOptions_.flattenConstant ? 1 : 0)
            << ", exportToView=" << (arrowBridgeOptions_.exportToView ? 1 : 0)
            << ", copySV=" << (arrowBridgeOptions_.stringViewCopyValues ? 1 : 0)
            << ", tsUnit="
            << static_cast<int>(arrowBridgeOptions_.timestampUnit);
  }

  void append(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      Scratch& scratch) override {
    vector_size_t total = 0;
    for (const auto& rg : ranges)
      total += rg.size;
    if (!vector || vector->size() == 0 || total == 0)
      return;

    if (arrowBridgeOptions_.exportToView) {
      heldVectors_.push_back(vector);
    }

    ArrowArray cArray{};
    ArrowSchema cSchema{};
    ArrowArrayReleaser arrayRel(&cArray);
    ArrowSchemaReleaser schemaRel(&cSchema);

    VectorPtr useVec = vector;
    if (ranges.size() == 1) {
      const auto& rg = ranges[0];
      const bool identity = (rg.begin == 0 && rg.size == vector->size());
      if (!identity) {
        useVec = vector->slice(rg.begin, rg.size);
      }
    } else {
      auto idx = AlignedBuffer::allocate<vector_size_t>(total, vector->pool());
      auto* raw = idx->asMutable<vector_size_t>();
      size_t p = 0;
      for (const auto& rg : ranges) {
        for (vector_size_t i = 0; i < rg.size; ++i) {
          raw[p++] = rg.begin + i;
        }
      }
      useVec = BaseVector::wrapInDictionary(nullptr, idx, total, vector);
    }

    // for lazy
    SelectivityVector sv(useVec->size(), true);
    LazyVector::ensureLoadedRows(useVec, sv);

    // Vector → Arrow C-Data
    exportToArrow(useVec, cArray, vector->pool(), arrowBridgeOptions_);
    exportToArrow(useVec, cSchema, arrowBridgeOptions_, {});

    // C-Data → Arrow C++
    auto schRes = ::arrow::ImportSchema(&cSchema);
    BOLT_USER_CHECK(
        schRes.ok(), "ImportSchema failed: {}", schRes.status().ToString());
    auto schema = *schRes;

    auto rbRes = ::arrow::ImportRecordBatch(&cArray, schema);
    BOLT_USER_CHECK(
        rbRes.ok(), "ImportRecordBatch failed: {}", rbRes.status().ToString());
    auto rb = *rbRes;

    schemaRel.disarm();
    arrayRel.disarm();

    if (!openedSchema_) {
      openedSchema_ = rb->schema();
    } else {
      BOLT_USER_CHECK(
          rb->schema()->Equals(*openedSchema_),
          "Arrow schema changed within the same stream.\nold: {}\nnew: {}",
          openedSchema_->ToString(),
          rb->schema()->ToString());
    }

    totalRows_ += rb->num_rows();
    { // increase stream Arena Logical size, to trigger flush
      size_t delta = 0;

      if (batches_.empty()) {
        delta += static_cast<size_t>(kHeaderSize) + 7;

        auto schRes = ::arrow::ipc::SerializeSchema(*rb->schema());
        BOLT_USER_CHECK(
            schRes.ok(),
            "SerializeSchema failed: {}",
            schRes.status().ToString());
        auto schBuf = *schRes;
        delta += static_cast<size_t>(schBuf->size());
      }

      int64_t batchSize = 0;
      ::arrow::ipc::IpcWriteOptions wopts =
          ::arrow::ipc::IpcWriteOptions::Defaults();
      wopts.metadata_version = ::arrow::ipc::MetadataVersion::V4;
      auto st = ::arrow::ipc::GetRecordBatchSize(*rb, wopts, &batchSize);
      BOLT_USER_CHECK(st.ok(), "GetRecordBatchSize failed: {}", st.ToString());
      delta += static_cast<size_t>(batchSize);

      streamArena_->accountLogical(delta);
    }
    batches_.push_back(std::move(rb));
  }

  void append(
      const RowVectorPtr& vector,
      const folly::Range<const vector_size_t*>& rows,
      Scratch& scratch) override {
    const vector_size_t n = rows.size();
    if (!vector || vector->size() == 0 || n == 0)
      return;

    if (arrowBridgeOptions_.exportToView) {
      heldVectors_.push_back(vector);
    }

    ArrowArray cArray{};
    ArrowSchema cSchema{};
    ArrowArrayReleaser arrayRel(&cArray);
    ArrowSchemaReleaser schemaRel(&cSchema);

    VectorPtr useVec = vector;

    bool identity = (n == vector->size());
    if (identity) {
      for (vector_size_t i = 0; i < n; ++i) {
        BOLT_DCHECK_LT(rows[i], vector->size());
        if (rows[i] != i) {
          identity = false;
          break;
        }
      }
    }

    if (!identity) {
      auto idx = AlignedBuffer::allocate<vector_size_t>(n, vector->pool());
      auto* raw = idx->asMutable<vector_size_t>();
      memcpy(raw, rows.begin(), sizeof(vector_size_t) * n);
      useVec = BaseVector::wrapInDictionary(nullptr, idx, n, vector);
    }

    // for lazy
    SelectivityVector sv(useVec->size(), true);
    LazyVector::ensureLoadedRows(useVec, sv);

    // Vector → Arrow C-Data
    exportToArrow(useVec, cArray, vector->pool(), arrowBridgeOptions_);
    exportToArrow(useVec, cSchema, arrowBridgeOptions_, {});

    // C-Data → Arrow C++
    auto schRes = ::arrow::ImportSchema(&cSchema);
    BOLT_USER_CHECK(
        schRes.ok(), "ImportSchema failed: {}", schRes.status().ToString());
    auto schema = *schRes;

    auto rbRes = ::arrow::ImportRecordBatch(&cArray, schema);
    BOLT_USER_CHECK(
        rbRes.ok(), "ImportRecordBatch failed: {}", rbRes.status().ToString());
    auto rb = *rbRes;

    schemaRel.disarm();
    arrayRel.disarm();

    if (!openedSchema_) {
      openedSchema_ = rb->schema();
    } else {
      BOLT_USER_CHECK(
          rb->schema()->Equals(*openedSchema_),
          "Arrow schema changed within the same stream.\nold: {}\nnew: {}",
          openedSchema_->ToString(),
          rb->schema()->ToString());
    }

    totalRows_ += rb->num_rows();
    { // increase stream Arena Logical size, to trigger flush
      size_t delta = 0;

      if (batches_.empty()) {
        delta += static_cast<size_t>(kHeaderSize) + 7;

        auto schRes = ::arrow::ipc::SerializeSchema(*rb->schema());
        BOLT_USER_CHECK(
            schRes.ok(),
            "SerializeSchema failed: {}",
            schRes.status().ToString());
        auto schBuf = *schRes;
        delta += static_cast<size_t>(schBuf->size());
      }

      int64_t batchSize = 0;
      ::arrow::ipc::IpcWriteOptions wopts =
          ::arrow::ipc::IpcWriteOptions::Defaults();
      wopts.metadata_version = ::arrow::ipc::MetadataVersion::V4;
      auto st = ::arrow::ipc::GetRecordBatchSize(*rb, wopts, &batchSize);
      BOLT_USER_CHECK(st.ok(), "GetRecordBatchSize failed: {}", st.ToString());
      delta += static_cast<size_t>(batchSize);

      streamArena_->accountLogical(delta);
    }
    batches_.push_back(std::move(rb));
  }

  size_t maxSerializedSize() const override {
    struct CountingOutputStream : ::arrow::io::OutputStream {
      int64_t n{0};
      bool closed_{false};
      ::arrow::Status Close() override {
        closed_ = true;
        return ::arrow::Status::OK();
      }
      bool closed() const override {
        return closed_;
      }
      ::arrow::Result<int64_t> Tell() const override {
        return n;
      }
      ::arrow::Status Write(const void* data, int64_t nbytes) override {
        n += nbytes;
        return ::arrow::Status::OK();
      }
      ::arrow::Status Write(
          const std::shared_ptr<::arrow::Buffer>& buf) override {
        n += buf->size();
        return ::arrow::Status::OK();
      }
    } counter;

    ::arrow::ipc::IpcWriteOptions wopts =
        ::arrow::ipc::IpcWriteOptions::Defaults();
    wopts.metadata_version = ::arrow::ipc::MetadataVersion::V4;

    std::shared_ptr<::arrow::Schema> schema;
    if (!batches_.empty()) {
      schema = batches_.front()->schema();
    } else {
      ArrowSchema cSchema{};
      ArrowSchemaReleaser schemaRel(&cSchema);
      auto* pool = streamArena_->pool();
      auto empty = BaseVector::create<RowVector>(rowType_, 0, pool);
      exportToArrow(empty, cSchema, arrowBridgeOptions_, {});
      auto schRes = ::arrow::ImportSchema(&cSchema);
      if (!schRes.ok()) {
        return static_cast<size_t>(kHeaderSize) + 7 + 4096;
      }
      schema = *schRes;
      schemaRel.disarm();
    }

    auto mk = ::arrow::ipc::MakeStreamWriter(&counter, schema, wopts);
    if (!mk.ok()) {
      return static_cast<size_t>(kHeaderSize) + 7 + 4096;
    }
    auto writer = *mk;

    for (const auto& rb : batches_) {
      if (!rb)
        continue;
      auto st = writer->WriteRecordBatch(*rb);
      if (!st.ok()) {
        (void)writer->Close();
        return static_cast<size_t>(kHeaderSize) + 7 + 1024 * 1024;
      }
    }
    (void)writer->Close();

    const size_t header = static_cast<size_t>(kHeaderSize);
    const size_t worstAlign = 7;
    const size_t body = static_cast<size_t>(std::max<int64_t>(0, counter.n));
    return header + worstAlign + body;
  }

  void flush(OutputStream* stream) override {
    ::arrow::ipc::IpcWriteOptions wopts =
        ::arrow::ipc::IpcWriteOptions::Defaults();
    wopts.metadata_version = ::arrow::ipc::MetadataVersion::V4;

    // schema
    std::shared_ptr<::arrow::Schema> schema;
    if (!batches_.empty()) {
      schema = batches_.front()->schema();
    } else {
      ArrowSchema cSchema{};
      ArrowSchemaReleaser schemaRel(&cSchema);
      auto* pool = streamArena_->pool();
      auto empty = BaseVector::create<RowVector>(rowType_, 0, pool);
      exportToArrow(empty, cSchema, arrowBridgeOptions_, {});
      auto schRes = ::arrow::ImportSchema(&cSchema);
      BOLT_USER_CHECK(
          schRes.ok(),
          "ImportSchema(empty) failed: {}",
          schRes.status().ToString());
      schema = *schRes;
      schemaRel.disarm();
    }

#ifndef NDEBUG
    for (size_t i = 0; i < batches_.size(); ++i) {
      BOLT_CHECK(
          batches_[i]->schema()->Equals(*schema),
          "Schema mismatch at batch {}:\nexpected: {}\n   found: {}",
          i,
          schema->ToString(),
          batches_[i]->schema()->ToString());
    }
#endif

    const int32_t rowsToWrite = totalRows_;

    // header = rows | codec | uncompressed size | compressed size | checksum
    writeInt32(stream, rowsToWrite);
    const std::streampos codec_pos = stream->tellp();
    const char codecMask = 0;

    auto writeCodec = [&](char codecMask,
                          int32_t uncompressedSize,
                          int32_t compressedSize,
                          bool first = false) {
      auto pos = stream->tellp();
      stream->seekp(codec_pos);
      stream->write(&codecMask, 1);
      writeInt32(stream, uncompressedSize);
      writeInt32(stream, compressedSize);
#ifdef BOLT_ENABLE_CRC
      writeInt64(stream, 0);
#endif
      if (!first) {
        stream->seekp(pos);
        VLOG(1) << "[Flush] wrote codec header: "
                << " rows=" << rowsToWrite
                << " codecMask=" << static_cast<int>(codecMask)
                << " uncompressedSize=" << uncompressedSize
                << " compressedSize=" << compressedSize;
      }
    };

    writeCodec(0, 0, 0, true); // placeholder

    // 8 byte allign
    {
      const int64_t cur = static_cast<int64_t>(stream->tellp());
      const int64_t mis = cur & 7LL;
      if (mis != 0) {
        const int64_t pad = 8LL - mis;
        static const char kZeros[8] = {0};
        stream->write(kZeros, static_cast<std::streamsize>(pad));
        VLOG(1) << "[Flush] wrote padding: " << pad << " bytes";
      }
    }

    const std::streampos body_start = stream->tellp();
#ifndef NDEBUG
    BOLT_CHECK_EQ(
        static_cast<int64_t>(body_start) & 7LL,
        0LL,
        "body_start must be 8-byte aligned");
#endif

    auto writeBody = [&](OutputStream* outStream) {
      BoltArrowOutputStream aos(outStream);
      auto mk = ::arrow::ipc::MakeStreamWriter(&aos, schema, wopts);
      BOLT_USER_CHECK(
          mk.ok(), "MakeStreamWriter failed: {}", mk.status().ToString());
      auto writer = *mk;

      for (size_t i = 0; i < batches_.size(); ++i) {
        const auto& rb = batches_[i];
        if (!rb)
          continue;

        auto st = writer->WriteRecordBatch(*rb);
        BOLT_USER_CHECK(
            st.ok(),
            "WriteRecordBatch failed at index {}: {}",
            i,
            st.ToString());
      }
      BOLT_USER_CHECK(writer->Close().ok(), "Arrow IPC writer Close failed");
    };

    // write RecordBatches into bodyStream
    bool noCompression =
        (codec_->type() == folly::io::CodecType::NO_COMPRESSION);

    if (!noCompression) {
      const size_t headerOverhead = static_cast<size_t>(kHeaderSize) + 7;
      size_t estimatedBody = 0;
      const size_t estimatedTotal = maxSerializedSize();
      if (estimatedTotal > headerOverhead) {
        estimatedBody = estimatedTotal - headerOverhead;
      }
      if (estimatedBody >= codec_->maxUncompressedLength() ||
          estimatedBody >=
              static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        // Too large to buffer for compression, fall back to direct write.
        noCompression = true;
      }
    }

    if (!noCompression) {
      auto out = std::make_unique<IOBufOutputStream>(*streamArena_->pool());
      writeBody(out.get());
      auto iobuf = out->getIOBuf();
      auto uncompressedSize = iobuf->computeChainDataLength();
      BOLT_CHECK_LT(
          uncompressedSize,
          std::numeric_limits<int32_t>::max(),
          "Arrow IPC body too large.");
      if (uncompressedSize < codec_->maxUncompressedLength()) {
        std::unique_ptr<folly::IOBuf> compressed;
        try {
          compressed = codec_->compress(iobuf.get());
        } catch (const std::exception& e) {
          BOLT_FAIL(
              "got exception in compression, uncompressedSize: {}, maxUncompressedLength: {}, detail: {}",
              uncompressedSize,
              codec_->maxUncompressedLength(),
              e.what());
        }
        for (auto range : *compressed) {
          stream->write(
              reinterpret_cast<const char*>(range.data()), range.size());
        }
        int32_t compressedSize = compressed->computeChainDataLength();
        BOLT_CHECK_LT(
            compressedSize,
            std::numeric_limits<int32_t>::max(),
            "Arrow IPC compressed body too large.");
        writeCodec(kCompressedBitMask, uncompressedSize, compressedSize);
        clear();
        return;
      }
      // fallback to uncompressed
    }

    // serialize directly to output stream
    writeBody(stream);
    auto bodyLength = stream->tellp() - body_start;
    BOLT_CHECK_LT(
        bodyLength,
        std::numeric_limits<int32_t>::max(),
        "Arrow IPC body too large.");
    writeCodec(0, bodyLength, bodyLength);
    clear();
  }

  void clear() override {
    totalRows_ = 0;
    batches_.clear();
    openedSchema_.reset();
    heldVectors_.clear();
    streamArena_->resetLogical();
  }

 private:
  const RowTypePtr rowType_;
  int32_t numRows_;
  StreamArena* const streamArena_;
  const std::unique_ptr<folly::io::Codec> codec_;

  ArrowOptions arrowBridgeOptions_;
  std::vector<std::shared_ptr<::arrow::RecordBatch>> batches_;
  std::shared_ptr<::arrow::Schema> openedSchema_;
  int32_t totalRows_{0};
  std::vector<RowVectorPtr> heldVectors_;
}; // class ArrowVectorSerializer : public VectorSerializer

class ArrowBatchVectorSerializer : public BatchVectorSerializer {
 public:
  ArrowBatchVectorSerializer(
      memory::MemoryPool* pool,
      bool useLosslessTimestamp,
      common::CompressionKind compressionKind,
      const ArrowOptions* arrowBridgeOptions = nullptr)
      : pool_(pool),
        useLosslessTimestamp_(useLosslessTimestamp),
        compressionKind_(compressionKind) {
    if (arrowBridgeOptions) {
      arrowBridgeOptions_ = *arrowBridgeOptions;
    } else {
      arrowBridgeOptions_ = toArrowSerdeOptions(nullptr).arrowOptions;
    }
    arrowBridgeOptions_.timestampUnit =
        useLosslessTimestamp ? TimestampUnit::kNano : TimestampUnit::kMicro;
    arrowBridgeOptions_.flattenDictionary = false;
    arrowBridgeOptions_.flattenConstant = false;
  }

  void serialize(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      Scratch& scratch,
      OutputStream* stream) override {
    size_t numRows = 0;
    for (const auto& range : ranges) {
      numRows += range.size;
    }
    StreamArena arena(pool_);
    auto serializer = std::make_unique<ArrowVectorSerializer>(
        asRowType(vector->type()),
        std::vector<VectorEncoding::Simple>{},
        static_cast<int32_t>(numRows),
        &arena,
        useLosslessTimestamp_,
        compressionKind_,
        &arrowBridgeOptions_);
    serializer->append(vector, ranges, scratch);
    serializer->flush(stream);
  }

 private:
  memory::MemoryPool* pool_;
  const bool useLosslessTimestamp_;
  const common::CompressionKind compressionKind_;
  ArrowOptions arrowBridgeOptions_;
};
} // namespace

namespace { // for deserialize helper
std::string_view typeToEncodingName(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return "BYTE_ARRAY";
    case TypeKind::TINYINT:
      return "BYTE_ARRAY";
    case TypeKind::SMALLINT:
      return "SHORT_ARRAY";
    case TypeKind::INTEGER:
      return "INT_ARRAY";
    case TypeKind::BIGINT:
      return "LONG_ARRAY";
    case TypeKind::HUGEINT:
      return "INT128_ARRAY";
    case TypeKind::REAL:
      return "INT_ARRAY";
    case TypeKind::DOUBLE:
      return "LONG_ARRAY";
    case TypeKind::VARCHAR:
      return "VARIABLE_WIDTH";
    case TypeKind::VARBINARY:
      return "VARIABLE_WIDTH";
    case TypeKind::TIMESTAMP:
      return "LONG_ARRAY";
    case TypeKind::ARRAY:
      return "ARRAY";
    case TypeKind::MAP:
      return "MAP";
    case TypeKind::ROW:
      return isTimestampWithTimeZoneType(type) ? "LONG_ARRAY" : "ROW";
    case TypeKind::UNKNOWN:
      return "BYTE_ARRAY";
    default:
      BOLT_FAIL("Unknown type kind: {}", static_cast<int>(type->kind()));
  }
}

bool fastAppendScalar(
    VectorPtr& dst,
    const VectorPtr& src,
    vector_size_t dstOff,
    vector_size_t srcOff,
    vector_size_t count,
    bolt::memory::MemoryPool*) {
  if (src->encoding() != VectorEncoding::Simple::FLAT ||
      dst->encoding() != VectorEncoding::Simple::FLAT ||
      !dst->type()->equivalent(*src->type())) {
    return false;
  }

  const uint64_t* sNulls = src->rawNulls();
  uint64_t* dNulls = nullptr;
  if (sNulls || dst->rawNulls()) {
    auto& nb = dst->mutableNulls(dst->size());
    dNulls = nb->asMutable<uint64_t>();
    if (sNulls) {
      bits::copyBits(sNulls, srcOff, dNulls, dstOff, count);
    } else {
      bits::fillBits(dNulls, dstOff, count, bits::kNotNull);
    }
  }

  switch (dst->typeKind()) {
    case TypeKind::TINYINT: {
      auto d = dst->as<FlatVector<int8_t>>()->mutableRawValues();
      auto s = src->as<FlatVector<int8_t>>()->rawValues();
      std::memcpy(d + dstOff, s + srcOff, sizeof(int8_t) * count);
      return true;
    }
    case TypeKind::SMALLINT: {
      auto d = dst->as<FlatVector<int16_t>>()->mutableRawValues();
      auto s = src->as<FlatVector<int16_t>>()->rawValues();
      std::memcpy(d + dstOff, s + srcOff, sizeof(int16_t) * count);
      return true;
    }
    case TypeKind::INTEGER: {
      auto d = dst->as<FlatVector<int32_t>>()->mutableRawValues();
      auto s = src->as<FlatVector<int32_t>>()->rawValues();
      std::memcpy(d + dstOff, s + srcOff, sizeof(int32_t) * count);
      return true;
    }
    case TypeKind::BIGINT: {
      auto d = dst->as<FlatVector<int64_t>>()->mutableRawValues();
      auto s = src->as<FlatVector<int64_t>>()->rawValues();
      std::memcpy(d + dstOff, s + srcOff, sizeof(int64_t) * count);
      return true;
    }
    case TypeKind::REAL: {
      auto d = dst->as<FlatVector<float>>()->mutableRawValues();
      auto s = src->as<FlatVector<float>>()->rawValues();
      std::memcpy(d + dstOff, s + srcOff, sizeof(float) * count);
      return true;
    }
    case TypeKind::DOUBLE: {
      auto d = dst->as<FlatVector<double>>()->mutableRawValues();
      auto s = src->as<FlatVector<double>>()->rawValues();
      std::memcpy(d + dstOff, s + srcOff, sizeof(double) * count);
      return true;
    }
    default:
      return false;
  }
}

static void ensureOffsetsSizesWritableDeep(
    const VectorPtr& v,
    vector_size_t parentLen,
    bolt::memory::MemoryPool*) {
  if (!v)
    return;

  switch (v->encoding()) {
    case bolt::VectorEncoding::Simple::ROW: {
      auto row = std::static_pointer_cast<RowVector>(v);
      for (size_t i = 0; i < row->childrenSize(); ++i) {
        ensureOffsetsSizesWritableDeep(row->childAt(i), parentLen, nullptr);
      }
      break;
    }

    case bolt::VectorEncoding::Simple::ARRAY: {
      auto arr = std::static_pointer_cast<ArrayVector>(v);
      arr->mutableOffsets(parentLen);
      arr->mutableSizes(parentLen);
      ensureOffsetsSizesWritableDeep(
          arr->elements(), arr->elements()->size(), nullptr);
      break;
    }

    case bolt::VectorEncoding::Simple::MAP: {
      auto map = std::static_pointer_cast<MapVector>(v);
      map->mutableOffsets(parentLen);
      map->mutableSizes(parentLen);
      ensureOffsetsSizesWritableDeep(
          map->mapKeys(), map->mapKeys()->size(), nullptr);
      ensureOffsetsSizesWritableDeep(
          map->mapValues(), map->mapValues()->size(), nullptr);
      break;
    }

    default:
      break;
  }
}
} // namespace

namespace { // for size estimation helper
constexpr size_t kIpcFixedOverhead = 64;

static inline size_t stringLikeSizeAtBase(
    const BaseVector* base,
    vector_size_t idx) {
  if (auto sv = dynamic_cast<const FlatVector<StringView>*>(base)) {
    return static_cast<size_t>(sv->rawValues()[idx].size());
  }
  return 0;
}

static inline const BaseVector*
unwrapToBase(const BaseVector* v, vector_size_t row, vector_size_t& baseIndex) {
  baseIndex = row;
  using VE = VectorEncoding::Simple;

  size_t guard = 0;
  while (v) {
    if (++guard > 64)
      break;

    auto enc = v->encoding();
    if (enc == VE::DICTIONARY || enc == VE::SEQUENCE) {
      baseIndex = v->wrappedIndex(baseIndex);
      const BaseVector* next = v->wrappedVector();
      if (!next || next == v)
        break;
      v = next;
      continue;
    }
    if (enc == VE::CONSTANT) {
      baseIndex = v->wrappedIndex(0);
      const BaseVector* next = v->wrappedVector();
      if (!next || next == v)
        break;
      v = next;
      continue;
    }
    break;
  }
  return v;
}

static size_t estimateCellBytes(const VectorPtr& vec, vector_size_t row) {
  if (!vec)
    return 1;
  if (vec->type()->kind() == TypeKind::UNKNOWN) {
    return 1; // actually 1 bit
  }
  if (vec->encoding() == VectorEncoding::Simple::LAZY) {
    auto loaded = BaseVector::loadedVectorShared(vec);
    return estimateCellBytes(loaded, row);
  }

  vector_size_t baseIndex = row;
  const BaseVector* base = unwrapToBase(vec.get(), row, baseIndex);
  const TypePtr& t = base->type();

  switch (t->kind()) {
    case TypeKind::BOOLEAN:
      return 1 + 1;
    case TypeKind::TINYINT:
      return 1 + 1;
    case TypeKind::SMALLINT:
      return 1 + 2;
    case TypeKind::INTEGER:
      return 1 + 4;
    case TypeKind::BIGINT:
      return 1 + 8;
    case TypeKind::HUGEINT:
      return 1 + 16;
    case TypeKind::REAL:
      return 1 + 4;
    case TypeKind::DOUBLE:
      return 1 + 8;
    case TypeKind::TIMESTAMP:
      return 1 + 8;

    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      size_t payload = stringLikeSizeAtBase(base, baseIndex);
      return 1 + 4 + payload;
    }

    case TypeKind::ARRAY: {
      auto arr = static_cast<const ArrayVector*>(base);
      size_t bytes = 1 + 4; // validity + offsets
      if (!arr->isNullAt(baseIndex)) {
        auto off = arr->offsetAt(baseIndex);
        auto len = arr->sizeAt(baseIndex);
        auto elems = arr->elements();
        for (vector_size_t i = 0; i < len; ++i) {
          bytes += estimateCellBytes(elems, off + i);
        }
      }
      return bytes;
    }

    case TypeKind::MAP: {
      auto map = static_cast<const MapVector*>(base);
      size_t bytes = 1 + 4;
      if (!map->isNullAt(baseIndex)) {
        auto off = map->offsetAt(baseIndex);
        auto len = map->sizeAt(baseIndex);
        auto keys = map->mapKeys();
        auto vals = map->mapValues();
        for (vector_size_t i = 0; i < len; ++i) {
          bytes += estimateCellBytes(keys, off + i);
          bytes += estimateCellBytes(vals, off + i);
        }
      }
      return bytes;
    }

    case TypeKind::ROW: {
      auto rowv = static_cast<const RowVector*>(base);
      size_t bytes = 1; // struct validity
      for (size_t c = 0; c < rowv->childrenSize(); ++c) {
        bytes += estimateCellBytes(rowv->childAt(c), baseIndex);
      }
      return bytes;
    }

    default:
      return 1;
  }
}
} // namespace

ArrowVectorSerde::ArrowSerdeOptions
ArrowVectorSerde::ArrowSerdeOptions::fromPrestoOptions(
    presto::PrestoVectorSerde::PrestoOptions* prestoOptions) {
  if (prestoOptions == nullptr) {
    return ArrowVectorSerde::ArrowSerdeOptions();
  }
  return ArrowVectorSerde::ArrowSerdeOptions(
      prestoOptions->useLosslessTimestamp, prestoOptions->compressionKind);
}

void ArrowVectorSerde::estimateSerializedSize(
    VectorPtr vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    Scratch& scratch) {
  if (!vector || ranges.size() == 0 || sizes == nullptr)
    return;

  auto rowv = std::static_pointer_cast<RowVector>(vector);
  const size_t nRanges = ranges.size();

  const size_t perPageOverhead =
      static_cast<size_t>(kHeaderSize) + kIpcFixedOverhead;
  bool overheadAdded = false;

  for (size_t i = 0; i < nRanges; ++i) {
    const auto& rg = ranges[i];
    size_t bytes = 0;
    for (vector_size_t r = 0; r < rg.size; ++r) {
      const vector_size_t row = rg.begin + r;
      for (size_t c = 0; c < rowv->childrenSize(); ++c) {
        bytes += estimateCellBytes(rowv->childAt(c), row);
      }
    }
    if (!overheadAdded) {
      bytes += perPageOverhead;
      overheadAdded = true;
    }
    if (sizes[i]) {
      *sizes[i] += static_cast<vector_size_t>(bytes);
    }
  }
}

void ArrowVectorSerde::estimateSerializedSize(
    VectorPtr vector,
    const folly::Range<const vector_size_t*> rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  if (!vector || rows.size() == 0 || sizes == nullptr)
    return;

  auto rowv = std::static_pointer_cast<RowVector>(vector);
  const size_t n = rows.size();

  const size_t perPageOverhead =
      static_cast<size_t>(kHeaderSize) + kIpcFixedOverhead;

  for (size_t i = 0; i < n; ++i) {
    vector_size_t row = rows[i];
    size_t bytes = 0;
    for (size_t c = 0; c < rowv->childrenSize(); ++c) {
      bytes += estimateCellBytes(rowv->childAt(c), row);
    }
    if (i == 0)
      bytes += perPageOverhead;
    if (sizes[i]) {
      *sizes[i] += static_cast<vector_size_t>(bytes);
    }
  }
}

std::unique_ptr<VectorSerializer> ArrowVectorSerde::createSerializer(
    RowTypePtr type,
    int32_t numRows,
    StreamArena* streamArena,
    const Options* options) {
  const auto arrowSerdeOptions = toArrowSerdeOptions(options);
  return std::make_unique<ArrowVectorSerializer>(
      type,
      arrowSerdeOptions.encodings,
      numRows,
      streamArena,
      arrowSerdeOptions.useLosslessTimestamp,
      arrowSerdeOptions.compressionKind,
      &arrowSerdeOptions.arrowOptions);
}

std::unique_ptr<BatchVectorSerializer> ArrowVectorSerde::createBatchSerializer(
    memory::MemoryPool* pool,
    const Options* options) {
  const auto arrowSerdeOptions = toArrowSerdeOptions(options);
  return std::make_unique<ArrowBatchVectorSerializer>(
      pool,
      arrowSerdeOptions.useLosslessTimestamp,
      arrowSerdeOptions.compressionKind,
      &arrowSerdeOptions.arrowOptions);
}

void ArrowVectorSerde::deprecatedSerializeEncoded(
    const RowVectorPtr& vector,
    StreamArena* streamArena,
    const Options* options,
    OutputStream* out) {
  BOLT_NYI("ArrowVectorSerde::deprecatedSerializeEncoded");
}

void ArrowVectorSerde::deserialize(
    ByteInputStream* source,
    bolt::memory::MemoryPool* pool,
    RowTypePtr type,
    RowVectorPtr* result,
    vector_size_t resultOffset,
    const Options* options) {
  auto arrowSerdeOptions = toArrowSerdeOptions(options);
  arrowSerdeOptions.arrowOptions.stringViewCopyValues = false;
  arrowSerdeOptions.arrowOptions.flattenDictionary = false;
  arrowSerdeOptions.arrowOptions.flattenConstant = false;

  BOLT_USER_CHECK(type != nullptr, "Requested RowType must not be null");

  // Page header
  const int32_t numRows = source->read<int32_t>();
  const int8_t pageCodecMarker = source->read<int8_t>();
  const int32_t uncompressedSize = source->read<int32_t>();
  const int32_t sizeInBytes = source->read<int32_t>();
#ifdef BOLT_ENABLE_CRC
  (void)source->read<int64_t>();
  BOLT_CHECK_EQ(
      pageCodecMarker & kCheckSumBitMask,
      0,
      "bytedance arrow also don't support crc as well as presto");
#endif
  BOLT_USER_CHECK(
      (pageCodecMarker & kEncryptedBitMask) == 0,
      "Arrow page encryption is not supported");

  const bool isCompressed =
      (pageCodecMarker & kCompressedBitMask) == kCompressedBitMask;
  BOLT_CHECK_GE(uncompressedSize, 0);
  BOLT_CHECK_GE(sizeInBytes, 0);
  const int64_t bodyLen = isCompressed ? sizeInBytes : uncompressedSize;
  BOLT_USER_CHECK(
      !isCompressed ||
          arrowSerdeOptions.compressionKind !=
              common::CompressionKind::CompressionKind_NONE,
      "Arrow page is compressed but compression is disabled in options");

  { // 8-byte padding
    const int64_t cur = static_cast<int64_t>(source->tellp());
    const int64_t mis = (cur & 7LL);
    if (mis != 0) {
      const int32_t pad = static_cast<int32_t>(8LL - mis);
      source->skip(pad);
      VLOG(1) << "[deserialize] skipped padding: " << pad << " bytes";
    }
  }

  VLOG(1) << "[deserialize] decompressing Arrow page: "
          << " uncompressedSize=" << uncompressedSize
          << " compressedSize=" << sizeInBytes;
  std::shared_ptr<::arrow::ipc::RecordBatchReader> reader;
  if (!isCompressed) {
    auto in = std::make_shared<BoltArrowInputStream>(source, bodyLen, pool);
    auto openRes = ::arrow::ipc::RecordBatchStreamReader::Open(in);
    BOLT_USER_CHECK(
        openRes.ok(),
        "Arrow IPC StreamReader open failed: {}",
        openRes.status().ToString());
    VLOG(1) << "[deserialize] RecordBatchStreamReader opened (zero-copy input)";
    reader = *openRes;
  } else {
    // TODO: (Jun) Need to test compression

    auto compIO = folly::IOBuf::create(sizeInBytes);
    if (sizeInBytes > 0) {
      source->readBytes(compIO->writableData(), sizeInBytes);
    }
    compIO->append(sizeInBytes);
    std::unique_ptr<folly::IOBuf> uncompressed;
    try {
      auto codec =
          common::compressionKindToCodec(arrowSerdeOptions.compressionKind);
      uncompressed = codec->uncompress(compIO.get(), uncompressedSize);
    } catch (const std::exception& e) {
      BOLT_FAIL("Arrow page decompression failed: {}", e.what());
    }
    auto range = uncompressed->coalesce();
    auto* raw_buf = new ::arrow::Buffer(range.data(), range.size());
    std::shared_ptr<::arrow::Buffer> pageBuf(
        raw_buf, [buf = std::move(uncompressed)](::arrow::Buffer* p) {
          delete p;
          // buf will be released when go out of scope
        });
    auto input = std::make_shared<::arrow::io::BufferReader>(pageBuf);
    auto openRes = ::arrow::ipc::RecordBatchStreamReader::Open(input);
    BOLT_USER_CHECK(
        openRes.ok(),
        "Arrow IPC StreamReader open failed: {}",
        openRes.status().ToString());
    VLOG(1)
        << "[deserialize] RecordBatchStreamReader opened (decompressed buffer)";
    reader = *openRes;
  }

  const vector_size_t totalOut = resultOffset + numRows;
  int64_t rowsRead = 0;

  auto nextRes = reader->Next();
  BOLT_USER_CHECK(
      nextRes.ok(),
      "Arrow IPC Read next RecordBatch failed: {}",
      nextRes.status().ToString());
  std::shared_ptr<::arrow::RecordBatch> firstRb = *nextRes;

  // empty page
  if (!firstRb) {
    if (*result == nullptr) {
      *result = BaseVector::create<RowVector>(type, resultOffset, pool);
    } else if ((*result)->size() < resultOffset) {
      (*result)->resize(resultOffset);
    }
    BOLT_USER_CHECK_EQ(
        numRows,
        0,
        "Arrow page header numRows({}) != actual rows(0) in page.",
        numRows);
    VLOG(1) << "[deserialize][finalize] empty output, size="
            << (*result)->size();
    return;
  }

  rowsRead += firstRb->num_rows();

  // Arrow -> Bolt
  auto importOne =
      [&](const std::shared_ptr<::arrow::RecordBatch>& rb) -> RowVectorPtr {
    ArrowArray cArray{};
    ArrowSchema cSchema{};
    ArrowArrayReleaser arrayRel(&cArray);
    ArrowSchemaReleaser schemaRel(&cSchema);
    auto expSt = ::arrow::ExportRecordBatch(*rb, &cArray, &cSchema);
    BOLT_USER_CHECK(
        expSt.ok(), "Arrow C ExportRecordBatch failed: {}", expSt.ToString());

    VectorPtr imported = importFromArrowAsOwner(
        cSchema, cArray, arrowSerdeOptions.arrowOptions, pool);

    schemaRel.disarm();
    arrayRel.disarm();

    auto row = std::dynamic_pointer_cast<RowVector>(imported);
    BOLT_USER_CHECK(
        row != nullptr, "Top-level deserialized value is not a RowVector.");
    return row;
  };

  RowVectorPtr firstRow = importOne(firstRb);

  // schema check
  BOLT_USER_CHECK_EQ(
      (vector_size_t)firstRow->childrenSize(),
      type->size(),
      "Number of columns in serialized data doesn't match number of columns requested for deserialization");
  for (column_index_t i = 0; i < type->size(); ++i) {
    const auto expected = type->childAt(i);
    const auto actual = firstRow->childAt(i)->type();
    if (!actual->equivalent(*expected)) {
      BOLT_USER_FAIL(
          "Serialized encoding is not compatible with requested type: {}. "
          "Expected {}. Got {}.",
          expected->toString(),
          typeToEncodingName(expected),
          typeToEncodingName(actual));
    }
  }

  // single batch
  const bool tryZeroCopy = (*result == nullptr && resultOffset == 0);
  if (tryZeroCopy && firstRb->num_rows() == numRows) {
    *result = std::move(firstRow);

    while (true) {
      auto drainRes = reader->Next();
      BOLT_USER_CHECK(
          drainRes.ok(),
          "Arrow IPC drain failed: {}",
          drainRes.status().ToString());
      auto drainRb = *drainRes;
      if (!drainRb)
        break;
      rowsRead += drainRb->num_rows();
    }

    BOLT_USER_CHECK_EQ(
        rowsRead,
        static_cast<int64_t>(numRows),
        "Arrow page header numRows({}) != actual rows({}) in page.",
        numRows,
        rowsRead);

    VLOG(1) << "[deserialize][finalize] single-batch zero-copy commit size="
            << (*result)->size();
    return;
  }

  // multi-batch
  if (*result == nullptr) {
    *result = BaseVector::create<RowVector>(type, totalOut, pool);
  } else if ((*result)->size() < totalOut) {
    (*result)->resize(totalOut);
  }
  {
    bytedance::bolt::SelectivityVector all(totalOut);
    (*result)->ensureWritable(all);
  }

  // container offsets/sizes buffer writable
  ensureOffsetsSizesWritableDeep(*result, totalOut, pool);

  auto appendRowFast = [&](const RowVectorPtr& src, vector_size_t dstOffset) {
    const vector_size_t n = src->size();
    for (column_index_t c = 0; c < type->size(); ++c) {
      auto& dst = (*result)->childAt(c);
      auto& s = src->childAt(c);

      const auto expectedKind = type->childAt(c)->kind();
      bool usedFast = false;
      switch (expectedKind) {
        case TypeKind::TINYINT:
        case TypeKind::SMALLINT:
        case TypeKind::INTEGER:
        case TypeKind::BIGINT:
        case TypeKind::REAL:
        case TypeKind::DOUBLE:
          usedFast = fastAppendScalar(dst, s, dstOffset, 0, n, pool);
          if (usedFast)
            break;
          FMT_FALLTHROUGH;
        default:
          dst->copy(s.get(), dstOffset, 0, n);
          break;
      }
    }
  };

  vector_size_t out = resultOffset;

  // first batch
  appendRowFast(firstRow, out);
  out += firstRow->size();

  // remain batch
  size_t batchNo = 1;
  while (true) {
    auto nxt = reader->Next();
    BOLT_USER_CHECK(
        nxt.ok(),
        "Arrow IPC Read next RecordBatch failed: {}",
        nxt.status().ToString());
    std::shared_ptr<::arrow::RecordBatch> rb = *nxt;
    if (!rb)
      break;

    rowsRead += rb->num_rows();
    auto row = importOne(rb);

    appendRowFast(row, out);
    out += row->size();
    ++batchNo;
  }

  BOLT_USER_CHECK_EQ(
      rowsRead,
      static_cast<int64_t>(numRows),
      "Arrow page header numRows({}) != actual rows({}) in page.",
      numRows,
      rowsRead);

  if ((*result)->size() != out) {
    (*result)->resize(out);
  }

  VLOG(1) << "[deserialize][finalize] done rowsRead=" << rowsRead
          << " (input)resultOffset:" << resultOffset << " out=" << out
          << " result.size=" << ((*result) ? (*result)->size() : -1);
}

// static
void ArrowVectorSerde::registerVectorSerde() {
  bolt::registerVectorSerde(std::make_unique<ArrowVectorSerde>());
}

void ArrowVectorSerde::registerNamedVectorSerde() {
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kArrow)) {
    bolt::registerNamedVectorSerde(
        VectorSerde::Kind::kArrow, std::make_unique<ArrowVectorSerde>());
  }
}
} // namespace bytedance::bolt::serializer::arrowserde
