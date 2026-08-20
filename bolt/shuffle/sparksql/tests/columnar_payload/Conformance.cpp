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

#include "bolt/shuffle/sparksql/tests/columnar_payload/Conformance.h"


namespace bytedance::bolt::shuffle::sparksql::test {
namespace {

class ReferenceWriter : public PayloadWriter {
 public:
  ReferenceWriter(Codec& codec, GeneratorOptions options)
      : codec_(codec), generator_(&codec, std::move(options)) {}

  const char* name() const override {
    return "reference";
  }

  bool write(
      const RowVectorPtr& input,
      std::vector<uint8_t>& out,
      std::string& error) override {
    GeneratedPayload generated;
    if (!generatePayload(generator_, input, generated, error)) {
      return false;
    }
    out = std::move(generated.bytes);
    return true;
  }

  Codec& codec() override {
    return codec_;
  }

 private:
  Codec& codec_;
  ColumnarPayloadGenerator generator_;
};

class ReferenceReader : public PayloadReader {
 public:
  ReferenceReader(
      Codec& codec,
      memory::MemoryPool* pool,
      ValidationOptions options)
      : codec_(codec), pool_(pool), options_(std::move(options)) {}

  const char* name() const override {
    return "reference";
  }

  Codec& codec() override {
    return codec_;
  }

  bool read(
      const std::vector<uint8_t>& payload,
      const RowTypePtr& rowType,
      RowVectorPtr& out,
      std::string& error) override {
    std::vector<PhysicalType> schema;
    schema.reserve(rowType->size());
    for (uint32_t child = 0; child < rowType->size(); ++child) {
      PhysicalType physical{};
      if (!toPhysicalType(rowType->childAt(child), physical)) {
        error = "unsupported column type";
        return false;
      }
      schema.push_back(physical);
    }

    auto options = options_;
    options.payloadSizeProvided = true;
    options.payloadSize = payload.size();
    ColumnarPayloadValidator validator(&codec_, schema, options);

    const auto decoded = validatePayload(validator, payload, rowType, pool_);
    if (!decoded.result.ok()) {
      error = decoded.result.describe();
      return false;
    }
    if (decoded.decoded == nullptr) {
      error = "payload parsed but carried no column data";
      return false;
    }
    out = decoded.decoded;
    return true;
  }

 private:
  Codec& codec_;
  memory::MemoryPool* pool_;
  ValidationOptions options_;
};

/// Compares two row vectors position by position, returning a description of
/// the first difference. Used instead of assertEqualVectors so that the suite
/// can report rather than abort, which matters when it is driving an
/// implementation that is still being written.
std::string firstDifference(
    const RowVectorPtr& expected,
    const RowVectorPtr& actual) {
  if (actual == nullptr) {
    return "decoded vector is null";
  }
  if (expected->size() != actual->size()) {
    return "row count " + std::to_string(actual->size()) + ", expected " +
        std::to_string(expected->size());
  }
  const auto expectedType = asRowType(expected->type());
  const auto actualType = asRowType(actual->type());
  if (actualType->size() != expectedType->size()) {
    return "column count " + std::to_string(actualType->size()) +
        ", expected " + std::to_string(expectedType->size());
  }
  for (uint32_t column = 0; column < expectedType->size(); ++column) {
    if (!expectedType->childAt(column)->equivalent(
            *actualType->childAt(column))) {
      return "column " + std::to_string(column) + " has type " +
          actualType->childAt(column)->toString() + ", expected " +
          expectedType->childAt(column)->toString();
    }
    const auto& left = expected->childAt(column);
    const auto& right = actual->childAt(column);
    for (vector_size_t row = 0; row < expected->size(); ++row) {
      const bool leftNull = left->isNullAt(row);
      if (leftNull != right->isNullAt(row)) {
        return "column " + std::to_string(column) + " row " +
            std::to_string(row) + ": null mismatch";
      }
      if (!leftNull && !left->equalValueAt(right.get(), row, row)) {
        return "column " + std::to_string(column) + " row " +
            std::to_string(row) + ": value mismatch";
      }
    }
  }
  return {};
}

/// Ways of breaking a payload that an L1 conforming Reader has to notice.
/// Each returns false when the shape does not apply to this payload.
struct Corruption {
  const char* name;
  bool (*apply)(std::vector<uint8_t>&);
};

bool truncateHalf(std::vector<uint8_t>& payload) {
  if (payload.size() < 4) {
    return false;
  }
  payload.resize(payload.size() / 2);
  return true;
}

bool reservedNullTag(std::vector<uint8_t>& payload) {
  // The Null body starts right after the 24 byte fixed header.
  if (payload.size() <= kFixedHeaderBytes) {
    return false;
  }
  payload[kFixedHeaderBytes] =
      static_cast<uint8_t>(payload[kFixedHeaderBytes] | 0x03);
  return true;
}

bool inflatedRowCount(std::vector<uint8_t>& payload) {
  if (payload.size() < 4) {
    return false;
  }
  payload[0] = static_cast<uint8_t>(payload[0] + 1);
  payload[3] = static_cast<uint8_t>(payload[3] | 0x40);
  return true;
}

constexpr Corruption kCorruptions[] = {
    {"truncated to half", truncateHalf},
    {"reserved NullTag", reservedNullTag},
    {"inflated row_count", inflatedRowCount},
};

} // namespace

std::unique_ptr<PayloadWriter> makeReferenceWriter(
    Codec& codec,
    GeneratorOptions options) {
  return std::make_unique<ReferenceWriter>(codec, std::move(options));
}

std::unique_ptr<PayloadReader> makeReferenceReader(
    Codec& codec,
    memory::MemoryPool* pool,
    ValidationOptions options) {
  return std::make_unique<ReferenceReader>(codec, pool, std::move(options));
}

std::string ConformanceReport::describe() const {
  std::string text = "ran " + std::to_string(casesRun) + " cases, skipped " +
      std::to_string(casesSkipped) + ", " + std::to_string(payloadBytes) +
      " payload bytes";
  if (failures.empty()) {
    return text + "; conforming";
  }
  text += "\n";
  for (const auto& failure : failures) {
    text += "  " + failure + "\n";
  }
  return text;
}

ConformanceReport runConformanceSuite(
    PayloadWriter& writer,
    PayloadReader& reader,
    memory::MemoryPool* pool,
    ConformanceOptions options) {
  ConformanceReport report;

  const auto fail = [&](const std::string& message) {
    if (report.failures.size() < options.maxFailures) {
      report.failures.push_back(message);
    }
  };

  // A payload carries no record of how it was compressed, so a Reader holding
  // a different codec decodes garbage with nothing to flag it. Catch the
  // misconfiguration here, where it can be named.
  if (std::string(writer.codec().name()) != reader.codec().name()) {
    fail(std::string("writer compresses with ") + writer.codec().name() +
         " but reader decompresses with " + reader.codec().name());
    return report;
  }

  // The reference validator judges conformance independently of the Reader,
  // so a Writer cannot pass by emitting bytes only its own Reader accepts.
  ReferenceReader judge(writer.codec(), pool, ValidationOptions{});

  for (const auto& entry : boundaryVectorCorpus(pool)) {
    const auto rowType = asRowType(entry.vector->type());
    if (!writer.supports(rowType) || !reader.supports(rowType)) {
      ++report.casesSkipped;
      continue;
    }
    const std::string where = std::string(entry.name) + " [" + writer.name() +
        " -> " + reader.name() + "]";

    std::vector<uint8_t> payload;
    std::string error;
    if (!writer.write(entry.vector, payload, error)) {
      fail(where + ": write failed: " + error);
      continue;
    }
    ++report.casesRun;
    report.payloadBytes += payload.size();

    RowVectorPtr judged;
    if (!judge.read(payload, rowType, judged, error)) {
      fail(where + ": payload is not conforming: " + error);
      continue;
    }

    RowVectorPtr decoded;
    if (!reader.read(payload, rowType, decoded, error)) {
      fail(where + ": read failed: " + error);
      continue;
    }
    const auto difference = firstDifference(entry.vector, decoded);
    if (!difference.empty()) {
      fail(where + ": " + difference);
      continue;
    }

    if (!options.checkRejection || !reader.rejectsMalformed()) {
      continue;
    }
    for (const auto& corruption : kCorruptions) {
      auto broken = payload;
      if (!corruption.apply(broken)) {
        continue;
      }
      RowVectorPtr ignored;
      std::string readerError;
      if (reader.read(broken, rowType, ignored, readerError)) {
        fail(where + ": accepted a payload with " +
             std::string(corruption.name));
      }
    }
  }

  return report;
}

} // namespace bytedance::bolt::shuffle::sparksql::test
