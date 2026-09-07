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

#include "bolt/common/memory/bm/compress/OpenZlCompression.h"

#include "bolt/common/base/Exceptions.h"

#include <openzl/codecs/zl_conversion.h>
#include <openzl/codecs/zl_field_lz.h>
#include <openzl/codecs/zl_split.h>
#include <openzl/codecs/zl_split_by_struct.h>
#include <openzl/codecs/zl_transpose.h>
#include <openzl/codecs/zl_zstd.h>
#include <openzl/openzl.h>
#include <openzl/zl_decompress.h>

#include <array>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

namespace bytedance::bolt::memory::bm::compress {
namespace {

using CompressorPtr =
    std::unique_ptr<ZL_Compressor, decltype(&ZL_Compressor_free)>;

void checkReport(
    ZL_Report report,
    const std::string& fingerprint,
    const char* operation) {
  if (ZL_isError(report)) {
    BOLT_FAIL(
        "BM OpenZL {} failed, schema={}, error={}",
        operation,
        fingerprint,
        ZL_ErrorCode_toString(ZL_errorCode(report)));
  }
}

void checkGraph(
    ZL_GraphID graph,
    const std::string& fingerprint,
    const char* operation) {
  BOLT_CHECK(
      ZL_GraphID_isValid(graph),
      "BM OpenZL {} returned an invalid graph, schema={}",
      operation,
      fingerprint);
}

bool isSupportedIntegerWidth(uint32_t width) {
  return width == 1 || width == 2 || width == 4 || width == 8;
}

bool isSupportedFloatingWidth(uint32_t width) {
  return width == 4 || width == 8;
}

} // namespace

OpenZlCompressor::OpenZlCompressor(void* native, std::string fingerprint)
    : native(native), fingerprint(std::move(fingerprint)) {
  BOLT_CHECK_NOT_NULL(native);
}

OpenZlCompressor::~OpenZlCompressor() {
  ZL_Compressor_free(static_cast<ZL_Compressor*>(native));
}

OpenZlCompressionContext::OpenZlCompressionContext()
    : native(ZL_CCtx_create()) {
  BOLT_CHECK_NOT_NULL(native);
}

OpenZlCompressionContext::~OpenZlCompressionContext() {
  ZL_CCtx_free(static_cast<ZL_CCtx*>(native));
}

OpenZlDecompressionContext::OpenZlDecompressionContext()
    : native(ZL_DCtx_create()) {
  BOLT_CHECK_NOT_NULL(native);
}

OpenZlDecompressionContext::~OpenZlDecompressionContext() {
  ZL_DCtx_free(static_cast<ZL_DCtx*>(native));
}

bool OpenZlSupportsDescriptor(
    const BlockDescriptor& descriptor,
    size_t blockSize) {
  ValidateBlockDescriptor(descriptor, blockSize);
  if (descriptor.schemaKind != BlockSchemaKind::kFixedRow ||
      descriptor.elementCount == 0) {
    return false;
  }
  const auto& schema = std::get<FixedRowBlockSchema>(descriptor.schema);
  for (const auto& field : NormalizeFixedRowFields(schema)) {
    switch (field.kind) {
      case BlockFieldKind::kSignedInteger:
      case BlockFieldKind::kUnsignedInteger:
        if (!isSupportedIntegerWidth(field.width)) {
          return false;
        }
        break;
      case BlockFieldKind::kFloatingPoint:
        if (!isSupportedFloatingWidth(field.width)) {
          return false;
        }
        break;
      case BlockFieldKind::kOpaque:
        break;
    }
  }
  return true;
}

std::string OpenZlDescriptorFingerprint(
    const BlockDescriptor& descriptor,
    size_t blockSize) {
  ValidateBlockDescriptor(descriptor, blockSize);
  BOLT_CHECK(
      descriptor.schemaKind == BlockSchemaKind::kFixedRow,
      "BM OpenZL fingerprint requires a fixed-row descriptor");
  const auto& schema = std::get<FixedRowBlockSchema>(descriptor.schema);
  std::string result = "fixed-row:";
  result.append(std::to_string(schema.rowStride));
  result.push_back(':');
  result.append(std::to_string(descriptor.elementCount));
  result.push_back(':');
  result.append(std::to_string(blockSize));
  for (const auto& field : NormalizeFixedRowFields(schema)) {
    result.push_back(':');
    result.append(std::to_string(static_cast<uint8_t>(field.kind)));
    result.push_back(',');
    result.append(std::to_string(field.offset));
    result.push_back(',');
    result.append(std::to_string(field.width));
  }
  return result;
}

std::shared_ptr<const OpenZlCompressor> BuildOpenZlCompressor(
    const BlockDescriptor& descriptor,
    size_t blockSize,
    std::string fingerprint) {
  BOLT_CHECK(OpenZlSupportsDescriptor(descriptor, blockSize));
  const auto& schema = std::get<FixedRowBlockSchema>(descriptor.schema);
  const auto fields = NormalizeFixedRowFields(schema);

  CompressorPtr compressor(ZL_Compressor_create(), ZL_Compressor_free);
  BOLT_CHECK_NOT_NULL(compressor);

  const auto integerGraph = ZL_Compressor_registerStaticGraph_fromNode1o(
      compressor.get(), ZL_NODE_INTERPRET_TOKEN_AS_LE, ZL_GRAPH_FIELD_LZ);
  checkGraph(integerGraph, fingerprint, "integer graph creation");
  const auto floatingGraph = ZL_Compressor_registerStaticGraph_fromNode1o(
      compressor.get(), ZL_NODE_TRANSPOSE_SPLIT, ZL_GRAPH_ZSTD);
  checkGraph(floatingGraph, fingerprint, "float graph creation");

  std::vector<size_t> fieldSizes;
  std::vector<ZL_GraphID> successors;
  fieldSizes.reserve(fields.size());
  successors.reserve(fields.size());
  for (const auto& field : fields) {
    fieldSizes.push_back(field.width);
    switch (field.kind) {
      case BlockFieldKind::kSignedInteger:
      case BlockFieldKind::kUnsignedInteger:
        successors.push_back(integerGraph);
        break;
      case BlockFieldKind::kFloatingPoint:
        successors.push_back(floatingGraph);
        break;
      case BlockFieldKind::kOpaque:
        successors.push_back(ZL_GRAPH_ZSTD);
        break;
    }
  }

  const auto fixedRowGraph = ZL_Compressor_registerSplitByStructGraph(
      compressor.get(),
      fieldSizes.data(),
      successors.data(),
      successors.size());
  checkGraph(fixedRowGraph, fingerprint, "fixed-row graph creation");

  const auto structuredBytes =
      static_cast<size_t>(descriptor.elementCount) * schema.rowStride;
  auto startingGraph = fixedRowGraph;
  if (structuredBytes < blockSize) {
    const std::array<size_t, 2> segmentSizes{structuredBytes, 0};
    const std::array<ZL_GraphID, 2> segmentGraphs{fixedRowGraph, ZL_GRAPH_ZSTD};
    startingGraph = ZL_Compressor_registerSplitGraph(
        compressor.get(),
        ZL_Type_serial,
        segmentSizes.data(),
        segmentGraphs.data(),
        segmentGraphs.size());
    checkGraph(startingGraph, fingerprint, "tail graph creation");
  }

  checkReport(
      ZL_Compressor_setParameter(
          compressor.get(), ZL_CParam_formatVersion, ZL_MAX_FORMAT_VERSION),
      fingerprint,
      "format configuration");
  checkReport(
      ZL_Compressor_setParameter(
          compressor.get(), ZL_CParam_compressionLevel, 3),
      fingerprint,
      "level configuration");
  checkReport(
      ZL_Compressor_selectStartingGraphID(compressor.get(), startingGraph),
      fingerprint,
      "graph validation");

  return std::make_shared<const OpenZlCompressor>(
      compressor.release(), std::move(fingerprint));
}

size_t OpenZlInitialCompressedCapacity(size_t rawSize) {
  BOLT_CHECK_LE(
      rawSize,
      std::numeric_limits<size_t>::max() / 2,
      "BM OpenZL input is too large to calculate compression capacity");
  const auto capacity = ZL_compressBound(rawSize);
  BOLT_CHECK_GE(capacity, rawSize);
  return capacity;
}

OpenZlCompressionResult OpenZlCompress(
    OpenZlCompressionContext& context,
    const OpenZlCompressor& compressor,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetCapacity) {
  auto* cctx = static_cast<ZL_CCtx*>(context.native);
  checkReport(
      ZL_CCtx_refCompressor(
          cctx, static_cast<const ZL_Compressor*>(compressor.native)),
      compressor.fingerprint,
      "compressor binding");
  const auto report =
      ZL_CCtx_compress(cctx, target, targetCapacity, source, sourceSize);
  if (ZL_isError(report)) {
    if (ZL_errorCode(report) == ZL_ErrorCode_dstCapacity_tooSmall) {
      return OpenZlCompressionResult{.capacityTooSmall = true};
    }
    checkReport(report, compressor.fingerprint, "fixed-row compression");
  }
  return OpenZlCompressionResult{.size = ZL_validResult(report)};
}

void OpenZlDecompress(
    OpenZlDecompressionContext& context,
    const char* source,
    size_t sourceSize,
    char* target,
    size_t targetSize,
    uint64_t blockId) {
  const auto report = ZL_DCtx_decompress(
      static_cast<ZL_DCtx*>(context.native),
      target,
      targetSize,
      source,
      sourceSize);
  if (ZL_isError(report)) {
    BOLT_FAIL(
        "BM OpenZL decompression failed, block_id={}, error={}",
        blockId,
        ZL_ErrorCode_toString(ZL_errorCode(report)));
  }
  BOLT_CHECK_EQ(
      ZL_validResult(report),
      targetSize,
      "BM OpenZL decompressed size mismatch, block_id={}",
      blockId);
}

} // namespace bytedance::bolt::memory::bm::compress
