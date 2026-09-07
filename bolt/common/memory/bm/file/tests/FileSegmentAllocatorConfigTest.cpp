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

#include "bolt/common/memory/bm/file/FileSegmentAllocatorConfig.h"
#include "bolt/common/memory/bm/file/tests/FileSegmentAllocatorTestUtil.h"

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;
using namespace bytedance::bolt::memory::bm::test;

TEST(FileSegmentAllocatorConfigTest, AcceptsValidConfig) {
  EXPECT_EQ(
      FileErrorCode::kOk, ValidateFileSegmentAllocatorConfig(ValidConfig()));
}

TEST(FileSegmentAllocatorConfigTest, RejectsEmptyDirectory) {
  auto config = ValidConfig();
  config.directory.clear();
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileSegmentAllocatorConfig(config));
}

TEST(FileSegmentAllocatorConfigTest, RejectsEmptyBuckets) {
  auto config = ValidConfig();
  config.bucket_sizes.clear();
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileSegmentAllocatorConfig(config));
}

TEST(FileSegmentAllocatorConfigTest, RejectsNonIncreasingBuckets) {
  auto config = ValidConfig();
  config.bucket_sizes = {4 * 1024, 16 * 1024, 8 * 1024};
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileSegmentAllocatorConfig(config));
}

TEST(FileSegmentAllocatorConfigTest, RejectsDuplicateBuckets) {
  auto config = ValidConfig();
  config.bucket_sizes = {4 * 1024, 8 * 1024, 8 * 1024};
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileSegmentAllocatorConfig(config));
}

TEST(FileSegmentAllocatorConfigTest, RejectsNonAlignedBucket) {
  auto config = ValidConfig();
  config.bucket_sizes = {4 * 1024, 6 * 1024};
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileSegmentAllocatorConfig(config));
}

TEST(FileSegmentAllocatorConfigTest, RejectsSmallFileLimit) {
  auto config = ValidConfig();
  config.file_size_limit_bytes = 8 * 1024;
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileSegmentAllocatorConfig(config));
}

TEST(FileSegmentAllocatorConfigTest, RejectsZeroOpenFileLimit) {
  auto config = ValidConfig();
  config.max_open_files_per_bucket = 0;
  EXPECT_EQ(
      FileErrorCode::kInvalidConfig,
      ValidateFileSegmentAllocatorConfig(config));
}
