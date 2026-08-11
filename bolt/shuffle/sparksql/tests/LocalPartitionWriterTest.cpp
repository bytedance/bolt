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

#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>
#include <arrow/result.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

namespace bytedance::bolt::shuffle::sparksql {

// Defined in partition_writer/LocalPartitionWriter.cpp. Opens a spill file for
// append. When 'bufferWrite' is true the raw file stream is wrapped in a 16KB
// arrow::io::BufferedOutputStream (buffer allocated from 'pool'); otherwise the
// raw arrow::io::FileOutputStream is returned as-is.
arrow::Result<std::shared_ptr<arrow::io::OutputStream>> openSpillFile(
    const std::string& file,
    bool bufferWrite,
    arrow::MemoryPool* pool);

} // namespace bytedance::bolt::shuffle::sparksql

namespace bytedance::bolt::shuffle::sparksql::test {
namespace {

// The buffer size hard-coded in openSpillFile's BufferedOutputStream.
constexpr int64_t kSpillBufferSize = 16384;

void writeAll(arrow::io::OutputStream* os, std::string_view data) {
  ASSERT_TRUE(os->Write(data.data(), static_cast<int64_t>(data.size())).ok());
}

std::string readFile(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  return std::string(
      std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

class OpenSpillFileTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    tempDir_ = std::filesystem::temp_directory_path() /
        (std::string("bolt_open_spill_file_") + info->name());
    std::filesystem::remove_all(tempDir_);
    std::filesystem::create_directories(tempDir_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(tempDir_, ec);
  }

  std::string filePath(const std::string& name) const {
    return (tempDir_ / name).string();
  }

  std::filesystem::path tempDir_;
};

// Unbuffered mode returns the raw file stream, so bytes hit the file
// immediately (visible before Close) and the memory pool is never touched.
TEST_F(OpenSpillFileTest, UnbufferedWriteIsImmediateOnDisk) {
  auto pool = arrow::MemoryPool::CreateDefault();
  const auto file = filePath("unbuffered.bin");

  auto maybeOs = openSpillFile(file, /*bufferWrite=*/false, pool.get());
  ASSERT_TRUE(maybeOs.ok()) << maybeOs.status().ToString();
  auto os = maybeOs.ValueOrDie();

  const std::string data(1000, 'a');
  writeAll(os.get(), data);

  // No buffering: the data is already on disk before Close().
  EXPECT_EQ(std::filesystem::file_size(file), data.size());
  // The raw FileOutputStream does not allocate from the pool.
  EXPECT_EQ(pool->bytes_allocated(), 0);

  ASSERT_TRUE(os->Close().ok());
  EXPECT_EQ(readFile(file), data);
}

// Buffered mode holds small writes (< 16KB) in an in-memory buffer charged to
// the provided pool, flushing to the file only on Close().
TEST_F(OpenSpillFileTest, BufferedWriteIsDelayedUntilClose) {
  auto pool = arrow::MemoryPool::CreateDefault();
  const auto file = filePath("buffered.bin");

  auto maybeOs = openSpillFile(file, /*bufferWrite=*/true, pool.get());
  ASSERT_TRUE(maybeOs.ok()) << maybeOs.status().ToString();
  auto os = maybeOs.ValueOrDie();

  const std::string data(1000, 'b'); // smaller than the 16KB buffer
  writeAll(os.get(), data);

  // Still buffered in memory: nothing has reached the file yet.
  EXPECT_EQ(std::filesystem::file_size(file), 0u);
  // The 16KB write buffer is charged to the supplied pool.
  EXPECT_GE(pool->bytes_allocated(), kSpillBufferSize);

  ASSERT_TRUE(os->Close().ok());
  EXPECT_EQ(std::filesystem::file_size(file), data.size());
  EXPECT_EQ(readFile(file), data);
}

// A single write larger than the 16KB buffer cannot be held, so it is flushed
// straight through to disk (bytes visible before Close()).
TEST_F(OpenSpillFileTest, BufferedLargeWriteOverflowsToDisk) {
  auto pool = arrow::MemoryPool::CreateDefault();
  const auto file = filePath("buffered_large.bin");

  auto maybeOs = openSpillFile(file, /*bufferWrite=*/true, pool.get());
  ASSERT_TRUE(maybeOs.ok()) << maybeOs.status().ToString();
  auto os = maybeOs.ValueOrDie();

  const std::string big(64 * 1024, 'q'); // 64KB > 16KB buffer
  writeAll(os.get(), big);

  // Overflowed the buffer, so at least part has already been written out.
  EXPECT_GT(std::filesystem::file_size(file), 0u);

  ASSERT_TRUE(os->Close().ok());
  EXPECT_EQ(readFile(file), big);
}

// Regardless of buffering, a stream of mixed small/large writes must produce
// byte-identical file content.
TEST_F(OpenSpillFileTest, BufferedAndUnbufferedProduceIdenticalContent) {
  const std::vector<std::string> chunks = {
      std::string(10, 'x'),
      std::string(50 * 1024, 'y'), // exceeds the 16KB buffer
      std::string(100, 'z'),
      std::string(5, 'w'),
  };
  std::string expected;
  for (const auto& c : chunks) {
    expected += c;
  }

  const auto bufferedFile = filePath("mixed_buffered.bin");
  const auto unbufferedFile = filePath("mixed_unbuffered.bin");

  for (bool buffered : {true, false}) {
    auto pool = arrow::MemoryPool::CreateDefault();
    const auto file = buffered ? bufferedFile : unbufferedFile;
    auto maybeOs = openSpillFile(file, buffered, pool.get());
    ASSERT_TRUE(maybeOs.ok()) << maybeOs.status().ToString();
    auto os = maybeOs.ValueOrDie();
    for (const auto& c : chunks) {
      writeAll(os.get(), c);
    }
    ASSERT_TRUE(os->Close().ok());
  }

  EXPECT_EQ(readFile(bufferedFile), expected);
  EXPECT_EQ(readFile(unbufferedFile), expected);
  EXPECT_EQ(readFile(bufferedFile), readFile(unbufferedFile));
}

// openSpillFile opens the file in append mode, so writing to an existing file
// preserves its prior content instead of truncating it.
TEST_F(OpenSpillFileTest, OpensInAppendMode) {
  const auto file = filePath("append.bin");
  {
    std::ofstream out(file, std::ios::binary);
    out << "HEAD";
  }

  auto pool = arrow::MemoryPool::CreateDefault();
  auto maybeOs = openSpillFile(file, /*bufferWrite=*/false, pool.get());
  ASSERT_TRUE(maybeOs.ok()) << maybeOs.status().ToString();
  auto os = maybeOs.ValueOrDie();

  writeAll(os.get(), "TAIL");
  ASSERT_TRUE(os->Close().ok());

  EXPECT_EQ(readFile(file), "HEADTAIL");
}

} // namespace
} // namespace bytedance::bolt::shuffle::sparksql::test
