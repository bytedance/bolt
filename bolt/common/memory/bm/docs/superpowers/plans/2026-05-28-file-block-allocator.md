# File Block Allocator 实现计划

> **给 agentic workers：** 必须使用子技能：superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans，并按任务逐项执行本计划。步骤使用 checkbox（`- [ ]`）语法跟踪进度。

**目标：** 在 `bolt/common/memory/bm/file` 下实现一个线程安全的单例文件块分配器。调用方传入字节大小，分配器返回可用于显式 offset IO 的 `{fd, offset}`；小块按 bucket 文件复用，大块使用独立文件；支持释放、bucket 文件回收和每 bucket 最大打开文件数限制。

**架构：** 生产代码通过 singleton facade 使用，单测可以直接实例化 `FileBlockAllocatorImpl`，避免全局状态污染。bucket 分配使用每 bucket 独立锁、lazy 文件创建、显式 offset IO 兼容、打开文件数上限，以及空 bucket 文件删除。初始化和目录/文件创建等低频错误抛异常；`allocate()` / `free()` 高频路径返回错误码。

**技术栈：** C++20、`std::filesystem`、POSIX fd API（`open`、`close`、`unlink`、`pread`、`pwrite`、`fcntl`）、GTest、现有 Bolt CMake target 风格。

---

## 文件结构

- 新建 `bolt/common/memory/bm/file/FileBlockAllocatorConfig.h`：公共配置、错误码、extent 结构体、返回结构体、配置校验 helper。
- 新建 `bolt/common/memory/bm/file/FileBlockAllocator.h`：公共 singleton facade 和 allocator 抽象接口。
- 新建 `bolt/common/memory/bm/file/FileBlockAllocator.cpp`：singleton 生命周期实现。
- 新建 `bolt/common/memory/bm/file/FileBlockAllocatorImpl.h`：可测试实现类和内部元数据声明。
- 新建 `bolt/common/memory/bm/file/FileBlockAllocatorImpl.cpp`：目录初始化、bucket 分配、dedicated 分配、释放、文件删除和 fd 清理。
- 新建 `bolt/common/memory/bm/file/CMakeLists.txt`：`bolt_memory_bm_file` library target 和测试目录接入。
- 新建 `bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp`：配置校验、目录初始化、分配、释放、文件删除、显式 offset 写、singleton 和并发测试。
- 新建 `bolt/common/memory/bm/file/tests/CMakeLists.txt`：`bolt_memory_bm_file_test` executable target。
- 修改 `bolt/common/memory/bm/CMakeLists.txt`：在 `add_subdirectory(io)` 后增加 `add_subdirectory(file)`。

## 任务 1：CMake 骨架

**文件：**
- 新建：`bolt/common/memory/bm/file/CMakeLists.txt`
- 新建：`bolt/common/memory/bm/file/tests/CMakeLists.txt`
- 新建：`bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp`
- 修改：`bolt/common/memory/bm/CMakeLists.txt`

- [ ] **步骤 1：添加最小测试文件**

新建 `bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp`：

```cpp
#include <gtest/gtest.h>

TEST(FileBlockAllocatorSkeletonTest, testBinaryRuns) {
  EXPECT_TRUE(true);
}
```

- [ ] **步骤 2：添加 file 模块 CMake target**

新建 `bolt/common/memory/bm/file/CMakeLists.txt`：

```cmake
add_library(
  bolt_memory_bm_file
)

target_compile_features(bolt_memory_bm_file PUBLIC cxx_std_20)

target_include_directories(bolt_memory_bm_file PUBLIC ${PROJECT_SOURCE_DIR})

target_link_libraries(
  bolt_memory_bm_file
  PUBLIC bolt_common_base
         bolt_exception
         Folly::folly
         glog::glog
)

if(${BOLT_BUILD_TESTING})
  add_subdirectory(tests)
endif()
```

- [ ] **步骤 3：添加测试 CMake target**

新建 `bolt/common/memory/bm/file/tests/CMakeLists.txt`：

```cmake
include(GoogleTest)

add_executable(
  bolt_memory_bm_file_test
  FileBlockAllocatorTest.cpp
)

target_compile_features(bolt_memory_bm_file_test PRIVATE cxx_std_20)

target_link_libraries(
  bolt_memory_bm_file_test
  PRIVATE bolt_memory_bm_file
          GTest::gmock
          GTest::gtest
          GTest::gtest_main
          pthread
)

gtest_add_tests(bolt_memory_bm_file_test "" AUTO)

target_sources(
  bolt_memory_bm_file_test
  PRIVATE $<TARGET_OBJECTS:bolt_exception>
          $<TARGET_OBJECTS:bolt_flag_definitions>
          $<TARGET_OBJECTS:bolt_process>
)
```

- [ ] **步骤 4：把 file 目录接入 BM CMake**

修改 `bolt/common/memory/bm/CMakeLists.txt`：

```cmake
add_subdirectory(io)
add_subdirectory(file)
```

- [ ] **步骤 5：构建骨架测试**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
```

预期：构建成功，并生成 `_build/Release/bolt/common/memory/bm/file/tests/bolt_memory_bm_file_test`。

- [ ] **步骤 6：提交**

```bash
git add bolt/common/memory/bm/CMakeLists.txt bolt/common/memory/bm/file
git commit -m "Add BM file allocator test target"
```

## 任务 2：公共类型和配置校验

**文件：**
- 新建：`bolt/common/memory/bm/file/FileBlockAllocatorConfig.h`
- 修改：`bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp`

- [ ] **步骤 1：用失败测试定义配置契约**

将 `bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp` 更新为：

```cpp
#include "bolt/common/memory/bm/file/FileBlockAllocatorConfig.h"

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

using namespace bytedance::bolt::memory::bm;

namespace {

FileBlockAllocatorConfig validConfig() {
  FileBlockAllocatorConfig config;
  config.directory = "/tmp/bolt-bm-file-allocator-test";
  config.bucketSizes = {4 * 1024, 8 * 1024, 16 * 1024};
  config.fileSizeLimitBytes = 64 * 1024;
  config.maxOpenFilesPerBucket = 2;
  return config;
}

} // namespace

TEST(FileBlockAllocatorConfigTest, acceptsValidConfig) {
  EXPECT_EQ(FileErrorCode::Ok, validateFileBlockAllocatorConfig(validConfig()));
}

TEST(FileBlockAllocatorConfigTest, rejectsEmptyDirectory) {
  auto config = validConfig();
  config.directory.clear();
  EXPECT_EQ(
      FileErrorCode::InvalidConfig,
      validateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, rejectsEmptyBuckets) {
  auto config = validConfig();
  config.bucketSizes.clear();
  EXPECT_EQ(
      FileErrorCode::InvalidConfig,
      validateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, rejectsNonIncreasingBuckets) {
  auto config = validConfig();
  config.bucketSizes = {4 * 1024, 16 * 1024, 8 * 1024};
  EXPECT_EQ(
      FileErrorCode::InvalidConfig,
      validateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, rejectsDuplicateBuckets) {
  auto config = validConfig();
  config.bucketSizes = {4 * 1024, 8 * 1024, 8 * 1024};
  EXPECT_EQ(
      FileErrorCode::InvalidConfig,
      validateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, rejectsNonAlignedBucket) {
  auto config = validConfig();
  config.bucketSizes = {4 * 1024, 6 * 1024};
  EXPECT_EQ(
      FileErrorCode::InvalidConfig,
      validateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, rejectsSmallFileLimit) {
  auto config = validConfig();
  config.fileSizeLimitBytes = 8 * 1024;
  EXPECT_EQ(
      FileErrorCode::InvalidConfig,
      validateFileBlockAllocatorConfig(config));
}

TEST(FileBlockAllocatorConfigTest, rejectsZeroOpenFileLimit) {
  auto config = validConfig();
  config.maxOpenFilesPerBucket = 0;
  EXPECT_EQ(
      FileErrorCode::InvalidConfig,
      validateFileBlockAllocatorConfig(config));
}
```

- [ ] **步骤 2：运行测试并确认失败**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
```

预期：构建失败，因为 `FileBlockAllocatorConfig.h` 还不存在。

- [ ] **步骤 3：添加公共类型和配置校验**

新建 `bolt/common/memory/bm/file/FileBlockAllocatorConfig.h`：

```cpp
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace bytedance::bolt::memory::bm {

constexpr int64_t kFileBlockAlignment = 4 * 1024;

enum class FileErrorCode : uint8_t {
  Ok,
  InvalidConfig,
  InvalidSize,
  InvalidExtent,
  DoubleFree,
  TooManyOpenFiles,
  IoError,
  Shutdown,
};

enum class FileExtentKind : uint8_t {
  Bucket,
  Dedicated,
};

struct FileBlockAllocatorConfig {
  std::string directory;
  std::vector<int64_t> bucketSizes;
  int64_t fileSizeLimitBytes{0};
  uint32_t maxOpenFilesPerBucket{0};
};

struct FileExtent {
  int fd{-1};
  uint64_t offset{0};
  uint64_t requestedSize{0};
  uint64_t allocatedSize{0};
  FileExtentKind kind{FileExtentKind::Bucket};
  uint64_t id{0};
};

struct FileAllocateResult {
  FileErrorCode error{FileErrorCode::Ok};
  int nativeErrorCode{0};
  FileExtent extent;

  bool ok() const {
    return error == FileErrorCode::Ok;
  }
};

struct FileFreeResult {
  FileErrorCode error{FileErrorCode::Ok};
  int nativeErrorCode{0};

  bool ok() const {
    return error == FileErrorCode::Ok;
  }
};

inline bool isFileBlockAligned(int64_t value) {
  return value > 0 && value % kFileBlockAlignment == 0;
}

inline FileErrorCode validateFileBlockAllocatorConfig(
    const FileBlockAllocatorConfig& config) {
  if (config.directory.empty() || config.bucketSizes.empty() ||
      config.fileSizeLimitBytes <= 0 ||
      config.maxOpenFilesPerBucket == 0) {
    return FileErrorCode::InvalidConfig;
  }
  if (!isFileBlockAligned(config.fileSizeLimitBytes)) {
    return FileErrorCode::InvalidConfig;
  }
  int64_t previous = 0;
  for (const auto bucketSize : config.bucketSizes) {
    if (!isFileBlockAligned(bucketSize) || bucketSize <= previous) {
      return FileErrorCode::InvalidConfig;
    }
    previous = bucketSize;
  }
  if (config.fileSizeLimitBytes < config.bucketSizes.back()) {
    return FileErrorCode::InvalidConfig;
  }
  return FileErrorCode::Ok;
}

} // namespace bytedance::bolt::memory::bm
```

- [ ] **步骤 4：运行配置测试**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
_build/Release/bolt/common/memory/bm/file/tests/bolt_memory_bm_file_test --gtest_filter='FileBlockAllocatorConfigTest.*'
```

预期：所有 `FileBlockAllocatorConfigTest.*` 测试通过。

- [ ] **步骤 5：提交**

```bash
git add bolt/common/memory/bm/file/FileBlockAllocatorConfig.h bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp
git commit -m "Add BM file allocator config types"
```

## 任务 3：实现骨架和目录初始化

**文件：**
- 新建：`bolt/common/memory/bm/file/FileBlockAllocatorImpl.h`
- 新建：`bolt/common/memory/bm/file/FileBlockAllocatorImpl.cpp`
- 修改：`bolt/common/memory/bm/file/CMakeLists.txt`
- 修改：`bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp`

- [ ] **步骤 1：添加失败的目录初始化测试**

在 `bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp` 追加：

```cpp
#include "bolt/common/memory/bm/file/FileBlockAllocatorImpl.h"

#include <filesystem>
#include <fstream>

namespace {

std::string uniqueTempDir(const std::string& name) {
  return (std::filesystem::temp_directory_path() / name).string();
}

FileBlockAllocatorConfig validConfigWithDirectory(const std::string& path) {
  auto config = validConfig();
  config.directory = path;
  return config;
}

} // namespace

TEST(FileBlockAllocatorImplTest, removesAndRecreatesExistingDirectory) {
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-existing");
  std::filesystem::remove_all(directory);
  std::filesystem::create_directories(directory);
  {
    std::ofstream oldFile(directory + "/old-file");
    oldFile << "stale";
  }

  FileBlockAllocatorImpl allocator(validConfigWithDirectory(directory));

  EXPECT_TRUE(std::filesystem::exists(directory));
  EXPECT_FALSE(std::filesystem::exists(directory + "/old-file"));
}

TEST(FileBlockAllocatorImplTest, doesNotCreateBucketFilesDuringInit) {
  const auto directory = uniqueTempDir("bolt-bm-file-allocator-lazy-init");
  std::filesystem::remove_all(directory);

  FileBlockAllocatorImpl allocator(validConfigWithDirectory(directory));

  EXPECT_TRUE(std::filesystem::is_empty(directory));
}
```

- [ ] **步骤 2：运行测试并确认失败**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
```

预期：构建失败，因为 `FileBlockAllocatorImpl.h` 还不存在。

- [ ] **步骤 3：添加实现骨架**

新建 `bolt/common/memory/bm/file/FileBlockAllocatorImpl.h`：

```cpp
#pragma once

#include "bolt/common/memory/bm/file/FileBlockAllocatorConfig.h"

namespace bytedance::bolt::memory::bm {

class FileBlockAllocatorImpl {
 public:
  explicit FileBlockAllocatorImpl(FileBlockAllocatorConfig config);
  ~FileBlockAllocatorImpl();

  FileBlockAllocatorImpl(const FileBlockAllocatorImpl&) = delete;
  FileBlockAllocatorImpl& operator=(const FileBlockAllocatorImpl&) = delete;

  FileAllocateResult allocate(int64_t size);
  FileFreeResult free(const FileExtent& extent);

 private:
  FileBlockAllocatorConfig config_;
  bool shutdown_{false};
};

} // namespace bytedance::bolt::memory::bm
```

新建 `bolt/common/memory/bm/file/FileBlockAllocatorImpl.cpp`：

```cpp
#include "bolt/common/memory/bm/file/FileBlockAllocatorImpl.h"

#include "bolt/common/base/BoltException.h"

#include <filesystem>

namespace bytedance::bolt::memory::bm {

FileBlockAllocatorImpl::FileBlockAllocatorImpl(FileBlockAllocatorConfig config)
    : config_(std::move(config)) {
  BOLT_CHECK(
      validateFileBlockAllocatorConfig(config_) == FileErrorCode::Ok,
      "invalid FileBlockAllocatorConfig");
  std::filesystem::remove_all(config_.directory);
  std::filesystem::create_directories(config_.directory);
}

FileBlockAllocatorImpl::~FileBlockAllocatorImpl() {
  shutdown_ = true;
}

FileAllocateResult FileBlockAllocatorImpl::allocate(int64_t /*size*/) {
  return FileAllocateResult{FileErrorCode::Shutdown};
}

FileFreeResult FileBlockAllocatorImpl::free(const FileExtent& /*extent*/) {
  return FileFreeResult{FileErrorCode::Shutdown};
}

} // namespace bytedance::bolt::memory::bm
```

- [ ] **步骤 4：把实现源文件加入 CMake**

修改 `bolt/common/memory/bm/file/CMakeLists.txt` 中的 library 定义：

```cmake
add_library(
  bolt_memory_bm_file
  FileBlockAllocatorImpl.cpp
)
```

- [ ] **步骤 5：运行目录初始化测试**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
_build/Release/bolt/common/memory/bm/file/tests/bolt_memory_bm_file_test --gtest_filter='FileBlockAllocatorImplTest.removesAndRecreatesExistingDirectory:FileBlockAllocatorImplTest.doesNotCreateBucketFilesDuringInit'
```

预期：两个测试都通过。

- [ ] **步骤 6：提交**

```bash
git add bolt/common/memory/bm/file/FileBlockAllocatorImpl.h bolt/common/memory/bm/file/FileBlockAllocatorImpl.cpp bolt/common/memory/bm/file/CMakeLists.txt bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp
git commit -m "Initialize BM file allocator directory"
```

## 任务 4：Bucket 分配

**文件：**
- 修改：`bolt/common/memory/bm/file/FileBlockAllocatorImpl.h`
- 修改：`bolt/common/memory/bm/file/FileBlockAllocatorImpl.cpp`
- 修改：`bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp`

- [ ] **步骤 1：添加失败的 bucket 分配测试**

在 `bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp` 追加 bucket 选择、连续 offset、文件 rollover、`maxOpenFilesPerBucket` 四类测试。测试代码以任务 2 的 `validConfigWithDirectory()` 为基础，分别断言：

- `6K` 请求落到 `8K` bucket；
- 两个 `4K` 请求在同一个 fd 上得到 offset `0` 和 `4096`；
- `fileSizeLimitBytes=8K` 时第三个 `4K` 请求创建新 bucket 文件；
- `maxOpenFilesPerBucket=1` 且单文件已满时返回 `FileErrorCode::TooManyOpenFiles`。

- [ ] **步骤 2：运行 bucket 测试并确认失败**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
_build/Release/bolt/common/memory/bm/file/tests/bolt_memory_bm_file_test --gtest_filter='FileBlockAllocatorImplTest.allocatesRequestToFirstFittingBucket:FileBlockAllocatorImplTest.allocatesSequentialOffsetsInSameBucket:FileBlockAllocatorImplTest.createsNextBucketFileWhenCurrentFileIsFull:FileBlockAllocatorImplTest.respectsMaxOpenFilesPerBucket'
```

预期：测试失败，因为 `allocate()` 仍返回 `Shutdown`。

- [ ] **步骤 3：添加 bucket 内部元数据**

在 `FileBlockAllocatorImpl.h` 中添加：

```cpp
struct BucketFile {
  uint64_t fileIndex{0};
  std::string path;
  int fd{-1};
  uint64_t nextOffset{0};
  uint64_t activeBlocks{0};
  std::vector<uint64_t> freeOffsets;
};

struct BucketState {
  explicit BucketState(uint64_t size) : bucketSize(size) {}

  uint64_t bucketSize{0};
  std::mutex mutex;
  uint64_t nextFileIndex{0};
  std::vector<std::unique_ptr<BucketFile>> files;
};

struct ExtentRecord {
  FileExtent extent;
  size_t bucketIndex{0};
  uint64_t fileIndex{0};
  bool freed{false};
};
```

同时添加 `buckets_`、`registry_`、`nextExtentId_`、`allocateBucket()`、`findReusableBucketFileLocked()`、`createBucketFileLocked()`、`nextExtentId()`、`registerExtent()`。

- [ ] **步骤 4：实现 bucket 分配**

在 `FileBlockAllocatorImpl.cpp` 中实现：

- 构造函数根据 `config_.bucketSizes` 初始化 `buckets_`；
- `allocate(size)` 对 `size <= 0` 返回 `InvalidSize`；
- 用 `std::lower_bound` 找第一个 `bucketSize >= size` 的 bucket；
- 锁住目标 bucket；
- 优先从 `freeOffsets` 取 offset；
- 否则从 `nextOffset` 追加分配；
- 如果现有文件都没有空间，检查 `maxOpenFilesPerBucket`，未达上限则用 `open(O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600)` 创建 `bucket_<bucketSize>_<fileIndex>.bm`；
- 文件打开 flags 不能包含 `O_APPEND`；
- 分配成功后登记 `ExtentRecord` 并返回 `FileAllocateResult{FileErrorCode::Ok, 0, extent}`。

- [ ] **步骤 5：运行 bucket 分配测试**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
_build/Release/bolt/common/memory/bm/file/tests/bolt_memory_bm_file_test --gtest_filter='FileBlockAllocatorImplTest.allocatesRequestToFirstFittingBucket:FileBlockAllocatorImplTest.allocatesSequentialOffsetsInSameBucket:FileBlockAllocatorImplTest.createsNextBucketFileWhenCurrentFileIsFull:FileBlockAllocatorImplTest.respectsMaxOpenFilesPerBucket'
```

预期：所有列出的测试通过。

- [ ] **步骤 6：提交**

```bash
git add bolt/common/memory/bm/file/FileBlockAllocatorImpl.h bolt/common/memory/bm/file/FileBlockAllocatorImpl.cpp bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp
git commit -m "Add BM file bucket allocation"
```

## 任务 5：Dedicated 文件分配

**文件：**
- 修改：`bolt/common/memory/bm/file/FileBlockAllocatorImpl.h`
- 修改：`bolt/common/memory/bm/file/FileBlockAllocatorImpl.cpp`
- 修改：`bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp`

- [ ] **步骤 1：添加失败的 dedicated 分配测试**

添加 `allocatesDedicatedFileForLargeRequest` 测试：使用默认最大 bucket `16K`，分配 `128K`，断言返回 `FileExtentKind::Dedicated`、offset 为 `0`、`requestedSize == allocatedSize == 128K`、fd 有效。

- [ ] **步骤 2：运行 dedicated 测试并确认失败**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
_build/Release/bolt/common/memory/bm/file/tests/bolt_memory_bm_file_test --gtest_filter='FileBlockAllocatorImplTest.allocatesDedicatedFileForLargeRequest'
```

预期：测试失败，因为大请求仍返回 `InvalidSize`。

- [ ] **步骤 3：添加 dedicated 元数据和分配逻辑**

在 `FileBlockAllocatorImpl.h` 中补充：

```cpp
struct DedicatedFile {
  std::string path;
  int fd{-1};
};

FileAllocateResult allocateDedicated(int64_t size);

std::mutex dedicatedMutex_;
std::unordered_map<uint64_t, DedicatedFile> dedicatedFiles_;
```

在 `allocate()` 中把超过最大 bucket 的请求转给 `allocateDedicated(size)`。

`allocateDedicated()` 创建 `dedicated_<id>.bm`，打开方式为 `O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC`，返回 offset `0` 的 dedicated extent，并登记 registry 和 `dedicatedFiles_`。

- [ ] **步骤 4：运行 dedicated 测试**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
_build/Release/bolt/common/memory/bm/file/tests/bolt_memory_bm_file_test --gtest_filter='FileBlockAllocatorImplTest.allocatesDedicatedFileForLargeRequest'
```

预期：测试通过。

- [ ] **步骤 5：提交**

```bash
git add bolt/common/memory/bm/file/FileBlockAllocatorImpl.h bolt/common/memory/bm/file/FileBlockAllocatorImpl.cpp bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp
git commit -m "Add BM file dedicated allocation"
```

## 任务 6：释放、复用和文件删除

**文件：**
- 修改：`bolt/common/memory/bm/file/FileBlockAllocatorImpl.h`
- 修改：`bolt/common/memory/bm/file/FileBlockAllocatorImpl.cpp`
- 修改：`bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp`

- [ ] **步骤 1：添加失败的 free 测试**

添加以下测试：

- `reusesFreedBucketOffset`：分配一个 `4K` extent，free 后再次分配，断言 fd 和 offset 被复用。
- `rejectsDoubleFree`：同一个 extent 第二次 free 返回 `FileErrorCode::DoubleFree`。
- `deletesDedicatedFileOnFree`：dedicated extent free 后，对应 `dedicated_<id>.bm` 不存在。
- `deletesEmptyBucketFileAndReleasesOpenFileSlot`：单 block bucket 文件 free 后被删除，并释放 `maxOpenFilesPerBucket` 配额，后续可再次分配。

- [ ] **步骤 2：运行 free 测试并确认失败**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
_build/Release/bolt/common/memory/bm/file/tests/bolt_memory_bm_file_test --gtest_filter='FileBlockAllocatorImplTest.reusesFreedBucketOffset:FileBlockAllocatorImplTest.rejectsDoubleFree:FileBlockAllocatorImplTest.deletesDedicatedFileOnFree:FileBlockAllocatorImplTest.deletesEmptyBucketFileAndReleasesOpenFileSlot'
```

预期：测试失败，因为 `free()` 仍返回 `InvalidExtent`。

- [ ] **步骤 3：添加 free helper**

在 `FileBlockAllocatorImpl.h` 中补充：

```cpp
FileFreeResult freeBucket(const ExtentRecord& record);
FileFreeResult freeDedicated(const ExtentRecord& record);
BucketFile* findBucketFileByIndexLocked(
    BucketState& bucket,
    uint64_t fileIndex);
```

- [ ] **步骤 4：实现 free 逻辑**

实现规则：

- `free()` 先在 `registry_` 中按 `extent.id` 查找并移除 record；
- 未找到返回 `DoubleFree`；
- dedicated extent：从 `dedicatedFiles_` 移除，关闭 fd，删除文件；
- bucket extent：锁目标 bucket，找到 file，把 offset 放回 `freeOffsets`，`activeBlocks--`；
- 如果 bucket file 的 `activeBlocks == 0`，关闭 fd，删除文件，并从 `bucket.files` 移除。

- [ ] **步骤 5：运行 free 测试**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
_build/Release/bolt/common/memory/bm/file/tests/bolt_memory_bm_file_test --gtest_filter='FileBlockAllocatorImplTest.reusesFreedBucketOffset:FileBlockAllocatorImplTest.rejectsDoubleFree:FileBlockAllocatorImplTest.deletesDedicatedFileOnFree:FileBlockAllocatorImplTest.deletesEmptyBucketFileAndReleasesOpenFileSlot'
```

预期：测试通过。

- [ ] **步骤 6：提交**

```bash
git add bolt/common/memory/bm/file/FileBlockAllocatorImpl.h bolt/common/memory/bm/file/FileBlockAllocatorImpl.cpp bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp
git commit -m "Add BM file extent free logic"
```

## 任务 7：显式 Offset IO 行为测试

**文件：**
- 修改：`bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp`

- [ ] **步骤 1：添加乱序 offset 写测试**

添加 `supportsOutOfOrderExplicitOffsetWrites` 测试：

- 配置 bucket size 为 `4K`，file limit 为 `8K`；
- 连续分配两个 `4K` extent，断言 offset 分别为 `0` 和 `4096`；
- 用 `fcntl(fd, F_GETFL)` 检查 fd flags 不包含 `O_APPEND`；
- 先用 `pwrite()` 写 offset `4096`，再写 offset `0`；
- 用 `pread()` 分别读回两个位置；
- 断言两段数据互不覆盖。

- [ ] **步骤 2：运行显式 offset 测试**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
_build/Release/bolt/common/memory/bm/file/tests/bolt_memory_bm_file_test --gtest_filter='FileBlockAllocatorImplTest.supportsOutOfOrderExplicitOffsetWrites'
```

预期：测试通过。

- [ ] **步骤 3：提交**

```bash
git add bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp
git commit -m "Test BM file explicit offset writes"
```

## 任务 8：Singleton Facade

**文件：**
- 新建：`bolt/common/memory/bm/file/FileBlockAllocator.h`
- 新建：`bolt/common/memory/bm/file/FileBlockAllocator.cpp`
- 修改：`bolt/common/memory/bm/file/CMakeLists.txt`
- 修改：`bolt/common/memory/bm/file/FileBlockAllocatorImpl.h`
- 修改：`bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp`

- [ ] **步骤 1：添加失败的 singleton 测试**

添加：

- `allocatesThroughSingleton`：`shutdownFileBlockAllocator()` 后 init，调用 `fileBlockAllocator().allocate(4K)` 成功，再 shutdown。
- `rejectsRepeatedInitWithoutShutdown`：重复 init 抛 `bytedance::bolt::BoltException`。

- [ ] **步骤 2：运行 singleton 测试并确认失败**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
```

预期：构建失败，因为 `FileBlockAllocator.h` 还不存在。

- [ ] **步骤 3：添加 singleton facade 头文件**

新建 `bolt/common/memory/bm/file/FileBlockAllocator.h`：

```cpp
#pragma once

#include "bolt/common/memory/bm/file/FileBlockAllocatorConfig.h"

namespace bytedance::bolt::memory::bm {

class FileBlockAllocator {
 public:
  virtual ~FileBlockAllocator() = default;

  virtual FileAllocateResult allocate(int64_t size) = 0;
  virtual FileFreeResult free(const FileExtent& extent) = 0;
};

void initFileBlockAllocator(FileBlockAllocatorConfig config);
FileBlockAllocator& fileBlockAllocator();
void shutdownFileBlockAllocator();

} // namespace bytedance::bolt::memory::bm
```

- [ ] **步骤 4：让实现类继承 facade 接口**

更新 `FileBlockAllocatorImpl.h`：

```cpp
#include "bolt/common/memory/bm/file/FileBlockAllocator.h"

class FileBlockAllocatorImpl : public FileBlockAllocator {
 public:
  explicit FileBlockAllocatorImpl(FileBlockAllocatorConfig config);
  ~FileBlockAllocatorImpl() override;

  FileAllocateResult allocate(int64_t size) override;
  FileFreeResult free(const FileExtent& extent) override;
```

- [ ] **步骤 5：添加 singleton 实现**

新建 `bolt/common/memory/bm/file/FileBlockAllocator.cpp`：

```cpp
#include "bolt/common/memory/bm/file/FileBlockAllocator.h"

#include "bolt/common/base/BoltException.h"
#include "bolt/common/memory/bm/file/FileBlockAllocatorImpl.h"

#include <memory>
#include <mutex>

namespace bytedance::bolt::memory::bm {

namespace {

std::mutex gAllocatorMutex;
std::unique_ptr<FileBlockAllocatorImpl> gAllocator;

} // namespace

void initFileBlockAllocator(FileBlockAllocatorConfig config) {
  std::lock_guard<std::mutex> lock(gAllocatorMutex);
  BOLT_CHECK(gAllocator == nullptr, "FileBlockAllocator already initialized");
  gAllocator = std::make_unique<FileBlockAllocatorImpl>(std::move(config));
}

FileBlockAllocator& fileBlockAllocator() {
  std::lock_guard<std::mutex> lock(gAllocatorMutex);
  BOLT_CHECK(gAllocator != nullptr, "FileBlockAllocator is not initialized");
  return *gAllocator;
}

void shutdownFileBlockAllocator() {
  std::lock_guard<std::mutex> lock(gAllocatorMutex);
  gAllocator.reset();
}

} // namespace bytedance::bolt::memory::bm
```

- [ ] **步骤 6：把 facade 源文件加入 CMake**

更新 `bolt/common/memory/bm/file/CMakeLists.txt`：

```cmake
add_library(
  bolt_memory_bm_file
  FileBlockAllocator.cpp
  FileBlockAllocatorImpl.cpp
)
```

- [ ] **步骤 7：运行 singleton 测试**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
_build/Release/bolt/common/memory/bm/file/tests/bolt_memory_bm_file_test --gtest_filter='FileBlockAllocatorSingletonTest.*'
```

预期：singleton 测试通过。

- [ ] **步骤 8：提交**

```bash
git add bolt/common/memory/bm/file/FileBlockAllocator.h bolt/common/memory/bm/file/FileBlockAllocator.cpp bolt/common/memory/bm/file/FileBlockAllocatorImpl.h bolt/common/memory/bm/file/CMakeLists.txt bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp
git commit -m "Add BM file allocator singleton"
```

## 任务 9：并发测试

**文件：**
- 修改：`bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp`

- [ ] **步骤 1：添加同 bucket 并发分配测试**

添加 `concurrentSameBucketAllocationsAreUnique` 测试：

- 配置单个 `4K` bucket，较大的 `fileSizeLimitBytes`，`maxOpenFilesPerBucket=8`；
- 8 个线程，每个线程分配 128 个 extent；
- 收集所有 `{fd, offset}`；
- 用 `std::set<std::pair<int, uint64_t>>` 断言所有 alive extent 的位置都唯一。

- [ ] **步骤 2：添加并发 allocate/free 测试**

添加 `concurrentAllocateFreeCompletes` 测试：

- 配置 `4K` 和 `8K` 两个 bucket；
- 8 个线程循环 allocate/free；
- 每次 allocate 和 free 都必须返回 ok。

- [ ] **步骤 3：运行并发测试**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
_build/Release/bolt/common/memory/bm/file/tests/bolt_memory_bm_file_test --gtest_filter='FileBlockAllocatorImplTest.concurrentSameBucketAllocationsAreUnique:FileBlockAllocatorImplTest.concurrentAllocateFreeCompletes'
```

预期：两个测试都通过，可重复运行。

- [ ] **步骤 4：提交**

```bash
git add bolt/common/memory/bm/file/tests/FileBlockAllocatorTest.cpp
git commit -m "Test BM file allocator concurrency"
```

## 任务 10：最终验证

**文件：**
- 预期无代码变更。

- [ ] **步骤 1：运行全部 BM file allocator 测试**

运行：

```bash
cmake --build _build/Release --target bolt_memory_bm_file_test -j 8
_build/Release/bolt/common/memory/bm/file/tests/bolt_memory_bm_file_test
```

预期：全部 BM file allocator 测试通过。

- [ ] **步骤 2：检查工作区状态**

运行：

```bash
git status --short
```

预期：只包含本计划涉及的 BM file allocator 文件。已有的无关用户改动，例如 `Makefile`，不能被修改或回滚。

- [ ] **步骤 3：可选更大范围构建**

仅在用户要求或 CMake 接入风险需要更大范围验证时运行：

```bash
PATH=/data00/home/wangxinshuo.db/tools/miniconda3/bin:$PATH make release_with_test
```

预期：release 构建和测试成功。

## 自查

- 需求覆盖：计划覆盖 singleton 生命周期、配置校验、初始化删除目录、严格递增且 4K 对齐的 buckets、lazy bucket 文件创建、每 bucket 独立锁、每 bucket 最大打开文件数、dedicated 文件、释放和删除、显式 offset 写行为、并发测试。
- 占位检查：没有开放式占位说明；每个实现任务都包含明确文件路径、代码片段、命令和预期结果。
- 类型一致性：公共类型在任务 2 定义，并在实现、singleton facade 和测试中保持一致。
