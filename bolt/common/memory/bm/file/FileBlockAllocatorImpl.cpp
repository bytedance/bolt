#include "bolt/common/memory/bm/file/FileBlockAllocatorImpl.h"

#include "bolt/common/base/BoltException.h"
#include "bolt/common/base/Exceptions.h"

#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <string>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

namespace bytedance::bolt::memory::bm {

namespace {

void closeFd(int fd) {
  if (fd >= 0) {
    ::close(fd);
  }
}

} // namespace

FileBlockAllocatorImpl::FileBlockAllocatorImpl(FileBlockAllocatorConfig config)
    : config_(std::move(config)) {
  BOLT_CHECK(
      validateFileBlockAllocatorConfig(config_) == FileErrorCode::Ok,
      "invalid FileBlockAllocatorConfig");
  std::filesystem::remove_all(config_.directory);
  std::filesystem::create_directories(config_.directory);
  buckets_.reserve(config_.bucketSizes.size());
  for (const auto bucketSize : config_.bucketSizes) {
    buckets_.push_back(std::make_unique<BucketState>(bucketSize));
  }
}

FileBlockAllocatorImpl::~FileBlockAllocatorImpl() {
  shutdown_ = true;
  for (auto& bucket : buckets_) {
    std::lock_guard<std::mutex> lock(bucket->mutex);
    for (auto& file : bucket->files) {
      closeFd(file->fd);
      std::filesystem::remove(file->path);
      file->fd = -1;
    }
    bucket->files.clear();
  }
  {
    std::lock_guard<std::mutex> lock(dedicatedMutex_);
    for (auto& [_, file] : dedicatedFiles_) {
      closeFd(file.fd);
      std::filesystem::remove(file.path);
      file.fd = -1;
    }
    dedicatedFiles_.clear();
  }
}

FileAllocateResult FileBlockAllocatorImpl::allocate(int64_t size) {
  if (shutdown_) {
    FileAllocateResult result;
    result.error = FileErrorCode::Shutdown;
    return result;
  }
  if (size <= 0) {
    FileAllocateResult result;
    result.error = FileErrorCode::InvalidSize;
    return result;
  }
  const auto it = std::lower_bound(
      config_.bucketSizes.begin(), config_.bucketSizes.end(), size);
  if (it == config_.bucketSizes.end()) {
    return allocateDedicated(size);
  }
  return allocateBucket(
      size, static_cast<size_t>(it - config_.bucketSizes.begin()));
}

FileAllocateResult FileBlockAllocatorImpl::allocateDedicated(int64_t size) {
  const auto extentId = nextExtentId();
  const auto path =
      config_.directory + "/dedicated_" + std::to_string(extentId) + ".bm";
  const int fd =
      ::open(path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) {
    FileAllocateResult result;
    result.error = FileErrorCode::IoError;
    result.nativeErrorCode = errno;
    return result;
  }

  FileExtent extent;
  extent.fd = fd;
  extent.offset = 0;
  extent.requestedSize = static_cast<uint64_t>(size);
  extent.allocatedSize = static_cast<uint64_t>(size);
  extent.kind = FileExtentKind::Dedicated;
  extent.id = extentId;

  {
    std::lock_guard<std::mutex> lock(dedicatedMutex_);
    dedicatedFiles_.emplace(extentId, DedicatedFile{path, fd});
  }
  ExtentRecord record;
  record.extent = extent;
  registerExtent(std::move(record));

  FileAllocateResult result;
  result.extent = extent;
  return result;
}

FileAllocateResult FileBlockAllocatorImpl::allocateBucket(
    int64_t size,
    size_t bucketIndex) {
  const auto extentId = nextExtentId();
  FileExtent extent;
  ExtentRecord record;

  {
    auto& bucket = *buckets_[bucketIndex];
    std::lock_guard<std::mutex> lock(bucket.mutex);

    BucketFile* file = findReusableBucketFileLocked(bucket);
    if (file == nullptr) {
      if (bucket.files.size() >= config_.maxOpenFilesPerBucket) {
        FileAllocateResult result;
        result.error = FileErrorCode::TooManyOpenFiles;
        return result;
      }
      auto created = createBucketFileLocked(bucket);
      if (!created.ok()) {
        return created;
      }
      file = bucket.files.back().get();
    }

    uint64_t offset = 0;
    if (!file->freeOffsets.empty()) {
      offset = file->freeOffsets.back();
      file->freeOffsets.pop_back();
    } else {
      offset = file->nextOffset;
      file->nextOffset += bucket.bucketSize;
    }
    ++file->activeBlocks;

    extent.fd = file->fd;
    extent.offset = offset;
    extent.requestedSize = static_cast<uint64_t>(size);
    extent.allocatedSize = bucket.bucketSize;
    extent.kind = FileExtentKind::Bucket;
    extent.id = extentId;

    record.extent = extent;
    record.bucketIndex = bucketIndex;
    record.fileIndex = file->fileIndex;
  }

  registerExtent(std::move(record));
  FileAllocateResult result;
  result.extent = extent;
  return result;
}

FileBlockAllocatorImpl::BucketFile*
FileBlockAllocatorImpl::findReusableBucketFileLocked(BucketState& bucket) {
  for (auto& file : bucket.files) {
    if (!file->freeOffsets.empty() ||
        file->nextOffset + bucket.bucketSize <=
            static_cast<uint64_t>(config_.fileSizeLimitBytes)) {
      return file.get();
    }
  }
  return nullptr;
}

FileAllocateResult FileBlockAllocatorImpl::createBucketFileLocked(
    BucketState& bucket) {
  const auto fileIndex = bucket.nextFileIndex++;
  const auto path = config_.directory + "/bucket_" +
      std::to_string(bucket.bucketSize) + "_" + std::to_string(fileIndex) +
      ".bm";
  const int fd =
      ::open(path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) {
    FileAllocateResult result;
    result.error = FileErrorCode::IoError;
    result.nativeErrorCode = errno;
    return result;
  }

  auto file = std::make_unique<BucketFile>();
  file->fileIndex = fileIndex;
  file->path = path;
  file->fd = fd;
  bucket.files.push_back(std::move(file));
  return FileAllocateResult{};
}

uint64_t FileBlockAllocatorImpl::nextExtentId() {
  std::lock_guard<std::mutex> lock(registryMutex_);
  return nextExtentId_++;
}

void FileBlockAllocatorImpl::registerExtent(ExtentRecord record) {
  std::lock_guard<std::mutex> lock(registryMutex_);
  registry_.emplace(record.extent.id, std::move(record));
}

FileFreeResult FileBlockAllocatorImpl::free(const FileExtent& extent) {
  ExtentRecord record;
  {
    std::lock_guard<std::mutex> lock(registryMutex_);
    const auto it = registry_.find(extent.id);
    if (it == registry_.end()) {
      FileFreeResult result;
      result.error = FileErrorCode::DoubleFree;
      return result;
    }
    record = it->second;
    registry_.erase(it);
  }

  if (record.extent.kind == FileExtentKind::Dedicated) {
    return freeDedicated(record);
  }
  return freeBucket(record);
}

FileFreeResult FileBlockAllocatorImpl::freeDedicated(
    const ExtentRecord& record) {
  DedicatedFile file;
  {
    std::lock_guard<std::mutex> lock(dedicatedMutex_);
    const auto it = dedicatedFiles_.find(record.extent.id);
    if (it == dedicatedFiles_.end()) {
      FileFreeResult result;
      result.error = FileErrorCode::InvalidExtent;
      return result;
    }
    file = it->second;
    dedicatedFiles_.erase(it);
  }

  closeFd(file.fd);
  std::filesystem::remove(file.path);
  return FileFreeResult{};
}

FileFreeResult FileBlockAllocatorImpl::freeBucket(const ExtentRecord& record) {
  auto& bucket = *buckets_[record.bucketIndex];
  std::lock_guard<std::mutex> lock(bucket.mutex);
  auto* file = findBucketFileByIndexLocked(bucket, record.fileIndex);
  if (file == nullptr || file->activeBlocks == 0) {
    FileFreeResult result;
    result.error = FileErrorCode::InvalidExtent;
    return result;
  }

  file->freeOffsets.push_back(record.extent.offset);
  --file->activeBlocks;

  if (file->activeBlocks == 0) {
    const auto path = file->path;
    closeFd(file->fd);
    bucket.files.erase(
        std::remove_if(
            bucket.files.begin(),
            bucket.files.end(),
            [&](const std::unique_ptr<BucketFile>& candidate) {
              return candidate->fileIndex == record.fileIndex;
            }),
        bucket.files.end());
    std::filesystem::remove(path);
  }

  return FileFreeResult{};
}

FileBlockAllocatorImpl::BucketFile*
FileBlockAllocatorImpl::findBucketFileByIndexLocked(
    BucketState& bucket,
    uint64_t fileIndex) {
  for (auto& file : bucket.files) {
    if (file->fileIndex == fileIndex) {
      return file.get();
    }
  }
  return nullptr;
}

} // namespace bytedance::bolt::memory::bm
