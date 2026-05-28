#pragma once

#include "bolt/common/memory/bm/file/FileBlockAllocator.h"

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace bytedance::bolt::memory::bm {

class FileBlockAllocatorImpl : public FileBlockAllocator {
 public:
  explicit FileBlockAllocatorImpl(FileBlockAllocatorConfig config);
  ~FileBlockAllocatorImpl() override;

  FileBlockAllocatorImpl(const FileBlockAllocatorImpl&) = delete;
  FileBlockAllocatorImpl& operator=(const FileBlockAllocatorImpl&) = delete;

  FileAllocateResult allocate(int64_t size) override;
  FileFreeResult free(const FileExtent& extent) override;

 private:
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
  };

  struct DedicatedFile {
    std::string path;
    int fd{-1};
  };

  FileAllocateResult allocateBucket(int64_t size, size_t bucketIndex);
  FileAllocateResult allocateDedicated(int64_t size);
  BucketFile* findReusableBucketFileLocked(BucketState& bucket);
  FileAllocateResult createBucketFileLocked(BucketState& bucket);
  FileFreeResult freeBucket(const ExtentRecord& record);
  FileFreeResult freeDedicated(const ExtentRecord& record);
  BucketFile* findBucketFileByIndexLocked(
      BucketState& bucket,
      uint64_t fileIndex);
  uint64_t nextExtentId();
  void registerExtent(ExtentRecord record);

  FileBlockAllocatorConfig config_;
  std::vector<std::unique_ptr<BucketState>> buckets_;
  std::mutex registryMutex_;
  uint64_t nextExtentId_{1};
  std::unordered_map<uint64_t, ExtentRecord> registry_;
  std::mutex dedicatedMutex_;
  std::unordered_map<uint64_t, DedicatedFile> dedicatedFiles_;
  bool shutdown_{false};
};

} // namespace bytedance::bolt::memory::bm
