# BM File Segment Allocator Usage Guide

The `file` module provides an in-process file-segment allocator. A caller
provides the number of bytes it needs to write and receives a writable file
location:

```cpp
struct FileSegment {
  int fd;
  uint64_t offset;
  uint64_t requested_size;
  uint64_t allocated_size;
  FileSegmentKind kind;
  uint64_t id;
};
```

Use `fd + offset` with explicit-offset I/O such as `pwrite()`, `pread()`, or an
io_uring request that carries an offset. The allocator owns file space and file
descriptor lifetimes. It does not perform I/O or track I/O completion.

## Basic Usage

```cpp
#include "bolt/common/memory/bm/file/FileSegmentAllocator.h"

using namespace bytedance::bolt::memory::bm;

std::shared_ptr<FileSegmentAllocator> CreateAllocator() {
  FileSegmentAllocatorConfig config;
  config.directory = "/tmp/bolt-bm-file";
  config.bucket_sizes = {32 * 1024, 64 * 1024, 128 * 1024, 256 * 1024};
  config.file_size_limit_bytes = 1024 * 1024 * 1024; // 1 GiB
  config.max_open_files_per_bucket = 16;
  return CreateFileSegmentAllocator(std::move(config));
}

bool WriteData(FileSegmentAllocator& allocator, const char* data, int64_t size) {
  auto allocation = allocator.Allocate(size);
  if (!allocation.ok()) {
    return false;
  }

  const auto& segment = allocation.segment;
  const ssize_t written = ::pwrite(segment.fd, data, size, segment.offset);
  if (written != size) {
    allocator.Free(segment);
    return false;
  }

  // Free the file space only after it is no longer needed.
  auto freeResult = allocator.Free(segment);
  return freeResult.ok();
}
```

## Allocation Strategy

- `bucket_sizes` lists small-allocation bucket sizes in bytes.
- `Allocate(size)` selects the first bucket where `bucket_size >= size`.
- A bucket file uses fixed-size slots, so `allocated_size` equals its bucket
  size.
- If `size > bucket_sizes.back()`, the allocator creates a dedicated file with
  `offset = 0` and `allocated_size = requested_size`.
- Files are created lazily.
- A bucket may own multiple open files, limited by
  `max_open_files_per_bucket`.
- When every segment in a bucket file is free, the allocator closes and deletes
  that file and releases its file-descriptor slot.

## Configuration Requirements

`CreateFileSegmentAllocator()` validates its configuration and raises a
`BOLT_CHECK` exception for invalid input:

- `directory` must not be empty.
- Each allocator creates a UUID-named instance directory under `directory`.
  Bucket and dedicated files live inside that directory.
- Creating an allocator does not delete existing contents of the base
  directory.
- `bucket_sizes` must be non-empty, strictly increasing, unique, and 4 KiB
  aligned.
- `file_size_limit_bytes` must be positive, 4 KiB aligned, and no smaller than
  the largest bucket.
- `max_open_files_per_bucket` must be positive.

Call `ValidateFileSegmentAllocatorConfig(config)` before construction when
explicit validation is useful.

## I/O Contract

Always use explicit offsets:

```cpp
::pwrite(segment.fd, data, size, segment.offset);
::pread(segment.fd, buffer, size, segment.offset);
```

Do not depend on the file's current offset and do not treat the returned file
descriptor as append-only. Files are not opened with `O_APPEND`.

Two threads may write offsets 0 and 4096 in either order when both operations
use explicit offsets.

## Freeing Segments

Free a segment only after its corresponding I/O has completed:

```cpp
auto result = allocator.Free(segment);
```

- `Free()` makes the space reusable and may make its file deletable.
- Freeing before asynchronous I/O completes can allow offset reuse or early
  file deletion.
- Each segment may be freed once. A second call returns
  `FileErrorCode::kDoubleFree`.
- Return a segment to the allocator that created it.
- Callers must not close `segment.fd`; the allocator owns it.

## Error Handling

Construction is infrequent and reports invalid configuration through
exceptions. Hot-path `Allocate()` and `Free()` calls return error codes:

- `kInvalidSize`: the requested size is zero or negative.
- `kTooManyOpenFiles`: a bucket reached its file limit and has no free slot.
- `kIoError`: file creation failed; `native_error_code` contains `errno`.
- `kDoubleFree`: the segment is unknown or already free.
- `kInvalidSegment`: segment metadata is inconsistent.
- `kShutdown`: the allocator is shutting down.

## Lifetime and Thread Safety

Production callers should own the allocator explicitly:

```cpp
auto allocator = CreateFileSegmentAllocator(config);
auto allocation = allocator->Allocate(size);
allocator->Free(allocation.segment);
```

- `FileSegmentAllocator` is not thread-safe. Serialize `Allocate()` and
  `Free()` for each instance with an object-level lock, sharded lock, or other
  outer synchronization.
- Multiple allocators may use the same base directory because each uses a
  distinct UUID subdirectory.
- Destruction closes and deletes all managed bucket and dedicated files, then
  removes the allocator's UUID directory.
- Before destruction, ensure that no I/O is pending and no thread still uses an
  old file descriptor.

Tests may instantiate `FileSegmentAllocatorImpl` directly or use
`CreateFileSegmentAllocator()` to exercise the public interface.
