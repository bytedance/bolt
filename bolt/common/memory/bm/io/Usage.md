# DiskIoScheduler Usage Guide

`DiskIoScheduler` is the public facade for BM disk I/O scheduling. It queues
read and write requests by priority, submits them to an io_uring backend under
a controlled queue depth, and returns `std::future<IoResult>` for every
request.

## Intended Use

Use it for asynchronous buffered file I/O in BM:

- reading and writing files that back BM memory blocks;
- submitting multiple independent requests without blocking the caller on each
  system call;
- observing queue depth, inflight depth, latency, throughput, and backend
  failures.

Use `pread()` or `pwrite()` directly when explicit synchronous semantics are
required.

## Entry Point

Business code uses the process-wide facade:

```cpp
#include "bolt/common/memory/bm/io/DiskIoScheduler.h"

using namespace bytedance::bolt::memory::bm;

DiskIoScheduler& scheduler = diskIoScheduler();
```

`diskIoScheduler()` owns a process-lifetime scheduler. Business code must not
start, stop, or destroy it, and the public facade intentionally exposes no
shutdown API.

## Request Model

```cpp
struct IoRequest {
  IoOpcode opcode;
  IoPriority priority;
  int fd;
  uint64_t fileOffset;
  IoBuffer buffer;
};
```

`IoBuffer` is move-only and owns contiguous memory plus its release logic. It
supports `std::unique_ptr<char[]>` and custom deleters through `fromOwned()`:

```cpp
auto* data = new char[size];
auto buffer = IoBuffer::fromOwned(
    data,
    size,
    0,
    size,
    [](char* p) noexcept { delete[] p; });
```

After `submit(std::move(request))`, ownership belongs to the scheduler. When
the future becomes ready, `IoResult::buffer` returns ownership to the caller.

For `MemoryPool` storage, prefer `allocateFromPool()`. Destruction then frees
memory through the same pool:

```cpp
request.buffer = IoBuffer::allocateFromPool(pool, size);
```

Without shared ownership, the caller must keep `MemoryPool*` alive until all
related `IoResult` objects are destroyed.

## Submitting a Write

```cpp
#include "bolt/common/memory/bm/io/DiskIoScheduler.h"

#include <cstring>
#include <memory>

using namespace bytedance::bolt::memory::bm;

std::future<IoResult> submitWrite(int fd, uint64_t offset) {
  constexpr size_t kSize = 4096;
  auto data = std::make_unique<char[]>(kSize);
  std::memset(data.get(), 'x', kSize);

  IoRequest request;
  request.opcode = IoOpcode::Write;
  request.priority = IoPriority::Medium;
  request.fd = fd;
  request.fileOffset = offset;
  request.buffer = IoBuffer{std::move(data), kSize, 0, kSize};
  return diskIoScheduler().submit(std::move(request));
}
```

```cpp
auto result = submitWrite(fd, 0).get();
if (!result.ok()) {
  // result.error describes a scheduler or backend failure.
  // result.nativeErrorCode contains errno for a system I/O failure.
}
```

## Submitting a Read

```cpp
std::future<IoResult> submitRead(int fd, uint64_t offset, size_t size) {
  auto data = std::make_unique<char[]>(size);

  IoRequest request;
  request.opcode = IoOpcode::Read;
  request.priority = IoPriority::High;
  request.fd = fd;
  request.fileOffset = offset;
  request.buffer = IoBuffer{std::move(data), size, 0, size};
  return diskIoScheduler().submit(std::move(request));
}

auto result = submitRead(fd, 0, 4096).get();
if (result.ok()) {
  const char* bytes = result.buffer.data();
  const uint64_t bytesRead = result.bytes;
}
```

## Priorities

`IoPriority` supports `High`, `Medium`, and `Low`. The scheduler uses weighted
dispatch. Use `Medium` for normal BM reads and writes, `High` for
latency-sensitive work, and `Low` for background maintenance.

## Validation and Error Codes

An invalid request returns an immediately ready future with
`IoErrorCode::InvalidRequest`. Invalid cases include:

- `fd < 0`;
- a null data pointer or zero buffer length;
- an offset past the buffer size;
- an offset-plus-length range past the buffer size;
- an invalid opcode or priority.

`IoErrorCode` values:

- `Ok`: the request completed successfully.
- `InvalidRequest`: validation rejected the request before enqueueing.
- `Shutdown`: the scheduler is stopping or rejected queued work during
  shutdown.
- `BackendSubmitFailed`: the backend could not submit the request.
- `BackendIoError`: completion reported a system I/O error.
- `ShortIo`: completion transferred fewer bytes than requested.

Always check `result.ok()` before reading data or treating a write as complete.

## Observability

```cpp
const auto stats = diskIoScheduler().stats();
```

Important fields include:

- `queuedRequests` by priority;
- `inflightRequests`;
- `completedRequests`, `successfulRequests`, and `failedRequests`;
- `backendSubmitFailedRequests` and `backendIoErrorRequests`;
- `averageDeviceLatencyUs`, `averageQueueWaitUs`, and
  `averageEndToEndLatencyUs`;
- `averageSubmitBatchSize` and `averageCompletionBatchSize`;
- `depthControl` mode and current-depth statistics.

Average fields are snapshot values derived from cumulative counters and sample
counts. The record path updates counters, totals, minima, and maxima; floating
point averages are computed by `stats()` or `toString()` to keep the I/O worker
hot path lean.

The scheduler does not log statistics automatically. Sample them at low-rate
boundaries and export them through existing runtime statistics or monitoring.
Do not log every submission, completion, or future fulfillment.

## Operational Notes

- The current backend uses io_uring. Unsupported or restricted runtimes fail
  through the project's existing exception path.
- Current operation is buffered I/O and does not require `O_DIRECT` alignment.
- The caller owns the file descriptor and must keep it open until the future is
  ready.
- Callers should wait on or get the `std::future` from their own thread and must
  not assume which thread makes it ready.
- `IoBuffer::size()` describes the allocation; `offset()` and `length()` select
  the range used by the current request.
