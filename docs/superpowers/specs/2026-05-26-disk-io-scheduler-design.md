# Disk IO Scheduler Design

## Context

`bolt/common/memory/bm` is currently empty. Existing io_uring usage lives in
`bolt/common/file/File.*` and is called directly by users such as spill code.
This design adds an independent Disk IO foundation under
`bolt/common/memory/bm` without changing existing spill or file behavior.

The first version focuses on a small, testable scheduling layer over io_uring:
single-buffer positional reads and writes, three priority classes, weighted
fair scheduling, and adaptive inflight depth for throughput.

## Goals

- Provide an independent Disk IO scheduler library in `bolt/common/memory/bm`.
- Use io_uring for real disk IO when `IO_URING_SUPPORTED` is available.
- Keep IO scheduling separate from the io_uring backend.
- Expose a `std::future<IoResult>` API.
- Support `Read` and `Write` requests on caller-owned file descriptors.
- Support three priorities: `High`, `Medium`, and `Low`.
- Make priority weights configurable.
- Dynamically adjust effective inflight depth to improve throughput.
- Use shared buffer ownership to keep asynchronous IO buffers alive.
- Provide a mock backend for deterministic unit tests.

## Non-Goals

- No integration with existing spill or file paths in the first version.
- No open, close, fsync, fdatasync, cancel, scatter/gather, or request
  coalescing support.
- No multi-shard scheduler API or implementation.
- No Folly dependency in the public API.
- No latency target or latency guard in the adaptive-depth controller.

## Architecture

The module has three layers:

```text
DiskIoScheduler API
  -> std::future<IoResult> submit(IoRequest)

Scheduler core
  -> priority queues
  -> weighted fair dispatch
  -> adaptive currentDepth control
  -> shutdown and drain

IoBackend
  -> IoUringBackend for real IO
  -> MockIoBackend for tests
```

The scheduler owns the request queues, scheduling policy, inflight accounting,
and promise completion. The backend owns only the low-level submit and reap
mechanics.

The caller owns file descriptor lifetime. A request references an already-open
`fd`, a file offset, and one continuous buffer range.

## Public API

The public API is standard-library based:

```cpp
enum class IoOpcode : uint8_t {
  Read,
  Write,
};

enum class IoPriority : uint8_t {
  High = 0,
  Medium = 1,
  Low = 2,
};

struct IoBuffer {
  std::shared_ptr<void> data;
  size_t size;
  size_t offset;
  size_t length;
};

struct IoRequest {
  IoOpcode opcode;
  IoPriority priority;
  int fd;
  uint64_t fileOffset;
  IoBuffer buffer;
};

struct IoResult {
  uint64_t bytes;
  int errorCode;
};
```

`errorCode == 0` means success. Ordinary IO failures are represented by
`IoResult`, not exceptions from `future.get()`.

The scheduler facade:

```cpp
class DiskIoScheduler {
 public:
  explicit DiskIoScheduler(DiskIoSchedulerConfig config);
  ~DiskIoScheduler();

  std::future<IoResult> submit(IoRequest request);

  void stopAndDrain();
  DiskIoSchedulerStats stats() const;
};
```

`submit()` validates basic request shape before enqueueing:

- `fd >= 0`
- buffer is not null
- `length > 0`
- `offset + length <= size` without overflow
- opcode and priority are valid
- scheduler is still accepting requests

Invalid requests return an already completed future with an error code such as
`EINVAL`. Submitting after shutdown starts returns an already completed future
with `ESHUTDOWN` or the closest available platform error.

## Configuration

```cpp
struct AdaptiveDepthConfig {
  bool enabled = true;
  uint32_t minDepth = 1;
  uint32_t initialDepth = 64;
  uint32_t maxDepth = 256;
  std::chrono::milliseconds controlInterval{200};
  uint32_t increaseStep = 4;
  double minThroughputGain = 0.02;
};

struct DiskIoSchedulerConfig {
  uint32_t ringDepth = 256;
  std::array<uint32_t, 3> priorityWeights = {
      8, // High
      4, // Medium
      1, // Low
  };
  AdaptiveDepthConfig adaptiveDepth;
};
```

Validation rules:

- `ringDepth > 0`
- all priority weights are greater than zero
- `minDepth > 0`
- `minDepth <= initialDepth <= maxDepth`
- `maxDepth <= ringDepth`
- `controlInterval > 0`
- `increaseStep > 0`
- `minThroughputGain >= 0`

## Scheduling Policy

The scheduler has one FIFO queue per priority:

```text
High
Medium
Low
```

It uses Deficit Weighted Round Robin. Each priority queue has a configurable
weight and a deficit counter. Each scheduling round adds the configured weight
to the corresponding non-empty queue. While the queue is non-empty, its deficit
is positive, and `inflight < currentDepth`, the scheduler submits one request
and decrements that queue's deficit by one.

With the default weights, sustained load across all priorities receives
approximately this long-term submit ratio:

```text
High : Medium : Low ~= 8 : 4 : 1
```

Requests in the same priority class preserve FIFO order. Priority affects only
requests that have not yet been submitted to io_uring. Once submitted, requests
are completed according to backend and kernel behavior.

Each request has equal scheduling cost in the first version. The scheduler does
not weight by byte size.

## Adaptive Depth

The io_uring ring depth is fixed at initialization. The dynamic control variable
is the scheduler's effective inflight limit:

```text
currentDepth <= ringDepth
```

The first version uses throughput-oriented hill climbing:

1. Start at `initialDepth`.
2. For each `controlInterval`, measure completed bytes per second.
3. If queues are backlogged and a previous depth increase improved throughput
   by at least `minThroughputGain`, increase `currentDepth` by `increaseStep`.
4. If throughput is flat or lower after an increase, return to the best observed
   recent depth.
5. Clamp `currentDepth` to `[minDepth, maxDepth]`.

Latency is recorded in stats but does not affect control decisions in the first
version.

If adaptive depth is disabled, the scheduler uses `initialDepth` as a fixed
inflight limit.

## Threading Model

`DiskIoScheduler` starts one background scheduler thread.

Caller threads call `submit()`, which validates the request, creates a promise,
enqueues the request, and returns the future.

The scheduler thread:

- dispatches queued requests by weighted priority while `inflight < currentDepth`
- submits selected requests to the backend
- reaps backend completions
- completes the corresponding promises
- updates stats and adaptive-depth state

The first version is single-shard only.

## Shutdown

`stopAndDrain()` has two phases:

1. Stop accepting new requests.
2. Continue dispatching queued requests and reaping inflight requests until all
   accepted requests complete.

`DiskIoScheduler` destructor calls `stopAndDrain()` if the caller has not
already done so. Requests are not cancelled.

## Backend Interface

The backend interface is internal to the module. It should expose only the
operations the scheduler needs:

- submit a request with a scheduler-owned request id
- reap zero or more completions
- wait or poll for completion events
- shutdown backend resources after all inflight requests complete

`IoUringBackend` uses `io_uring_prep_read` and `io_uring_prep_write` for
single-buffer positional IO. It stores the scheduler request id in `user_data`
to route completions back to promises.

`MockIoBackend` is deterministic and test-only. It lets tests control completion
order, returned byte counts, error codes, and synthetic latency.

## Error Handling

IO errors are returned as `IoResult`:

```text
success: IoResult{bytes, 0}
failure: IoResult{bytes, errno_value}
```

Partial reads or writes return the byte count reported by the backend. The
scheduler does not retry partial IO in the first version.

Configuration errors are detected during construction. Request validation errors
return completed futures with error codes.

## Stats

The scheduler exposes a stats snapshot containing at least:

- queued requests per priority
- inflight requests
- current depth
- completed request count
- completed bytes
- successful and failed request counts
- recent throughput
- average observed latency
- per-priority submitted and completed counts

Stats are used for tests, operational visibility, and future tuning.

## Tests

Unit tests use `MockIoBackend` to cover:

- request validation
- same-priority FIFO ordering
- weighted fair scheduling across `High`, `Medium`, and `Low`
- configurable priority weights
- `currentDepth` limiting inflight requests
- adaptive depth increase when throughput improves
- adaptive depth rollback when throughput stops improving
- shutdown drain behavior
- IO error propagation through `IoResult`
- stats updates

io_uring integration tests compile only with `IO_URING_SUPPORTED` and cover:

- temporary-file read and write
- multiple concurrent requests
- invalid fd error handling
- `stopAndDrain()` with real inflight IO

## Build Integration

The implementation should add the new `bm` sources to `bolt_memory` or a new
internal library under `bolt/common/memory`, depending on the final file layout.
If the io_uring backend is compiled into the memory library, CMake must add the
same conditional liburing dependency used by `bolt/common/file`.

The mock backend and scheduler tests should live under the existing memory test
tree and avoid requiring io_uring.
