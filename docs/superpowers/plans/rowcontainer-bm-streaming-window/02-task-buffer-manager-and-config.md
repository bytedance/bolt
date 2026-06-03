# 02. Task 级 BufferManager 和配置

## Task 持有 BM

修改 `bolt/exec/Task.h`：

```cpp
std::shared_ptr<memory::bm::BufferManager> bmBufferManager_;

std::shared_ptr<memory::bm::BufferManager> bufferManager() const;

void maybeCreateBufferManager();
```

创建策略：

- 在 `Task::create()` 流程内创建 BM。
- 具体创建点放在 `Task::init()` 中，位于 `initTaskPool()` 和 `setSpillDiskConfig()` 之后。
- 不在 `Task` 构造函数里创建，因为构造函数里还没有 `pool_`，也还没处理 `spillDiskOpts`。
- 不在 getter 里懒创建，避免第一次 operator 使用时才暴露配置错误。
- 只在 query config 显式开启 Task 级 execution BM 时创建，避免普通 Task 和无 spill 配置测试受影响。

当前创建路径是：

```cpp
Task::create(...)
  -> new Task(...)
  -> task->init(spillDiskOpts)
```

`Task::init()` 当前顺序是：

```cpp
initTaskPool();
setSpillDiskConfig(std::move(spillDiskOpts));
```

建议改成：

```cpp
initTaskPool();
setSpillDiskConfig(std::move(spillDiskOpts));
maybeCreateBufferManager();
```

getter 只返回已创建对象：

```cpp
std::shared_ptr<memory::bm::BufferManager>
Task::bufferManager() const {
  return bmBufferManager_;
}
```

`maybeCreateBufferManager()` 示例：

```cpp
void Task::maybeCreateBufferManager() {
  if (!queryCtx_->queryConfig().bufferManagerEnabled()) {
    return;
  }
  BOLT_CHECK_NOT_NULL(pool_);
  BOLT_CHECK_NULL(bmBufferManager_);
  BOLT_CHECK(
      !spillDirectory_.empty() || spillDirectoryCallback_,
      "BufferManager is enabled, but spill directory is not configured");

  const auto& spillDir = getOrCreateSpillDirectory();
  memory::bm::BufferManagerConfig config;
  config.poolName = fmt::format("task-bm-{}", taskId_);
  config.spillStoreConfig.fileAllocatorConfig.directory =
      fmt::format("{}/bm", spillDir);
  config.spillStoreConfig.fileAllocatorConfig.bucket_sizes = {
      32 * 1024,
      64 * 1024,
      128 * 1024,
      256 * 1024,
      512 * 1024,
      1024 * 1024,
      2 * 1024 * 1024,
      4 * 1024 * 1024,
  };
  config.spillStoreConfig.fileAllocatorConfig.file_size_limit_bytes =
      1024LL * 1024LL * 1024LL;
  config.spillStoreConfig.fileAllocatorConfig.max_open_files_per_bucket = 64;
  bmBufferManager_ =
      memory::bm::BufferManager::Create(*pool_, std::move(config));
}
```

这里的 `bucket_sizes` 是 BM spill file allocator 的落盘分片 bucket，不是 RowContainer 逻辑 row block 大小。建议第一版直接使用上面的固定值，覆盖 32KiB 到 4MiB 的常见 spill segment，避免只有 `AllocateSize::{kSmall,kMedium,kLarge}` 时粒度过粗。

`file_size_limit_bytes` 第一版建议使用 1GiB，和 `bolt/common/memory/bm/Usage.md` 保持一致。`max_open_files_per_bucket` 第一版建议使用 64，适合 Task 级 BM 未来被多个算子共用。

如果当前 query 没有合法 spill path：

- 默认配置关闭时，不创建 BM。
- 配置开启时，`maybeCreateBufferManager()` 直接 fail-fast，错误信息说明需要 spill directory。

## DriverCtx 暴露 BM

修改 `bolt/exec/Driver.h`：

```cpp
std::shared_ptr<memory::bm::BufferManager> bufferManager() const;
```

修改 `bolt/exec/Driver.cpp`：

```cpp
std::shared_ptr<memory::bm::BufferManager>
DriverCtx::bufferManager() const {
  return task ? task->bufferManager() : nullptr;
}
```

测试里的 `DriverCtx(nullptr, ...)` 返回 nullptr。

由于 BM 已在 `Task::init()` 创建，DriverCtx getter 不做创建动作。

## QueryConfig 开关

新增默认关闭配置：

```cpp
static constexpr const char* kBufferManagerEnabled =
    "buffer-manager-enabled";
```

getter：

```cpp
bool bufferManagerEnabled() const {
  return get<bool>(kBufferManagerEnabled, false);
}
```

不要把这个开关命名为 Window 专用。BM 挂在 Task 上，第一阶段只有 StreamingWindowBuild 使用，但后续 Sort、Agg、HashBuild 或其他 RowContainer 用户也可以复用同一个 Task 级 BM。

第一阶段只影响：

```text
WindowBuildType::kStreamingWindowBuild && needSort_ == false
```

## 测试建议

### 默认关闭时不创建 BM

创建不带 spillDiskOpts 的普通 Task：

```cpp
auto task = Task::create(..., std::nullopt);
ASSERT_EQ(nullptr, task->bufferManager());
```

### 开启且有 spillDiskOpts 时创建 BM

```cpp
auto task = Task::create(..., spillDiskOptsWithTempDir);
ASSERT_NE(nullptr, task->bufferManager());
ASSERT_EQ(task->bufferManager().get(),
          DriverCtx(task, 0, 0, 0, 0).bufferManager().get());
```

### 开启但没有 spill path 时 fail-fast

配置开启 `buffer-manager-enabled=true`，但不传 `spillDiskOpts`：

```cpp
ASSERT_THROW(
    Task::create(..., std::nullopt),
    BoltException);
```

错误信息应包含：

```text
BufferManager is enabled, but spill directory is not configured
```
