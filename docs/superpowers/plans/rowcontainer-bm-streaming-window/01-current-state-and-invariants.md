# 01. 当前状态和不变量

## WindowBuild 当前创建 RowContainer

`bolt/exec/WindowBuild.cpp` 当前统一创建 `data_`：

```cpp
data_ = std::make_unique<RowContainer>(
    slice(inputType_->children(), 0, numKeys),
    slice(inputType_->children(), numKeys, inputType_->size()),
    pool);
```

这会走默认非 append-only 构造路径。

## StreamingWindowBuild 当前保存裸指针

`bolt/exec/StreamingWindowBuild.h` 当前有：

```cpp
std::vector<char*> sortedRows_;
std::vector<char*> inputRows_;
char* previousRow_ = nullptr;
```

`bolt/exec/StreamingWindowBuild.cpp` 当前流程：

```cpp
char* newRow = data_->newRow();
data_->store(decodedInputVectors_[col], row, newRow, col);
inputRows_.push_back(newRow);
previousRow_ = newRow;
```

`nextPartition()` 直接把 `sortedRows_` 的 `char**` range 传给：

```cpp
WindowPartitionImpl<RowFormat::kRowContainer>
```

## RowContainer append-only 当前状态

提交 `0f6b42a9cf...` 后，append-only 路径的关键成员是：

```cpp
std::vector<BufferPtr> rowsBuffers_;
char* currRowsBufferPosition_{nullptr};
char* currRowsBufferEnd_{nullptr};

std::vector<BufferPtr> stringBuffers_;
char* currStringPosition_{nullptr};
char* currStringBufferEnd_{nullptr};
```

这解决了 free-list 和变长字段逐行释放的问题，但还没有解决 BM 场景下地址可能变化的问题。

## 必须遵守的不变量

### RowId 是长期身份

所有跨函数、跨 operator 调用、跨 partition 生命周期保存的行引用都必须是 RowId。

裸 `char*` 只允许作为 pin 后的短期访问地址。

### BufferHandle 决定地址有效期

BM 的语义是：

- `BlockHandle` 可以长期保存。
- `BufferHandle` 代表一次 pin。
- `BufferHandle::Ptr()` 返回的地址只在该 `BufferHandle` 生命周期内有效。
- block unpin 后可能被 spill；再次 pin 时地址可能不同。

任何从 RowId 转换出来的 `char*` 必须绑定到一个 RAII pin holder。

### append-only 第一版不复用行槽

StreamingWindowBuild 的行是单调 append，partition 消费后可以整体释放或标记释放。第一版不做 slot reuse，不需要 generation。

后续如果 HashTable/Aggregation 也迁移 RowId 并要求复用 slot，再引入 generation。

### 默认路径不变

没有显式开启 BM RowContainer 时：

- 原有 RowContainer 构造函数行为不变。
- 原有 `newRow()`/`eraseRows()`/`listRows()` 语义不变。
- SortWindowBuild、SpillableWindowBuild、HashTable、SortBuffer、Aggregation 不受影响。

