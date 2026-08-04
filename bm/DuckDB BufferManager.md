# DuckDB BufferManager

# 一、先给你一句总纲（抓住本质）

> **DuckDB BufferManager = 一个统一内存调度器（不是传统 buffer cache）**
> 
> 

它同时管理：

- persistent data（磁盘页）

- temporary data（算子中间结果）

并且通过 **pin / unpin \+ 全局内存预算 \+ 延迟淘汰** 来协调。

---

# 二、核心对象关系（最小模型）

你只需要记住这 6 个类：

```Plain Text
StandardBufferManager   // 入口
        ↓
BufferPool              // 全局内存调度
        ↓
BlockHandle             // 逻辑页
BlockMemory             // 物理内存
        ↓
BufferHandle            // pin 生命周期
        ↓
TemporaryMemoryManager  // 算子内存
```

---

# 三、核心机制（用“源码骨架”讲）

下面我用**高度精简但保留精髓的伪源码**给你讲（基本 1:1 对应真实实现逻辑）

---

## 1️⃣ Pin（核心入口）

```C++
BufferHandle StandardBufferManager::Pin(handle) {
    auto &mem = handle->memory;
    lock(mem.lock);

    // ✅ fast path：已经在内存
    if (mem.state == LOADED) {
        mem.readers++;
        unlock(mem.lock);
        return BufferHandle(this, handle);
    }

    // ❗ slow path：需要加载
    size = mem.memory_usage;
    unlock(mem.lock);

    // ⭐ 关键：先抢内存额度（可能触发 eviction）
    reservation = buffer_pool.TryReserveMemory(size);

    lock(mem.lock);

    // ⚠️ double check（避免并发重复加载）
    if (mem.state == LOADED) {
        mem.readers++;
        unlock(mem.lock);
        return BufferHandle(this, handle);
    }

    // ⭐ 真正加载（persistent 或 temp）
    mem.buffer = LoadFromDiskOrTemp(handle);
    mem.state = LOADED;
    mem.memory_charge = reservation;
    mem.readers++;

    unlock(mem.lock);
    return BufferHandle(this, handle);
}
```

---

### 👉 精髓总结

- 先判断是否 loaded（fast path）

- **先抢内存，再 load（非常关键）**

- double\-check 防并发

- memory\_charge 记录谁占了内存

---

## 2️⃣ Unpin（释放使用权）

```C++
void StandardBufferManager::Unpin(handle) {
    auto &mem = handle->memory;
    lock(mem.lock);

    mem.readers--;

    if (mem.readers == 0) {
        // ⭐ 进入 eviction queue
        mem.eviction_seq = global_seq++;
        buffer_pool.AddToEvictionQueue(handle, mem.eviction_seq);
    }

    unlock(mem.lock);
}
```

---

### 👉 精髓总结

- unpin ≠ 释放内存

- 只是变成“**可淘汰**”

---

## 3️⃣ BufferPool：TryReserveMemory

```C++
Reservation BufferPool::TryReserveMemory(size) {
    if (used + size <= limit) {
        used += size;
        return Reservation(size);
    }

    // ⭐ 不够 → 触发 eviction
    EvictBlocks(size);

    if (used + size > limit) {
        throw OOM;
    }

    used += size;
    return Reservation(size);
}
```

---

## 4️⃣ Eviction（核心调度）

```C++
void BufferPool::EvictBlocks(required) {
    while (used + required > limit) {
        auto node = eviction_queue.pop();

        if (!node.valid()) continue;

        auto mem = node.memory.lock();

        if (!mem || mem.readers > 0) continue;

        // ⭐ 真正卸载
        mem.Unload();
    }
}
```

---

## 5️⃣ BlockMemory::Unload（关键分支）

```C++
void BlockMemory::Unload() {
    if (persistent) {
        // 直接丢
        buffer.reset();
    } else {
        // ⭐ temporary → spill
        WriteToTempStorage(buffer);
        buffer.reset();
    }

    state = UNLOADED;
    memory_charge.Release();
}
```

---

# 四、完整执行流程（你要脑补这个）

## 场景：Pin 一个 block，但内存不够

```Plain Text
Pin()
 ↓
TryReserveMemory()
 ↓
EvictBlocks()
 ↓
找到 unpinned block
 ↓
BlockMemory::Unload()
 ↓
（persistent → 丢）
（temporary → spill）
 ↓
腾出空间
 ↓
Load 当前 block
```

---

# 五、TemporaryMemoryManager 是怎么接进来的？

```C++
auto reservation = buffer_manager.ReserveMemory(size);
```

本质上：

```Plain Text
temporary memory
    ↓
也是 TryReserveMemory
    ↓
和 persistent 共用 budget
```

---

# 六、设计精髓（5 条）

---

## 1️⃣ BlockHandle / BlockMemory 分离（最重要）

```Plain Text
handle = identity
memory = resource
```

👉 允许：

- unload / reload

- eviction 不影响逻辑结构

---

## 2️⃣ pin/unpin 驱动生命周期

不是“访问缓存”，而是：

```Plain Text
Pin → 占资源
Unpin → 可淘汰
```

---

## 3️⃣ eviction 是延迟决策

```Plain Text
Unpin → 只是入队
Evict → 才真正判断
```

👉 避免复杂同步

---

## 4️⃣ unified memory（最核心创新）

```Plain Text
persistent + temporary
        ↓
共享一个 buffer pool
```

---

## 5️⃣ memory\_charge（非常关键）

每个 block：

```Plain Text
memory_charge = Reservation
```

👉 精确 tracking：

- 谁占了多少内存

- 释放时回收

---

# 七、优点（非常强）

---

## ✅ 1\. 内存利用率极高

传统：

```Plain Text
buffer pool 1GB（固定）
operator 内存 另算
```

DuckDB：

```Plain Text
1GB 全部动态分配
```

👉 没有浪费

---

## ✅ 2\. 自动权衡 I/O vs 计算

系统会自动选择：

- evict page（IO）

- spill temp（IO \+ compute）

👉 **成本驱动决策**

---

## ✅ 3\. 统一机制，代码简单

所有路径：

```Plain Text
Pin / ReserveMemory
        ↓
BufferPool
```

👉 不需要两套系统

---

## ✅ 4\. 非常适合 OLAP

- 大扫描

- 大 join

- spill\-heavy workload

👉 DuckDB 强项

---

## ✅ 5\. 并发设计干净

- block 级锁

- 无全局锁

- eviction 延迟校验

---

# 八、缺点（很真实）

---

## ❌ 1\. 实现复杂度高

你已经感受到了：

- double\-check

- eviction queue

- memory\_charge

👉 不容易写对

---

## ❌ 2\. 行为不够“可预测”

传统 DB：

```Plain Text
buffer pool = 10GB
```

DuckDB：

```Plain Text
动态变化
```

👉 调优更难

---

## ❌ 3\. 临时数据 spill 成本可能很高

如果判断不准：

- spill 太早 → 性能差

- spill 太晚 → OOM

---

## ❌ 4\. 对 workload 敏感

比如：

- 大 join \+ 大 scan 同时发生

👉 决策难度高

---

## ❌ 5\. debug 难度高

因为：

```Plain Text
persistent / temporary 混在一起
```

👉 很难一眼看出是谁占了内存

---

# 九、你可以用一句话记住整个系统

> **DuckDB BufferManager = 用 pin/unpin 生命周期 \+ eviction queue，把所有内存变成“可调度资源”。**
> 
> 

