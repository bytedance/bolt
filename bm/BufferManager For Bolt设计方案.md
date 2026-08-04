# BufferManager For Bolt设计方案

# **概述**

- 从Bolt内部视角来说：Bolt 当前已经具备 spill 能力，但从源码现状看，spill操作 仍然主要以“算子感知、容器感知、格式感知”的方式存在，而不是一层统一的 内存 生命周期管理能力。本文讨论的 BufferManager For Bolt，目标是把这件事情上升为更底层的基础设施。

- 从Spark应用角度来说：降低Container的规格到Usage的P99（[Spark内存优化方案](https://bytedance.larkoffice.com/wiki/XH0FwvVH8iwT8okdOpHcG1x5n98?from=from_copylink)）会产生10%\~20%的核存比提升，而BufferManager预期可以赋予Bolt在极低内存情况下运行而不产生OOM报错的能力，因此实现BufferManager机制对于降低整体内存资源消耗也具有重要意义

## BufferManager思想

BufferManager思想来自于DuckDB，下面我以一段简单的示例代码讲清楚什么是BufferManager机制，细节参见：[DuckDB BufferManager](https://bytedance.larkoffice.com/wiki/EiqawWCPAiPptWkkFWqcYdohnQc?from=from_copylink)

```C++
// 描述 BlockHandle 的 payload 当前是否驻留在内存中
enum MemoryStatus {
    IN_MEMORY,
    SPILLED
};

struct BlockHandle {
    int id;
    MemoryStatus status;

    // 数据实际存储在 payload 中。
    // 当 block 被 spill 到磁盘后，payload 对应的内存可以被释放。
    unique_ptr<BlockMemory> payload;
};

// 模拟数据库执行过程中不断访问 block 数据，block数据可以是任意大小
void Pipeline(BufferManager &buffer_manager,
              vector<shared_ptr<BlockHandle>> &blocks) {
    for (auto &block : blocks) {
        // Pin 的语义是：
        // 返回一个可访问的 BufferHandle。
        // 如果 block 已在内存中，可直接使用；
        // 如果 block 已 spill，则需要由 BufferManager 负责把数据读回内存。
        BufferHandle handle = buffer_manager.Pin(block);

        // 调用方不关心数据原来是否 spilled，
        // 只关心现在可以通过 handle 访问
        char *ptr = handle.Ptr();
        Consume(ptr);

        // handle 在这里析构。
        // 通过 RAII 结束对该 payload 的持有，使其重新变为可驱逐。
    }
}
```



## 可以解决的问题

本设计方案希望解决以下所列问题：

1. 降低 Bolt 引擎中现有 spill 操作逻辑复杂度。

    1. spill 操作仍然主要以“算子感知、容器感知、格式感知”的方式存在

    2. 新算子不必先重复发明一套 spill 状态机，整体实现会更统一。

2. 目前有很多长尾case不能spill，对于稳定性来说是个挑战（Spill时机问题）

    1. 比如目前遇到的很多Window OOM问题[线上Window OOM报错捞取](https://bytedance.larkoffice.com/wiki/Od2Pw8MYDihb4JkCgREcz74tnIc?from=from_copylink)

    2. 比如对于TableScan OOM中的“大字符串”case



## 性能收益

本设计方案预期会获得以下所列收益：

1. 更少的数据拷贝：spill操作不再需要数据搬运，从内存直接落盘。即使是row based spill还是会有一次内存拷贝操作

2. 更小的Spill代价：以RowContainer为例，一旦实现BufferManager机制，可以实现部分/按需Spill的功能，有可能不再需要全量Spill

3. 目前的机制里依赖maybeReserve接口触发Spill，所以程序倾向于申请较多内存，不是按需申请，Spill量会偏大

    1. maybeReserve会尽量多的把内存腾出来，如果maybeReserve的少了，错过了这个时机，就再也没法spill了，内存不够就只能失败了

4. Executor级别统一的IO管理，减少竞争和Disk忙碌



## 后续潜在收益点

实施本设计方案之后还会有以下潜在收益点：

1. 创造 IO 与计算 overlap 的空间

    1. 目前的设计中，依赖算子使用reserve/mayBeReserve操作触发Spill操作，是同步的，后续可以在计算的同时进行spill操作，overlap掉部分开销

2. 创造spill文件prefetch、spill异构存储的可能性

    1. 以Block粒度结合算子的context做Prefetch

    2. 区分冷、热Block 分级存储在不同速度的介质（SDD、HDD）上

3. 在MemoryManager机制的加持下，Bolt有望能够在极低的内存下稳定运行（虽然性能有损，但不会失败），这对提升Spark任务核存比具有重要意义



## Prototype选取

当前首个聚焦场景是 `StreamingWindowBuild`，选取逻辑参见这里：[目前Bolt中的Window计算实现](https://bytedance.larkoffice.com/wiki/RQhEw7hl5i4AX3kVm69c6v88nId?from=from_copylink)，StreamingWindowBuild的逻辑在这里：[StreamingWindowBuild目前逻辑](https://bytedance.larkoffice.com/wiki/ZVKmw8lxliO4CvkRr4nczwwTnyf?from=from_copylink)



# **设计方案**

## **方案 A：DuckDB 风格 BufferManager / BlockManager**

### **DuckDB 中 BufferManager 的设计机制**

DuckDB 的BufferManager设计，本质上是把“逻辑 block 身份”和“resident memory 访问权”拆开处理。

从 DuckDB 的接口定义看，`BufferHandle` 是一次有效访问句柄，内部持有 `BlockHandle` 和实际 `FileBuffer` 指针，析构时释放句柄，且禁止拷贝、支持移动，这就是典型的 RAII 访问语义。



DuckDB 里可以把这套机制理解成四层：

|概念|作用|
|---|---|
|BlockManager|负责 block 的逻辑身份、外存位置和读写|
|BlockHandle|代表 block 的稳定身份和状态，不等于可直接访问的数据指针。|
|BlockMemory|是block的数据实际存储位置|
|BufferManager<br>|负责 resident memory 预算、block pinning、eviction、temporary memory 注册，以及 block 从磁盘回到内存时的装载协调。|
|BufferHandle<br>|是一次短生命周期访问权。调用方拿到 handle 才能合法使用内存指针，handle 析构后自动释放 pin。|

- block 的逻辑身份稳定，但其物理地址不是长期稳定承诺。

- **调用方不能长期缓存裸指针，而是要在需要访问时获取 handle。**

- resident memory 的回收不要求上层对象自己记住所有 spill 细节，底层 manager 可以依据 pin count 和内存压力选择驱逐对象。

- 外存读写和内存驻留由不同层负责，因此职责更清晰。



上面简介BufferManager时候所使用的演示代码依旧有效，可以阅读下面的代码来理解DuckDB的BufferManager机制

```C++
// 描述 BlockHandle 的 payload 当前是否驻留在内存中
enum MemoryStatus {
    IN_MEMORY,
    SPILLED
};

struct BlockHandle {
    int id;
    MemoryStatus status;

    // 数据实际存储在 payload 中。
    // 当 block 被 spill 到磁盘后，payload 对应的内存可以被释放。
    unique_ptr<BlockMemory> payload;
};

// 模拟数据库执行过程中不断访问 block 数据，block数据可以是任意大小
void Pipeline(BufferManager &buffer_manager,
              vector<shared_ptr<BlockHandle>> &blocks) {
    for (auto &block : blocks) {
        // Pin 的语义是：
        // 返回一个可访问的 BufferHandle。
        // 如果 block 已在内存中，可直接使用；
        // 如果 block 已 spill，则需要由 BufferManager 负责把数据读回内存。
        BufferHandle handle = buffer_manager.Pin(block);

        // 调用方不关心数据原来是否 spilled，
        // 只关心现在可以通过 handle 访问
        char *ptr = handle.Ptr();
        Consume(ptr);

        // handle 在这里析构。
        // 通过 RAII 结束对该 payload 的持有，使其重新变为可驱逐。
    }
}
```



### **与Bolt结合**

方案 A 在 Bolt 中的核心思想是：新建 `BufferManagerRowContainer`，不再让上层长期持有稳定的 `char* row`，而是把 row 的逻辑身份表示为 `RowId`，把“访问数据”变成一次受控的、短生命周期的 pin 行为。



可以把 Bolt 里的对象边界定义成和DuckDB中类似的概念：

|概念/类名|作用|
|---|---|
|BlockManager|负责 `block_id` 分配、spill 文件布局、block 写出和 block 读回。|
|BufferManager|负责 resident block 的内存预算、pin、驱逐、restore、reclaim 集成|
|BlockHandle|`BlockHandle` 表示 block 身份|
|BufferHandle<br>|`BufferHandle` 表示一次有效的 resident 访问。系统需要显式 `Pin`，但 `Unpin` 通过 RAII 自动完成。|
|BufferManagerRowContainer|行数据按 block/page 组织。append 一行时返回 `RowId`，**访问一行时由 ****`RowId -> block_id + offset`**** 找到对应 block，随后拿到 ****`BufferHandle`**** 再解析 row**。|
|BufferManagerWindowPartition<br>|不再保存 `folly::Range<char**>`，而是保存 `RowId` 序列、row\-range descriptor，或按 block 分段的 row list。它对 `Window` 暴露的仍然是“抽取列、计算 peer/frame”能力，但内部通过 `RowId` 访问数据。|

执行流程图如下所示：

```Plain Text
StreamingWindowBuild append row
  -> BufferManagerRowContainer
      -> return RowId
  -> store RowId in partition metadata
  -> build WindowPartition logical view
  -> Window execution requests rows
      -> locate block by RowId
      -> BufferManager Pin
      -> BufferHandle lifetime
      -> read / compare / extract row data
      -> handle leaves scope
      -> automatic Unpin
```



### **与\`StreamingWindowBuild\` 的结合**

在 `StreamingWindowBuild` 场景里，本方案不是简单把一个 spill 组件塞进去，而是要替换当前 `RowContainer + char*` 访问模型。



具体变化是：

- `StreamingWindowBuild::addInput()` 不再 `data_->newRow()` 后长期把 `char*` 推进 `inputRows_` / `sortedRows_`，而是 append 后拿到 `RowId`。

- 当前 `partitionStartRows_` 这种“索引到 `sortedRows_` 中的 char\* 数组”的方式，需要改成索引到 `RowId` 数组，或索引到一组 row range descriptor。

- `WindowPartition` 不能再基于 `folly::Range<char**>` 直接调用 `RowContainer::extractColumn(...)`；需要改成面向 `RowId` 的 `extractColumn` / `compareRows` / `computePeerBuffers`。

- **`WindowPartition`**** 的构建阶段只建立逻辑视图，不批量 pin；真正 pin 发生在窗口计算需要读具体行时。**



### **\`RowId\` 替代 \`char\*\` 后，\`Window::callApplyForPartitionRows\` 如何继续计算？**



需要的改造主要包括：

1. 新建 `BufferManagerWindowPartition`：把 `partition_` 从 `folly::Range<char**>` 改成 `RowId` 视图。

2. 给 `BufferManagerRowContainer` 增加面向 `RowId` 的访问接口：包括 `extractColumn(RowId...)`、`compare(RowId, RowId, ...)`、`extractNulls(RowId...)`、`decodeRow(RowId)` 等。

3. 重写 peer/frame 相关逻辑：当前 `computePeerBuffers` 和 `searchFrameValue` 都假设 row 可通过 `char*` 直接比较；迁移后需要在比较路径内短暂 pin 对应 block。

4. 评估 pin 粒度：如果每次比较都单独 pin 一行，会有很高开销；更合理的是在 `extractColumn`、peer 比较、frame 搜索时按 block 批量 pin，或使用小范围 cache。





## **方案 B：基于 Linux User Page Fault 的自动换页**

### **Linux User Page Fault 机制简介**

Linux 提供了一类允许用户态参与页错误处理的机制，代表性接口是 `userfaultfd`。它的基本思想是：某些虚拟内存区域发生缺页时，不由内核直接完成所有恢复动作，而是把 fault 事件交给用户态处理线程。用户态线程可以决定如何准备这页数据，然后通过专门的 ioctl 把数据拷回 faulting page，再让原线程继续运行。接口参见：[Linux ](https://www.man7.org/linux/man-pages/man2/ioctl_userfaultfd.2.html)[`ioctl_userfaultfd(2)`](https://www.man7.org/linux/man-pages/man2/ioctl_userfaultfd.2.html)[ 手册](https://www.man7.org/linux/man-pages/man2/ioctl_userfaultfd.2.html)



从系统行为上看，它至少能支持三件事：

1. 监控一段虚拟地址区间的缺页。

2. 在用户态决定这页数据从哪里来，例如零页、远端内存、spill 文件、压缩页。

3. 由用户态在合适时机把页内容恢复，再恢复原始执行流。

这种机制让应用可以自己决定哪些页先驱逐、驱逐到哪里、再次访问时如何恢复，而不需要把“restore”写成上层算子代码的一部分。



### **一个最小化的\`userfaultfd\` 示例**

下面这段代码不是完整可运行工程，只是一个最小示意，展示典型流程：创建 `userfaultfd`、协商 API、注册地址区间、在 handler 线程里收到 fault 后用 `UFFDIO_COPY` 填页。

```C++
#include <fcntl.h>
#include <linux/userfaultfd.h>
#include <poll.h>
#include <pthread.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

constexpr size_t kPageSize = 4096;

struct HandlerArgs {
  int uffd;
};

void* faultHandler(void* arg) {
  auto* args = reinterpret_cast<HandlerArgs*>(arg);
  struct pollfd pfd;
  pfd.fd = args->uffd;
  pfd.events = POLLIN;

  for (;;) {
    // poll等待，可以优化成epoll
    int nready = poll(&pfd, 1, -1);
    if (nready <= 0) continue;
    // 从fd中读取kernel传下来的参数
    struct uffd_msg msg;
    ssize_t nread = read(args->uffd, &msg, sizeof(msg));
    if (nread != sizeof(msg)) continue;
    if (msg.event != UFFD_EVENT_PAGEFAULT) continue;
    // 拿到触发page fault的地址
    void* fault_addr = reinterpret_cast<void*>(msg.arg.pagefault.address & ~(kPageSize - 1));
    // 分配内存，赋值
    void* page = mmap(nullptr,kPageSize,PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS,-1,0);
    // 示例：真实场景可改为从 spill 文件读页
    memset(page, 0x5A, kPageSize);
    
    // 把分配且赋值过的内存页传到kernel中，kernel会把这页的数据拷贝到对应的页面
    struct uffdio_copy copy;
    memset(&copy, 0, sizeof(copy));
    copy.src = reinterpret_cast<unsigned long>(page);
    copy.dst = reinterpret_cast<unsigned long>(fault_addr);
    copy.len = kPageSize;

    ioctl(args->uffd, UFFDIO_COPY, &copy);
    munmap(page, kPageSize);
  }

  return nullptr;
}

int main() {
  void* region = mmap(nullptr,16 * kPageSize,PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_ANONYMOUS,-1,0);
  
  // 注册user page fault fd
  int uffd = syscall(SYS_userfaultfd, O_CLOEXEC | O_NONBLOCK);
  struct uffdio_api api;
  memset(&api, 0, sizeof(api));
  api.api = UFFD_API;
  ioctl(uffd, UFFDIO_API, &api);

  // 把region注册到系统中，一旦在region中发生缺页中断，就会在user page fault fd上产生可读信号
  struct uffdio_register reg;
  memset(&reg, 0, sizeof(reg));
  reg.range.start = reinterpret_cast<unsigned long>(region);
  reg.range.len = 16 * kPageSize;
  reg.mode = UFFDIO_REGISTER_MODE_MISSING;
  ioctl(uffd, UFFDIO_REGISTER, &reg);

  // 开启page fault handler线程
  HandlerArgs args{uffd};
  pthread_t tid;
  pthread_create(&tid, nullptr, faultHandler, &args);

  // 由于是第一次访问region区域，在此之前kernel没有给他分配page，所以会触发page fault
  volatile uint8_t value = *reinterpret_cast<volatile uint8_t*>(region);
  printf("first byte = %u\n", value);

  pause();
  return 0;
}
```



### **在 Bolt 中的详细设计**



本方案的核心思想是：尽量不改 `StreamingWindowBuild`、`WindowPartition`、`RowContainer` 这些上层对象的访问协议，而是在底层为 `RowContainer` 承载的页建立受控虚拟内存映射，并用用户态 page fault 机制实现自动 restore。



其基本运行方式可以概括为：

1. `RowContainer` 仍然保存 row 数据，上层仍然按现有方式使用 `char*` 行地址和现有遍历逻辑。

2. “新增的BufferManager组件”维护一组受控虚拟页，并把它们注册到 `userfaultfd`。

3. 内存压力来临时，“新增的BufferManager组件” 选择冷页写入磁盘。

4. 上层再次访问这些地址时触发 page fault。

5. 用户态 fault handler 从 backing store 恢复页内容，再让原线程继续执行。



执行流程图如下所示：

```Plain Text
Existing RowContainer page
  -> if no memory pressure: keep resident
  -> if memory pressure:
       pager selects cold page
       -> write page to backing store
       -> mark page non-resident
  -> operator accesses row
  -> page fault
  -> user-space fault handler
  -> read page from spill store
  -> UFFDIO_COPY restore page
  -> continue existing access path
```



### **如何与\`StreamingWindowBuild\` 结合？**

方案 B 的最大特点是：原则上不引入新的数据结构，不修改上层应用代码，也不要求把 `WindowPartition` 改造成 `RowId` 视图。



这意味着：

- `StreamingWindowBuild::addInput()` 仍然可以继续写 `char* newRow = data_->newRow()`。

- `inputRows_`、`sortedRows_`、`windowPartitions_` 这些结构理论上都可以继续保存 `char*`。

- `Window::callApplyForPartitionRows()`、`WindowPartition::extractColumn(...)`、`computePeerBuffers(...)` 这套上层逻辑可以基本不变。



### **方案 B 如何触发 spill？**

1. 当 `MemoryArbitrator / MemoryPool` 判定需要回收内存时，pager 从 `RowContainer` 所在页里挑选 victim pages。

2. 将 victim page 写入 spill/backing store。

3. 写回成功后，把这些页在虚拟地址空间上标成 non\-resident。

4. 后续上层任何对这些 `char*` 的访问都会自然触发 page fault 和 restore。



## **横向对比**

### **方案A：DuckDB 风格 BufferManager 方案优缺点分析**

#### 优点

- 相比方案B，所有操作均在用户态实现，受Kernel影响小，而且不会对Kernel有依赖

#### 缺点

- 需要的改造工作比较多，几乎所有的数据结构和计算逻辑都需要重写

- 所有开发者都需要理解用法



### **方案B：基于 Linux User Page Fault 的自动换页 方案优缺点分享**

#### 优点

- 对上层代码改动最小，这是方案 B 最核心的优势。

#### **缺点**

- 对 Linux 特定机制依赖强。

- 可观测性和调试难度更高。

- 相比方案A，在数据restore的时候，多了一次内存拷贝



### 总结

方案 A 更像一条“Bolt 显式接管 block/page 生命周期”的工程化路线，优点是边界清晰、memory accounting 和 reclaim 更容易统一，但代价是必须改造 `RowContainer`、`WindowPartition` 和 row access 协议。  

方案 B 更像一条“尽量保留上层、把自动换页下沉到底层 pager”的系统化路线，优点是对现有 `StreamingWindowBuild` 和 `Window::callApplyForPartitionRows` 改动最小，但代价是高度依赖 Linux `userfaultfd` 一类机制，底层实现风险更高。  



# Q\&A

## **为什么BufferManager机制能降低 spill 逻辑复杂度？**

从源码上看，当前 spill 相关逻辑明显分散在多个层次中，而不是收敛在统一的 buffer/page 管理层：

- `Window` 需要自己决定在什么阶段触发 `sortSpill()` 或 `spill()`，见 \[Window\.cpp\]\(exec/Window\.cpp:467\) 之后的 `reclaim()` 路径。

- `Spiller` 自己维护 `spillRuns_`、`fillSpillRuns()`、`runSpill()`、`markAllPartitionsSpilled()` 等一整套运行时状态机，见 \[Spiller\.cpp\]\(exec/Spiller\.cpp:723\)。

- `RowContainer` 的 layout、序列化、spill size 计算本身也包含 spill 特判逻辑，例如 `rowSizeOffset` 之前的数据才参与 spill，见 \[RowContainer\.h\]\(exec/RowContainer\.h:1807\)。

- `RowsStreamingWindowBuild` 甚至区分了普通内存行和 `SerializedRows` 两套路径，还要自己处理 `buildNextInputOrPartitionFromSpill()`、`loadNextPartitionFromSpill()`、`storeRows()` 这些流程，见 \[RowsStreamingWindowBuild\.cpp\]\(exec/RowsStreamingWindowBuild\.cpp:62\)、\[RowsStreamingWindowBuild\.cpp\]\(exec/RowsStreamingWindowBuild\.cpp:181\)、\[RowsStreamingWindowBuild\.cpp\]\(exec/RowsStreamingWindowBuild\.cpp:201\)。

这些现状说明：当前 spill 并不是由统一底层机制接管，而是被分散地嵌在 `Window`、`Spiller`、`RowContainer`、`WindowPartition` 等多个对象里。这正是“复杂度高”的直接依据。



同样，这也是工程复杂度收益和可维护性收益的直接依据：如果不建立统一层，后续新算子大概率还会继续复制这种“算子自管 spill”模式。



## **BufferManager机制为什么能提升 spill性能？**

当前实现里，spill / restore 路径存在明显的格式转换、复制和同步处理开销：

- `RowsStreamingWindowBuild::storeRows()` 会把临时排序结果重新 copy 到一块新分配的连续内存里，再转成 `sortRows_`，见 \[RowsStreamingWindowBuild\.cpp\]\(exec/RowsStreamingWindowBuild\.cpp:181\)。

- `RowContainer::sizeIncrement()` 明确写着“for spilling the practical minimum increment is a huge page”，说明现有容器增长和 spill 粒度并不天然匹配，见 \[RowContainer\.cpp\]\(exec/RowContainer\.cpp:1137\)。

- `Spiller` 的 row\-based spill 需要先把容器里的 row 整理成 spill run，再执行 `runSpill(lastRun)`，这是一条显式的批处理路径，见 \[Spiller\.cpp\]\(exec/Spiller\.cpp:732\)。

- `WindowPartition` 和 `Window` 的读取路径本质上仍然依赖 `char*` 行指针和 `RowContainer::extractColumn(...)` 做列提取，见 \[Window\.cpp\]\(exec/Window\.cpp:827\)、\[WindowPartition\.cpp\]\(exec/WindowPartition\.cpp:49\)。



## **BufferManager机制为什么能创造 IO 与计算 overlap 的空间？**

当前 `Window` 侧的 reclaim / spill 语义比较同步，系统难以自然形成“后台换入换出 \+ 前台继续计算”的运行模式：

- `Window::reclaim()` 里直接调用 `windowBuild_->sortSpill()` 或 `windowBuild_->spill()`，然后 `pool()->release()`，见 \[Window\.cpp\]\(exec/Window\.cpp:467\)。

- `RowsStreamingWindowBuild::loadNextPartitionFromSpill()` 是一条显式的“下一步再从 spill 合并流里读回来”的路径，计算与恢复是串行交替的，见 \[RowsStreamingWindowBuild\.cpp\]\(exec/RowsStreamingWindowBuild\.cpp:201\)。

- `Window::callApplyForPartitionRows()` 自己并不具备独立的页驻留协议，它只会调用 `currentPartition_->extractColumn(...)` 和 `computePeerAndFrameBuffers(...)`，见 \[Window\.cpp\]\(exec/Window\.cpp:827\)。这意味着一旦底层数据不在内存里，当前实现缺少一个天然的“按需 pin / fault restore”层把 IO 和计算解耦开。



## 为什么非要实现BufferManager机制？每个算子实现Spill不就好了？

如果以算子维度来看，可能仅仅只有20\+算子，每个算子实现Spill就好了，但是其实每个算子内部的情况也都有重复，比如目前Window面临的单Partition过大问题，之前HashBuild就已经遇到并解决过了，现在Window再次遇到就还需要再实现一遍，再比如之前实现的RowBasedSpill，Agg实现了一遍，HashJoin也实现了一遍。



