# OverAcquireMemoryTarget 保留原始异常设计

## 背景

`OverAcquireMemoryTarget::borrow` 当前捕获 `BoltException` 后调用
`BOLT_FAIL`。这会创建一个新的 `BoltRuntimeError`，从而替换原异常的动态
类型、错误码、错误来源及原始抛出位置。

## 方案

保持现有捕获范围不变，仅处理 `BoltException`。捕获后：

1. 使用 `LOG(ERROR)` 输出 over-acquire 失败的上下文，包括已授权内存、
   目标 over-acquire 大小、当前 over-acquire 大小、比例及原异常消息。
2. 使用裸 `throw;` 重新抛出当前活动异常，保留原异常语义。

不使用 `throw e;`，因为它不是原样重抛；也不使用
`std::rethrow_exception(std::current_exception())`，因为在 catch 块中裸
`throw;` 更直接。

## 测试

为 `OverAcquireMemoryTarget` 增加异常传播单元测试：让 over-target 抛出带
可识别错误信息的 `BoltException`，验证调用方收到的仍是原异常，并保留
关键异常属性。日志只保留必要的人工检查，不为日志文本建立脆弱的精确匹配。

## 范围

不扩大到 `std::exception` 或未知异常，不修改正常借用、归还和内存统计
逻辑。
