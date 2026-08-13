# BM Window 不支持与非原生覆盖情况

本文整理当前 `BmStreamingWindowBuild` 相关 window 计算边界。这里需要区分三类情况：

- 现有 Window 基线本身不支持。
- BM 原生路径不支持，但自动选择时可以 fallback 到现有 WindowBuild。
- 强制使用 BM 时会直接报错。

## RANGE 常量边界

当前 Window 层不支持 `RANGE` frame 的常量 k-bound，例如：

```sql
sum(v) over (
  partition by p
  order by s
  range between 10 preceding and 10 following
)
```

会报错：

```text
Window frame of type RANGE does not support constant arguments
```

原因在 `Window::checkKRangeFrameBounds()`：`RANGE k PRECEDING/FOLLOWING` 的 `k` 必须来自输入列，不能是常量。支持的写法是：

```sql
sum(v) over (
  partition by p
  order by s
  range between off preceding and off following
)
```

同时，frame bound 列类型必须和 `ORDER BY` 列类型一致，否则也会报错：

```text
Window frame of type RANGE does not match types of the ORDER BY and frame column
```

这个限制是现有 Window 层限制，不是 BM 特有问题。`ROWS BETWEEN 10 PRECEDING ...` 支持常量边界。

## 基线不支持的 aggregate window

以下 aggregate 作为 window function 时，当前 `SortWindowBuild` 基线也不支持：

- `merge`
- `reduce_agg`

语义覆盖测试里将它们明确归类为 baseline unsupported，并验证原始 `SortWindowBuild` 会抛异常。因此这类不属于 BM 功能缺失。

## BM 原生路径不覆盖的复杂类型输入 aggregate

以下复杂类型输入相关 aggregate 不走 BM 原生 window read 路径，自动 BM feature 下会 fallback 到现有 WindowBuild：

- `aggregate_map_sum`
- `array_addition`
- `array_count`
- `bit_days_or`
- `map_union`
- `map_union_avg`
- `map_union_count`
- `map_union_max`
- `map_union_min`
- `map_union_sum`
- `non_null_count`
- `set_union`

原因是当前 `BmRowContainer` 的 window 路径主要覆盖 scalar/fixed/string 等可直接行式存取的类型；ARRAY/MAP/ROW 等复杂类型不作为 BM 原生第一路径处理。

这类函数不是语义不支持。自动选择 BM 时会 fallback，语义仍由现有 WindowBuild 覆盖；强制使用 BM 时则取决于输入 RowType 是否被 `BmRowContainer` 支持。

## BM 输入类型限制

`BmStreamingWindowBuild` 要求输入 RowType 可被 `BmRowContainer` 存储和读取。

自动选择 BM 时，如果输入类型不支持，会 fallback 到现有 WindowBuild。强制指定 `WindowBuildType::kBmStreamingWindowBuild` 时，如果输入类型不支持，会直接报错：

```text
BmStreamingWindowBuild does not support input type ...
```

当前语义覆盖测试已覆盖的 BM 支持类型包括：

- integer family：TINYINT、SMALLINT、INTEGER、BIGINT
- floating point：REAL、DOUBLE
- boolean
- VARCHAR、VARBINARY
- DATE、TIMESTAMP
- short decimal、long decimal

## 结论

当前需要特别注意的是 `RANGE` 常量边界：它不是 BM 特有问题，而是现有 Window 层就不支持。BM 原生路径的主要限制是复杂类型输入；自动 BM feature 会通过 fallback 保证语义覆盖，但强制 BM 时会暴露为不支持输入类型。
