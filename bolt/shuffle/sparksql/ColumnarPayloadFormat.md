---
spec: ColumnarPayload
format-version: 0
doc-revision: 3
status: Draft
updated: 2026-08-20
---

# ColumnarPayload Binary Format

Shuffle 的列式二进制 Payload 格式，与既有的行式 `RowBlockPayload` 并列。

**术语**：本文档中的 **Payload** 一律指 ColumnarPayload 这一线格式单元，与
`bolt/shuffle/sparksql/Payload.h` 中的 `Payload` 类层次没有关系 —— 后者是 Writer
的内存与序列化抽象，本文档只描述它序列化出来的字节。

§1–§11 是规范性的，定义线格式与解析语义；§12 是非规范性的，记录设计取舍与实现
指引，两者冲突时以 §1–§11 为准。

本文档原地更新，不新增版本文档。版本号含义与更新规则见 §11。

## 1. 约定

### 1.1 关键词

- **必须 / 不得**：违反即为非法 Payload，Reader 必须以错误终止，不得猜测或修复。
- **应当**：强烈建议；Writer 违反不导致 Reader 失败。
- **可以**：可选。

### 1.2 编码

- 多字节整数使用 little-endian；bit-packed 数据使用 LSB-first；
- 字段紧密排列，无隐式 padding；
- 所有 size / length 以 byte 为单位；
- 记号：`u8 / u32 / u64` 为无符号定长整数，`bytes[n]` 为 n 个字节，
  `ceil(a / b)` 为向上取整整数除法。

### 1.3 物理类型

| 物理类型 | `type_width` | Signedness | 值流编码 |
|---|---:|---|---|
| TinyInt | 1 | signed | Raw Data |
| SmallInt | 2 | signed | Encoding Loop |
| Integer | 4 | signed | Encoding Loop |
| Bigint | 8 | signed | Encoding Loop |
| Date | 4 | signed | Encoding Loop |
| Float | 4 | — | Raw Data |
| Double | 8 | — | Raw Data |
| String | 变长 | — | §7.1、§8 |

Schema 中出现表外类型的 Payload 非法。

### 1.4 Stream

Stream 是格式内的最小物理单位，由外部 Schema 唯一确定：

```text
stream_count(c) := 2, if column_schema[c].type == String
                   1, otherwise
stream_count_total (S) := sum over c in [0, C) of stream_count(c)
```

Stream 按列顺序展开，String 列先 Length/Index Stream、后 Data Stream：

```text
column 0 (Integer) -> stream 0
column 1 (String)  -> stream 1 (Length/Index), stream 2 (Data)
column 2 (Double)  -> stream 3
```

## 2. 外部上下文

Payload 不自描述。Reader 必须从外层协议取得：

| 名称 | 含义 |
|---|---|
| `column_count` (`C`) | 列数量。必须 `>= 1`。 |
| `column_schema[C]` | 固定列顺序、物理类型、类型宽度、signedness。 |
| `schema_identity` | 生产端与消费端一致的 Schema ID 或 fingerprint。 |
| `codec` | 整个 Payload 共用的外部 codec 抽象。 |

同一 Payload 只存在一个 `codec` 上下文，不存在 Null / Run / Buffer 级 codec
选择字段。本 RFC 不定义 codec 本身，也不对其名称、类型或返回路径做任何分支。

`payload_size` **不是**必需上下文：每个字段的长度要么固定、要么由已读字段
导出，Reader 单遍推进即可，总字节数在解析结束时自然得出。外层协议可以提供
`payload_size`，提供时应当用作边界检查上界并在结束时校验相等。

Payload 内没有任何冗余可用于检测与 `schema_identity` 的错配。Reader 不负责
检测 Schema 是否对应，Schema 不对应时行为未定义。

## 3. Payload

```text
ColumnarPayload :=
    row_count          : u32
    run_count          : u32
    variable_size      : u64
    null_stored_size   : u32
    null_decoded_size  : u32
    null_stored_body   : bytes[null_stored_size]
    encoding_tags      : bytes[ceil(C / 8)]
    runs               : Run[run_count]
```

| 字段 | 含义 |
|---|---|
| `row_count` | 逻辑总行数。 |
| `run_count` | Run 数量。 |
| `variable_size` | 变长数据原始字节总数的**估计值**，见 §3.1。 |
| `null_stored_size` | Null stored body 的字节数。必须 `>= 1`。 |
| `null_decoded_size` | Null 解压后的字节数；`0` 表示未压缩。 |
| `null_stored_body` | Null 区域，见 §4。 |
| `encoding_tags` | 编码 bitset，每列 1 bit，见 §3.2。 |
| `runs` | 连续排列的 Run，见 §5。 |

前 24 bytes 为定长头，每个字段落在其自身宽度的自然对齐偏移上（0 / 4 / 8 /
16 / 20）。变长的 `encoding_tags` 位于 Null body 之后、`runs` 之前。

### 3.1 variable_size

供 Reader 一次性预分配变长数据 buffer 的估计值，**不是校验量**。统计口径：
所有非 Null 变长值的原始 payload bytes，在 Dictionary、Narrowing 和 Compression
之前计算（字典命中的值按完整原始长度计入）；不含 Length、Index、Null bitmap、
Dictionary metadata 或任何 Header；Null 值贡献 0。

Writer 可以给出近似值，应当保证 `variable_size >= 实际值`，偏小不构成非法
Payload。Reader 不得将其用于正确性校验，必须能处理预分配不足。变长数据的
真实边界始终由 Length Stream（§7.1）与字典结构（§8）给出。

### 3.2 EncodingTag

```text
tag = (encoding_tags[c / 8] >> (c % 8)) & 0x01
```

| 值 | 含义 |
|---:|---|
| `0` | RAW String Encoding |
| `1` | Dictionary String Encoding |

EncodingTag 只对 String 列有意义，是**列级**属性并覆盖整个 Payload：同一列不得
在部分 Run 用 Dictionary、其余 Run 用 RAW（RAW fallback 是 Dictionary 编码内部的
机制，见 §8.3）。非 String 列对应 bit 必须为 0；最后一个 byte 中超出 `C` 的
未使用高 bit 必须为 0。

## 4. Null 区域

### 4.1 压缩包络

```text
if null_decoded_size == 0:
    null_decoded_body = null_stored_body      // 未压缩
    expected_size     = null_stored_size
else:
    null_decoded_body = decompress(codec, null_stored_body)
    expected_size     = null_decoded_size
```

`len(null_decoded_body)` 必须精确等于 `expected_size`。

`null_decoded_size == 0` 只表示未压缩，不表示空 body：`C >= 1` 蕴含 decoded body
至少含 1 byte 的 NullTag，故其长度恒 `>= 1`，`null_stored_size` 亦必须 `>= 1`。

### 4.2 Null decoded body

```text
Nulls :=
    tags             : bytes[ceil(C * 2 / 8)]
    raw_null_bitmaps : RawNullBitmap[raw_null_column_count]

null_tag(c) = (tags[c / 4] >> ((c % 4) * 2)) & 0x03
```

| 值 | 名称 | 含义 |
|---:|---|---|
| `0b00` | `ALL_NULL` | 当前列全部为 Null。 |
| `0b01` | `NO_NULL` | 当前列没有 Null。 |
| `0b10` | `RAW_NULL` | 后续存在当前列的 RawNull bitmap。 |
| `0b11` | Reserved | Reader 必须拒绝。 |

最后一个 tags byte 中超出 `C` 的未使用高 bit 必须为 0。
`raw_null_column_count` 是 NullTag 为 `RAW_NULL` 的列数，由 tags 派生。

RawNull bitmap：

- 只为 `RAW_NULL` 列出现，按 column ID 递增排列；
- 每个含 `row_count` 个有效 bit，线格式长度为 `ceil(row_count / 8)`；
- row `r` 对应 byte `r / 8` 的 bit `r % 8`；bit `0` 为 Null，bit `1` 为非 Null；
- 最后一个 byte 中超出 `row_count` 的 bit 必须为 0。

Writer 应当在 bitmap 全 0 或全 1 时改用 `ALL_NULL` / `NO_NULL`；Reader 必须接受
退化的 `RAW_NULL` bitmap。

Null decoded body 的长度必须等于：

```text
ceil(C * 2 / 8) + raw_null_column_count * ceil(row_count / 8)
```

## 5. Run

Run 是物理数据片段，不携带行数语义，也不要求覆盖完整行。

```text
Run :=
    compression_layout : u8
    stored_sizes       : u64[stored_buffer_count]
    decoded_sizes      : u64[stream_count_total]
    stored_buffers     : bytes[sum(stored_sizes)]

stored_buffer_count := 1, if compression_layout in {COMBINED, COMBINED_STORED}
                       S, if compression_layout == SEPARATE
```

| 值 | Layout | 含义 |
|---:|---|---|
| `0x00` | `COMBINED` | 所有 Stream 合并为一个物理 Buffer，整体经 codec 压缩。 |
| `0x01` | `SEPARATE` | 每个 Stream 一个物理 Buffer，是否压缩逐 Stream 标记。 |
| `0x02` | `COMBINED_STORED` | 所有 Stream 合并为一个物理 Buffer，整体未压缩。 |
| 其他 | — | Reader 必须拒绝。 |

不同 Run 可以使用不同的 `compression_layout`。

### 5.1 SEPARATE 的压缩包络

第 `s` 个 stored buffer 对应第 `s` 个 Stream，`decoded_sizes[s] == 0` 表示未压缩：

| `stored_sizes[s]` | `decoded_sizes[s]` | 含义 |
|---|---|---|
| `0` | `0` | 空 Stream；不得调用 codec。 |
| `0` | `> 0` | 非法，Reader 必须拒绝。 |
| `> 0` | `0` | 未压缩，长度为 `stored_sizes[s]`；不得调用 codec。 |
| `> 0` | `> 0` | 压缩，实际解压长度必须精确等于 `decoded_sizes[s]`。 |

Writer 应当在压缩无收益时改用未压缩形式。

### 5.2 COMBINED 的压缩包络与切分

```text
COMBINED:        combined_bytes = decompress(codec, stored_buffers[0])
                 len(combined_bytes) 必须等于 sum(decoded_sizes)
COMBINED_STORED: combined_bytes = stored_buffers[0]
                 stored_sizes[0] 必须等于 sum(decoded_sizes)
```

两种 layout 下 `decoded_sizes[s]` 均携带每个 Stream 的真实长度，不兼作压缩标记。
`combined_bytes` 按 §1.4 的 Stream 顺序拼接，第 `s` 个 Stream 为：

```text
stream_offset[s] = sum(decoded_sizes[0:s])
combined_bytes[stream_offset[s] : stream_offset[s] + decoded_sizes[s]]
```

### 5.3 Run 拼接

不同 Run 的数据按 Run ID 递增顺序连接。对每个 Stream `s`：

```text
stream_bytes[s] = concat(stream_of(run[0], s), ..., stream_of(run[n-1], s))
```

§7、§8 的所有解析都在 `stream_bytes[s]` 上进行，而不是在单个 Run 的切片上进行。
Run 边界不携带逻辑语义：既不对应行边界，也不蕴含 Value 数量的整除关系。

### 5.4 Run 边界约束

| Stream 区域 | 约束 |
|---|---|
| Encoding Loop | Run 边界必须落在完整 Encoding Block 之间；EncodingByte 及其 Body 不得跨 Run。 |
| 单个 Dictionary（`Entry*` + 终止标记 + `matched_row_count`） | 必须完整位于一个 Run 内，不得跨 Run。 |
| Raw Data、`FallbackRawBytes`、Dictionary index 段 | 无约束，可落在任意字节位置。 |

以上均为**单个 Stream 内部**的约束，Stream 之间不做任何同步：某个 Dictionary 的
entry 表与消费它的 index 段可以分处不同 Run。Reader 不得假设读完 Run `k` 即可
解出 Run `k` 的全部值。

## 6. Value 与 Null 的对应

Null 行在值流中不存在，也不占位。对列 `c`：

```text
non_null_count[c] := 0,                          if ALL_NULL
                     row_count,                  if NO_NULL
                     popcount(RawNullBitmap[c]), if RAW_NULL
```

第 `i` 个解码值对应该列按 row ID 递增排列的第 `i` 个非 Null 行；所有 Run 拼接后的
逻辑 value 数量必须等于 `non_null_count[c]`。

`non_null_count[c]` 是该列所有 Stream 的唯一长度权威：Encoding Block 划分（§7.2）
与 Dictionary fallback 数量（§8.3）均由它导出。Reader 必须在解析 Null 区域之后、
解析任何 Stream 之前算出全部 `non_null_count[c]`。

## 7. Stream 编码

### 7.1 Stream 内容

| Stream | 内容 | 编码 |
|---|---|---|
| SmallInt / Integer / Bigint / Date 值流 | 非 Null 值 | Encoding Loop |
| TinyInt / Float / Double 值流 | 非 Null 值 | Raw Data |
| String Length/Index（RAW tag） | 每个非 Null 值的 byte 长度 | Bigint Encoding Loop |
| String Length/Index（Dictionary tag） | index 段 + fallback 长度段 | Raw `u8` + Bigint Encoding Loop |
| String Data（RAW tag） | 非 Null 值的原始字节 | Raw Data |
| String Data（Dictionary tag） | DictionarySequence + FallbackRawBytes | §8 |

String 长度使用 Bigint（`type_width = 8`）Encoding Loop；长度值非负，按 signed 与
unsigned 解释结果一致。

### 7.2 Encoding Block 划分

Encoding Loop 把 Stream 的源数据切成 Block 逐块独立编码，
`value_count(block) = source_bytes(block) / type_width`。`type_width` 必须整除 64，
因此定长 Block 的 `value_count` 为 32 / 16 / 8。

源数据总量由 §6 导出，不单独存储：

```text
total_source_bytes = non_null_count[c] * type_width      // 定宽值流
                   | non_null_count[c] * 8               // RAW 长度流
                   | fallback_value_count[c] * 8         // Dictionary 长度流尾段

full_block_count  = total_source_bytes / 64
tail_source_bytes = total_source_bytes % 64
```

- 前 `full_block_count` 个 Block 的 `source_bytes` 固定为 64；
- 若 `tail_source_bytes > 0`，其后追加恰好一个尾块，`source_bytes` 等于
  `tail_source_bytes`；尾块只可能出现在整个 Stream 的末尾，即最后一个 Run 中；
- 尾块的 Body 长度按 §7.3 的公式用缩小后的 `value_count` 计算，`PLAIN` 尾块的
  Body 长度为 `tail_source_bytes` 而非 64。

Reader 解析完最后一个 Block 后，Stream 必须恰好耗尽。

### 7.3 EncodingByte

每个 Block 以 1 byte 的 EncodingByte 开头：低 2 bit 为 kind，高 6 bit 为参数
（`0..63`）。

```text
encoding_kind  = encoding_byte & 0x03
encoding_param = encoding_byte >> 2
```

| Kind | 值 | 参数 | Body bytes |
|---|---:|---|---|
| `CONST_NARROW` | `0` | `narrow_bytes` | `narrow_bytes` |
| `BIT_PACK` | `1` | `bit_width` | `ceil(value_count * bit_width / 8)` |
| `FOR_BIT_PACK` | `2` | `delta_bit_width` | `type_width + ceil(value_count * delta_bit_width / 8)` |
| `PLAIN` | `3` | 必须为 0 | `source_bytes` |

Block 的线格式长度为 `1 + Body bytes`。Writer 应当选择 Body 最小的合法编码；
Reader 必须接受任何合法编码。

**CONST_NARROW** — 本 Block 覆盖的所有 value 必须相同。Body 是该常量的低
`narrow_bytes` 个字节（little-endian），`1 <= narrow_bytes <= type_width`。解码时
按物理类型的 signedness 扩展到 `type_width`，再重复 `value_count` 次。窄化必须
无损，即扩展结果等于原常量。

**BIT_PACK** — `1 <= bit_width <= min(63, type_width * 8)`，需要 64 bit 时必须用
`PLAIN`。value 按原顺序、LSB-first 连续写入 bit stream，跨 byte 边界不做对齐，
尾部未使用 bit 必须为 0。解码得到的 `bit_width` 位整数按物理类型的 signedness
扩展（signed 按二进制补码符号扩展）。

**FOR_BIT_PACK** — Body 前 `type_width` 个字节是 Base（little-endian，按物理类型
解释），其后是 `value_count` 个 `delta = value - base`，按**无符号**数 BIT_PACK。
`0 <= delta_bit_width <= 63`，`0` 表示所有 delta 为 0 且 Delta 区长度为 0；需要
64 bit delta 时必须用 `PLAIN`。Writer 必须保证 `base + delta` 落在物理类型范围内，
应当取 `base = min(values)`。Reader 对合法 Payload 无需处理算术溢出，检测到溢出时
可以拒绝。

**PLAIN** — Body 是完整的 `source_bytes` 原始数据，不做转换。

### 7.4 Raw Data

与原始数据的 byte representation 完全一致，无 Block 结构和 EncodingByte；Null 值
不出现在 Raw Data 中。期望长度：定宽类型值流为 `non_null_count[c] * type_width`，
String Data Stream（RAW tag）为该列所有长度之和。非 Dictionary String bytes 按
非 Null value 顺序连续排列，边界由 Length Stream 给出。

## 8. String Dictionary

EncodingTag 为 1 时，String 列使用 Dictionary Encoding：Writer 维护一个小于
64 byte 的字典，字典满或命中率下降时切换到下一个字典，最终切换到 RAW。

Reader 必须先解析 Data Stream 中的 DictionarySequence（§8.1），取得各字典的 entry
表与 `matched_row_count`，才能解释 Length/Index Stream（§8.2）。

### 8.1 Dictionary Data Stream

```text
DictionarySequence :=
    DictionaryWithNext*
    FinalDictionary
    FallbackRawBytes

DictionaryWithNext := Entry* 0xFE matched_row_count:u32
FinalDictionary    := Entry* 0xFF matched_row_count:u32
Entry              := byte_length:u8 value:bytes[byte_length]
```

- 单个 Dictionary 的所有 Entry 的 serialized space 总和必须小于 64 bytes，因此
  `byte_length` 取值范围为 `[0, 63]`（允许空字符串），永远不会与终止标记冲突：
  Reader 在 Entry 边界读到 `>= 0xFE` 的 byte 即为终止标记；
- 单个 Dictionary 的 Entry 数上限为 63，Index 固定 1 byte；这两项与
  `byte_length < 64` 互相耦合，不预留放宽路径，需要更大字典时必须切换到下一个
  Dictionary；
- Dictionary 数量不固定。`0xFE` 表示当前 Dictionary 结束，读取其
  `matched_row_count` 后继续下一个；`0xFF` 表示最后一个 Dictionary 结束，读取其
  `matched_row_count` 后进入 RAW fallback。最后一个 Dictionary 必须使用 `0xFF`，
  即使 fallback 值数量为 0；
- `matched_row_count` 不包含 Null 值；
- 同一 Dictionary 内的 Entry 应当互不相同；Entry 数量为 0 的 Dictionary 合法但
  Writer 不应当产生。

### 8.2 String Length/Index Stream

```text
StringLengthOrIndexStream :=
    dictionary_indexes : u8[sum(matched_row_count)]
    fallback_lengths   : BigintEncodingLoop[fallback_value_count]
```

- fallback 之前存放 Dictionary Index，每个字典命中的非 Null value 对应一个 1-byte
  index；
- Index 从 0 开始，只在**当前 Dictionary 内**解释，必须小于当前 Dictionary 的
  entry 数；
- 各 Dictionary 的 index 段按 Dictionary 顺序连接，第 `d` 段的 index 数量等于该
  Dictionary 的 `matched_row_count`；
- `fallback_lengths` 是一段独立的 Encoding Loop，
  `total_source_bytes = fallback_value_count * 8`，Block 划分见 §7.2；其第一个
  Block 从 index 段结束处紧接开始，不做对齐。

### 8.3 RAW fallback

fallback 发生后，该列剩余的非 Null String 全部使用 RAW：长度进入同一个 Length
Stream（紧跟所有 index），bytes 按顺序连续存放在 `FallbackRawBytes`。Null 值不占用
Index、Length 或 Raw bytes。必须满足：

```text
fallback_value_count = non_null_count[c] - sum(matched_row_count) >= 0
sum(fallback_lengths) == len(FallbackRawBytes)
```

且 Data Stream 必须恰好在 `FallbackRawBytes` 末尾耗尽。

## 9. 边界条件

| 情况 | 规定 |
|---|---|
| `C == 0` | 非法，Reader 必须拒绝。 |
| `row_count == 0` | 合法。所有列的 NullTag 应当为 `NO_NULL`，`variable_size` 与 `run_count` 应当为 0。 |
| `run_count == 0` 且 `row_count > 0` | 仅当所有列均为 `ALL_NULL` 时合法。 |
| 某列 `ALL_NULL`，或 `non_null_count == 0` | 该列的所有 Stream 在每个 Run 中必须为空，即 `stored_sizes[s] == 0` 且 `decoded_sizes[s] == 0`。 |
| String 列全为空串 | 合法。Data Stream 长度为 0，Length Stream 仍需编码 `non_null_count` 个 0。 |
| 某个 Run 的所有 Stream 均为空 | 合法，但 Writer 不应当产生。 |

## 10. Reader 校验

- **L1**：位于结构解析路径上，每项为 O(1) 或可与已有循环合并，省略会导致越界
  访问或大额分配。Reader 必须实现。
- **L2**：需要额外遍历、累加或分支的一致性检查。Reader 可以只在 debug / fuzz /
  不可信来源模式下开启，生产路径省略不违反本规范。

所有由 Payload 字段参与的长度计算必须做溢出检查，且计算结果必须先与剩余可用
字节数比较再落地为读取动作（属 L1）。

本节所有校验都以「Payload 由同一版格式的 Writer 产出」为前提（§11.1）。格式版本
错配无法在运行时检测，其行为不由本节覆盖。

### 10.1 L1

| # | 校验 |
|---:|---|
| 1 | 剩余字节足以读出 24 bytes 定长头 |
| 2 | `null_stored_size >= 1`，且读取 Null body 不越界 |
| 3 | 读取 `encoding_tags`（`ceil(C/8)` bytes）不越界 |
| 4 | 若外层协议提供 `payload_size`：所有读取偏移不超过它 |
| 5 | Null decoded body 实际长度等于 §4.1 的 `expected_size` |
| 6 | Null decoded body 长度等于 §4.2 由 tags 导出的期望长度（该条同时保证后续所有 bitmap 读取不越界） |
| 7 | 无 `0b11` NullTag |
| 8 | `compression_layout` 为 `0x00` / `0x01` / `0x02` |
| 9 | `sum(stored_sizes)`、`sum(decoded_sizes)` 不溢出，且 Run 声明的总长度不超过剩余字节 |
| 10 | `stored_sizes[s] == 0` 时 `decoded_sizes[s] == 0`（§5.1） |
| 11 | 每个 Buffer 的实际解压长度等于声明的 decoded size |
| 12 | `COMBINED` 的实际解压长度、`COMBINED_STORED` 的 `stored_sizes[0]` 等于 `sum(decoded_sizes)` |
| 13 | `PLAIN` 的 `encoding_param == 0` |
| 14 | `CONST_NARROW` 的 `1 <= narrow_bytes <= type_width` |
| 15 | `BIT_PACK` 的 `1 <= bit_width <= min(63, type_width * 8)` |
| 16 | `FOR_BIT_PACK` 的 `delta_bit_width <= 63` |
| 17 | 每个 Block 的 Body 长度不超过 Stream 剩余字节 |
| 18 | Dictionary Entry 的 `byte_length` 读取不越界 |
| 19 | Dictionary index `< entry_count`；不得用未检查的 index 访问 entry 表 |

13–16 是读出 EncodingByte 时顺带完成的范围判断，省略会导致 Body 长度计算本身错误。

### 10.2 L2

| # | 校验 |
|---:|---|
| 20 | Encoding Loop 的 Block 序列恰好耗尽 Stream |
| 21 | Raw Data Stream 的实际长度等于 §7.4 的期望长度 |
| 22 | `sum(matched_row_count) <= non_null_count[c]` |
| 23 | `sum(fallback_lengths) == len(FallbackRawBytes)` |
| 24 | `encoding_tags` 中非 String 列的 bit 为 0 |
| 25 | tags / bitmap / bit-packed 尾部未使用 bit 为 0 |
| 26 | 单个 Dictionary 的 entry serialized space 之和小于 64 bytes，且每个 `byte_length <= 63`（§8.1） |
| 27 | Encoding Block 与单个 Dictionary 都不跨 Run 边界（§5.4）。需要在拼接 Stream 时保留每个 Run 的贡献区间 |
| 28 | `run_count == 0` 且 `row_count > 0` 时，所有列必须为 `ALL_NULL`（§9） |
| 29 | `FOR_BIT_PACK` 的 `base + delta` 落在物理类型范围内（§7.3 把该义务压在 Writer 上，Reader 可以据此拒绝） |

`variable_size` 不作为校验项（§3.1）。

### 10.3 不可信输入

以上分级的前提是 Payload 来自可信 Writer。若 Payload 可能来自不可信来源，Reader
应当开启全部 L2 校验，并对 `row_count`、`run_count`、`C` 和单次解压输出长度施加
实现级上限。

## 11. 版本与演进

Payload 不落盘，生命周期不超过一次 Shuffle，因此本格式**不提供也不打算提供任何
向后兼容**。本文档原地更新，任何时刻只描述当前生效的那一个版本，不为新版本新增
文档，也不保留历史版本的规范。

### 11.1 线格式一致性由构建保证，不由版本号保证

Payload 内不携带 magic number 与版本号，外层协议也不传递版本。线格式的一致性完全
依赖一个部署事实：**Writer 与 Reader 编译自同一份代码，随同一个二进制部署**。
一次 Shuffle 的写侧与读侧属于同一个应用，不存在版本协商，也不存在混合版本的场景。

这带来两个必须写明的后果：

- **不存在运行时的版本校验**。Reader 无法察觉自己在解析另一版格式产生的数据；本
  规范也不要求任何一方检测这种情况，因为在上述部署前提下它不会发生。
- **格式改动的成本很低**。改动只需在一个 PR 内同时修改 Writer、Reader、测试向量与
  本文档，没有存量数据要迁移，也没有部署窗口要协调。

前提被打破时必须重新设计版本机制。触发条件：Shuffle 数据跨应用或跨二进制版本流动
（例如由持久化的 shuffle service 中转到另一个版本的应用），或同一代码库中并存多个
线格式实现由配置选择。届时需要在 Payload 或外层协议中引入真正的版本标识与拒绝
逻辑，而不是沿用本节。

### 11.2 版本号只用于跟踪

既然没有运行时校验，frontmatter 里的两个计数器是**纯粹的跟踪标识**，不参与任何
解析决策：

| 字段 | 何时 `+1` | 用途 |
|---|---|---|
| `format-version` | 线格式改变 | 在 commit、issue、日志中指代某一版格式；判断某个实现是否落后于本文档 |
| `doc-revision` | 文档任何实质性修改（含伴随格式改动的那次） | 指代文档状态 |

刻意不使用 `MAJOR.MINOR` 形式：点分版本号会让人按 SemVer 去读，把 `2.0` 理解成
"大版本、改动大、不稳定"。本格式不做兼容，"改动大小"这个维度不存在，用点分版本
表达只会误导。改动规模在附录 C 的「需改动」列与变更描述中表达，不进入版本号。

`format-version` 涨到多少都不代表格式不成熟或不稳定，只代表线格式被改过多少次。
不要为了让这个数字好看而合并或跳过版本 —— 它唯一的价值就是与变更历史一一对应。

代码中**可以**定义一个常量对应当前 `format-version`，仅用于日志与问题定位；不得
把它当作正确性保障。

### 11.3 阶段与更新规则

`format-version: 0` 保留表示草稿期，线格式尚未冻结：

| 阶段 | `format-version` | `status` | 线格式改动 | 仅文档改动 |
|---|---|---|---|---|
| 设计中，未冻结 | `0` | `Draft` | 只 `doc-revision + 1` | `doc-revision + 1` |
| 已实现，已冻结 | `>= 1` | `Active` | `format-version + 1`，同时 `doc-revision + 1` | `doc-revision + 1` |

**冻结时机**：Writer 与 Reader 实现完成、通过测试（含附录 B 的测试向量）、并在真实
负载上跑过之后，置 `format-version: 1`、`status: Active`。冻结不建立任何兼容承诺
（§11.1），它标记的是"这一版是生产在跑的格式，此后每次改动都要有对应的变更记录"。
草稿期不需要这层记录开销，因此不必急于冻结。

**判据**：改动后 Writer 或 Reader 的代码需要跟着改，就是线格式改动；只改文档、
一个字节都不变，就只是文档修订。等价的精确表述是：若存在一份按旧版本编码的合法
Payload，其在新版本下的解析结果或合法性发生变化，则属于线格式改动。

| 改动 | 例子 |
|---|---|
| 线格式 | 增删字段、改字段顺序或宽度、给保留取值赋予语义、改变编码含义、收紧或放宽合法性约束 |
| 仅文档 | 澄清措辞、补充推导、修正笔误、调整 L1/L2 分级、补充测试向量、改写 §12 |
| 纯排版 | 换行、表格对齐；两个计数器都不动 |

补充规则：

- **不要吝惜 `format-version`**。它是免费的；把线格式改动记成纯文档修订，会让变更
  历史无法回答"生产上跑的到底是哪一版格式"，这是它唯一的价值所在；
- 已发布过的 `format-version` 不得复用；
- 线格式改动应当**攒批实施**。虽然改动本身成本很低（§11.1），但每次都要改 Writer、
  Reader、测试向量与本文档四处，一次改一处和一次改五处的工作量相差不大。已识别但
  尚未实施的改动记入附录 D，攒够一批再一次性实施；
- 每次计数器变更必须同时更新 frontmatter 的 `updated` 与附录 C。

### 11.4 保留取值

以下保留取值在当前版本下必须被拒绝：NullTag `0b11`、`compression_layout >= 0x03`、
`PLAIN` 的非零 `encoding_param`。

因为不做兼容也没有运行时版本校验，保留它们的目的不是"给后续版本留门"，而是**尽早
暴露 Writer 实现 bug 与数据损坏** —— 这些位模式在正确的 Writer 输出中不可能出现，
一旦读到就说明输入有问题，此时报错远好于继续解析出错误数据。出于同样的理由，
tags / bitmap / bit-packed 的尾部未使用 bit 必须写 0，但对其校验是 L2 可选项。

### 11.5 与实现的同步

- 实现位置：待补（Writer / Reader 落地后填写具体路径）；
- Writer 与 Reader 必须始终位于同一构建单元中，这是 §11.1 的一致性前提，不得让
  其中一侧单独发布或单独回滚；
- `format-version` 变更的 PR 必须一次性完成：改本文档、改 Writer、改 Reader、更新
  附录 B 的测试向量，并**删除旧版本的代码路径** —— 同一代码库不保留多版本分支；
- 附录 B 是规范的一部分，Writer 与 Reader 应当各有一个用例覆盖它。它同时是这套
  版本机制下唯一的实际防线：格式改错时，由测试向量失败来暴露，而不是由运行时校验。

## 12. 设计说明（非规范性）

### 12.1 目标与非目标

本格式针对 Shuffle 的特定约束设计：Schema 已知、单 codec、批量小、解码在关键路径
上。既有方案都不贴合 —— 行式格式（UnsafeRow / CompactRow）无法利用列内同质性；
通用列存（Parquet / ORC）的 Header / Footer / Page 元数据在小批量下开销占比过高；
Presto / Arrow 序列化器缺少整数列的窄化与位打包。

设计取向：不自描述、单一 codec 上下文、Null 与 Value 完全分离、Run 只是物理切片。

以下**不在设计范围内**，也不应当为其扩展本格式：

- 长期存储与跨版本兼容 —— Payload 生命周期不超过一次 Shuffle；
- 随机访问、谓词下推、列裁剪 —— Reader 总是完整解码 Payload；
- 嵌套类型（Array / Map / Row）、Decimal、Timestamp、Boolean；
- codec 本身，以及分帧、校验和与传输协议。

### 12.2 设计决策

以下决策点曾存在备选方案。改变其中任何一条都会改变线格式，版本号按 §11.3 处理。

| 决策 | 结论与理由 |
|---|---|
| String 列展开为两个 Stream（§1.4） | 备选是每列一个 buffer、列内加 `u64 length_stream_size` 子头部。长度流与数据流的可压缩性差异很大，SEPARATE 下分开压缩收益明显；代价只是每个 String 列每个 Run 多 8 bytes 的 `decoded_sizes` 项。 |
| `decoded_size == 0` 表示未压缩（§5.1） | 备选是依赖 codec 自行退化为 store-only，或用 `decoded == stored` 表示未压缩 —— 后者与「恰好压缩到等长」冲突。空 Stream 已由 `stored_size == 0` 表达，故三态无歧义。COMBINED 下 `decoded_sizes[s]` 要用于切分、无法兼作压缩标记，这才是 `COMBINED_STORED` 存在的原因。 |
| 尾块而非补齐（§7.2） | 备选是 Writer 补齐到 64 的倍数、Reader 按 `non_null_count` 截断。补齐最多浪费 63 bytes 且要求 Writer 维护额外缓冲；尾块的解码开销是 O(1)、不进主循环（§12.3）。尾块不需要任何额外线上字段，因为 `total_source_bytes` 完全由 `non_null_count` 导出。 |
| `encoding_tags` 放在 Null body 之后（§3） | 变长字段插在定长头中间会让其后的 `u32` 落在非对齐偏移上。移出后前 24 bytes 可直接按结构体读取；`encoding_tags` 是 byte 数组，放在何处都不需要对齐。 |
| 字典容量与 Index 宽度不放宽（§8.1） | `byte_length < 64` 是终止标记不产生歧义的前提，Entry 数上限 63 使 1-byte Index 永远够用。放宽容量会同时破坏 Entry 长度编码与终止标记，不是局部改动；需要更大字典时切换到下一个 Dictionary。 |
| Run 边界只保证结构完整性（§5.4） | Stream 之间不同步，使 Writer 可以按各 Stream 的缓冲水位独立切分 Run；代价是 Reader 需要维持多条 Stream 的独立游标。 |
| `payload_size` / `variable_size` 都不是校验量（§2、§3.1） | 前者是解析的结果而非输入；后者的精确统计需要在 Dictionary 切换时回溯，而它唯一的用途是一次性预分配，精确性没有收益。 |
| Schema 错配不由本格式负责（§2） | 格式内没有 magic number，也没有列类型标记，绝大多数位模式在任意 Schema 下都合法，因此「Reader 必须确认 `schema_identity` 一致」是无法落实的要求。 |

### 12.3 尾块与解码性能

尾块规则被刻意设计成不影响主循环：`full_block_count` 与 `tail_source_bytes` 在进入
循环之前由 `non_null_count[c]` 一次算出，循环内不需要任何「是否最后一块」的判断；
主循环固定处理 `source_bytes == 64` 的 Block，`value_count` 是编译期常量
（32 / 16 / 8），可完全展开并向量化；尾块最多一个，由循环后的标量慢路径处理，
代价与 Stream 长度无关。

```text
decode_stream(s):
    n_full = total_source_bytes[s] / 64
    tail   = total_source_bytes[s] % 64
    for i in [0, n_full):            // 定长、无分支、可 SIMD
        decode_block(src, dst, 64)
    if tail > 0:                     // 至多执行一次
        decode_block_scalar(src, dst, tail)
```

每种 Encoding kind 的尾块都直接复用定长实现：`PLAIN` 退化为一次 `memcpy(tail)`；
`CONST_NARROW` 只是重复次数从 `64 / type_width` 变成 `tail / type_width`；
`BIT_PACK` / `FOR_BIT_PACK` 的 bit 游标逻辑与定长完全一致，只是循环次数变小。
解码器不需要为尾块准备另一套编码格式。

### 12.4 Writer 侧建议

- **编码选择**：优先 `CONST_NARROW`，其次 `FOR_BIT_PACK`（取 `base = min`），再次
  `BIT_PACK`，最后 `PLAIN`。`BIT_PACK` 只适用于值域关于 0 对称或全为小非负数的
  场景，带偏移的值域应当用 `FOR_BIT_PACK`。
- **Layout 选择**：Stream 都很短时用 `COMBINED`（省下 `S - 1` 次 codec 调用和各自的
  压缩头）；Stream 较大且可压缩性差异明显时用 `SEPARATE`。
- **Run 切分**：按各 Stream 的缓冲水位独立切分即可，只需遵守 §5.4 的结构完整性
  约束，无需在 Stream 之间对齐。

### 12.5 Reader 侧建议

最简单且正确的实现顺序：

1. 读定长头 → 解 Null 区域 → 算出全部 `non_null_count[c]`；
2. 读 `encoding_tags`；
3. 遍历所有 Run，把每个 Stream 的字节拼接成 `stream_bytes[s]`；
4. 逐 Stream 解析：Dictionary 列先解 Data Stream 的 DictionarySequence，再解
   Length/Index Stream。

流式 Reader 可以省掉第 3 步的物化，但必须维持每条 Stream 的独立游标 —— §5.4 保证
跨 Run 不会遇到被截断的自描述结构，但不保证 Stream 之间同步。

校验分级（§10）的判定标准：L1 是位于结构解析路径上、O(1) 或可与已有循环合并、
省略会导致越界或大额分配的检查；L2 是需要额外遍历、累加或分支的一致性检查。例如
「Null decoded body 长度等于由 tags 导出的期望长度」属 L1 —— 一次比较守住了后续所有
bitmap 读取；而 `sum(fallback_lengths) == len(FallbackRawBytes)` 属 L2 —— 要额外
遍历长度流。

## 附录 A：派生量

以下均不在线格式中，由 Reader 计算得出：

```text
stream_count_total (S)  = sum over c of (2 if String else 1)
runs_offset             = 24 + null_stored_size + ceil(C / 8)
raw_null_column_count   = #{c : null_tag(c) == RAW_NULL}
null_body_expected_size = ceil(C * 2 / 8)
                        + raw_null_column_count * ceil(row_count / 8)
non_null_count[c]       = 0 | row_count | popcount(bitmap[c])
total_source_bytes[s]   = non_null_count[c] * type_width   // 定宽值流
                        | non_null_count[c] * 8            // RAW 长度流
                        | fallback_value_count[c] * 8      // Dict 长度流尾段
full_block_count[s]     = total_source_bytes[s] / 64
tail_source_bytes[s]    = total_source_bytes[s] % 64
fallback_value_count[c] = non_null_count[c] - sum(matched_row_count)
stream_offset[s]        = sum(decoded_sizes[0:s])          // COMBINED 切分
```

## 附录 B：测试向量

本附录是规范的一部分，Writer 与 Reader 应当各有一个用例覆盖它。

Schema：`col0 : Integer`，`col1 : String`（EncodingTag = 0，RAW）。3 行：

| row | col0 | col1 |
|---:|---|---|
| 0 | 10 | `"ab"` |
| 1 | NULL | `"cd"` |
| 2 | 12 | `"ef"` |

派生量：`C = 2`，`S = 3`（col0 值流、col1 长度流、col1 数据流），
`non_null_count = [2, 3]`，`variable_size = 6`。

- **Null 区域**：`tags` 1 byte，col0 = `RAW_NULL(0b10)`、col1 = `NO_NULL(0b01)`
  → `0b0000_0110 = 0x06`。col0 bitmap 1 byte：非 Null / Null / 非 Null →
  `0b0000_0101 = 0x05`。decoded body = `06 05`，未压缩，故 `null_stored_size = 2`、
  `null_decoded_size = 0`。
- **Stream 0（col0 值流）**：`total_source_bytes = 2 * 4 = 8`，
  `full_block_count = 0`、`tail_source_bytes = 8` → 单个尾块，`value_count = 2`。
  取 `PLAIN`：`0x03` + `0A 00 00 00 0C 00 00 00`，共 9 bytes。
- **Stream 1（col1 长度流）**：3 个 Bigint 长度均为 2，`total_source_bytes = 24`
  → 单个尾块，`value_count = 3`。取 `CONST_NARROW`、`narrow_bytes = 1`：
  `0x04` + `02`，共 2 bytes。
- **Stream 2（col1 数据流）**：Raw Data `61 62 63 64 65 66`，6 bytes。

Run（单 Run，SEPARATE，三条 Stream 均未压缩）：

```text
01                          compression_layout = SEPARATE
09 00 .. 02 00 .. 06 00 ..  stored_sizes  = [9, 2, 6]   (3 * u64 = 24 bytes)
00 00 .. 00 00 .. 00 00 ..  decoded_sizes = [0, 0, 0]   (3 * u64 = 24 bytes)
03 0A 00 00 00 0C 00 00 00  stream 0
04 02                       stream 1
61 62 63 64 65 66           stream 2
```

Run 长度 = `1 + 24 + 24 + 17 = 66`。完整 Payload 共 93 bytes：

| 偏移 | 长度 | 字段 | 值 |
|---:|---:|---|---|
| 0 | 4 | `row_count` | `03 00 00 00` |
| 4 | 4 | `run_count` | `01 00 00 00` |
| 8 | 8 | `variable_size` | `06 00 00 00 00 00 00 00` |
| 16 | 4 | `null_stored_size` | `02 00 00 00` |
| 20 | 4 | `null_decoded_size` | `00 00 00 00` |
| 24 | 2 | `null_stored_body` | `06 05` |
| 26 | 1 | `encoding_tags` | `00` |
| 27 | 66 | `runs` | 见上 |

## 附录 C：变更历史

每次计数器变更在此追加一行，最新在上。`format-version` 列未变化时留空。

「需改动」列记录该次变更要求哪些组件跟着改（Writer / Reader / 测试向量 / 仅文档）。
改动规模只在此列与变更描述中表达，**不进入版本号** —— 版本号不区分改动大小
（§11.2）。

| `doc-revision` | `format-version` | 日期 | 需改动 | 变更 |
|---:|---:|---|---|---|
| 3 | | 2026-08-20 | 仅文档 | §10.2 补入校验 27–29：Run 边界结构、`run_count == 0` 的前提、`FOR_BIT_PACK` 结果值域。三条约束正文早有，校验清单漏列。 |
| 2 | | 2026-08-20 | 仅文档 | §10.2 补入校验 26：字典容量约束。§8.1 早已规定，但校验清单漏列。 |
| 1 | 0 | 2026-08-19 | 仅文档 | 初稿。线格式未冻结，Writer / Reader 尚未实现。 |

## 附录 D：待定格式改动

记录已识别但尚未实施的线格式改动。每次 `format-version` 变更都要同时改 Writer、
Reader、测试向量与本文档四处，而一次改一处和一次改五处的工作量相差不大，因此改动
应当在此累积、攒批实施（§11.3）。

草稿期（`format-version: 0`）不需要使用本表，直接改正文即可。

| 提出日期 | 改动 | 动机 | 影响面 |
|---|---|---|---|
| — | 暂无 | | |

改动实施后从本表移除，并在附录 C 记录。
