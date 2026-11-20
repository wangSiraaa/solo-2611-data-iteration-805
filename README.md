# Parquet footer 统计驱动的保守 plan/scan

用 Python 3.11 + PyArrow 19 实现的小型 Parquet 查询工具：计划阶段只读 footer 统计、
保守剪枝行组；执行阶段先读过滤列做 SQL 三值逻辑精确过滤，再只为命中行读投影列。
不解析 SQL、不做损坏恢复、不使用 page index、不含远程 Range 服务与性能压测。

另含一套**离线等值连接分派业务**（`pqscan.join`，与平面查询互不影响）：两侧 Parquet
分片按各自码表版本把字符串键标准化成标准键，先生成只含键列信息的连接清单，再按
工作节点的输出行上限求“行组领取份数最小”的精确分工，最后由节点本地真实执行并
带双侧物理出处落盘。入口见下文“离线等值连接业务”。

## 目录

```
fixtures/                 输入：files.json、queries.json、stats.parquet、no-stats.parquet
fixtures/join/            连接业务输入：两版码表、字典/普通编码 Parquet、任务与节点配置
tools/make_fixtures.py    从同一份数据生成“有统计/无统计”两个 Parquet（行组与行序一致）
tools/make_join_fixtures.py 生成连接业务的全部夹具
src/pqscan/
  expr.py       谓词树解析、schema 校验、PyArrow 表达式翻译、纯 Python 3VL 逐行求值
  stats.py      只读 footer 的行组统计提取与可靠性判定
  planner.py    三值逻辑保守行组推理（can_be_true / can_be_false、trace）
  scanner.py    两阶段列读取、精确行选择、读取请求在执行入口即时登记
  reference.py  独立核对：无剪枝全扫描 + 纯 Python 3VL（与引擎不同代码路径）
  runner.py     串联计划/扫描/核对，落盘 plans、results、run_record、verification
  cli.py        命令行入口（平面查询 + 连接业务子命令）
  join/         离线等值连接：codes/job/manifest/dispatch/executor/reference/runner
tests/          pytest 套件（平面 12 用例 + 连接 20 用例）
docs/plan_format.md  平面查询计划与运行记录的 JSON 格式契约
docs/join_format.md  连接清单/分工/结果/核对的格式契约
output/         运行产物（git 忽略）
```

## 运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python tools/make_fixtures.py           # 重新生成两个夹具（可选，仓库已带）
PYTHONPATH=src .venv/bin/python -m pqscan.cli run \
    --files fixtures/files.json --queries fixtures/queries.json --out output
PYTHONPATH=src .venv/bin/python -m pytest tests/ -q
```

输出：`output/plans/*.json`（行组选择说明）、`output/results/*.parquet`（查询结果）、
`output/run_record.json`（实际列读取请求）、`output/verification.json`（独立核对结论）。

## 离线等值连接业务

```bash
PYTHONPATH=src .venv/bin/python tools/make_join_fixtures.py     # 生成连接夹具（仓库已带可跳过）

# 一条龙：清单 -> 精确分工 -> 节点执行 -> 独立核对
PYTHONPATH=src .venv/bin/python -m pqscan.cli join-run \
    --job fixtures/join/demo_job.json \
    --nodes fixtures/join/nodes_3_2.json --out output/join/demo32

# 也可分阶段独立运行（清单可单独查看，不依赖分工）
PYTHONPATH=src .venv/bin/python -m pqscan.cli join-manifest --job ... --out ...
PYTHONPATH=src .venv/bin/python -m pqscan.cli join-dispatch --job ... --nodes ... --out ...
PYTHONPATH=src .venv/bin/python -m pqscan.cli join-execute --job ... --nodes ... --out ...
```

### 业务规则（与需求逐条对应）

1. **按文件版本查码表。** 每个 Parquet 文件在任务 JSON 里指定 `code_version`；
   键列无论字典编码还是普通编码，都先解码成 Python 文本，再只查该版码表。
2. **空串 / NULL / 未登记三分。** 空字符串是合法码表键、可参与连接；NULL 不查码表、
   不参与连接（行号记入清单 `null_rows`）；非空值与空字符串未登记时，报出
   `(文件, 行组, 组内行号, 文本)` 并停止该批（`PlanError`，退出码 1）。
3. **清单只读键列。** 每个行组仅一次 `read_row_group(columns=[键列])`，业务列在节点
   执行前完全不读；清单只存标准键、两侧物理行号数组与配对行数
   （`pair_rows = 左行数 × 右行数`），不展开成对明细；零匹配组对略去。
4. **物理行才算数。** 字典里存在但未被任何物理行引用的值（夹具 `sem_left.parquet`
   的 `GHOST`）不会产生命中；重复内容按物理行号逐条保留（左 2 行右 3 行 = 6 对）。
5. **精确分工。** 每批 ≤3 节点、≤10 个非空组对；一个组对整体分给一个节点，禁止只做
   部分重键配对。完整枚举所有分派（至多 3^10），在满足各节点行上限的方案中最小化
   行组领取份数（同节点复用同侧同行组只算一份），并报告最优方案总数；无可行分工时
   `feasible=false`、退出码 3，不执行节点阶段。
6. **节点真实执行。** 节点按领到的组对与清单展开，同节点同一 `(文件,行组)` 的业务列
   只读一次；结果含 `std_key`、双侧业务列与六个物理出处列，重复行保留，空闲节点
   写出同 schema 零行文件。
7. **独立核对。** `join/reference.py` 不看清单/分工，整表全量读取后纯 Python 重算，
   逐行比对节点 Parquet 的内容、顺序与出处，并复核清单与行上限/全覆盖约束。

### 夹具覆盖的三个分工场景（即需求中的例子）

标准化后左 `L1=[a,a]`、`L2=[b]`，右 `R1=[a,b]`、`R2=[a]`（标准键 STD_A/STD_B）：

| 任务/节点配置 | 结果 |
| --- | --- |
| `demo_job` + 上限 3、2 | 节点0 领 `L1×R1`、`L2×R1`（3 行），节点1 领 `L1×R2`（2 行），共 5 份 |
| `demo_job` + 上限 4、1 | 节点0 改领 L1 的两对 `L1×R1`、`L1×R2`（4 行），节点1 领 `L2×R1` |
| `demo_heavy_job`（R2 再加一行 a）+ 上限 3、2 | `L1×R2` 单独 4 行超过最大上限，整批无法分派，退出码 3 |

`semantics_job` 覆盖：两版码表对同一文本 `alpha` 给出不同解释（v1→STD_A、
v2→STD_B，因此不连接）、字典编码 GHOST 不命中、普通编码重复行保留、
NULL 不连接、空字符串合法映射并命中。`bad_job.json` 覆盖未登记文本
（`bad_right.parquet` 行组0 行1 的 `zzz`）报错并停止该批，退出码 1。

### 手工核对方式

```bash
# 看清单（未展开明细）与分工
python -m json.tool output/join/demo32/join_manifest.json
python -m json.tool output/join/demo32/dispatch_plan.json
# 看节点实际行与双侧出处
.venv/bin/python -c "import pyarrow.parquet as pq; \
print(pq.read_table('output/join/demo32/join_results/node0.parquet').to_pylist())"
# 独立核对结论
python -m json.tool output/join/demo32/join_verification.json
```

## 支持范围

- 平面 schema；整数（有/无符号 int32/int64 等）与字符串列；
- 比较 `eq/ne/lt/le/gt/ge`、`is_null`、`and/or/not`、列投影；
- 谓词以 JSON 前缀树表达（见 `fixtures/queries.json`），不解析 SQL；
- 类型不匹配、列不存在、嵌套 schema 等在计划期报错；不处理文件损坏。

## 关键设计约束（与需求逐条对应）

1. **计划只读 footer。** `stats.py` 只访问 `FileMetaData/RowGroupMetaData/Statistics`，
   不调用任何数据读取接口。
2. **三值逻辑双结论。** 每个谓词子树求 `can_be_true`/`can_be_false`，
   NULL 产生的 UNKNOWN 既不算 TRUE 也不算 FALSE。`NOT` 同时翻转两个结论，
   不会出现“把 UNKNOWN 当 FALSE 再取反”的错误剪枝（见 `planner.py`）。
3. **统计缺失/不可靠即保守保留。** 无 Statistics、`has_min_max=False`（全 NULL 组的
   典型形态）、`null_count` 缺失、物理类型不受支持，叶子结论退化为
   `(true, true)` 并在 trace 的 `unknown` 中登记原因。PyArrow 19 的 Statistics
   不暴露 min/max 截断标记，因此对存在且类型受支持的 min/max 视为精确；
   若环境提供截断信息，应在 `stats.py` 将其降级为不可靠。
4. **AND/OR 不跨列关联极值。** 只组合独立子树的两个结论，不假设不同列的
   min/max 来自同一行。只有证明 `can_be_true=false` 才 `skip`。
5. **两阶段扫描。** 保留组先读过滤列；`Table.filter` 的 Kleene 语义丢弃
   UNKNOWN/FALSE、只留 TRUE，再用临时行号列取回组内物理行号。无命中组到此结束，
   不读 payload 等投影专用列；有命中组才读剩余投影列。过滤列同时在投影中时复用
   第一阶段数组，不会发起第二次读取（空列集合不产生请求）。
6. **读取计数来自执行入口。** `InstrumentedParquetFile.read_row_group` 在真实
   调用发生时登记 `(文件, 行组, phase, 列集合)`，计划器不参与填写；记录的是工具
   发起的列读取请求，不是操作系统字节数，也不用 footer 的压缩大小冒充 I/O。
7. **结果与溯源。** 输出按 files.json 文件顺序、行组物理索引、组内行号排列；
   附 `_source_file/_source_row_group/_source_row_in_group` 三列；重复数据保留；
   空结果写出同 schema 的零行 Parquet。
8. **独立核对。** `reference.py` 不看任何统计、每个行组全列全量读取，用纯 Python
   3VL 逐行求值，逐行比对引擎结果的内容、顺序与溯源。

## 夹具与现成查询的预期现象

两个 Parquet 由同一份 20 行数据写成，各 5 个行组、每组 4 行；RG2 的 `k` 全 NULL。

| 查询 | stats.parquet 计划 | no-stats.parquet 计划 |
| --- | --- | --- |
| `range`（k∈[14,30)） | 跳过 RG0/RG1/RG4；RG2 全 NULL 保守保留但过滤后无命中；仅 RG3 命中 | 5 组全保留，结果完全一致 |
| `nulls`（k IS NULL） | null_count=0 的 RG1/RG3/RG4 证明跳过；RG0/RG2 命中 | 全保留，结果一致 |
| `negated`（NOT kind='A'） | 全部保留（含 NULL 行，UNKNOWN 不取反为 TRUE） | 全保留，结果一致 |
| `or_missing_stats`（kind='A' OR amount>160） | 全部保留（OR 跨列独立推理） | 全保留，结果一致 |
| `no_hits`（k>1000） | RG0/RG1/RG3/RG4 证明跳过；RG2 统计缺失保守保留、过滤后无命中 | 全保留、全部无命中 |

可在 `output/run_record.json` 验证：`no_hits` 与 `range` 中所有
`no_hits_after_filter` 行组只有 `phase=filter` 的请求，从不读取投影专用列
（`row_id/payload/kind/amount`）；`planned_skip` 行组零请求。

## 测试覆盖

`tests/test_pqscan.py`（12 用例）覆盖：范围剪枝、全 NULL 组、IS NULL 的
null_count 推理、NOT 的 UNKNOWN 处理与可证明 FALSE、OR 跨列不关联极值、
无统计全保留、3VL 真值表、无命中不读投影列、过滤列复用、端到端与无剪枝
全扫描一致、有/无统计结果相同而计划保守度不同、空结果 schema 与输出顺序。

`tests/test_join.py`（20 用例）覆盖：空串/NULL/未登记三分及报错定位（含仓库
`bad_job.json` 夹具）、
字典 GHOST 不算命中、字典与普通编码同路径、清单阶段只读键列的读取断言、
重复物理行与配对行数算术、
跨版本同文本异义、NULL 行不连接、题面三组分工场景、最小份数与最优方案计数、
超 3 节点拒绝、节点内组只读一次与零行 schema、端到端产物与独立核对、
随机实例对 itertools 蛮力枚举的最优性校验、零匹配清单边界。
