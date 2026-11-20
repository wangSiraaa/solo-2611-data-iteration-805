# 离线等值连接产物格式契约

连接业务分四个阶段，全部产物落在 `--out` 目录下；原有平面查询（`run`/`verify`）
的产物与目录结构不受影响。

| 阶段 | 命令 | 产物 |
| --- | --- | --- |
| 清单 | `join-manifest` | `join_manifest.json` |
| 分工 | `join-dispatch` | `dispatch_plan.json` |
| 执行 | `join-execute` | `join_results/node<index>.parquet` |
| 一条龙 | `join-run` | 上述全部 + `join_run_record.json`、`join_verification.json` |

## 任务文件（`--job`）

```json
{
  "job_id": "demo_dispatch",
  "code_tables": {"v1": "codes/v1.json", "v2": "codes/v2.json"},
  "left": {
    "files": [{"path": "parquet/demo_left.parquet", "code_version": "v1"}],
    "key_column": "lk",
    "columns": ["lv"]
  },
  "right": { "同构，键列 rk、业务列 rv }
}
```

- 所有路径相对任务文件所在目录解析；每个文件自带 `code_version`。
- 码表文件：`{"version": "v1", "mappings": {"本地文本": "标准键"}}`。
  空字符串 `""` 是合法键（可连接）；NULL 不查码表、不参与连接；
  非空值与空字符串未登记时报出 `(文件, 行组, 组内行号, 文本)` 并停止该批。
- 键列允许普通 `string` 或字符串字典编码，读取时统一解码为 Python 文本后查码表。
- 两侧业务列名不得冲突；`std_key` 与六个溯源列名为保留名。

节点文件（`--nodes`）：`{"nodes": [{"node_id": "n0", "max_output_rows": 3}]}`，
每批最多 3 个节点、10 个非空组对。

## join_manifest.json

- `groups[]`：每个物理行组一条，含 `group_id`（`L<文件序号>g<行组号>` /
  `R<文件序号>g<行组号>`）、`key_encoding`（`dictionary`/`plain`）、
  `null_rows`（NULL 物理行号）、`keys[]`。
- `keys[]` 每项：`std_key`、映射到该标准键的去重 `raw_texts`、
  引用该标准键的全部物理行号 `rows`（重复内容各自成行）。
  字典里存在但没有任何物理行引用的值不会出现。
- `pairs[]`：只含非空组对，零匹配组对略去。每项含双方 group/文件/行组号、
  `total_pair_rows` 与 `matches[]`；每个匹配给出标准键、两侧原始文本、
  两侧物理行号数组与 `pair_rows = left_count × right_count`。
- 清单阶段每个行组只读取键列一次，不读取业务列，也不展开成对明细。

## dispatch_plan.json

- `feasible=false` 时给出 `reason`（单任务超最大上限，或枚举后无完整分派），
  不进入执行阶段，CLI 退出码 3。
- 可行时 `nodes[]` 给出每个节点的 `assigned_pairs`、`output_rows`、
  `claimed_groups`（去重后的行组份数）与 `claimed_group_copies`。
- 目标值 `total_group_copies`：全节点领取份数之和，同一节点复用同一侧同一行组
  只算一份；`optimal_solution_count` 是完整枚举得到的最优方案总数，
  输出其中节点编号字典序最小的一个，保证可复现。

## join_results/node<index>.parquet

列顺序固定：

```
std_key, <左业务列...>, <右业务列...>,
left_file, left_row_group, left_row_in_group,
right_file, right_row_group, right_row_in_group
```

- 行序：组对（清单序）→ 标准键（字典序）→ 左物理行号 → 右物理行号；
- 重复物理行保留；空闲节点写出同 schema 的零行 Parquet；
- 同一节点对同一 `(文件, 行组)` 只发起一次业务列读取，记录在
  `join_run_record.json` 的 `group_reads`。

## join_verification.json

独立核对路径（`join/reference.py`）不看清单与分工，整表全量读取两侧所有列、
用纯 Python 重算标准化与多对多连接：

- `manifest_problems`：清单组对集合、每键物理行号、配对行数算术是否一致；
- `execution.passed`：按分工切分的期望行与节点 Parquet 逐行（含顺序与出处）比对、
  行上限、任务全覆盖、节点内无重复行组读取。
