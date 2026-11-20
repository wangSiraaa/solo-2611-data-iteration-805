"""节点本地执行：按领到的行组对与连接清单真实读取 Parquet 并写连接结果。

- 每个节点维护 (文件, 行组) 读取缓存：同一节点处理多对任务而复用同一侧同一行组时，
  该组的业务列只读取一次（对应分工里“只领一份”）；不同节点各自独立读取。
- 业务列在此时才第一次被读取；清单阶段从未触碰业务列。
- 成对明细只在节点本地展开：按清单位序（组对 -> 标准键 -> 左物理行 -> 右物理行）产出，
  重复物理行保留；每行附双侧物理出处。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.parquet as pq

from .dispatch import DispatchPlan
from .job import JOIN_PROVENANCE, JOIN_STD_KEY_COLUMN, JoinJob
from .manifest import JoinManifest, PairEntry


@dataclass
class GroupRead:
    file: str
    row_group: int
    columns: tuple[str, ...]


@dataclass
class NodeResult:
    node_index: int
    output_path: str
    pair_ids: list[str]
    result_rows: int
    group_reads: list[GroupRead] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "node_index": self.node_index,
            "output_path": self.output_path,
            "assigned_pairs": self.pair_ids,
            "result_rows": self.result_rows,
            "group_reads": [
                {"file": r.file, "row_group": r.row_group, "columns": list(r.columns)}
                for r in self.group_reads
            ],
        }


class _NodeGroupCache:
    """单节点范围内的行组业务列缓存：同一个 (文件, 行组) 只发起一次读取。"""

    def __init__(self) -> None:
        self._cache: dict[tuple[str, int], pa.Table] = {}
        self.reads: list[GroupRead] = []

    def get(self, path: str, file_name: str, row_group: int, columns: list[str]) -> pa.Table | None:
        key = (path, row_group)
        if key in self._cache:
            return self._cache[key]
        if not columns:
            # 该侧没有选择任何业务列：不发起 Parquet 读取，出处仍由清单给出。
            return None
        table = pq.ParquetFile(path).read_row_group(row_group, columns=columns, use_threads=False)
        self._cache[key] = table
        self.reads.append(
            GroupRead(file=file_name, row_group=row_group, columns=tuple(columns))
        )
        return table


def _output_schema(job: JoinJob) -> pa.Schema:
    left_pf = pq.ParquetFile(job.left.files[0].path)
    right_pf = pq.ParquetFile(job.right.files[0].path)
    fields = [pa.field(JOIN_STD_KEY_COLUMN, pa.string())]
    fields += [left_pf.schema_arrow.field(c) for c in job.left.columns]
    fields += [right_pf.schema_arrow.field(c) for c in job.right.columns]
    fields += [
        pa.field("left_file", pa.string()),
        pa.field("left_row_group", pa.int32()),
        pa.field("left_row_in_group", pa.int64()),
        pa.field("right_file", pa.string()),
        pa.field("right_row_group", pa.int32()),
        pa.field("right_row_in_group", pa.int64()),
    ]
    return pa.schema(fields)


def _emit_pair(
    job: JoinJob,
    manifest: JoinManifest,
    pair: PairEntry,
    cache: _NodeGroupCache,
    records: list[dict[str, object]],
) -> None:
    lg = manifest.groups[pair.left_group_id]
    rg = manifest.groups[pair.right_group_id]

    left_tbl = cache.get(lg.file.path, lg.file.file_name, lg.row_group, job.left.columns)
    right_tbl = cache.get(rg.file.path, rg.file.file_name, rg.row_group, job.right.columns)
    left_data = left_tbl.to_pylist() if left_tbl is not None else None
    right_data = right_tbl.to_pylist() if right_tbl is not None else None

    for match in pair.matches:  # 清单内已按 std_key 排序
        for li in match.left_rows:  # 物理行号升序，重复行各自独立
            left_biz = left_data[li] if left_data is not None else {}
            for ri in match.right_rows:
                right_biz = right_data[ri] if right_data is not None else {}
                record = {JOIN_STD_KEY_COLUMN: match.std_key}
                record.update({c: left_biz[c] for c in job.left.columns})
                record.update({c: right_biz[c] for c in job.right.columns})
                record.update(
                    {
                        "left_file": lg.file.file_name,
                        "left_row_group": lg.row_group,
                        "left_row_in_group": li,
                        "right_file": rg.file.file_name,
                        "right_row_group": rg.row_group,
                        "right_row_in_group": ri,
                    }
                )
                records.append(record)


def execute_plan(
    job: JoinJob, manifest: JoinManifest, plan: DispatchPlan, out_dir: str
) -> list[NodeResult]:
    if not plan.feasible:
        raise ValueError("分工不可行，不能执行")

    results_dir = os.path.join(out_dir, "join_results")
    os.makedirs(results_dir, exist_ok=True)
    schema = _output_schema(job)
    pairs_by_id = {p.pair_id: p for p in manifest.pairs}

    results: list[NodeResult] = []
    for node in plan.nodes:
        cache = _NodeGroupCache()
        records: list[dict[str, object]] = []
        for pair_id in node.pair_ids:  # 节点上的任务保持清单顺序
            _emit_pair(job, manifest, pairs_by_id[pair_id], cache, records)

        table = pa.Table.from_pylist(records, schema=schema)
        output_path = os.path.join(results_dir, f"node{node.node_index}.parquet")
        pq.write_table(table, output_path, write_statistics=True)
        results.append(
            NodeResult(
                node_index=node.node_index,
                output_path=output_path,
                pair_ids=list(node.pair_ids),
                result_rows=len(records),
                group_reads=cache.reads,
            )
        )
    return results


# 供 runner/测试引用的溯源列顺序。
PROVENANCE_ORDER = JOIN_PROVENANCE
