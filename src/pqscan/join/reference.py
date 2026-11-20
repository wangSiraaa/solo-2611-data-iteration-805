"""独立核对：不看连接清单与分工，全量读取两侧 Parquet 后用纯 Python 重算连接。

与生产路径（manifest.py 只读键列 + executor.py 按清单展开）刻意走不同代码路径：
- 这里每个文件整表全量读取所有业务列与键列；
- 自己逐行标准化、自己做多对多笛卡尔配对；
- 再按分工把期望行切到各节点，与节点写出的 Parquet 逐行（含顺序与双侧出处）比对，
  并独立复核清单里每个组对的标准键、行号集合与配对行数。
"""
from __future__ import annotations

import os

import pyarrow.parquet as pq

from .codes import UnregisteredOccurrence, standardize_values
from .dispatch import DispatchPlan
from .executor import PROVENANCE_ORDER
from .job import JOIN_STD_KEY_COLUMN, JoinJob
from .manifest import JoinManifest, _group_id


def _full_side_rows(job: JoinJob, side_spec):
    """返回 (group_id -> 标准键 -> 物理行号) 与全量业务行（按组）。"""
    groups: dict[str, dict[str, list[int]]] = {}
    business: dict[str, list[dict[str, object]]] = {}
    unregistered: list[UnregisteredOccurrence] = []
    for jf in side_spec.files:
        pf = pq.ParquetFile(jf.path)
        all_cols = [side_spec.key_column] + side_spec.columns
        for rgi in range(pf.num_row_groups):
            tbl = pf.read_row_group(rgi, columns=all_cols, use_threads=False)
            keys = tbl.column(side_spec.key_column).to_pylist()
            gid = _group_id(side_spec.side, jf.index, rgi)
            std = standardize_values(
                side=side_spec.side, file=jf.file_name, row_group=rgi,
                raw_values=keys, table=job.code_tables[jf.code_version],
                unregistered=unregistered,
            )
            groups[gid] = std.std_rows
            business[gid] = tbl.to_pylist()
    if unregistered:
        raise AssertionError(f"核对路径发现未登记键值: {unregistered}")
    return groups, business


def reference_join(job: JoinJob) -> list[dict[str, object]]:
    """全量读取 + 纯 Python 多对多等值连接，返回与执行器同序的全部期望行。"""
    left_groups, left_biz = _full_side_rows(job, job.left)
    right_groups, right_biz = _full_side_rows(job, job.right)

    expected: list[dict[str, object]] = []
    for lg_id in sorted(left_groups):
        for rg_id in sorted(right_groups):
            common = sorted(set(left_groups[lg_id]) & set(right_groups[rg_id]))
            for std_key in common:
                for li in left_groups[lg_id][std_key]:
                    left_row = left_biz[lg_id][li]
                    for ri in right_groups[rg_id][std_key]:
                        right_row = right_biz[rg_id][ri]
                        record = {JOIN_STD_KEY_COLUMN: std_key}
                        record.update({c: left_row[c] for c in job.left.columns})
                        record.update({c: right_row[c] for c in job.right.columns})
                        record.update(_provenance(job, lg_id, li, rg_id, ri))
                        expected.append(record)
    return expected


def _provenance(job: JoinJob, lg_id: str, li: int, rg_id: str, ri: int) -> dict[str, object]:
    lf, l_rgi = _locate(job, lg_id)
    rf, r_rgi = _locate(job, rg_id)
    return {
        "left_file": lf.file_name,
        "left_row_group": l_rgi,
        "left_row_in_group": li,
        "right_file": rf.file_name,
        "right_row_group": r_rgi,
        "right_row_in_group": ri,
    }


def _locate(job: JoinJob, group_id: str):
    side = job.left if group_id.startswith("L") else job.right
    file_index, rgi = _split_group_id(group_id)
    return side.files[file_index], rgi


def _split_group_id(group_id: str) -> tuple[int, int]:
    # group_id 形态：L<file_index>g<row_group> / R<file_index>g<row_group>
    middle = group_id.index("g")
    return int(group_id[1:middle]), int(group_id[middle + 1 :])


def verify_manifest(job: JoinJob, manifest: JoinManifest) -> list[str]:
    """独立复核清单：重算每个组对的键集合、行号与配对行数。"""
    left_groups, _ = _full_side_rows(job, job.left)
    right_groups, _ = _full_side_rows(job, job.right)
    problems: list[str] = []

    expected_pairs: dict[tuple[str, str], dict[str, tuple[list[int], list[int]]]] = {}
    for lg_id in sorted(left_groups):
        for rg_id in sorted(right_groups):
            per_key = {}
            for std_key in sorted(set(left_groups[lg_id]) & set(right_groups[rg_id])):
                per_key[std_key] = (
                    sorted(left_groups[lg_id][std_key]),
                    sorted(right_groups[rg_id][std_key]),
                )
            if per_key:
                expected_pairs[(lg_id, rg_id)] = per_key

    got_pairs = {
        (p.left_group_id, p.right_group_id): {
            m.std_key: (sorted(m.left_rows), sorted(m.right_rows)) for m in p.matches
        }
        for p in manifest.pairs
    }
    if set(got_pairs) != set(expected_pairs):
        problems.append(
            f"清单组对集合不一致: 多出 {sorted(set(got_pairs) - set(expected_pairs))}, "
            f"遗漏 {sorted(set(expected_pairs) - set(got_pairs))}"
        )
    for key_pair, per_key in expected_pairs.items():
        if got_pairs.get(key_pair) != per_key:
            problems.append(f"组对 {key_pair} 的标准键/物理行号与全量重算不一致")

    for pair in manifest.pairs:
        calc = sum(len(m.left_rows) * len(m.right_rows) for m in pair.matches)
        if calc != pair.total_rows:
            problems.append(f"{pair.pair_id} total_pair_rows 求和错误: {calc} != {pair.total_rows}")
    return problems


def verify_execution(
    job: JoinJob, manifest: JoinManifest, plan: DispatchPlan, node_results, out_dir: str
) -> dict[str, object]:
    """按分工切分全量重算结果，逐节点逐行比对实际 Parquet 输出。"""
    all_expected = reference_join(job)
    expected_by_pair: dict[tuple[str, str], list[dict[str, object]]] = {}
    for row in all_expected:
        key = _pair_key_from_provenance(job, row)
        expected_by_pair.setdefault(key, []).append(row)

    problems: list[str] = []
    covered = 0
    for node, result in zip(plan.nodes, node_results):
        expected: list[dict[str, object]] = []
        for pair_id in node.pair_ids:
            task = next(p for p in plan.tasks if p["pair_id"] == pair_id)
            key = (task["left_group_id"], task["right_group_id"])
            expected.extend(expected_by_pair.get(key, []))
        got_table = pq.read_table(os.path.join(out_dir, "join_results",
                                               f"node{node.node_index}.parquet"))
        got = got_table.to_pylist()
        if got != expected:
            problems.append(
                f"节点 {node.node_index} 输出与全量重算不一致（行数 {len(got)} != {len(expected)} "
                f"或内容/顺序/出处不符）"
            )
        if tuple(n for n in got_table.schema.names if n in PROVENANCE_ORDER) != PROVENANCE_ORDER:
            problems.append(f"节点 {node.node_index} 溯源列缺失或顺序错误")
        if got_table.schema.field(JOIN_STD_KEY_COLUMN) is None:
            problems.append(f"节点 {node.node_index} 缺少 {JOIN_STD_KEY_COLUMN} 列")
        covered += len(got)
        if result.result_rows != len(got):
            problems.append(f"节点 {node.node_index} 记录行数与实际 Parquet 行数不符")

    if covered != len(all_expected):
        problems.append(f"节点输出总行数 {covered} 不等于全量连接行数 {len(all_expected)}")

    for node in plan.nodes:
        if node.output_rows > node.limit:
            problems.append(f"节点 {node.node_index} 超出行上限")
    assigned = [pid for n in plan.nodes for pid in n.pair_ids]
    if sorted(assigned) != sorted(p["pair_id"] for p in plan.tasks):
        problems.append("分工未覆盖全部非空组对或出现重复分派")

    # 每个节点同一 (文件, 行组) 只能真实读取一次
    for result in node_results:
        seen = [(r.file, r.row_group) for r in result.group_reads]
        if len(seen) != len(set(seen)):
            problems.append(f"节点 {result.node_index} 存在同一行组的重复读取")

    return {
        "passed": not problems,
        "problems": problems,
        "reference_total_rows": len(all_expected),
        "node_output_total_rows": covered,
    }


def _pair_key_from_provenance(job: JoinJob, row: dict[str, object]) -> tuple[str, str]:
    lg = _group_id("left", _file_index(job, "left", row["left_file"]), row["left_row_group"])
    rg = _group_id("right", _file_index(job, "right", row["right_file"]), row["right_row_group"])
    return lg, rg


def _file_index(job: JoinJob, side: str, file_name: str) -> int:
    spec = job.left if side == "left" else job.right
    for jf in spec.files:
        if jf.file_name == file_name:
            return jf.index
    raise KeyError(file_name)
