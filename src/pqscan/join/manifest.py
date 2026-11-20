"""连接清单：每个行组只读键列，给出全部非空行组对的精确配对信息。

关键约束（与业务需求逐条对应）：
1. 每个行组只通过 read_row_group 读取键列一次，业务列此时完全不碰；
2. 字典编码与普通编码都先解码成 Python 文本再查码表，两条路径同一套代码；
3. 仅统计“物理行真正引用”的标准键：字典里存在但没有任何行引用的值自然不会出现；
4. 重复内容按物理行号逐条保留（一个键左 2 行右 3 行 -> 6 对），清单不展开成对明细，
   只存两边各自的物理行号数组与配对行数，供分工阶段直接求和；
5. NULL 行不参与连接；未登记文本在全部行组扫描完后一次性报出并停止该批；
6. 零匹配组对不写入清单。
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow.parquet as pq

from ..errors import PlanError
from .codes import GroupStandardization, UnregisteredOccurrence, standardize_values
from .job import MAX_NON_EMPTY_PAIRS, JoinFile, JoinJob, SideSpec


@dataclass
class GroupInfo:
    side: str
    group_id: str
    file: JoinFile
    row_group: int
    num_rows: int
    standardization: GroupStandardization
    key_encoding: str                 # "RLE_DICTIONARY" 出现即字典编码，否则普通编码


@dataclass
class PairMatch:
    std_key: str
    left_raw_texts: list[str]
    right_raw_texts: list[str]
    left_rows: list[int]
    right_rows: list[int]

    @property
    def pair_rows(self) -> int:
        return len(self.left_rows) * len(self.right_rows)


@dataclass
class PairEntry:
    pair_id: str
    left_group_id: str
    right_group_id: str
    left_file: str
    right_file: str
    left_row_group: int
    right_row_group: int
    matches: list[PairMatch] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return sum(m.pair_rows for m in self.matches)


@dataclass
class JoinManifest:
    job_id: str
    groups: dict[str, GroupInfo]                 # group_id -> 信息
    pairs: list[PairEntry]                       # 已剔除零匹配组对，按 id 排序
    unregistered: list[UnregisteredOccurrence]
    null_summary: list[dict[str, object]]

    def to_dict(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "rule": "每对左右行组只读键列；按解码文本查该文件版本码表；NULL 不参与连接；"
                    "字典中未被物理行引用的值不算命中；重复物理行全部保留；"
                    "pair_rows = 左行数 × 右行数；零匹配组对已略去",
            "groups": [
                {
                    "group_id": g.group_id,
                    "side": g.side,
                    "file": g.file.file_name,
                    "code_version": g.file.code_version,
                    "row_group": g.row_group,
                    "num_rows": g.num_rows,
                    "key_encoding": g.key_encoding,
                    "null_rows": g.standardization.null_rows,
                    "keys": g.standardization.to_key_dicts(),
                }
                for g in sorted(self.groups.values(), key=lambda g: g.group_id)
            ],
            "pairs": [
                {
                    "pair_id": p.pair_id,
                    "left_group_id": p.left_group_id,
                    "right_group_id": p.right_group_id,
                    "left_file": p.left_file,
                    "right_file": p.right_file,
                    "left_row_group": p.left_row_group,
                    "right_row_group": p.right_row_group,
                    "total_pair_rows": p.total_rows,
                    "matches": [
                        {
                            "std_key": m.std_key,
                            "left_raw_texts": m.left_raw_texts,
                            "right_raw_texts": m.right_raw_texts,
                            "left_rows": m.left_rows,
                            "right_rows": m.right_rows,
                            "left_count": len(m.left_rows),
                            "right_count": len(m.right_rows),
                            "pair_rows": m.pair_rows,
                        }
                        for m in p.matches
                    ],
                }
                for p in self.pairs
            ],
        }


def _group_id(side: str, file_index: int, row_group: int) -> str:
    prefix = "L" if side == "left" else "R"
    return f"{prefix}{file_index}g{row_group}"


def _read_side_groups(spec: SideSpec) -> tuple[dict[str, GroupInfo], list[UnregisteredOccurrence],
                                               list[dict[str, object]]]:
    groups: dict[str, GroupInfo] = {}
    unregistered: list[UnregisteredOccurrence] = []
    null_summary: list[dict[str, object]] = []

    for jf in spec.files:
        pf = pq.ParquetFile(jf.path)
        key_column_index = pf.schema_arrow.get_field_index(spec.key_column)
        for rgi in range(pf.num_row_groups):
            # 唯一一次数据读取：仅键列。use_threads=False 保证读取与登记一一对应。
            table = pf.read_row_group(rgi, columns=[spec.key_column], use_threads=False)
            values = table.column(spec.key_column).to_pylist()

            col_meta = pf.metadata.row_group(rgi).column(key_column_index)
            key_encoding = (
                "dictionary" if "RLE_DICTIONARY" in col_meta.encodings else "plain"
            )
            std = standardize_values(
                side=spec.side, file=jf.file_name, row_group=rgi,
                raw_values=values, table=_code_table_for(spec, jf),
                unregistered=unregistered,
            )
            gid = _group_id(spec.side, jf.index, rgi)
            groups[gid] = GroupInfo(
                side=spec.side, group_id=gid, file=jf, row_group=rgi,
                num_rows=len(values), standardization=std, key_encoding=key_encoding,
            )
            null_summary.append(
                {
                    "group_id": gid,
                    "file": jf.file_name,
                    "row_group": rgi,
                    "null_key_rows": std.null_rows,
                    "null_count": len(std.null_rows),
                }
            )
    return groups, unregistered, null_summary


def _code_table_for(spec: SideSpec, jf: JoinFile) -> dict[str, str]:
    # 码表在 job 上按版本缓存；这里通过 spec 上的引用取（runner 注入），保持纯函数易测。
    return spec.tables[jf.code_version]


def build_manifest(job: JoinJob) -> JoinManifest:
    # 把码表挂到 side 视图上，供读取阶段按文件版本取用。
    for spec in (job.left, job.right):
        spec.tables = job.code_tables

    left_groups, left_unreg, left_nulls = _read_side_groups(job.left)
    right_groups, right_unreg, right_nulls = _read_side_groups(job.right)
    unregistered = left_unreg + right_unreg
    if unregistered:
        details = "; ".join(
            f"{u.file} 行组{u.row_group} 行{u.row_in_group} 的未登记文本 {u.text!r}"
            for u in unregistered
        )
        raise PlanError(
            f"发现 {len(unregistered)} 个未在对应版本码表登记的非 NULL 键值，"
            f"按规则停止该批: {details}"
        )

    all_groups = {**left_groups, **right_groups}
    pairs: list[PairEntry] = []
    pair_seq = 0
    for lg in sorted(left_groups.values(), key=lambda g: g.group_id):
        for rg in sorted(right_groups.values(), key=lambda g: g.group_id):
            matches: list[PairMatch] = []
            common = sorted(set(lg.standardization.std_rows) & set(rg.standardization.std_rows))
            for std_key in common:
                matches.append(
                    PairMatch(
                        std_key=std_key,
                        left_raw_texts=sorted(lg.standardization.raw_texts[std_key]),
                        right_raw_texts=sorted(rg.standardization.raw_texts[std_key]),
                        left_rows=list(lg.standardization.std_rows[std_key]),
                        right_rows=list(rg.standardization.std_rows[std_key]),
                    )
                )
            if not matches:
                continue  # 零匹配组对不进入清单
            pair_seq += 1
            pairs.append(
                PairEntry(
                    pair_id=f"P{pair_seq:02d}",
                    left_group_id=lg.group_id,
                    right_group_id=rg.group_id,
                    left_file=lg.file.file_name,
                    right_file=rg.file.file_name,
                    left_row_group=lg.row_group,
                    right_row_group=rg.row_group,
                    matches=matches,
                )
            )

    if len(pairs) > MAX_NON_EMPTY_PAIRS:
        raise PlanError(
            f"非空行组对 {len(pairs)} 个，超过每批上限 {MAX_NON_EMPTY_PAIRS}，请拆批"
        )

    return JoinManifest(
        job_id=job.job_id,
        groups=all_groups,
        pairs=pairs,
        unregistered=unregistered,
        null_summary=left_nulls + right_nulls,
    )
