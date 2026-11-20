"""连接任务 JSON 的加载与校验。

任务文件形态（所有路径相对任务文件所在目录解析）：

    {
      "job_id": "demo",
      "code_tables": {"v1": "codes/v1.json", "v2": "codes/v2.json"},
      "left": {
        "files": [{"path": "parquet/left.parquet", "code_version": "v1"}],
        "key_column": "lk",
        "columns": ["lv"]
      },
      "right": { ... 同构 ... }
    }

每个文件自带码表版本；两侧业务列名不允许冲突（标准键与溯源列名由工具保留）。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.parquet as pq

from ..errors import PlanError
from .codes import load_code_table

JOIN_STD_KEY_COLUMN = "std_key"
JOIN_PROVENANCE = (
    "left_file", "left_row_group", "left_row_in_group",
    "right_file", "right_row_group", "right_row_in_group",
)
RESERVED_COLUMNS = (JOIN_STD_KEY_COLUMN,) + JOIN_PROVENANCE

# 业务批量约束：每批最多三个节点、十个非空行组对。
MAX_NODES = 3
MAX_NON_EMPTY_PAIRS = 10


@dataclass(frozen=True)
class JoinFile:
    side: str
    index: int
    path: str                 # 已解析为绝对路径
    file_name: str            # 任务 JSON 中写的相对名，用于稳定标识
    code_version: str


@dataclass
class SideSpec:
    side: str
    files: list[JoinFile]
    key_column: str
    columns: list[str]
    tables: dict[str, dict[str, str]] = field(default_factory=dict)


@dataclass
class JoinJob:
    job_id: str
    base_dir: str
    code_tables: dict[str, dict[str, str]]   # 版本 -> 码表（本地文本 -> 标准键）
    left: SideSpec
    right: SideSpec

    def sides(self) -> tuple[SideSpec, SideSpec]:
        return self.left, self.right


def _resolve(base_dir: str, rel: str) -> str:
    return os.path.abspath(os.path.join(base_dir, rel))


def _parse_side(side: str, raw: dict[str, object], base_dir: str,
                code_versions: set[str]) -> SideSpec:
    if not isinstance(raw, dict):
        raise PlanError(f"{side} 侧配置必须是对象")
    key_column = raw.get("key_column")
    if not isinstance(key_column, str) or not key_column:
        raise PlanError(f"{side} 侧缺少字符串型 key_column")
    columns = raw.get("columns", [])
    if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
        raise PlanError(f"{side} 侧 columns 必须是字符串数组")
    if key_column in columns:
        raise PlanError(f"{side} 侧 key_column({key_column}) 不需要写进 columns，"
                        "标准键会以 std_key 单列输出")

    raw_files = raw.get("files")
    if not isinstance(raw_files, list) or not raw_files:
        raise PlanError(f"{side} 侧 files 必须是非空数组")

    join_files: list[JoinFile] = []
    for index, item in enumerate(raw_files):
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            raise PlanError(f"{side} 侧 files[{index}] 缺少 path")
        version = item.get("code_version")
        if not isinstance(version, str) or version not in code_versions:
            raise PlanError(f"{side} 侧 files[{index}] 的 code_version={version!r} "
                            f"未在 code_tables 中登记")
        file_name = item["path"]
        path = _resolve(base_dir, file_name)
        if not os.path.exists(path):
            raise PlanError(f"{side} 侧文件不存在: {file_name}")
        join_files.append(
            JoinFile(side=side, index=index, path=path, file_name=file_name,
                     code_version=version)
        )

    return SideSpec(side=side, files=join_files, key_column=key_column, columns=list(columns))


def load_job(job_json: str) -> JoinJob:
    with open(job_json, encoding="utf-8") as fh:
        raw = json.load(fh)
    if not isinstance(raw, dict):
        raise PlanError("任务文件顶层必须是 JSON 对象")
    job_id = raw.get("job_id") or "join_job"
    if not isinstance(job_id, str):
        raise PlanError("job_id 必须是字符串")

    base_dir = os.path.dirname(os.path.abspath(job_json))
    raw_tables = raw.get("code_tables")
    if not isinstance(raw_tables, dict) or not raw_tables:
        raise PlanError("code_tables 必须是非空对象: 版本 -> 码表文件路径")
    code_tables: dict[str, dict[str, str]] = {}
    for version, table_path in raw_tables.items():
        if not isinstance(version, str) or not isinstance(table_path, str):
            raise PlanError("code_tables 的键与值都必须是字符串")
        code_tables[version] = load_code_table(_resolve(base_dir, table_path))

    left = _parse_side("left", raw.get("left", {}), base_dir, set(code_tables))
    right = _parse_side("right", raw.get("right", {}), base_dir, set(code_tables))

    _validate_parquet_schemas(left, right)

    collision = set(left.columns) & set(right.columns)
    if collision:
        raise PlanError(f"两侧业务列名冲突: {sorted(collision)}，请在数据侧改名后再连接")
    reserved_hit = (set(left.columns) | set(right.columns)) & set(RESERVED_COLUMNS)
    if reserved_hit:
        raise PlanError(f"业务列使用了保留列名 {sorted(reserved_hit)}，保留名: {RESERVED_COLUMNS}")

    return JoinJob(
        job_id=job_id, base_dir=base_dir, code_tables=code_tables, left=left, right=right,
    )


def _validate_parquet_schemas(left: SideSpec, right: SideSpec) -> None:
    for spec in (left, right):
        reference: pa.Schema | None = None
        for jf in spec.files:
            pf = pq.ParquetFile(jf.path)
            schema = pf.schema_arrow
            if any(pa.types.is_nested(t) for t in schema.types):
                raise PlanError(f"{jf.file_name}: 只支持平面 schema，发现嵌套列")
            names = set(schema.names)
            if spec.key_column not in names:
                raise PlanError(f"{jf.file_name}: 找不到键列 {spec.key_column}")
            key_type = schema.field(spec.key_column).type
            # 字典编码读回为 dictionary<..., string>；普通编码为 string，两者都允许。
            if pa.types.is_dictionary(key_type):
                if not pa.types.is_string(key_type.value_type):
                    raise PlanError(f"{jf.file_name}: 键列 {spec.key_column} 字典值必须是字符串")
            elif not pa.types.is_string(key_type):
                raise PlanError(f"{jf.file_name}: 键列 {spec.key_column} 必须是字符串类型")
            missing = [c for c in spec.columns if c not in names]
            if missing:
                raise PlanError(f"{jf.file_name}: 业务列不存在 {missing}")
            if pf.num_row_groups == 0:
                raise PlanError(f"{jf.file_name}: 没有任何行组")
            # 同一侧多个分片的输出列类型必须一致，节点结果才有确定的单一 schema。
            wanted = pa.schema([schema.field(c) for c in spec.columns])
            if reference is None:
                reference = wanted
            elif wanted != reference:
                raise PlanError(
                    f"{jf.file_name}: 业务列类型与同侧首个分片不一致 "
                    f"({wanted} != {reference})"
                )


def load_node_limits(nodes_json: str) -> list[int]:
    """节点配置：{"nodes": [{"node_id": "n1", "max_output_rows": 3}, ...]}，返回行上限列表。"""
    with open(nodes_json, encoding="utf-8") as fh:
        raw = json.load(fh)
    if not isinstance(raw, dict) or not isinstance(raw.get("nodes"), list):
        raise PlanError("节点配置必须是含 nodes 数组的 JSON")
    limits: list[int] = []
    for i, item in enumerate(raw["nodes"]):
        if not isinstance(item, dict) or not isinstance(item.get("max_output_rows"), int):
            raise PlanError(f"nodes[{i}] 需要整数 max_output_rows")
        limit = item["max_output_rows"]
        if limit < 0:
            raise PlanError(f"nodes[{i}] 行上限不能为负")
        limits.append(limit)
    if not limits:
        raise PlanError("每批至少需要一个节点")
    return limits
