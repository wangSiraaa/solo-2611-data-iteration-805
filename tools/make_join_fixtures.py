"""生成离线等值连接业务所需的全部夹具：两套 Parquet 分片、两个版本码表、任务与节点配置。

直接运行：PYTHONPATH=src .venv/bin/python tools/make_join_fixtures.py

产物（fixtures/join/ 下）：
- codes/v1.json、codes/v2.json     ：两个版本码表（同一文本可映射到不同标准键）
- parquet/demo_left.parquet        ：字典编码键列，行组 L1=[a,a]、L2=[b]
- parquet/demo_right.parquet       ：普通编码键列，行组 R1=[a,b]、R2=[a]
- parquet/demo_right_heavy.parquet ：普通编码，R2=[a,a]（制造单个 4 行任务）
- parquet/sem_left.parquet         ：字典编码，字典含未被任何物理行引用的 GHOST
- parquet/sem_right.parquet        ：普通编码，含 NULL/空串/跨版本同文本异义
- *job.json / nodes_*.json         ：任务与节点上限配置
"""
from __future__ import annotations

import json
import os

import pyarrow as pa
import pyarrow.parquet as pq

BASE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "fixtures", "join")


# ---------------------------------------------------------------------------
# 码表：两个版本对同一本地文本给出不同解释
# ---------------------------------------------------------------------------
CODE_TABLES = {
    "v1": {
        # v1: a/A/alpha 都归一到 STD_A；空字符串合法登记到 STD_EMPTY
        "a": "STD_A",
        "A": "STD_A",
        "alpha": "STD_A",
        "b": "STD_B",
        "": "STD_EMPTY",
    },
    "v2": {
        # v2: alpha 的含义与 v1 不同 -> STD_B；同文本异义的核心用例
        "a": "STD_A",
        "A": "STD_A",
        "alpha": "STD_B",
        "b": "STD_B",
        "beta": "STD_B",
        "": "STD_EMPTY",
    },
}


def _write_code_tables() -> None:
    os.makedirs(os.path.join(BASE, "codes"), exist_ok=True)
    for version, mappings in CODE_TABLES.items():
        with open(os.path.join(BASE, "codes", f"{version}.json"), "w", encoding="utf-8") as fh:
            json.dump({"version": version, "mappings": mappings}, fh,
                      ensure_ascii=False, indent=2)


def _write_grouped(path: str, schema: pa.Schema, groups: list[pa.Table],
                   use_dictionary: bool) -> None:
    writer = pq.ParquetWriter(path, schema, use_dictionary=use_dictionary)
    try:
        for table in groups:
            writer.write_table(table)
    finally:
        writer.close()


def _dict_table(keys: list[str | None], values: list[str],
                extra_dictionary: list[str] | None = None) -> pa.Table:
    """构造字典编码键列表：extra_dictionary 里的值进字典但不被任何物理行引用。"""
    unique = list(dict.fromkeys(k for k in keys if k is not None))
    dictionary = unique + [v for v in (extra_dictionary or []) if v not in unique]
    index_of = {text: i for i, text in enumerate(dictionary)}
    indices = [None if k is None else index_of[k] for k in keys]
    darr = pa.DictionaryArray.from_arrays(
        pa.array(indices, type=pa.int32()), pa.array(dictionary, type=pa.string())
    )
    return pa.table({"lk": darr, "lv": pa.array(values, type=pa.string())})


def _plain_string_table(keys: list[str | None], values: list[str],
                        col: str = "rk") -> pa.Table:
    return pa.table({col: pa.array(keys, type=pa.string()),
                     "rv": pa.array(values, type=pa.string())})


# ---------------------------------------------------------------------------
# 分工演示数据（与需求中的例子一一对应）
# 左：L1=[a,a]、L2=[b]；右：R1=[a,b]、R2=[a]（heavy 版 R2=[a,a]）
# ---------------------------------------------------------------------------
LEFT_SCHEMA = pa.schema([
    pa.field("lk", pa.dictionary(pa.int32(), pa.string())),
    pa.field("lv", pa.string()),
])
RIGHT_SCHEMA = pa.schema([
    pa.field("rk", pa.string()),
    pa.field("rv", pa.string()),
])


def _write_demo_parquets() -> None:
    os.makedirs(os.path.join(BASE, "parquet"), exist_ok=True)
    left_groups = [
        _dict_table(["a", "a"], ["L1-0", "L1-1"]),                       # L1
        _dict_table(["b"], ["L2-0"]),                                    # L2
    ]
    _write_grouped(os.path.join(BASE, "parquet", "demo_left.parquet"),
                   LEFT_SCHEMA, left_groups, use_dictionary=True)

    right_groups = [
        _plain_string_table(["a", "b"], ["R1-0", "R1-1"]),               # R1
        _plain_string_table(["a"], ["R2-0"]),                            # R2
    ]
    _write_grouped(os.path.join(BASE, "parquet", "demo_right.parquet"),
                   RIGHT_SCHEMA, right_groups, use_dictionary=False)

    heavy_groups = [
        _plain_string_table(["a", "b"], ["R1-0", "R1-1"]),               # R1
        _plain_string_table(["a", "a"], ["R2-0", "R2-1"]),               # R2=[a,a]
    ]
    _write_grouped(os.path.join(BASE, "parquet", "demo_right_heavy.parquet"),
                   RIGHT_SCHEMA, heavy_groups, use_dictionary=False)


# ---------------------------------------------------------------------------
# 语义演示数据：字典 GHOST、NULL、空串、跨版本同文本异义、重复行
# 左(v1)：SG1 字典含 GHOST；右(v2)：alpha 在 v2 映射到 STD_B，与左 v1 的 STD_A 不连
# ---------------------------------------------------------------------------
def _write_semantics_parquets() -> None:
    sem_left_groups = [
        # 物理行：a, A, a, NULL, "" —— GHOST 只存在于字典，无物理行引用
        _dict_table(["a", "A", "a", None, ""],
                    ["sL0", "sL1", "sL2", "sL3", "sL4"],
                    extra_dictionary=["GHOST"]),
    ]
    _write_grouped(os.path.join(BASE, "parquet", "sem_left.parquet"),
                   LEFT_SCHEMA, sem_left_groups, use_dictionary=True)

    sem_right_groups = [
        # a 重复两行；alpha 在 v2 -> STD_B（左 v1 alpha->STD_A）；b、NULL、空串
        _plain_string_table(["a", "a", "alpha", "b", None, ""],
                            ["sR0", "sR1", "sR2", "sR3", "sR4", "sR5"]),
    ]
    _write_grouped(os.path.join(BASE, "parquet", "sem_right.parquet"),
                   RIGHT_SCHEMA, sem_right_groups, use_dictionary=False)

    # 未登记文本夹具：键 "zzz" 不在 v1 码表中（a 已登记、NULL 跳过、空串已登记），
    # 清单阶段必须带着所在行位置报错并停止该批
    bad_groups = [
        _plain_string_table(["a", "zzz", None, ""],
                            ["bR0", "bR1", "bR2", "bR3"]),
    ]
    _write_grouped(os.path.join(BASE, "parquet", "bad_right.parquet"),
                   RIGHT_SCHEMA, bad_groups, use_dictionary=False)


# ---------------------------------------------------------------------------
# 任务与节点配置
# ---------------------------------------------------------------------------
def _job(job_id: str, left: str, right: str, left_ver: str, right_ver: str) -> dict:
    return {
        "job_id": job_id,
        "code_tables": {
            "v1": "codes/v1.json",
            "v2": "codes/v2.json",
        },
        "left": {
            "files": [{"path": f"parquet/{left}", "code_version": left_ver}],
            "key_column": "lk",
            "columns": ["lv"],
        },
        "right": {
            "files": [{"path": f"parquet/{right}", "code_version": right_ver}],
            "key_column": "rk",
            "columns": ["rv"],
        },
    }


def _write_json(rel: str, doc: dict) -> None:
    with open(os.path.join(BASE, rel), "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2)


def _write_configs() -> None:
    _write_json("demo_job.json",
                _job("demo_dispatch", "demo_left.parquet", "demo_right.parquet", "v1", "v1"))
    _write_json("demo_heavy_job.json",
                _job("demo_heavy", "demo_left.parquet", "demo_right_heavy.parquet", "v1", "v1"))
    _write_json("semantics_job.json",
                _job("semantics", "sem_left.parquet", "sem_right.parquet", "v1", "v2"))
    _write_json("bad_job.json",
                _job("unregistered_abort", "sem_left.parquet", "bad_right.parquet", "v1", "v1"))

    nodes = {
        "nodes_3_2.json": [3, 2],
        "nodes_4_1.json": [4, 1],
        "nodes_3_2_heavy.json": [3, 2],
        "nodes_sem.json": [8, 8],
    }
    for name, limits in nodes.items():
        _write_json(name, {"nodes": [
            {"node_id": f"n{i}", "max_output_rows": lim} for i, lim in enumerate(limits)
        ]})


def main() -> None:
    _write_code_tables()
    _write_demo_parquets()
    _write_semantics_parquets()
    _write_configs()
    print(f"wrote join fixtures under {BASE}")


if __name__ == "__main__":
    main()
