"""码表加载与按版本标准化。

码表是“本地文本 -> 标准键”的纯字符串映射，JSON 形态：
    {"version": "v1", "mappings": {"a": "STD_A", "": "STD_EMPTY"}}
空字符串是合法的 JSON 键，因此空字符串可以登记、可以命中连接；NULL 不是字符串、
不进码表查询，直接按“不参与连接”处理；非空值与空字符串若在该版码表中未登记，
登记为 UnregisteredOccurrence，由上层汇总报出所在行并停止该批。
"""
from __future__ import annotations

import json
from dataclasses import dataclass

from ..errors import PqScanError


class CodeTableError(PqScanError):
    """码表文件结构非法。"""


@dataclass(frozen=True)
class UnregisteredOccurrence:
    """一个未在对应版本码表中登记的非 NULL 键值。"""

    side: str
    file: str
    row_group: int
    row_in_group: int
    text: str

    def to_dict(self) -> dict[str, object]:
        return {
            "side": self.side,
            "file": self.file,
            "row_group": self.row_group,
            "row_in_group": self.row_in_group,
            "text": self.text,
        }


def load_code_table(path: str) -> dict[str, str]:
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    if not isinstance(doc, dict) or not isinstance(doc.get("mappings"), dict):
        raise CodeTableError(f"{path}: 码表必须是含 mappings 对象的 JSON")
    mappings = doc["mappings"]
    for raw, std in mappings.items():
        if not isinstance(raw, str) or not isinstance(std, str):
            raise CodeTableError(f"{path}: 码表 mappings 的键与值都必须是字符串")
    return dict(mappings)


@dataclass
class GroupStandardization:
    """一个行组键列标准化后的结果。"""

    std_rows: dict[str, list[int]]          # 标准键 -> 组内物理行号（含重复行）
    raw_texts: dict[str, list[str]]         # 标准键 -> 映射到它的去重本地文本
    null_rows: list[int]                    # NULL 键的物理行号（不参与连接）

    def to_key_dicts(self) -> list[dict[str, object]]:
        return [
            {
                "std_key": std,
                "raw_texts": sorted(self.raw_texts[std]),
                "rows": self.std_rows[std],
            }
            for std in sorted(self.std_rows)
        ]


def standardize_values(
    side: str,
    file: str,
    row_group: int,
    raw_values: list[str | None],
    table: dict[str, str],
    unregistered: list[UnregisteredOccurrence],
) -> GroupStandardization:
    """对一个行组解码后的键值逐行标准化。

    入参 raw_values 已经是解码后的 Python 值（字典编码与普通编码走同一路径）：
    - None（NULL）：记入 null_rows，不查码表、不参与连接；
    - 任意字符串（含 ""）：查该文件指定版本的码表；查不到就登记未登记出现；
    - 同一标准键被重复引用时保留每个物理行号，重复内容仍是不同明细。
    """
    std_rows: dict[str, list[int]] = {}
    raw_texts: dict[str, list[str]] = {}
    null_rows: list[int] = []

    for row_in_group, text in enumerate(raw_values):
        if text is None:
            null_rows.append(row_in_group)
            continue
        if text not in table:
            unregistered.append(
                UnregisteredOccurrence(
                    side=side, file=file, row_group=row_group,
                    row_in_group=row_in_group, text=text,
                )
            )
            continue
        std_key = table[text]
        std_rows.setdefault(std_key, []).append(row_in_group)
        texts = raw_texts.setdefault(std_key, [])
        if text not in texts:
            texts.append(text)

    return GroupStandardization(std_rows=std_rows, raw_texts=raw_texts, null_rows=null_rows)
