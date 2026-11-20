"""离线等值连接业务测试：标准化、清单语义、精确分工、节点执行与独立核对。

运行：PYTHONPATH=src .venv/bin/python -m pytest tests/test_join.py -q
"""
from __future__ import annotations

import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pqscan.errors import PlanError
from pqscan.join.codes import standardize_values
from pqscan.join.dispatch import dispatch
from pqscan.join.executor import execute_plan
from pqscan.join.job import load_job, load_node_limits
from pqscan.join.manifest import build_manifest
from pqscan.join.reference import verify_execution, verify_manifest
from pqscan.join.runner import run_join

JOIN_FIX = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "fixtures", "join")


def _path(name: str) -> str:
    return os.path.join(JOIN_FIX, name)


def _manifest(job_name: str):
    job = load_job(_path(job_name))
    return job, build_manifest(job)


# ---------------------------------------------------------------------------
# 标准化：空串 / NULL / 未登记文本三分
# ---------------------------------------------------------------------------
def test_standardize_distinguishes_empty_null_unregistered():
    table = {"": "STD_EMPTY", "a": "STD_A"}
    unregistered = []
    result = standardize_values(
        "left", "f.parquet", 0, ["a", None, "", "a", "zzz"], table, unregistered
    )
    # NULL 不查码表、不参与连接
    assert result.null_rows == [1]
    # 空字符串合法映射
    assert result.std_rows["STD_EMPTY"] == [2]
    # 重复文本保留两个物理行号
    assert result.std_rows["STD_A"] == [0, 3]
    # 未登记的非空文本被报出
    assert len(unregistered) == 1
    assert unregistered[0].text == "zzz"
    assert (unregistered[0].row_group, unregistered[0].row_in_group) == (0, 4)


def test_unregistered_empty_string_is_also_an_error():
    table = {"a": "STD_A"}  # 空串未登记
    unregistered = []
    standardize_values("right", "f.parquet", 2, [""], table, unregistered)
    assert len(unregistered) == 1 and unregistered[0].text == ""
    assert unregistered[0].row_group == 2


def test_unregistered_aborts(tmp_path):
    bad = tmp_path / "bad.parquet"
    t = pa.table({"rk": pa.array(["a", "nope"]), "rv": pa.array(["x", "y"])})
    pq.write_table(t, bad, use_dictionary=False)

    os.makedirs(tmp_path / "codes", exist_ok=True)
    with open(_path("codes/v1.json"), encoding="utf-8") as src:
        (tmp_path / "codes" / "v1.json").write_text(src.read(), encoding="utf-8")
    left_rel = os.path.relpath(_path("parquet/demo_left.parquet"), tmp_path)
    job_doc = {
        "job_id": "bad",
        "code_tables": {"v1": "codes/v1.json"},
        "left": {
            "files": [{"path": left_rel, "code_version": "v1"}],
            "key_column": "lk",
            "columns": ["lv"],
        },
        "right": {
            "files": [{"path": "bad.parquet", "code_version": "v1"}],
            "key_column": "rk",
            "columns": ["rv"],
        },
    }
    job_path = tmp_path / "job.json"
    job_path.write_text(json.dumps(job_doc), encoding="utf-8")

    with pytest.raises(PlanError) as exc:
        build_manifest(load_job(str(job_path)))
    message = str(exc.value)
    assert "nope" in message and "行组0 行1" in message


def test_unregistered_repo_fixture_reports_location():
    # 仓库自带 bad_job.json：bad_right.parquet 行组0 行1 的 zzz 未登记
    with pytest.raises(PlanError) as exc:
        build_manifest(load_job(_path("bad_job.json")))
    message = str(exc.value)
    assert "zzz" in message and "bad_right.parquet" in message
    assert "行组0 行1" in message


# ---------------------------------------------------------------------------
# 清单：只含键列信息、GHOST 不算命中、零匹配略去、重复行保留
# ---------------------------------------------------------------------------
def test_key_column_not_first_position(tmp_path):
    # 键列放在第二列，验证编码检测与读取都按列名/列索引定位而非写死 column(0)
    left = tmp_path / "l.parquet"
    right = tmp_path / "r.parquet"
    pq.write_table(pa.table({"lv": pa.array(["x"]), "lk": pa.array(["a"])}),
                   left, use_dictionary=True)
    pq.write_table(pa.table({"rv": pa.array(["y"]), "rk": pa.array(["a"])}),
                   right, use_dictionary=False)
    os.makedirs(tmp_path / "codes", exist_ok=True)
    (tmp_path / "codes" / "v1.json").write_text(
        json.dumps({"version": "v1", "mappings": {"a": "STD_A"}}), encoding="utf-8")
    job_doc = {
        "job_id": "pos",
        "code_tables": {"v1": "codes/v1.json"},
        "left": {"files": [{"path": "l.parquet", "code_version": "v1"}],
                 "key_column": "lk", "columns": ["lv"]},
        "right": {"files": [{"path": "r.parquet", "code_version": "v1"}],
                  "key_column": "rk", "columns": ["rv"]},
    }
    job_path = tmp_path / "job.json"
    job_path.write_text(json.dumps(job_doc), encoding="utf-8")
    job = load_job(str(job_path))
    manifest = build_manifest(job)
    assert manifest.groups["L0g0"].key_encoding == "dictionary"
    assert manifest.groups["R0g0"].key_encoding == "plain"
    assert manifest.pairs and manifest.pairs[0].total_rows == 1


def test_manifest_reads_only_key_columns(monkeypatch):
    import pyarrow.parquet as pq

    seen: list[list[str]] = []
    original = pq.ParquetFile.read_row_group

    def spy(self, row_group, columns, use_threads=True):
        seen.append(list(columns))
        return original(self, row_group, columns=columns, use_threads=use_threads)

    monkeypatch.setattr(pq.ParquetFile, "read_row_group", spy)
    _manifest("demo_job.json")
    assert seen, "清单阶段必须真实读取键列"
    assert all(cols == ["lk"] or cols == ["rk"] for cols in seen)
    assert all("lv" not in cols and "rv" not in cols for cols in seen)


def test_manifest_ghost_dictionary_value_not_a_hit():
    _, manifest = _manifest("semantics_job.json")
    group = next(g for g in manifest.groups.values() if g.group_id == "L0g0")
    assert group.key_encoding == "dictionary"
    std_keys = {k["std_key"] for k in group.standardization.to_key_dicts()}
    # GHOST 只在字典里、无物理行引用：清单里绝不出现
    assert "GHOST" not in std_keys and not any("GHOST" in k["raw_texts"]
                                               for k in group.standardization.to_key_dicts())
    right = manifest.groups["R0g0"]
    assert right.key_encoding == "plain"


def test_manifest_pair_rows_and_duplicate_physical_rows():
    _, manifest = _manifest("demo_job.json")
    by_pair = {(p.left_group_id, p.right_group_id): p for p in manifest.pairs}
    # 三个非空组对，零匹配的 L2×R2 被略去
    assert set(by_pair) == {("L0g0", "R0g0"), ("L0g0", "R0g1"), ("L0g1", "R0g0")}

    p_l1_r1 = by_pair[("L0g0", "R0g0")]
    match = p_l1_r1.matches[0]
    assert match.std_key == "STD_A"
    assert match.left_rows == [0, 1] and match.right_rows == [0]
    assert match.pair_rows == 2 and p_l1_r1.total_rows == 2

    p_l2_r1 = by_pair[("L0g1", "R0g0")]
    assert p_l2_r1.total_rows == 1
    assert sum(p.total_rows for p in manifest.pairs) == 5


def test_manifest_cross_version_same_text_different_meaning():
    _, manifest = _manifest("semantics_job.json")
    pair = manifest.pairs[0]
    std_keys = {m.std_key for m in pair.matches}
    # 左 v1: alpha->STD_A；右 v2: alpha->STD_B，因此 alpha 不产生连接
    assert "STD_B" not in std_keys
    # 空串两侧都登记到 STD_EMPTY，命中一次
    emp = next(m for m in pair.matches if m.std_key == "STD_EMPTY")
    assert emp.left_rows == [4] and emp.right_rows == [5] and emp.pair_rows == 1
    # 右 a 重复两行，左 a/A 共三行 -> 6 对
    std_a = next(m for m in pair.matches if m.std_key == "STD_A")
    assert std_a.pair_rows == 6
    assert pair.total_rows == 7


def test_manifest_null_rows_recorded_but_never_joined():
    _, manifest = _manifest("semantics_job.json")
    nulls = {n["group_id"]: n["null_count"] for n in manifest.null_summary}
    assert nulls == {"L0g0": 1, "R0g0": 1}
    # NULL 所在物理行（左行3、右行4）不会出现在任何命中里
    assert 3 not in [r for p in manifest.pairs for m in p.matches for r in m.left_rows]
    assert 4 not in [r for p in manifest.pairs for m in p.matches for r in m.right_rows]


# ---------------------------------------------------------------------------
# 精确分工：题面三组场景 + 最优方案计数
# ---------------------------------------------------------------------------
def test_dispatch_demo_caps_3_2_matches_worked_example():
    _, manifest = _manifest("demo_job.json")
    plan = dispatch(manifest, [3, 2])
    assert plan.feasible and plan.total_copies == 5 and plan.total_output_rows == 5
    n0, n1 = plan.nodes
    assert n0.pair_ids == ["P01", "P03"] and n0.output_rows == 3
    assert n1.pair_ids == ["P02"] and n1.output_rows == 2
    # 同一节点 L0g0 在两对中复用，只领一份
    assert n0.claimed_groups == ["L0g0", "L0g1", "R0g0"]


def test_dispatch_demo_caps_4_1_moves_both_l1_tasks_to_node0():
    _, manifest = _manifest("demo_job.json")
    plan = dispatch(manifest, [4, 1])
    assert plan.feasible and plan.total_copies == 5
    n0, n1 = plan.nodes
    assert n0.pair_ids == ["P01", "P02"]      # L1 的两对任务都改由第一节点领
    assert n1.pair_ids == ["P03"]
    assert all(n.output_rows <= n.limit for n in plan.nodes)


def test_dispatch_heavy_single_task_exceeding_cap_is_infeasible():
    _, manifest = _manifest("demo_heavy_job.json")
    plan = dispatch(manifest, [3, 2])
    assert not plan.feasible and plan.optimal_solution_count == 0
    # 单个 4 行任务（L1×R2）超过最大上限 3，任务不可拆
    assert "4 行" in plan.reason


def test_dispatch_minimizes_group_copies_and_counts_optima():
    # 构造两个任务共享同一左组：合并到一个节点应只算 3 份，拆开算 4 份
    _, manifest = _manifest("demo_job.json")
    plan = dispatch(manifest, [5, 5])
    # P01/P02 共享 L0g0；枚举最优应把它们放同一节点，总份数最小
    assert plan.feasible
    # 全量最小份数：一个节点做全部三对 -> L0g0,L0g1,R0g0,R0g1 共 4 份
    assert plan.total_copies == 4
    assert plan.optimal_solution_count >= 1
    # 所有可行方案都满足上限且任务全覆盖
    assigned = [pid for n in plan.nodes for pid in n.pair_ids]
    assert sorted(assigned) == [p["pair_id"] for p in plan.tasks]


def test_dispatch_rejects_more_than_three_nodes():
    _, manifest = _manifest("demo_job.json")
    with pytest.raises(PlanError):
        dispatch(manifest, [1, 1, 1, 1])


# ---------------------------------------------------------------------------
# 节点执行 + 独立核对
# ---------------------------------------------------------------------------
def test_execute_and_reference_verification(tmp_path):
    job, manifest = _manifest("demo_job.json")
    plan = dispatch(manifest, [3, 2])
    results = execute_plan(job, manifest, plan, str(tmp_path))
    assert verify_manifest(job, manifest) == []
    check = verify_execution(job, manifest, plan, results, str(tmp_path))
    assert check["passed"] and check["reference_total_rows"] == 5

    # 节点 0 复用 L0g0：该组在节点内只真实读取一次
    node0_reads = [(r.file, r.row_group) for r in results[0].group_reads]
    assert len(node0_reads) == len(set(node0_reads))

    t0 = pq.read_table(str(tmp_path / "join_results" / "node0.parquet"))
    assert t0.schema.names == [
        "std_key", "lv", "rv",
        "left_file", "left_row_group", "left_row_in_group",
        "right_file", "right_row_group", "right_row_in_group",
    ]
    # 空节点也产出同 schema 的零行文件
    execute_plan(job, manifest, dispatch(manifest, [3, 2, 0]), str(tmp_path / "idle"))
    idle_tbl = pq.read_table(str(tmp_path / "idle" / "join_results" / "node2.parquet"))
    assert idle_tbl.num_rows == 0 and idle_tbl.schema == t0.schema


def test_run_join_end_to_end_writes_all_artifacts(tmp_path):
    out = tmp_path / "out"
    doc = run_join(_path("demo_job.json"), _path("nodes_3_2.json"), str(out))
    assert doc["plan"].feasible and doc["verification"]["passed"]
    for name in ("join_manifest.json", "dispatch_plan.json",
                 "join_run_record.json", "join_verification.json"):
        assert (out / name).exists()
    assert (out / "join_results" / "node0.parquet").exists()


def test_node_limits_loader():
    limits = load_node_limits(_path("nodes_4_1.json"))
    assert limits == [4, 1]


# ---------------------------------------------------------------------------
# 随机化蛮力：用独立的 itertools 参考枚举验证最优份数与方案计数
# ---------------------------------------------------------------------------
def test_dispatch_matches_brute_force_on_random_instances():
    import itertools
    from pqscan.join.dispatch import _plan_cost
    from pqscan.join.manifest import PairEntry, PairMatch

    rng = __import__("random").Random(20261120)
    for _ in range(60):
        task_count = rng.randint(1, 8)
        node_count = rng.randint(1, 3)
        # 随机生成共享行组的任务（行组数小于任务数，制造复用）
        group_pool = [f"L{i}" for i in range(rng.randint(1, 4))] + \
                     [f"R{i}" for i in range(rng.randint(1, 4))]
        left_groups = [g for g in group_pool if g.startswith("L")]
        right_groups = [g for g in group_pool if g.startswith("R")]
        pairs = []
        for i in range(task_count):
            pair = PairEntry(
                pair_id=f"P{i:02d}",
                left_group_id=rng.choice(left_groups),
                right_group_id=rng.choice(right_groups),
                left_file="l", right_file="r", left_row_group=0, right_row_group=0,
            )
            pair.matches.append(PairMatch(
                std_key="K", left_raw_texts=[], right_raw_texts=[],
                left_rows=[0], right_rows=list(range(rng.randint(1, 4))),
            ))
            pairs.append(pair)
        limits = [rng.randint(0, 12) for _ in range(node_count)]

        manifest = type("M", (), {"pairs": pairs})()
        plan = dispatch(manifest, limits)

        rows = [p.total_rows for p in pairs]
        feasible_enum = []
        for assignment in itertools.product(range(node_count), repeat=task_count):
            used = [0] * node_count
            ok = True
            for node_index, n in zip(assignment, rows):
                used[node_index] += n
                if used[node_index] > limits[node_index]:
                    ok = False
                    break
            if ok:
                feasible_enum.append(assignment)

        if not feasible_enum:
            assert not plan.feasible
            continue
        assert plan.feasible
        costs = {a: _plan_cost(list(a), pairs, node_count) for a in feasible_enum}
        min_cost = min(costs.values())
        assert plan.total_copies == min_cost
        assert plan.optimal_solution_count == sum(1 for c in costs.values() if c == min_cost)
        # 输出方案本身可行且达到最优
        got = [next(i for i, n in enumerate(plan.nodes) if pid in n.pair_ids)
               for pid in [p.pair_id for p in pairs]]
        assert costs[tuple(got)] == min_cost


def test_manifest_with_no_matching_pairs(tmp_path):
    # 两侧没有共同标准键：清单为空，分工可行为零任务
    left = tmp_path / "l.parquet"
    right = tmp_path / "r.parquet"
    pq.write_table(pa.table({"lk": pa.array(["b"]), "lv": pa.array(["x"])}),
                   left, use_dictionary=False)
    pq.write_table(pa.table({"rk": pa.array(["a"]), "rv": pa.array(["y"])}),
                   right, use_dictionary=False)
    os.makedirs(tmp_path / "codes", exist_ok=True)
    (tmp_path / "codes" / "v1.json").write_text(
        json.dumps({"version": "v1", "mappings": {"a": "STD_A", "b": "STD_B"}}),
        encoding="utf-8")
    job_doc = {
        "job_id": "empty",
        "code_tables": {"v1": "codes/v1.json"},
        "left": {"files": [{"path": "l.parquet", "code_version": "v1"}],
                 "key_column": "lk", "columns": ["lv"]},
        "right": {"files": [{"path": "r.parquet", "code_version": "v1"}],
                  "key_column": "rk", "columns": ["rv"]},
    }
    job_path = tmp_path / "job.json"
    job_path.write_text(json.dumps(job_doc), encoding="utf-8")
    job = load_job(str(job_path))
    manifest = build_manifest(job)
    assert manifest.pairs == []
    plan = dispatch(manifest, [3])
    assert plan.feasible and plan.total_copies == 0 and plan.nodes[0].pair_ids == []
