"""命令行入口。

平面查询（原有能力，保持可用）：
    python -m pqscan.cli run --files fixtures/files.json --queries fixtures/queries.json \
        --out output
    python -m pqscan.cli verify --files ... --queries ... --out output

离线等值连接业务（新增，互不影响）：
    python -m pqscan.cli join-manifest --job fixtures/join/demo_job.json --out output/join
    python -m pqscan.cli join-dispatch --job ... --nodes fixtures/join/nodes_3_2.json --out ...
    python -m pqscan.cli join-execute --job ... --out ...
    python -m pqscan.cli join-run --job ... --nodes ... --out ...   # 三阶段串起来 + 独立核对
"""
from __future__ import annotations

import argparse
import json
import sys

from .errors import PqScanError
from .expr import parse_queries, validate, validate_projection
from .reference import reference_scan
from .runner import run


def _cmd_run(args: argparse.Namespace) -> int:
    doc = run(args.files, args.queries, args.out)
    all_ok = all(q["passed"] for q in doc["verification"]["queries"])
    for q in doc["verification"]["queries"]:
        flag = "PASS" if q["passed"] else "FAIL"
        print(f"[verify:{flag}] {q['query_id']}: {q['detail']} "
              f"(engine={q['engine_rows']}, reference={q['reference_rows']})")
    print(f"\n计划与结果已写入 {args.out}/ (plans/、results/、run_record.json、verification.json)")
    return 0 if all_ok else 2


def _cmd_verify(args: argparse.Namespace) -> int:
    import os
    import pyarrow.parquet as pq

    fixture_dir = os.path.dirname(os.path.abspath(args.files))
    with open(args.files, encoding="utf-8") as fh:
        files = [os.path.join(fixture_dir, n) for n in json.load(fh)]
    with open(args.queries, encoding="utf-8") as fh:
        queries = parse_queries(json.load(fh))

    failed = 0
    for query in queries:
        for path in files:
            validate(query.where, pq.ParquetFile(path).schema_arrow)
            validate_projection(query.columns, pq.ParquetFile(path).schema_arrow)
        ref = reference_scan(query, files)
        result_path = os.path.join(args.out, "results", f"{query.id}.parquet")
        got = pq.read_table(result_path)
        same = got.schema == ref.schema and got.to_pylist() == ref.to_pylist()
        print(f"[{'PASS' if same else 'FAIL'}] {query.id}: {got.num_rows} 行")
        failed += 0 if same else 1
    return 0 if failed == 0 else 2


def _cmd_join_manifest(args: argparse.Namespace) -> int:
    from .join.runner import stage_manifest

    _, manifest = stage_manifest(args.job, args.out)
    print(f"连接清单已写入 {args.out}/join_manifest.json："
          f"{len(manifest.pairs)} 个非空行组对，"
          f"合计 {sum(p.total_rows for p in manifest.pairs)} 对明细行（未展开）")
    return 0


def _cmd_join_dispatch(args: argparse.Namespace) -> int:
    from .join.job import load_node_limits
    from .join.runner import stage_dispatch, stage_manifest

    _, manifest = stage_manifest(args.job, args.out)
    plan = stage_dispatch(manifest, load_node_limits(args.nodes), args.out)
    print(f"分工已写入 {args.out}/dispatch_plan.json")
    if not plan.feasible:
        print(f"无可行分工: {plan.reason}")
        return 3
    for node in plan.nodes:
        print(f"  节点{node.node_index}(上限{node.limit}): 组对 {node.pair_ids}，"
              f"产出 {node.output_rows} 行，领取 {node.copies} 份行组 {node.claimed_groups}")
    print(f"  总产出 {plan.total_output_rows} 行，总领取 {plan.total_copies} 份；"
          f"最优方案数 {plan.optimal_solution_count}")
    return 0


def _cmd_join_execute(args: argparse.Namespace) -> int:
    from .join.job import load_job, load_node_limits
    from .join.manifest import build_manifest
    from .join.dispatch import dispatch
    from .join.executor import execute_plan

    out_dir = args.out
    job = load_job(args.job)
    manifest = build_manifest(job)
    plan = dispatch(manifest, load_node_limits(args.nodes))
    if not plan.feasible:
        print(f"无可行分工，拒绝执行: {plan.reason}", file=sys.stderr)
        return 3
    results = execute_plan(job, manifest, plan, out_dir)
    for r in results:
        print(f"  节点{r.node_index}: {r.result_rows} 行 -> {r.output_path}，"
              f"实际读取 {len(r.group_reads)} 个行组（每组一次）")
    return 0


def _cmd_join_run(args: argparse.Namespace) -> int:
    from .join.runner import run_join

    doc = run_join(args.job, args.nodes, args.out)
    plan = doc["plan"]
    if not plan.feasible:
        print(f"无可行分工: {plan.reason}")
        return 3
    ok = doc["verification"]["passed"]
    flag = "PASS" if ok else "FAIL"
    print(f"[verify:{flag}] 清单复核 {doc['verification']['manifest_problems'] or '无问题'}；"
          f"执行核对 {doc['verification']['execution']}")
    print(f"连接产物已写入 {args.out}/ (join_manifest.json、dispatch_plan.json、"
          f"join_results/、join_run_record.json、join_verification.json)")
    return 0 if ok else 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pqscan", description="Parquet footer 统计驱动的保守 plan/scan")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="计划 + 扫描 + 独立核对")
    p_run.add_argument("--files", required=True)
    p_run.add_argument("--queries", required=True)
    p_run.add_argument("--out", default="output")
    p_run.set_defaults(func=_cmd_run)

    p_ver = sub.add_parser("verify", help="对已产出的结果再跑一次无剪枝全扫描核对")
    p_ver.add_argument("--files", required=True)
    p_ver.add_argument("--queries", required=True)
    p_ver.add_argument("--out", default="output")
    p_ver.set_defaults(func=_cmd_verify)

    p_jm = sub.add_parser("join-manifest", help="连接业务：只读键列产出连接清单")
    p_jm.add_argument("--job", required=True)
    p_jm.add_argument("--out", default="output/join")
    p_jm.set_defaults(func=_cmd_join_manifest)

    p_jd = sub.add_parser("join-dispatch", help="连接业务：按节点行上限求精确分工")
    p_jd.add_argument("--job", required=True)
    p_jd.add_argument("--nodes", required=True)
    p_jd.add_argument("--out", default="output/join")
    p_jd.set_defaults(func=_cmd_join_dispatch)

    p_je = sub.add_parser("join-execute", help="连接业务：节点本地真实执行并写连接结果")
    p_je.add_argument("--job", required=True)
    p_je.add_argument("--nodes", required=True)
    p_je.add_argument("--out", default="output/join")
    p_je.set_defaults(func=_cmd_join_execute)

    p_jr = sub.add_parser("join-run", help="连接业务：清单 + 分工 + 执行 + 独立核对一条龙")
    p_jr.add_argument("--job", required=True)
    p_jr.add_argument("--nodes", required=True)
    p_jr.add_argument("--out", default="output/join")
    p_jr.set_defaults(func=_cmd_join_run)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except PqScanError as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
