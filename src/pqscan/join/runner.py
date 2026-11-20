"""连接业务编排：清单 -> 分工 -> 节点执行 -> 独立核对，全部产物落盘。

产物（均在 --out 指定目录下）：
- join_manifest.json   ：可独立查看的连接清单（只含键列读取得到的信息）
- dispatch_plan.json   ：精确分工（不可行时 feasible=false 并写明原因，不执行节点阶段）
- join_results/node*.parquet：各节点真实执行写出的连接结果（带双侧物理出处与业务列）
- join_run_record.json ：节点实际发起的行组读取与逐节点行数
- join_verification.json：不看清单的全量重算核对结论
"""
from __future__ import annotations

import json
import os

from .dispatch import DispatchPlan, dispatch
from .executor import NodeResult, execute_plan
from .job import JoinJob, load_job, load_node_limits
from .manifest import JoinManifest, build_manifest
from .reference import verify_execution, verify_manifest


def _write(path: str, doc: dict[str, object]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2)


def stage_manifest(job_path: str, out_dir: str) -> tuple[JoinJob, JoinManifest]:
    os.makedirs(out_dir, exist_ok=True)
    job = load_job(job_path)
    manifest = build_manifest(job)
    _write(os.path.join(out_dir, "join_manifest.json"), manifest.to_dict())
    return job, manifest


def stage_dispatch(manifest: JoinManifest, limits: list[int], out_dir: str) -> DispatchPlan:
    plan = dispatch(manifest, limits)
    _write(os.path.join(out_dir, "dispatch_plan.json"), plan.to_dict())
    return plan


def run_join(job_path: str, nodes_path: str, out_dir: str) -> dict[str, object]:
    job, manifest = stage_manifest(job_path, out_dir)
    limits = load_node_limits(nodes_path)
    plan = stage_dispatch(manifest, limits, out_dir)

    node_results: list[NodeResult] = []
    verification: dict[str, object]
    if plan.feasible:
        node_results = execute_plan(job, manifest, plan, out_dir)
        manifest_problems = verify_manifest(job, manifest)
        execution_check = verify_execution(
            job, manifest, plan, node_results, out_dir
        )
        verification = {
            "passed": not manifest_problems and execution_check["passed"],
            "manifest_problems": manifest_problems,
            "execution": execution_check,
        }
    else:
        verification = {
            "passed": False,
            "manifest_problems": verify_manifest(job, manifest),
            "execution": {
                "passed": False,
                "problems": [f"分工不可行，未执行节点阶段: {plan.reason}"],
                "reference_total_rows": plan.total_output_rows,
                "node_output_total_rows": 0,
            },
        }

    record = {
        "job_id": job.job_id,
        "manifest_pairs": len(manifest.pairs),
        "dispatch_feasible": plan.feasible,
        "total_output_rows": plan.total_output_rows,
        "total_group_copies": plan.total_copies,
        "nodes": [r.to_dict() for r in node_results],
        "note": "group_reads 由节点执行器在真实 read_row_group 调用时登记；"
                "同一节点同一 (文件,行组) 只读取一次，不同节点各自独立读取",
    }
    _write(os.path.join(out_dir, "join_run_record.json"), record)
    _write(os.path.join(out_dir, "join_verification.json"), verification)
    return {"plan": plan, "verification": verification, "record": record}
