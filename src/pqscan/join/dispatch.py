"""精确分工：行上限约束下最小化“行组领取份数”的完整枚举。

规则：
- 一个非空行组对（任务）必须整个分给一个节点，不允许只做其中一部分重键配对；
- 节点承接多个任务时，输出行数累计不超过该节点的行上限；
- 同一节点处理多对任务而用到同一侧同一行组，只领一份；两个节点分别用到则各算一份；
- 目标：在所有任务都分派完的可行方案中，让全节点领取份数之和最小。

每批最多 3 个节点、10 个非空组对，分派空间至多 3^10=59049 种，直接完整枚举即可
得到精确最优解，不使用贪心近似。枚举按节点编号字典序进行，因此保留下来的首个
最优方案是所有最优方案中字典序最小、可复现的一个；同时统计最优方案总数。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from ..errors import PlanError
from .job import MAX_NODES, MAX_NON_EMPTY_PAIRS
from .manifest import JoinManifest, PairEntry


@dataclass
class NodeAssignment:
    node_index: int
    limit: int
    pair_ids: list[str] = field(default_factory=list)
    output_rows: int = 0
    claimed_groups: list[str] = field(default_factory=list)   # 去重后的 group_id

    @property
    def copies(self) -> int:
        return len(self.claimed_groups)


@dataclass
class DispatchPlan:
    feasible: bool
    reason: str
    limits: list[int]
    total_output_rows: int
    total_copies: int
    optimal_solution_count: int
    nodes: list[NodeAssignment]
    tasks: list[dict[str, object]]

    def to_dict(self) -> dict[str, object]:
        return {
            "feasible": self.feasible,
            "reason": self.reason,
            "objective": "在满足各节点行上限且所有非空组对整体分派的前提下，"
                         "最小化全节点行组领取份数（同一节点重复用同一侧同一行组只算一份）",
            "node_limits": self.limits,
            "total_output_rows": self.total_output_rows,
            "total_group_copies": self.total_copies,
            "optimal_solution_count": self.optimal_solution_count,
            "tasks": self.tasks,
            "nodes": [
                {
                    "node_index": n.node_index,
                    "limit": n.limit,
                    "assigned_pairs": n.pair_ids,
                    "output_rows": n.output_rows,
                    "claimed_group_copies": n.copies,
                    "claimed_groups": n.claimed_groups,
                }
                for n in self.nodes
            ],
        }


def _groups_of(pair: PairEntry) -> tuple[str, str]:
    return pair.left_group_id, pair.right_group_id


def _plan_cost(assignment: list[int], pairs: list[PairEntry], node_count: int) -> int:
    used: list[set[str]] = [set() for _ in range(node_count)]
    for node_index, pair in zip(assignment, pairs):
        left_group, right_group = _groups_of(pair)
        used[node_index].add(left_group)
        used[node_index].add(right_group)
    return sum(len(s) for s in used)


def dispatch(manifest: JoinManifest, limits: list[int]) -> DispatchPlan:
    if len(limits) > MAX_NODES:
        raise PlanError(f"每批最多 {MAX_NODES} 个节点，收到 {len(limits)} 个")
    if len(manifest.pairs) > MAX_NON_EMPTY_PAIRS:
        raise PlanError(f"非空组对超过每批上限 {MAX_NON_EMPTY_PAIRS}")

    pairs = manifest.pairs
    node_count = len(limits)
    task_rows = [p.total_rows for p in pairs]
    tasks_view = [
        {
            "pair_id": p.pair_id,
            "left_group_id": p.left_group_id,
            "right_group_id": p.right_group_id,
            "output_rows": p.total_rows,
        }
        for p in pairs
    ]
    total_rows = sum(task_rows)

    def empty_nodes() -> list[NodeAssignment]:
        return [NodeAssignment(node_index=i, limit=lim) for i, lim in enumerate(limits)]

    if not pairs:
        nodes = empty_nodes()
        return DispatchPlan(
            feasible=True, reason="清单中没有非空行组对，无需分派",
            limits=list(limits), total_output_rows=0, total_copies=0,
            optimal_solution_count=1, nodes=nodes, tasks=tasks_view,
        )

    # 单个任务超过最大节点上限：任何整体分派都不可能可行。
    biggest = max(task_rows)
    if biggest > max(limits):
        nodes = empty_nodes()
        return DispatchPlan(
            feasible=False,
            reason=f"组对 {pairs[task_rows.index(biggest)].pair_id} 单独就产出 {biggest} 行，"
                   f"超过最大节点行上限 {max(limits)}，任务不可拆分，整批无法分派",
            limits=list(limits), total_output_rows=total_rows, total_copies=0,
            optimal_solution_count=0, nodes=nodes, tasks=tasks_view,
        )

    used_rows = [0] * node_count
    assignment: list[int] = []
    best_cost: int | None = None
    best_assignment: list[int] | None = None
    best_count = 0

    def search(pos: int) -> None:
        nonlocal best_cost, best_assignment, best_count
        if pos == len(pairs):
            cost = _plan_cost(assignment, pairs, node_count)
            if best_cost is None or cost < best_cost:
                best_cost = cost
                best_assignment = list(assignment)
                best_count = 1
            elif cost == best_cost:
                best_count += 1
            return
        rows = task_rows[pos]
        for node_index in range(node_count):
            if used_rows[node_index] + rows <= limits[node_index]:
                used_rows[node_index] += rows
                assignment.append(node_index)
                search(pos + 1)
                assignment.pop()
                used_rows[node_index] -= rows

    search(0)

    nodes = empty_nodes()
    if best_assignment is None:
        return DispatchPlan(
            feasible=False,
            reason="枚举全部节点×任务组合后不存在满足行上限的完整分派（不允许省略重键配对）",
            limits=list(limits), total_output_rows=total_rows, total_copies=0,
            optimal_solution_count=0, nodes=nodes, tasks=tasks_view,
        )

    for pair, node_index in zip(pairs, best_assignment):
        node = nodes[node_index]
        node.pair_ids.append(pair.pair_id)
        node.output_rows += pair.total_rows
        for gid in _groups_of(pair):
            if gid not in node.claimed_groups:
                node.claimed_groups.append(gid)

    for node in nodes:
        node.claimed_groups.sort()

    return DispatchPlan(
        feasible=True,
        reason=f"完整枚举后最优领取份数为 {best_cost}（共 {best_count} 个最优方案，"
               f"输出字典序最小的一个）",
        limits=list(limits), total_output_rows=total_rows, total_copies=best_cost,
        optimal_solution_count=best_count, nodes=nodes, tasks=tasks_view,
    )
