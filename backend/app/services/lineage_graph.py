"""湖表血缘领域服务：维护数据从原始源到湖表再到消费端的完整血缘链路。

上游接收入湖管道和质量监控事件，下游联动治理报告。
支持血缘链路完整性检查和断裂修复。

血缘校验按「两段」执行：上游段（上游源 → 中间转换）与下游段
（中间转换 → 下游消费的每个分支）。每轮刷新把血缘待验证数据明细保留在
血缘记录的 `验证明细` 字段里：每段到底缺产物还是已有记录、实际看到的
产物范围、证据批次都落在明细上，治理报告据此生成复查项。

校验约定：
- 同一次刷新支持多个下游分支（逗号/顿号/分号分隔），逐分支出结论；
- 当次没有产物的路径不能当成已确认，只能留在待验证；
- 核验中断后重试时保留前一次实际看到的范围，不用本次的空观测覆盖。
"""
from __future__ import annotations

import datetime
import re
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 下游消费字段里多个分支的分隔符：中英文逗号、顿号、分号、斜杠、竖线、空白
_BRANCH_SPLIT = re.compile(r"[,，、;；/|\s]+")


class LineageGraphService(BaseModuleService):
    MODULE = "lineage_graph"
    ENTITY = "血缘关系"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["上游源", "中间转换", "下游消费", "血缘类型", "记录时间"]
    REQUIRED_FIELDS = ["上游源", "中间转换", "下游消费"]
    STATUS_ORDER = ["已建立", "待验证", "链路断裂"]
    ACTION_RULES = {"刷新血缘": "已建立", "标记断裂": "链路断裂"}
    NEGATIVE_ACTIONS = []

    # ---- 血缘校验 ----

    def refresh_lineage(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """刷新血缘链路：分段校验上下游产物，并保留血缘待验证数据明细。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "血缘关系不存在"

        upstream = str(entry.get("上游源", "") or "").strip()
        transform = str(entry.get("中间转换", "") or "").strip()
        downstream = str(entry.get("下游消费", "") or "").strip()
        branches = self._split_branches(downstream)

        # 每轮刷新是一个证据批次；重试时轮次递增，旧批次证据留在明细里
        round_no = int(entry.get("核验轮次", 0) or 0) + 1
        entry["核验轮次"] = round_no
        batch_id = f"L{entry_id}-R{round_no}"
        previous = {
            det["明细ID"]: det
            for det in entry.get("验证明细", [])
            if isinstance(det, dict) and det.get("明细ID")
        }

        details: list[dict[str, Any]] = [
            # 段一：上游段（上游源 → 中间转换）
            self._verify_segment(
                segment="上游段",
                branch=upstream,
                path=f"{upstream} → {transform}",
                previous=previous.get(f"上游段::{upstream}"),
                batch_id=batch_id,
                extra={"转换层产物数": self._safe_count(transform)},
            )
        ]
        # 段二：下游段（中间转换 → 每个下游分支），逐分支核验
        for branch in branches:
            details.append(self._verify_segment(
                segment="下游段",
                branch=branch,
                path=f"{transform} → {branch}",
                previous=previous.get(f"下游段::{branch}"),
                batch_id=batch_id,
            ))
        entry["验证明细"] = details

        upstream_detail = details[0]
        pending = [d for d in details if d["状态"] == "待验证"]

        if upstream_detail["结论"] == "缺产物":
            # 上游段当次没有产物：链路断裂
            entry["status"] = "链路断裂"
            entry["pending"] = True
            entry["abnormal"] = True
            store.log_event("lineage_broken", "lineage_graph", "govern_report", {
                "lineage_id": entry_id,
                "reason": f"上游段 {upstream} 缺产物",
                "证据批次": batch_id,
            })
            return entry, (
                f"血缘链路断裂：上游段 {upstream} 缺产物，"
                f"明细已保留在验证明细（批次 {batch_id}）"
            )

        if pending:
            # 存在缺产物或核验中断的段：当次不能确认
            entry["status"] = "待验证"
            entry["pending"] = True
            entry["abnormal"] = True
            missing = "、".join(d["分支"] or "（空分支）" for d in pending)
            store.log_event("lineage_uncertain", "lineage_graph", "ingest_pipeline", {
                "lineage_id": entry_id,
                "reason": f"{len(pending)} 段待验证：{missing}",
                "证据批次": batch_id,
            })
            return entry, (
                f"血缘链路待验证：{len(pending)} 段未确认（{missing}），"
                f"复查明细见验证明细（批次 {batch_id}）"
            )

        # 链路完整：所有段都有当次确认的产物
        entry["status"] = "已建立"
        entry["pending"] = False
        entry["abnormal"] = False

        # 联动治理报告记录血缘概览
        store.log_event("lineage_refreshed_to_govern", "lineage_graph", "govern_report", {
            "lineage_id": entry_id,
            "upstream": upstream,
            "transform": transform,
            "downstream": downstream,
            "upstream_count": upstream_detail["观测产物数"],
            "downstream_count": sum(d["观测产物数"] for d in details[1:]),
            "证据批次": batch_id,
        })

        return entry, (
            f"血缘链路已刷新：{upstream} → {transform} → {downstream}，"
            f"{len(details)} 段全部已确认（批次 {batch_id}）"
        )

    def _verify_segment(
        self,
        *,
        segment: str,
        branch: str,
        path: str,
        previous: dict[str, Any] | None,
        batch_id: str,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """核验一段血缘并产出明细。

        当次没有产物的路径不能当成已确认；核验中断时保留前一次实际
        看到的范围，不用本次的空观测覆盖。
        """
        detail_id = f"{segment}::{branch}"
        now = datetime.datetime.now().isoformat()
        try:
            seen = self._observe_branch(branch)
        except Exception as exc:  # 核验中断：防御性边界，保留前次范围
            if previous is not None:
                detail = dict(previous)
                detail["最近核验时间"] = now
                detail["备注"] = f"本次核验中断（{exc}），保留前次实际看到的范围"
                return detail
            detail = {
                "明细ID": detail_id,
                "段": segment,
                "分支": branch,
                "路径": path,
                "观测产物数": 0,
                "实际看到范围": [],
                "前次观测产物数": None,
                "前次看到范围": None,
                "结论": "核验中断",
                "状态": "待验证",
                "证据批次": batch_id,
                "最近核验时间": now,
                "备注": f"首次核验即中断（{exc}），没有可保留的历史范围",
            }
            if extra:
                detail.update(extra)
            return detail

        count = len(seen)
        confirmed = count > 0  # 当次没有产物的路径不能当成已确认
        detail = {
            "明细ID": detail_id,
            "段": segment,
            "分支": branch,
            "路径": path,
            "观测产物数": count,
            "实际看到范围": seen,
            "前次观测产物数": previous.get("观测产物数") if previous else None,
            "前次看到范围": previous.get("实际看到范围") if previous else None,
            "结论": "已有记录" if confirmed else "缺产物",
            "状态": "已确认" if confirmed else "待验证",
            "证据批次": batch_id,
            "最近核验时间": now,
            "备注": "" if confirmed else "当次没有产物，不能当成已确认",
        }
        if extra:
            detail.update(extra)
        return detail

    def _observe_branch(self, module: str) -> list[int]:
        """观测一个分支当前实际产物（记录 id 列表）。

        模块尚未注册或没有任何记录时返回空列表——当次没有产物。
        读取异常向上抛出，由核验流程按「核验中断」保留前次范围。
        """
        if not module or module not in set(store.module_names()):
            return []
        return [int(row.get("id", 0)) for row in store.rows(module)]

    def _safe_count(self, module: str) -> int:
        """观测产物数；读取失败按 0 处理（仅用于转换层上下文信息）。"""
        try:
            return len(self._observe_branch(module))
        except Exception:
            return 0

    @staticmethod
    def _split_branches(downstream: str) -> list[str]:
        """下游消费可能一次刷新多个分支：按分隔符拆开、去空、去重。"""
        branches = [b.strip() for b in _BRANCH_SPLIT.split(downstream or "") if b.strip()]
        if not branches:
            return [""]
        return list(dict.fromkeys(branches))

    def mark_broken(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """标记血缘链路断裂：通知下游模块。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "血缘关系不存在"

        entry["status"] = "链路断裂"
        entry["pending"] = False
        entry["abnormal"] = True

        store.log_event("lineage_marked_broken", "lineage_graph", "govern_report", {
            "lineage_id": entry_id,
            "downstream": entry.get("下游消费", ""),
        })

        return entry, f"血缘链路 {entry.get('上游源')} → {entry.get('下游消费')} 已标记为断裂"

    def lineage_for_table(self, table_name: str) -> list[dict[str, Any]]:
        """查询指定湖表的血缘链路。"""
        return [
            lin for lin in store.rows(self.MODULE)
            if table_name in (lin.get("上游源", ""), lin.get("中间转换", ""),
                              lin.get("下游消费", ""))
        ]

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "刷新血缘":
            return self.refresh_lineage(entry_id)
        if action == "标记断裂":
            return self.mark_broken(entry_id)
        return super().run_action(entry_id, action)
