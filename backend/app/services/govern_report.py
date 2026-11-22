"""治理报告领域服务：定期生成湖内治理报告和自动优化建议。

治理报告是跨模块数据流的最终汇聚点：汇总冷热分层迁移、小文件合并、
保留策略清理、质量监控结果和血缘链路状态，生成综合治理报告。

血缘部分读取血缘待验证数据明细（lineage_graph 每条记录的 `验证明细`），
生成按照实际下游产物核对的复查项。报告统计和血缘明细使用同一批已确认
的证据：生成开始时冻结一份明细快照，待验数量能沿明细逐条数出来。
"""
from __future__ import annotations

import copy
import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class GovernReportService(BaseModuleService):
    MODULE = "govern_report"
    ENTITY = "治理报告"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["报告周期", "治理指标", "优化建议", "执行状态", "审批状态"]
    REQUIRED_FIELDS = ["报告周期", "治理指标", "优化建议"]
    STATUS_ORDER = ["待生成", "已生成", "已审批", "已执行"]
    ACTION_RULES = {"生成报告": "已生成", "执行优化": "已执行"}
    NEGATIVE_ACTIONS = []

    def generate_report(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """生成治理报告：跨模块聚合数据生成综合治理报告。
        这是整个系统的最终数据产物消费入口。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "治理报告不存在"
        if entry.get("status") != "待生成":
            return None, f"治理报告状态为{entry.get('status')}，不可生成"

        # 1) 汇总冷热分层迁移
        tier_events = store.events_for_module("storage_tier")
        tier_migrations = [e for e in tier_events if e["event_type"] == "tier_migration_executed"]
        total_migrated = sum(
            e["payload"].get("migrated_partition_count", 0) for e in tier_migrations
        )

        # 2) 汇总小文件合并
        compaction_events = store.events_for_module("file_compaction")
        completed_compactions = [e for e in compaction_events if e["event_type"] == "compaction_completed"]

        # 3) 汇总保留策略清理
        retention_events = store.events_for_module("retention_mgr")
        completed_cleanups = [e for e in retention_events if e["event_type"] == "retention_cleanup_executed"]

        # 4) 汇总质量监控结果
        quality_events = store.events_for_module("quality_monitor")
        quality_failures = [e for e in quality_events if e["event_type"] == "quality_failed_to_lineage"]
        quality_passes = [e for e in quality_events if e["event_type"] == "quality_passed_to_govern"]

        # 5) 汇总血缘链路状态：读取血缘待验证数据明细。
        #    统计与复查项必须使用同一批已确认的证据——生成开始时冻结一份
        #    明细快照；下游数据迟到撞上本轮生成时，复查范围仍沿明细数得出来。
        lineage_rows = store.rows("lineage_graph")
        broken_lineages = [l for l in lineage_rows if l.get("status") == "链路断裂"]
        uncertain_lineages = [l for l in lineage_rows if l.get("status") == "待验证"]

        detail_snapshot: list[dict[str, Any]] = []
        for lin in lineage_rows:
            for det in lin.get("验证明细", []):
                detail_snapshot.append({
                    "血缘ID": lin.get("id"),
                    **copy.deepcopy(det),
                })

        pending_details = [d for d in detail_snapshot if d.get("状态") == "待验证"]
        confirmed_details = [d for d in detail_snapshot if d.get("状态") == "已确认"]

        # 复查项：按实际下游产物核对，值班员据此决定现在复查哪一段
        recheck_items = [
            {
                "血缘ID": d.get("血缘ID"),
                "明细ID": d.get("明细ID"),
                "复查段": d.get("段"),
                "下游分支": d.get("分支"),
                "路径": d.get("路径"),
                "结论": d.get("结论"),
                "实际下游产物数": d.get("观测产物数", 0),
                "证据批次": d.get("证据批次"),
                "复查建议": (
                    f"核对 {d.get('路径')} 的实际下游产物"
                    f"（当次可见 {d.get('观测产物数', 0)} 条，{d.get('结论')}）"
                ),
            }
            for d in pending_details
        ]
        evidence_batches = sorted({
            d.get("证据批次") for d in detail_snapshot if d.get("证据批次")
        })

        # 6) 汇总回填任务
        backfill_events = store.events_for_module("backfill_job")
        completed_backfills = [e for e in backfill_events if e["event_type"] == "backfill_completed_to_govern"]

        # 组装报告
        report_data = {
            "report_period": entry.get("报告周期", ""),
            "generated_at": datetime.datetime.now().isoformat(),
            "storage_governance": {
                "tier_migrations": len(tier_migrations),
                "total_partitions_migrated": total_migrated,
                "completed_compactions": len(completed_compactions),
                "completed_cleanups": len(completed_cleanups),
            },
            "quality_monitoring": {
                "quality_checks": len(quality_passes) + len(quality_failures),
                "failures": len(quality_failures),
                "passes": len(quality_passes),
            },
            "lineage_health": {
                "total_lineages": len(lineage_rows),
                "broken": len(broken_lineages),
                "uncertain": len(uncertain_lineages),
                "healthy": len(lineage_rows) - len(broken_lineages) - len(uncertain_lineages),
                "明细总数": len(detail_snapshot),
                "已确认段数": len(confirmed_details),
                "待验证段数": len(recheck_items),
                "证据批次": evidence_batches,
            },
            "血缘复查项": recheck_items,
            "backfill_summary": {
                "completed_backfills": len(completed_backfills),
            },
        }

        # 计算优化建议
        suggestions = []
        if len(broken_lineages) > 0:
            suggestions.append(f"修复 {len(broken_lineages)} 条断裂血缘链路")
        if recheck_items:
            suggestions.append(f"复查 {len(recheck_items)} 段血缘待验证明细（见血缘复查项）")
        if len(quality_failures) > 0:
            suggestions.append(f"处理 {len(quality_failures)} 个质量不达标指标")
        if total_migrated > 0:
            suggestions.append(f"冷热迁移有效，已迁移 {total_migrated} 个分区")
        if len(completed_compactions) > 0:
            suggestions.append(f"小文件合并完成 {len(completed_compactions)} 个任务")

        entry["status"] = "已生成"
        entry["pending"] = True
        entry["abnormal"] = False
        entry["优化建议"] = "；".join(suggestions) if suggestions else "当前治理状态良好"
        entry["执行状态"] = "报告已生成"
        entry["血缘复查项"] = recheck_items
        entry["治理数据"] = report_data

        store.log_event("govern_report_generated", "govern_report", "govern_report", {
            "report_id": entry_id,
            "report_data": report_data,
        })

        return entry, (
            f"治理报告已生成：迁移分区={total_migrated}，质量检查={len(quality_passes) + len(quality_failures)}，"
            f"血缘链路={len(lineage_rows)}，血缘待验证段={len(recheck_items)}，"
            f"回填任务={len(completed_backfills)}"
        )

    def execute_optimization(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """执行治理优化：根据报告建议触发实际的治理操作。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "治理报告不存在"
        if entry.get("status") not in ("已生成", "已审批"):
            return None, f"治理报告状态为{entry.get('status')}，不可执行优化"

        # 触发血缘链路修复
        broken_lineages = [
            l for l in store.rows("lineage_graph")
            if l.get("status") == "链路断裂"
        ]
        for lin in broken_lineages:
            lin["pending"] = True

        # 触发质量监控重新检查
        quality_rules = store.rows("quality_monitor")
        for qr in quality_rules:
            if qr.get("status") == "已启用":
                qr["pending"] = True

        entry["status"] = "已执行"
        entry["pending"] = False
        entry["abnormal"] = False
        entry["执行状态"] = "优化已执行"

        store.log_event("govern_optimization_executed", "govern_report", "govern_report", {
            "report_id": entry_id,
            "broken_lineages_triggered": len(broken_lineages),
            "quality_rules_retriggered": len(quality_rules),
        })

        return entry, f"治理优化已执行：触发 {len(broken_lineages)} 条血缘修复、{len(quality_rules)} 个质量规则"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "生成报告":
            return self.generate_report(entry_id)
        if action == "执行优化":
            return self.execute_optimization(entry_id)
        return super().run_action(entry_id, action)
