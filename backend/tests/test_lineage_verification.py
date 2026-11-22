"""血缘待验证明细与治理报告复查项的一致性测试。

场景：同一次刷新遇到两个下游分支——一个已经产出，另一个起初为空。
在空分支补齐前生成一份治理报告，再补齐重试刷新、生成第二份报告，
核对每份报告中的复查范围与可见的实际下游产物。
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.govern_report import GovernReportService
from app.services.lineage_graph import LineageGraphService
from app.store import store

READY_BRANCH = "verify_ready_branch"  # 已经有产出的下游分支
LATE_BRANCH = "verify_late_branch"    # 起初为空、之后补齐的下游分支


def _actual_count(module: str) -> int:
    """模块当前实际可见的产物数。"""
    return len(store.rows(module)) if module in set(store.module_names()) else 0


def _pending_details() -> list[dict]:
    """当前所有血缘记录里仍处于待验证的明细。"""
    return [
        det
        for lin in store.rows("lineage_graph")
        for det in lin.get("验证明细", [])
        if det.get("状态") == "待验证"
    ]


@pytest.fixture()
def lineage_entry():
    """登记一条双下游分支血缘：一个有产出，一个起初为空；用完清理。"""
    store.rows(READY_BRANCH)[:] = [
        {"id": 1, "status": "已产出"},
        {"id": 2, "status": "已产出"},
    ]
    store.rows(LATE_BRANCH)[:] = []
    svc = LineageGraphService()
    entry, missing = svc.create_entry({
        "上游源": "lake_table",
        "中间转换": "ingest_pipeline",
        "下游消费": f"{READY_BRANCH}、{LATE_BRANCH}",
        "血缘类型": "表级",
        "记录时间": "2026-11-22",
    })
    assert not missing
    yield entry
    store.rows("lineage_graph")[:] = [
        row for row in store.rows("lineage_graph") if row.get("id") != entry["id"]
    ]
    store._tables.pop(READY_BRANCH, None)
    store._tables.pop(LATE_BRANCH, None)


def _new_report() -> dict:
    svc = GovernReportService()
    entry, missing = svc.create_entry({
        "报告周期": "2026-11",
        "治理指标": "血缘待验证",
        "优化建议": "待生成",
        "执行状态": "",
        "审批状态": "",
    })
    assert not missing
    return entry


class TestEmptyDownstreamVerification:
    """空下游分支的分段校验明细。"""

    def test_segments_detail_missing_vs_recorded(self, lineage_entry):
        svc = LineageGraphService()
        entry, msg = svc.refresh_lineage(lineage_entry["id"])
        assert entry is not None, msg
        # 有一个分支当次没有产物，整条链路不能确认
        assert entry["status"] == "待验证"

        details = {d["明细ID"]: d for d in entry["验证明细"]}
        # 两段血缘：上游段一条，下游段按分支各一条
        assert set(details) == {
            "上游段::lake_table",
            f"下游段::{READY_BRANCH}",
            f"下游段::{LATE_BRANCH}",
        }

        upstream = details["上游段::lake_table"]
        assert upstream["结论"] == "已有记录"
        assert upstream["状态"] == "已确认"
        assert upstream["观测产物数"] == _actual_count("lake_table") > 0

        ready = details[f"下游段::{READY_BRANCH}"]
        assert ready["结论"] == "已有记录"
        assert ready["状态"] == "已确认"
        assert ready["观测产物数"] == _actual_count(READY_BRANCH) == 2

        late = details[f"下游段::{LATE_BRANCH}"]
        # 当次没有产物的路径不能当成已确认
        assert late["结论"] == "缺产物"
        assert late["状态"] == "待验证"
        assert late["观测产物数"] == 0
        assert late["实际看到范围"] == []

    def test_downstream_branch_splitting(self):
        svc = LineageGraphService()
        assert svc._split_branches("a、b") == ["a", "b"]
        assert svc._split_branches("a, b；c") == ["a", "b", "c"]
        assert svc._split_branches("a") == ["a"]
        assert svc._split_branches("a、a") == ["a"]


class TestReportRecheckScope:
    """治理报告复查项与可见实际下游产物的一致性。"""

    def test_recheck_items_traceable_to_details(self, lineage_entry):
        svc = LineageGraphService()
        svc.refresh_lineage(lineage_entry["id"])

        report = _new_report()
        generated, msg = GovernReportService().generate_report(report["id"])
        assert generated is not None, msg

        items = generated["血缘复查项"]
        pending = _pending_details()
        # 报告的待验数量能沿明细数出来
        assert len(items) == len(pending)
        health = generated["治理数据"]["lineage_health"]
        assert health["待验证段数"] == len(pending)

        # 复查范围按实际下游产物核对：空分支在列，有产出分支不在列
        ours = [i for i in items if i["血缘ID"] == lineage_entry["id"]]
        assert [i["下游分支"] for i in ours] == [LATE_BRANCH]
        assert ours[0]["实际下游产物数"] == 0 == _actual_count(LATE_BRANCH)
        assert ours[0]["结论"] == "缺产物"
        assert ours[0]["明细ID"] == f"下游段::{LATE_BRANCH}"

    def test_report_scope_tracks_visible_downstream_artifacts(self, lineage_entry):
        lin_svc = LineageGraphService()
        gov_svc = GovernReportService()
        lin_svc.refresh_lineage(lineage_entry["id"])

        # 空分支补齐前生成第一份报告
        report1 = _new_report()
        generated1, msg1 = gov_svc.generate_report(report1["id"])
        assert generated1 is not None, msg1
        scope1 = [i["下游分支"] for i in generated1["血缘复查项"]
                  if i["血缘ID"] == lineage_entry["id"]]
        assert scope1 == [LATE_BRANCH]

        # 下游数据补齐后重试刷新
        store.rows(LATE_BRANCH).append({"id": 1, "status": "已产出"})
        entry, msg = lin_svc.refresh_lineage(lineage_entry["id"])
        assert entry["status"] == "已建立"
        late = {d["明细ID"]: d for d in entry["验证明细"]}[f"下游段::{LATE_BRANCH}"]
        assert late["状态"] == "已确认"
        assert late["观测产物数"] == _actual_count(LATE_BRANCH) == 1
        # 重试保留前一次实际看到的范围
        assert late["前次观测产物数"] == 0
        assert late["前次看到范围"] == []

        # 第二份报告：该血缘没有复查项，待验数量仍沿明细数出来
        report2 = _new_report()
        generated2, msg2 = gov_svc.generate_report(report2["id"])
        assert generated2 is not None, msg2
        assert [i for i in generated2["血缘复查项"] if i["血缘ID"] == lineage_entry["id"]] == []
        assert len(generated2["血缘复查项"]) == len(_pending_details())

        # 第一份报告是当轮证据的冻结记录，复查范围不随后续刷新改变
        scope1_after = [i["下游分支"] for i in generated1["血缘复查项"]
                        if i["血缘ID"] == lineage_entry["id"]]
        assert scope1_after == [LATE_BRANCH]

    def test_report_and_details_share_same_confirmed_evidence(self, lineage_entry):
        svc = LineageGraphService()
        svc.refresh_lineage(lineage_entry["id"])
        report = _new_report()
        generated, msg = GovernReportService().generate_report(report["id"])
        assert generated is not None, msg

        details = [
            (lin["id"], det)
            for lin in store.rows("lineage_graph")
            for det in lin.get("验证明细", [])
        ]
        health = generated["治理数据"]["lineage_health"]
        items = generated["血缘复查项"]

        # 报告统计和血缘明细使用同一批已确认的证据
        assert health["明细总数"] == len(details)
        assert health["已确认段数"] + health["待验证段数"] == len(details)
        assert health["待验证段数"] == len(items) == len(_pending_details())
        assert set(health["证据批次"]) == {
            det.get("证据批次") for _, det in details if det.get("证据批次")
        }

        # 每个复查项都能沿明细ID数回血缘明细，产物数与批次一致
        by_key = {(lid, det["明细ID"]): det for lid, det in details}
        for item in items:
            src = by_key[(item["血缘ID"], item["明细ID"])]
            assert item["实际下游产物数"] == src["观测产物数"]
            assert item["证据批次"] == src["证据批次"]


class TestInterruptedVerification:
    """核验中断后的重试：保留前一次实际看到的范围。"""

    def test_interrupted_retry_keeps_previous_scope(self, lineage_entry, monkeypatch):
        svc = LineageGraphService()
        first, _ = svc.refresh_lineage(lineage_entry["id"])
        before = {d["明细ID"]: d for d in first["验证明细"]}[f"下游段::{READY_BRANCH}"]

        real_observe = LineageGraphService._observe_branch

        def boom(self, module):
            if module == READY_BRANCH:
                raise RuntimeError("下游存储暂时不可读")
            return real_observe(self, module)

        monkeypatch.setattr(LineageGraphService, "_observe_branch", boom)
        entry, _ = svc.refresh_lineage(lineage_entry["id"])
        kept = {d["明细ID"]: d for d in entry["验证明细"]}[f"下游段::{READY_BRANCH}"]
        # 核验中断：保留前一次实际看到的范围与结论
        assert kept["观测产物数"] == before["观测产物数"]
        assert kept["实际看到范围"] == before["实际看到范围"]
        assert kept["状态"] == "已确认"
        assert kept["证据批次"] == before["证据批次"]
        assert "中断" in kept["备注"]

        # 恢复后重试：重新产出当次证据
        monkeypatch.setattr(LineageGraphService, "_observe_branch", real_observe)
        again, _ = svc.refresh_lineage(lineage_entry["id"])
        renewed = {d["明细ID"]: d for d in again["验证明细"]}[f"下游段::{READY_BRANCH}"]
        assert renewed["观测产物数"] == _actual_count(READY_BRANCH)
        assert renewed["证据批次"] != before["证据批次"]

    def test_interrupted_first_verification_not_confirmed(self, monkeypatch):
        store.rows("verify_fresh_branch")[:] = []
        svc = LineageGraphService()
        entry, missing = svc.create_entry({
            "上游源": "lake_table",
            "中间转换": "ingest_pipeline",
            "下游消费": "verify_fresh_branch",
            "血缘类型": "表级",
            "记录时间": "2026-11-22",
        })
        assert not missing
        try:
            def boom(self, module):
                raise RuntimeError("存储不可读")

            monkeypatch.setattr(LineageGraphService, "_observe_branch", boom)
            got, _ = svc.refresh_lineage(entry["id"])
            details = {d["明细ID"]: d for d in got["验证明细"]}
            fresh = details["下游段::verify_fresh_branch"]
            # 没有历史范围可保留时，中断的路径只能待验证，不能当成已确认
            assert fresh["结论"] == "核验中断"
            assert fresh["状态"] == "待验证"
            assert got["status"] == "待验证"
        finally:
            store.rows("lineage_graph")[:] = [
                row for row in store.rows("lineage_graph") if row.get("id") != entry["id"]
            ]
            store._tables.pop("verify_fresh_branch", None)
