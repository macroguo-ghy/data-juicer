import json
from pathlib import Path


RATE_COUNTER_METRICS = {
    "ad.ai.data_forge.vlm.qps",
    "ad.ai.data_forge.rpc.qps",
    "ad.ai.data_forge.vlm.rate_limit.event",
    "ad.ai.data_forge.download.qps",
    "ad.ai.data_forge.dedup.rows",
}


def test_rate_counter_panels_do_not_compute_rate_twice():
    dashboard_path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "grafana"
        / "data_juicer_metrics_dashboard.json"
    )
    dashboard = json.loads(dashboard_path.read_text())

    offending_targets = []
    for panel in dashboard["panels"]:
        for target in panel.get("targets") or []:
            if target.get("metric") not in RATE_COUNTER_METRICS:
                continue
            if target.get("shouldComputeRate") is not False or "rateDownsampleType" in target:
                offending_targets.append(
                    {
                        "panel_id": panel["id"],
                        "panel_title": panel["title"],
                        "metric": target.get("metric"),
                        "ref_id": target.get("refId"),
                    }
                )

    assert offending_targets == []
