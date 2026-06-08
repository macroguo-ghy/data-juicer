import base64
import importlib.util
import json
import pickle
from types import SimpleNamespace
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / ".agents" / "skills" / "ray-helper" / "scripts"


def load_script(name):
    path = SCRIPTS_DIR / name
    spec = importlib.util.spec_from_file_location(name.replace(".py", ""), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_ray_job_urls():
    ray_job_summary = load_script("ray_job_summary.py")

    history = (
        "https://ray-history-server.byted.org/#/new/history/"
        "j-paubxt82r1tu-cs2-hl-rabbit:20260604095130-bx3xbosl/jobs/03000000"
    )
    parsed = ray_job_summary.parse_ray_job_url(history)
    assert parsed.api_base == (
        "https://ray-history-server.byted.org/history/"
        "j-paubxt82r1tu-cs2-hl-rabbit:20260604095130-bx3xbosl"
    )
    assert parsed.job_id == "03000000"

    godel = (
        "https://godel-stream-applications.byted.org/rabbit-hl/"
        "j-paubxt82r1tu-jw1-batch-dashboard/#/jobs/03000000"
    )
    parsed = ray_job_summary.parse_ray_job_url(godel)
    assert parsed.api_base == (
        "https://godel-stream-applications.byted.org/rabbit-hl/"
        "j-paubxt82r1tu-jw1-batch-dashboard"
    )
    assert parsed.job_id == "03000000"

    api_url = (
        "https://ray-history-server.byted.org/history/"
        "j-paubxt82r1tu-cs2-hl-rabbit:20260604095130-bx3xbosl/api/jobs/03000001"
    )
    parsed = ray_job_summary.parse_ray_job_url(api_url, job_id="03000001")
    assert parsed.api_base.endswith(
        "/history/j-paubxt82r1tu-cs2-hl-rabbit:20260604095130-bx3xbosl"
    )

    godel_api_url = (
        "https://godel-stream-applications.byted.org/rabbit-hl/"
        "j-paubxt82r1tu-jw1-batch-dashboard/api/jobs/03000002"
    )
    parsed = ray_job_summary.parse_ray_job_url(godel_api_url, job_id="03000002")
    assert parsed.api_base == (
        "https://godel-stream-applications.byted.org/rabbit-hl/"
        "j-paubxt82r1tu-jw1-batch-dashboard"
    )

    with pytest.raises(ValueError, match="Unsupported Ray job URL"):
        ray_job_summary.parse_ray_job_url("https://example.com/jobs/03000000")

    with pytest.raises(ValueError, match="Cannot find job id"):
        ray_job_summary.parse_ray_job_url(
            "https://godel-stream-applications.byted.org/rabbit-hl/cluster/#/jobs"
        )


def test_job_config_and_dataset_summary_are_compact_and_redacted():
    ray_job_summary = load_script("ray_job_summary.py")

    config = """
project_name: token-test
executor_type: ray
ray_data_checkpoint:
  enabled: true
  dir: hdfs://checkpoint
  delete_no_checkpoint_files: true
dataset:
  configs:
    - type: remote
      source: hdfs
      path: hdfs://input
      format: parquet
      override_num_blocks: 1024
process:
  - video_url_rpc_mapper:
      batch_size: 50
      num_proc: 512
      ak: visible-ak
      sk: visible-sk
export:
  target: hdfs
  type: parquet
  path: hdfs://output
  mode: overwrite
  extra_args:
    min_rows_per_file: 1000
"""
    encoded = base64.b64encode(config.encode()).decode()
    cfg = ray_job_summary.decode_config_from_entrypoint(
        f"python tools/process_data_base64.py --config-base64 {encoded}"
    )
    summary = ray_job_summary.summarize_config(cfg)

    assert summary["project_name"] == "token-test"
    assert summary["process"][0]["operator"] == "video_url_rpc_mapper"
    assert summary["process"][0]["ak"] == "<redacted>"
    assert summary["process"][0]["sk"] == "<redacted>"
    assert summary["export"]["extra_args"]["min_rows_per_file"] == 1000

    dataset_payload = {
        "datasets": [
            {
                "dataset": "dataset_1",
                "state": "RUNNING",
                "context": {
                    "target_max_block_size": 134217728,
                    "data_checkpoint_dir": "hdfs://checkpoint",
                    "unrelated_large_field": "x" * 1000,
                },
                "operators": [
                    {
                        "operator": "MapBatches(foo)_1",
                        "state": "RUNNING",
                        "progress": 3,
                        "total": 10,
                        "total_rows": 42,
                        "queued_blocks": 2,
                        "ray_data_output_rows_dist": {"avg": 12, "count": 3},
                        "ray_data_output_bytes_dist": {"avg": 128, "count": 3},
                        "extra_metrics": {
                            "min_rows_per_bundle": 50,
                            "transform_fns": ["BuildOutputBlocksMapTransformFn"],
                        },
                    }
                ],
            }
        ]
    }
    dataset_summary = ray_job_summary.summarize_dataset_payload(dataset_payload)
    assert dataset_summary["datasets"][0]["context"] == {
        "target_max_block_size": 134217728,
        "data_checkpoint_dir": "hdfs://checkpoint",
    }
    assert dataset_summary["datasets"][0]["operators"][0]["avg_output_rows"] == 12
    assert "unrelated_large_field" not in dataset_summary["datasets"][0]["context"]

    assert ray_job_summary.decode_config_from_entrypoint("python tools/process_data.py") is None
    assert ray_job_summary.redact_value({"session_token": "abc"}) == {
        "session_token": "<redacted>"
    }
    assert ray_job_summary.redact_value(["x"] * 22)[-1] == "<truncated 2 items>"
    assert "<truncated" in ray_job_summary.redact_value("x" * 400)
    assert ray_job_summary.summarize_process_step("noop") == {"operator": "str"}
    export_summary = ray_job_summary.summarize_export(
        {"schema": {"fields": [{"name": "vid"}, {"name": "url"}]}}
    )
    assert export_summary["schema"] == {"field_count": 2, "field_names": ["vid", "url"]}


def test_ray_job_summary_build_fetch_and_cli(monkeypatch, capsys):
    ray_job_summary = load_script("ray_job_summary.py")

    config = "project_name: cli-test\nexecutor_type: ray\nprocess: []\n"
    encoded = base64.b64encode(config.encode()).decode()

    def fake_json_get(url, timeout=30):
        if url.endswith("/api/jobs/03000000"):
            return {
                "job_id": "03000000",
                "status": "RUNNING",
                "entrypoint": f"python tools/process_data_base64.py --config-base64={encoded}",
                "runtime_env": {"working_dir": "s3://work", "env_vars": {"sk": "secret"}},
                "metadata": {"alpha": 1},
            }
        if url.endswith("/api/data/datasets"):
            return {"data": {"datasets": [{"dataset": "dataset_1", "operators": []}]}}
        raise AssertionError(url)

    monkeypatch.setattr(ray_job_summary, "_json_get", fake_json_get)
    summary = ray_job_summary.fetch_summary(
        "https://ray-history-server.byted.org/#/new/history/cluster-a/jobs/03000000"
    )
    assert summary["job"]["runtime_env"]["env_vars"]["sk"] == "<redacted>"
    assert summary["config"]["project_name"] == "cli-test"
    assert summary["datasets"][0]["dataset"] == "dataset_1"
    assert "Ray Job Summary" in ray_job_summary.format_markdown(summary)

    assert (
        ray_job_summary.main(
            [
                "https://ray-history-server.byted.org/#/new/history/cluster-a/jobs/03000000",
                "--format",
                "json",
            ]
        )
        == 0
    )
    printed = json.loads(capsys.readouterr().out)
    assert printed["job"]["job_id"] == "03000000"


def test_ray_job_summary_resolves_archived_godel_url(monkeypatch):
    ray_job_summary = load_script("ray_job_summary.py")

    godel_url = (
        "https://godel-stream-applications.byted.org/rabbit-hl/"
        "j-paubxt82r1tu-jw1-batch-dashboard/#/jobs/03000000"
    )
    history_key = "j-paubxt82r1tu-jw1-hl-rabbit:20260608035326-nhwluy6u"

    def fake_json_get(url, timeout=30):
        if url == (
            "https://godel-stream-applications.byted.org/rabbit-hl/"
            "j-paubxt82r1tu-jw1-batch-dashboard/api/jobs/03000000"
        ):
            raise ValueError("HTML redirect")
        if url == (
            "https://ray-history-server.byted.org/v2/history/"
            "j-paubxt82r1tu-jw1-hl-rabbit/api/event_logs"
        ):
            return {
                "result": True,
                "data": {
                    "eventlogs": [
                        {"name": "j-paubxt82r1tu-jw1-hl-rabbit:old", "lastUpdate": 1},
                        {"name": history_key, "lastUpdate": 2},
                    ]
                },
            }
        if url.endswith("/history/j-paubxt82r1tu-jw1-hl-rabbit:old/api/jobs/03000000"):
            raise ValueError("Job 03000000 does not exist")
        if url.endswith(f"/history/{history_key}/api/jobs/03000000"):
            return {
                "job_id": "03000000",
                "status": "SUCCEEDED",
                "message": "Job finished successfully.",
                "entrypoint": "",
                "runtime_env": {},
                "metadata": {},
                "end_time": 1780896740264,
                "driver_exit_code": 0,
            }
        if url.endswith(f"/history/{history_key}/api/data/datasets"):
            return {"datasets": [{"dataset": "dataset_12_0", "state": "FINISHED", "operators": []}]}
        raise AssertionError(url)

    monkeypatch.setattr(ray_job_summary, "_json_get", fake_json_get)

    summary = ray_job_summary.fetch_summary(godel_url)

    assert summary["job"]["status"] == "SUCCEEDED"
    assert summary["job"]["driver_exit_code"] == 0
    assert summary["datasets"][0]["state"] == "FINISHED"


def test_ray_job_summary_prefers_history_when_godel_live_status_is_stale(monkeypatch):
    ray_job_summary = load_script("ray_job_summary.py")

    godel_url = (
        "https://godel-stream-applications.byted.org/rabbit-hl/"
        "j-paubxt82r1tu-jw1-batch-dashboard/#/jobs/03000000"
    )
    history_key = "j-paubxt82r1tu-jw1-hl-rabbit:20260608035326-nhwluy6u"
    calls = []

    def fake_json_get(url, timeout=30):
        calls.append(url)
        if url == (
            "https://godel-stream-applications.byted.org/rabbit-hl/"
            "j-paubxt82r1tu-jw1-batch-dashboard/api/jobs/03000000"
        ):
            return {
                "job_id": "03000000",
                "status": "RUNNING",
                "message": "Job is currently running.",
                "entrypoint": "",
                "runtime_env": {},
                "metadata": {},
                "end_time": None,
                "driver_exit_code": None,
            }
        if url == (
            "https://godel-stream-applications.byted.org/rabbit-hl/"
            "j-paubxt82r1tu-jw1-batch-dashboard/api/data/datasets"
        ):
            return {"datasets": [{"dataset": "dataset_12_0", "state": "FINISHED", "operators": []}]}
        if url == (
            "https://ray-history-server.byted.org/v2/history/"
            "j-paubxt82r1tu-jw1-hl-rabbit/api/event_logs"
        ):
            return {"result": True, "data": {"eventlogs": [{"name": history_key, "lastUpdate": 2}]}}
        if url.endswith(f"/history/{history_key}/api/jobs/03000000"):
            return {
                "job_id": "03000000",
                "status": "SUCCEEDED",
                "message": "Job finished successfully.",
                "entrypoint": "",
                "runtime_env": {},
                "metadata": {},
                "end_time": 1780896740264,
                "driver_exit_code": 0,
            }
        if url.endswith(f"/history/{history_key}/api/data/datasets"):
            return {"datasets": [{"dataset": "dataset_12_0", "state": "FINISHED", "operators": []}]}
        raise AssertionError(url)

    monkeypatch.setattr(ray_job_summary, "_json_get", fake_json_get)

    summary = ray_job_summary.fetch_summary(godel_url)

    assert summary["job"]["status"] == "SUCCEEDED"
    assert any("/v2/history/j-paubxt82r1tu-jw1-hl-rabbit/api/event_logs" in call for call in calls)


def test_ray_job_summary_keeps_active_godel_live_status(monkeypatch):
    ray_job_summary = load_script("ray_job_summary.py")

    godel_url = (
        "https://godel-stream-applications.byted.org/rabbit-hl/"
        "j-paubxt82r1tu-jw1-batch-dashboard/#/jobs/03000000"
    )

    def fake_json_get(url, timeout=30):
        if url.endswith("/api/jobs/03000000"):
            return {
                "job_id": "03000000",
                "status": "RUNNING",
                "entrypoint": "",
                "runtime_env": {},
                "metadata": {},
            }
        if url.endswith("/api/data/datasets"):
            return {"datasets": [{"dataset": "dataset_12_0", "state": "RUNNING", "operators": []}]}
        raise AssertionError(url)

    monkeypatch.setattr(ray_job_summary, "_json_get", fake_json_get)

    summary = ray_job_summary.fetch_summary(godel_url)

    assert summary["job"]["status"] == "RUNNING"
    assert summary["datasets"][0]["state"] == "RUNNING"


def test_ray_job_summary_keeps_godel_live_summary_when_history_lookup_fails(monkeypatch):
    ray_job_summary = load_script("ray_job_summary.py")

    godel_url = (
        "https://godel-stream-applications.byted.org/rabbit-hl/"
        "j-paubxt82r1tu-jw1-batch-dashboard/#/jobs/03000000"
    )

    def fake_json_get(url, timeout=30):
        if url == (
            "https://godel-stream-applications.byted.org/rabbit-hl/"
            "j-paubxt82r1tu-jw1-batch-dashboard/api/jobs/03000000"
        ):
            return {
                "job_id": "03000000",
                "status": "RUNNING",
                "entrypoint": "",
                "runtime_env": {},
                "metadata": {},
            }
        if url == (
            "https://godel-stream-applications.byted.org/rabbit-hl/"
            "j-paubxt82r1tu-jw1-batch-dashboard/api/data/datasets"
        ):
            return {"datasets": [{"dataset": "dataset_12_0", "state": "FINISHED", "operators": []}]}
        if url.endswith("/api/event_logs"):
            raise ValueError("history temporarily unavailable")
        raise AssertionError(url)

    monkeypatch.setattr(ray_job_summary, "_json_get", fake_json_get)

    summary = ray_job_summary.fetch_summary(godel_url)

    assert summary["job"]["status"] == "RUNNING"
    assert summary["datasets"][0]["state"] == "FINISHED"


def test_compare_jobs_reports_config_and_operator_differences():
    ray_compare_jobs = load_script("ray_compare_jobs.py")

    old = {
        "job": {"status": "SUCCEEDED"},
        "config": {"ray_data_checkpoint": {"enabled": True}},
        "datasets": [
            {
                "dataset": "dataset_1",
                "context": {"data_checkpoint_dir": "hdfs://checkpoint"},
                "operators": [
                    {
                        "operator": "MapBatches(download)_5",
                        "total": 1100,
                        "avg_output_rows": 756,
                        "avg_output_bytes": 8600000000,
                    }
                ],
            }
        ],
    }
    current = {
        "job": {"status": "RUNNING"},
        "config": {"ray_data_checkpoint": None},
        "datasets": [
            {
                "dataset": "dataset_1",
                "context": {"data_checkpoint_dir": ""},
                "operators": [
                    {
                        "operator": "MapBatches(download)_5",
                        "total": 62136,
                        "avg_output_rows": 11,
                        "avg_output_bytes": 130000000,
                    }
                ],
            }
        ],
    }

    comparison = ray_compare_jobs.build_comparison(old, current)
    assert {
        "path": "ray_data_checkpoint",
        "left": {"enabled": True},
        "right": None,
    } in comparison["config_diffs"]
    assert comparison["operator_diffs"][0]["left_total"] == 1100
    assert comparison["operator_diffs"][0]["right_total"] == 62136
    assert comparison["context_diffs"][0]["path"] == "data_checkpoint_dir"
    assert "Ray Job Comparison" in ray_compare_jobs.format_markdown(comparison)


def test_compare_jobs_file_cli_and_edge_cases(tmp_path, monkeypatch, capsys):
    ray_compare_jobs = load_script("ray_compare_jobs.py")

    left = {
        "job": {"job_id": "left", "status": "SUCCEEDED"},
        "config": {"process": [{"mapper": {"a": 1}}]},
        "datasets": [{"job_id": "left-ds", "operators": [{"name": "Map(foo)_1"}]}],
    }
    right = {
        "job": {"job_id": "right", "status": "RUNNING"},
        "config": {"process": [{"mapper": {"a": 2}}]},
        "datasets": [
            "ignored",
            {
                "dataset": "right-ds",
                "operators": [
                    "ignored",
                    {"name": "Map(foo)_2", "total": 10, "avg_output_rows": 5},
                ],
            },
        ],
    }
    left_file = tmp_path / "left.json"
    right_file = tmp_path / "right.json"
    left_file.write_text(json.dumps(left), encoding="utf-8")
    right_file.write_text(json.dumps(right), encoding="utf-8")

    comparison = ray_compare_jobs.build_comparison(left, right)
    assert comparison["operator_diffs"]
    assert ray_compare_jobs.normalize_operator_name("Map(foo)_123") == "Map(foo)"
    assert ray_compare_jobs._diff_values(1, 2, max_diffs=0) == []
    assert ray_compare_jobs._diff_values([1], [1]) == []
    assert ray_compare_jobs._jsonable({"x": {2, 1}}) == {"x": [1, 2]}
    assert ray_compare_jobs._summary_from_arg(str(left_file), timeout=1)["job"]["job_id"] == "left"

    monkeypatch.setattr(
        ray_compare_jobs.ray_job_summary,
        "fetch_summary",
        lambda value, timeout=30: {"job": {"job_id": value}, "config": {}, "datasets": []},
    )
    assert ray_compare_jobs._summary_from_arg("https://job", timeout=1)["job"]["job_id"] == "https://job"

    assert ray_compare_jobs.main([str(left_file), str(right_file), "--format", "json"]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed["left_job"]["job_id"] == "left"

    assert ray_compare_jobs.main([str(left_file), str(right_file)]) == 0
    assert "Ray Job Comparison" in capsys.readouterr().out


def test_checkpoint_metadata_summary_classifies_completed_file_paths():
    hdfs_checkpoint_summary = load_script("hdfs_checkpoint_summary.py")

    metadata = {
        "version": "2.0",
        "job_id": {"job-a"},
        "checkpoint_id": "ckpt",
        "completed_files": [
            "/ad_base/output",
            "/ad_base/output",
        ],
        "global_progress": [{"progress": 2, "total": 2}],
    }
    raw = pickle.dumps(metadata)
    summary = hdfs_checkpoint_summary.summarize_checkpoint_metadata_bytes(raw)

    assert summary["completed_files_count"] == 2
    assert summary["completed_files_kind"] == "directory_like"
    assert summary["completed_files_sample"] == ["/ad_base/output"]
    assert summary["job_id"] == ["job-a"]

    file_summary = hdfs_checkpoint_summary.summarize_checkpoint_metadata(
        {"completed_files": ["/tmp/part-000.parquet", "/tmp/output.jsonl"]}
    )
    assert file_summary["completed_files_kind"] == "file_like"
    mixed_summary = hdfs_checkpoint_summary.summarize_checkpoint_metadata(
        {"completed_files": ["/tmp/output", "/tmp/part-000.parquet"]}
    )
    assert mixed_summary["completed_files_kind"] == "mixed"
    empty_summary = hdfs_checkpoint_summary.summarize_checkpoint_metadata({"completed_files": []})
    assert empty_summary["completed_files_kind"] == "empty"
    assert "HDFS Checkpoint Summary" in hdfs_checkpoint_summary.format_markdown(summary)

    with pytest.raises(TypeError, match="must be a dict"):
        hdfs_checkpoint_summary.summarize_checkpoint_metadata_bytes(pickle.dumps(["bad"]))


def test_hdfs_checkpoint_response_readers_and_cli(tmp_path, monkeypatch, capsys):
    hdfs_checkpoint_summary = load_script("hdfs_checkpoint_summary.py")

    metadata = {"job_id": {"job-b"}, "completed_files": ["/tmp/part-000.parquet"]}
    raw = pickle.dumps(metadata)
    response_file = tmp_path / "response.json"
    response_file.write_text(
        json.dumps(
            {
                "data": {
                    "resp_body_json": {
                        "output": base64.b64encode(raw).decode(),
                        "output_encoding": "base64",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    assert hdfs_checkpoint_summary._load_raw_from_response(str(response_file)) == raw

    text_response_file = tmp_path / "text_response.json"
    text_response_file.write_text(
        json.dumps({"output": raw.decode("latin1"), "encoding": "text"}),
        encoding="utf-8",
    )
    assert hdfs_checkpoint_summary._load_raw_from_response(str(text_response_file)) == raw

    bad_response_file = tmp_path / "bad_response.json"
    bad_response_file.write_text(json.dumps({"output": 1}), encoding="utf-8")
    with pytest.raises(ValueError, match="Cannot find string output"):
        hdfs_checkpoint_summary._load_raw_from_response(str(bad_response_file))

    metadata_file = tmp_path / "metadata.pickle"
    metadata_file.write_bytes(raw)
    assert (
        hdfs_checkpoint_summary.main(
            ["--metadata-file", str(metadata_file), "--format", "json", "--sample-size", "1"]
        )
        == 0
    )
    printed = json.loads(capsys.readouterr().out)
    assert printed["job_id"] == ["job-b"]

    assert hdfs_checkpoint_summary.main(["--metadata-response", str(response_file)]) == 0
    assert "HDFS Checkpoint Summary" in capsys.readouterr().out

    args = SimpleNamespace(
        metadata_file=str(metadata_file),
        metadata_response=None,
        checkpoint_dir=None,
        username=None,
        user_email=None,
        env="ppe_terranova",
        idc="hl",
        zone="CN",
        cluster="default",
    )
    assert hdfs_checkpoint_summary._read_raw(args) == raw

    args.metadata_file = None
    args.metadata_response = str(response_file)
    assert hdfs_checkpoint_summary._read_raw(args) == raw

    args.metadata_response = None
    args.checkpoint_dir = "hdfs://haruna/checkpoint"
    args.username = ""
    args.user_email = ""
    monkeypatch.delenv("USER", raising=False)
    monkeypatch.delenv("BYTE_USER_EMAIL", raising=False)
    with pytest.raises(ValueError, match="requires --username"):
        hdfs_checkpoint_summary._read_raw(args)

    args.username = "user"
    args.user_email = "user@example.com"
    monkeypatch.setattr(
        hdfs_checkpoint_summary,
        "_fetch_checkpoint_metadata_with_bytedcli",
        lambda checkpoint_dir, **kwargs: raw,
    )
    assert hdfs_checkpoint_summary._read_raw(args) == raw


def test_hdfs_checkpoint_bytedcli_command_shape(monkeypatch):
    hdfs_checkpoint_summary = load_script("hdfs_checkpoint_summary.py")
    raw = pickle.dumps({"completed_files": []})

    def fake_run(command, check, capture_output, text):
        assert command[:5] == ["bytedcli", "--json", "bits", "rpc-call", "ad.ai.data_forge"]
        body = json.loads(command[command.index("--body") + 1])
        assert body["command_line"] == "hdfs dfs -cat hdfs://haruna/ckpt/_metadata"
        return SimpleNamespace(
            stdout=json.dumps(
                {
                    "data": {
                        "resp_body_json": {
                            "output": base64.b64encode(raw).decode(),
                            "output_encoding": "base64",
                        }
                    }
                }
            )
        )

    monkeypatch.setattr(hdfs_checkpoint_summary.subprocess, "run", fake_run)
    assert hdfs_checkpoint_summary._fetch_checkpoint_metadata_with_bytedcli(
        "hdfs://haruna/ckpt/",
        username="u",
        user_email="u@example.com",
        env="ppe_terranova",
        idc="hl",
        zone="CN",
        cluster="default",
    ) == raw

    def fake_run_without_output(command, check, capture_output, text):
        return SimpleNamespace(stdout=json.dumps({"data": {"resp_body_json": {}}}))

    monkeypatch.setattr(hdfs_checkpoint_summary.subprocess, "run", fake_run_without_output)
    with pytest.raises(ValueError, match="did not contain output"):
        hdfs_checkpoint_summary._fetch_checkpoint_metadata_with_bytedcli(
            "hdfs://haruna/ckpt",
            username="u",
            user_email="u@example.com",
            env="ppe_terranova",
            idc="hl",
            zone="CN",
            cluster="default",
        )


def test_hdfs_ls_summary_parses_response_and_compares_growth(tmp_path, capsys):
    hdfs_ls_summary = load_script("hdfs_ls_summary.py")

    first_output = """Found 2 items
-rw-r--r--   3 user supergroup        100 2026-06-08 16:01 hdfs://haruna/out/part-000.parquet
-rw-r--r--   3 user supergroup        200 2026-06-08 16:02 hdfs://haruna/out/part-001.parquet
"""
    second_output = """Found 2 items
-rw-r--r--   3 user supergroup        150 2026-06-08 16:01 hdfs://haruna/out/part-000.parquet
-rw-r--r--   3 user supergroup        200 2026-06-08 16:02 hdfs://haruna/out/part-001.parquet
"""
    first_file = tmp_path / "first.json"
    second_file = tmp_path / "second.json"
    first_file.write_text(
        json.dumps(
            {
                "data": {
                    "resp_body_json": {
                        "status_code": 0,
                        "command": "hdfs dfs -ls hdfs://haruna/out",
                        "output": first_output,
                        "output_encoding": "utf-8",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    second_file.write_text(
        json.dumps({"data": {"resp_body_json": {"status_code": 0, "output": second_output}}}),
        encoding="utf-8",
    )

    summary = hdfs_ls_summary.load_summary_from_response(str(first_file))
    assert summary["file_count"] == 2
    assert summary["total_bytes"] == 300
    assert summary["latest_mtime"] == "2026-06-08 16:02"
    assert summary["latest_path"] == "hdfs://haruna/out/part-001.parquet"

    comparison = hdfs_ls_summary.compare_summaries(
        summary,
        hdfs_ls_summary.load_summary_from_response(str(second_file)),
    )
    assert comparison["delta_file_count"] == 0
    assert comparison["delta_total_bytes"] == 50
    assert comparison["changed_files"] == [
        {
            "path": "hdfs://haruna/out/part-000.parquet",
            "old_size": 100,
            "new_size": 150,
            "delta_bytes": 50,
            "old_mtime": "2026-06-08 16:01",
            "new_mtime": "2026-06-08 16:01",
        }
    ]

    assert (
        hdfs_ls_summary.main(
            ["--compare-response", str(first_file), str(second_file), "--format", "json"]
        )
        == 0
    )
    printed = json.loads(capsys.readouterr().out)
    assert printed["comparison"]["delta_total_bytes"] == 50

    assert hdfs_ls_summary.main(["--response", str(first_file)]) == 0
    assert "HDFS LS Summary" in capsys.readouterr().out


def test_hdfs_ls_summary_bytedcli_command_shape(monkeypatch):
    hdfs_ls_summary = load_script("hdfs_ls_summary.py")

    def fake_run(command, check, capture_output, text):
        assert command[:5] == ["bytedcli", "--json", "bits", "rpc-call", "ad.ai.data_forge"]
        body = json.loads(command[command.index("--body") + 1])
        assert body["command_line"] == "hdfs dfs -ls hdfs://haruna/out"
        assert body["user_context"]["username"] == "u"
        return SimpleNamespace(
            stdout=json.dumps(
                {
                    "data": {
                        "resp_body_json": {
                            "status_code": 0,
                            "output": (
                                "Found 1 items\n"
                                "-rw-r--r--   3 user supergroup 42 2026-06-08 16:01 "
                                "hdfs://haruna/out/part.parquet\n"
                            ),
                        }
                    }
                }
            )
        )

    monkeypatch.setattr(hdfs_ls_summary.subprocess, "run", fake_run)
    summary = hdfs_ls_summary.fetch_summary_with_bytedcli(
        "hdfs://haruna/out",
        username="u",
        user_email="u@example.com",
        env="ppe_terranova",
        idc="hl",
        zone="CN",
        cluster="default",
    )
    assert summary["file_count"] == 1
    assert summary["total_bytes"] == 42

    def fake_run_failed(command, check, capture_output, text):
        return SimpleNamespace(
            stdout=json.dumps(
                {"data": {"resp_body_json": {"status_code": 1, "status_message": "missing"}}}
            )
        )

    monkeypatch.setattr(hdfs_ls_summary.subprocess, "run", fake_run_failed)
    with pytest.raises(ValueError, match="status_code=1"):
        hdfs_ls_summary.fetch_summary_with_bytedcli(
            "hdfs://haruna/out",
            username="u",
            user_email="u@example.com",
            env="ppe_terranova",
            idc="hl",
            zone="CN",
            cluster="default",
        )

    def fake_run_without_output(command, check, capture_output, text):
        return SimpleNamespace(stdout=json.dumps({"data": {"resp_body_json": {}}}))

    monkeypatch.setattr(hdfs_ls_summary.subprocess, "run", fake_run_without_output)
    with pytest.raises(ValueError, match="did not contain output"):
        hdfs_ls_summary.fetch_summary_with_bytedcli(
            "hdfs://haruna/out",
            username="u",
            user_email="u@example.com",
            env="ppe_terranova",
            idc="hl",
            zone="CN",
            cluster="default",
        )


def test_hdfs_ls_summary_error_paths_and_live_sampling(tmp_path, monkeypatch, capsys):
    hdfs_ls_summary = load_script("hdfs_ls_summary.py")

    string_body_file = tmp_path / "string_body.json"
    string_body_file.write_text(
        json.dumps(
            {
                "data": {
                    "resp_body_json": json.dumps(
                        {
                            "status_code": 0,
                            "output": (
                                "Found 2 items\n"
                                "drwxr-xr-x   - user supergroup 0 2026-06-08 15:00 "
                                "hdfs://haruna/out/dir\n"
                                "-rw-r--r--   3 user supergroup 1024 2026-06-08 16:00 "
                                "hdfs://haruna/out/file.parquet\n"
                            ),
                        }
                    )
                }
            }
        ),
        encoding="utf-8",
    )
    summary = hdfs_ls_summary.load_summary_from_response(str(string_body_file))
    assert summary["file_count"] == 1
    assert summary["dir_count"] == 1
    assert "1.0KiB" in hdfs_ls_summary.format_summary_markdown(summary)

    bad_body_file = tmp_path / "bad_body.json"
    bad_body_file.write_text(json.dumps({"data": {"resp_body_json": 1}}), encoding="utf-8")
    with pytest.raises(ValueError, match="must be a JSON object"):
        hdfs_ls_summary.load_summary_from_response(str(bad_body_file))

    failed_file = tmp_path / "failed.json"
    failed_file.write_text(
        json.dumps({"data": {"resp_body_json": {"status_code": 1, "status_message": "denied"}}}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="status_code=1"):
        hdfs_ls_summary.load_summary_from_response(str(failed_file))

    missing_output_file = tmp_path / "missing_output.json"
    missing_output_file.write_text(json.dumps({"data": {"resp_body_json": {}}}), encoding="utf-8")
    with pytest.raises(ValueError, match="Cannot find string output"):
        hdfs_ls_summary.load_summary_from_response(str(missing_output_file))

    assert hdfs_ls_summary.summarize_ls_output("bad\n-rw bad\n-rw x y z not-int a b c\n")[
        "file_count"
    ] == 0

    samples = [
        {
            "file_count": 1,
            "dir_count": 0,
            "total_bytes": 10,
            "latest_mtime": "2026-06-08 16:00",
            "latest_path": "hdfs://haruna/out/file.parquet",
            "_files": {"hdfs://haruna/out/file.parquet": {"size": 10, "mtime": "2026-06-08 16:00"}},
        },
        {
            "file_count": 2,
            "dir_count": 0,
            "total_bytes": 30,
            "latest_mtime": "2026-06-08 16:01",
            "latest_path": "hdfs://haruna/out/new.parquet",
            "_files": {
                "hdfs://haruna/out/file.parquet": {"size": 15, "mtime": "2026-06-08 16:00"},
                "hdfs://haruna/out/new.parquet": {"size": 15, "mtime": "2026-06-08 16:01"},
            },
        },
    ]
    monkeypatch.setattr(
        hdfs_ls_summary,
        "fetch_summary_with_bytedcli",
        lambda *args, **kwargs: samples.pop(0),
    )
    monkeypatch.setattr(hdfs_ls_summary.time, "sleep", lambda interval: None)
    args = SimpleNamespace(
        response=None,
        path="hdfs://haruna/out",
        username="u",
        user_email="u@example.com",
        env="ppe_terranova",
        idc="hl",
        zone="CN",
        cluster="default",
        samples=2,
        interval=60,
        sample_size=10,
    )
    live = hdfs_ls_summary._read_summary(args)
    assert live["comparison"]["delta_file_count"] == 1
    assert live["comparison"]["delta_total_bytes"] == 20
    assert "HDFS LS Comparison" in hdfs_ls_summary.format_comparison_markdown(live["comparison"])

    args.samples = 1
    args.username = ""
    args.user_email = ""
    monkeypatch.delenv("USER", raising=False)
    monkeypatch.delenv("BYTE_USER_EMAIL", raising=False)
    with pytest.raises(ValueError, match="requires --username"):
        hdfs_ls_summary._read_summary(args)

    with pytest.raises(ValueError, match="samples must be"):
        hdfs_ls_summary.main(["--response", str(string_body_file), "--samples", "0"])

    assert hdfs_ls_summary.main(["--response", str(string_body_file), "--format", "json"]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert "_files" not in printed
