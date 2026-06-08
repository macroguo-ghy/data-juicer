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
