from pathlib import Path
from urllib.request import urlopen
from unittest.mock import patch
from uuid import uuid4

import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import yaml


WEBHDFS_STATUS_URL = "http://localhost:9870/webhdfs/v1/?op=GETFILESTATUS&user.name=root"


def _require_local_webhdfs():
    try:
        with urlopen(WEBHDFS_STATUS_URL, timeout=2) as response:
            if response.status != 200:
                pytest.skip(f"local WebHDFS returned status {response.status}")
    except Exception as exc:
        pytest.skip(f"local dj-arm-hdfs WebHDFS is unavailable: {exc}")


def _webhdfs_filesystem():
    fsspec = pytest.importorskip("fsspec")
    pytest.importorskip("pyarrow")
    return fsspec.filesystem("webhdfs", host="localhost", port=9870, user="root")


def _write_stub_tqs_result(output_uri: str, table: pa.Table) -> None:
    fs = _webhdfs_filesystem()
    hdfs_path = "/" + output_uri.split("/", 3)[3].strip("/")
    fs.rm(hdfs_path, recursive=True, missing_ok=True)
    fs.makedirs(hdfs_path, exist_ok=True)

    local_part = Path("/tmp") / f"dj_tqs_materialized_remote_{uuid4().hex}.parquet"
    try:
        pq.write_table(table, local_part)
        fs.put(str(local_part), f"{hdfs_path}/part-00000.parquet")
    finally:
        local_part.unlink(missing_ok=True)


def test_tqs_materialized_remote_local_ray_hdfs_to_parquet_export(tmp_path):
    _require_local_webhdfs()
    pytest.importorskip("ray")

    hdfs_root = f"hdfs://localhost:9000/tmp/dj_tqs_materialized_remote_{uuid4().hex}"
    export_path = tmp_path / "dj_output"
    work_dir = tmp_path / "work"
    config_path = tmp_path / "config.yaml"

    config = {
        "project_name": "tqs_materialized_remote_local",
        "executor_type": "ray",
        "work_dir": str(work_dir),
        "dataset": {
            "configs": [
                {
                    "type": "remote",
                    "source": "tqs",
                    "read_mode": "materialized_remote",
                    "query": (
                        "WITH left_t AS (SELECT 1 AS id, 'hello' AS text), "
                        "right_t AS (SELECT 1 AS id, 'joined' AS label) "
                        "SELECT left_t.id, left_t.text, right_t.label "
                        "FROM left_t JOIN right_t ON left_t.id = right_t.id"
                    ),
                    "output_uri": hdfs_root,
                    "tqs_app_id": "dummy-app-id",
                    "tqs_app_key": "dummy-app-key",
                    "user_name": "dummy-user",
                    "filesystem": "webhdfs",
                    "webhdfs": {"host": "localhost", "port": 9870, "user": "root"},
                    "override_num_blocks": 1,
                    "skip_zero_row_group_files": True,
                }
            ]
        },
        "process": [],
        "export": {
            "target": "local",
            "path": str(export_path),
            "type": "parquet",
            "extra_args": {"min_rows_per_file": 1},
        },
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    result_table = pa.table(
        {
            "id": pa.array([1], type=pa.int64()),
            "text": pa.array(["hello"], type=pa.string()),
            "label": pa.array(["joined"], type=pa.string()),
        }
    )

    def stub_run_tqs_query(query, output_uri, **kwargs):
        assert "JOIN" in query
        assert output_uri == hdfs_root
        _write_stub_tqs_result(output_uri, result_table)
        return output_uri

    try:
        from tools.process_data import run

        with patch("data_juicer.core.data.load_strategy.run_tqs_query", side_effect=stub_run_tqs_query):
            run(["--config", str(config_path), "--ray_address", "local"])

        exported = pq.read_table(export_path)
        assert exported.num_rows == 1
        assert exported.column_names == ["id", "text", "label"]
        assert exported.to_pydict() == {
            "id": [1],
            "text": ["hello"],
            "label": ["joined"],
        }
    finally:
        try:
            _webhdfs_filesystem().rm("/" + hdfs_root.split("/", 3)[3].strip("/"), recursive=True, missing_ok=True)
        except Exception:
            pass
        try:
            import ray

            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
