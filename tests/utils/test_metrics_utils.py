import os
import sys
import types
import unittest
from unittest.mock import call, patch

import pyarrow as pa

_register_extension_type = pa.register_extension_type


def _register_extension_type_once(extension_type):
    try:
        _register_extension_type(extension_type)
    except pa.ArrowKeyError:
        if not extension_type.extension_name.startswith("datasets.features.features."):
            raise


pa.register_extension_type = _register_extension_type_once
from data_juicer.utils.constant import (
    METRICS_JOB_ID_ENV_VAR,
    METRICS_RAY_ADDRESS_ENV_VAR,
)
from data_juicer.utils import metrics_utils

pa.register_extension_type = _register_extension_type


class MetricsUtilsTest(unittest.TestCase):
    def setUp(self):
        self._original_env = {
            METRICS_JOB_ID_ENV_VAR: os.environ.get(METRICS_JOB_ID_ENV_VAR),
            METRICS_RAY_ADDRESS_ENV_VAR: os.environ.get(METRICS_RAY_ADDRESS_ENV_VAR),
        }
        self._original_bytedance = sys.modules.get("bytedance")
        metrics_utils._reset_metrics_client_for_test()

    def tearDown(self):
        metrics_utils._reset_metrics_client_for_test()
        for key, value in self._original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        if self._original_bytedance is None:
            sys.modules.pop("bytedance", None)
        else:
            sys.modules["bytedance"] = self._original_bytedance

    def test_emit_qps_uses_metrics_1_client_prefix_and_context_tags(self):
        calls = []

        class FakeClient:
            def __init__(self, prefix):
                self.prefix = prefix

            def emit_rate_counter(self, name, value, tags):
                calls.append((self.prefix, name, value, tags))

        fake_metrics = types.SimpleNamespace(Client=FakeClient)
        sys.modules["bytedance"] = types.SimpleNamespace(metrics=fake_metrics)
        metrics_utils.set_metrics_context(job_id="job-1", ray_address="ray://cluster")

        metrics_utils.emit_rpc_qps(op_name="op", target="svc", method="M", status="success")

        self.assertEqual(len(calls), 1)
        prefix, name, value, tags = calls[0]
        self.assertEqual(prefix, "ad.ai.data_forge")
        self.assertEqual(name, "rpc.qps")
        self.assertEqual(value, 1)
        self.assertEqual(tags["job_id"], "job-1")
        self.assertEqual(tags["ray_address"], "ray://cluster")
        self.assertEqual(tags["op_name"], "op")
        self.assertEqual(tags["target"], "svc")
        self.assertEqual(tags["method"], "M")
        self.assertEqual(tags["status"], "success")

    def test_emit_vlm_rate_limit_metrics_use_counter_and_store(self):
        calls = []

        class FakeClient:
            def __init__(self, prefix):
                self.prefix = prefix

            def emit_rate_counter(self, name, value, tags):
                calls.append(("counter", self.prefix, name, value, tags))

            def emit_store(self, name, value, tags):
                calls.append(("store", self.prefix, name, value, tags))

        fake_metrics = types.SimpleNamespace(Client=FakeClient)
        sys.modules["bytedance"] = types.SimpleNamespace(metrics=fake_metrics)
        metrics_utils.set_metrics_context(job_id="job-1", ray_address="ray://cluster")

        metrics_utils.emit_vlm_rate_limit_event(
            event="429",
            op_name="vlm",
            model="m",
            target="host",
            method="/v1",
            extra_tags={"retry_index": 0},
        )
        metrics_utils.emit_vlm_rate_limit_value(
            metric="effective_rpm",
            value=250.0,
            op_name="vlm",
            model="m",
            target="host",
            method="/v1",
        )

        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][0], "counter")
        self.assertEqual(calls[0][2], "vlm.rate_limit.event")
        self.assertEqual(calls[0][3], 1)
        self.assertEqual(calls[0][4]["event"], "429")
        self.assertEqual(calls[0][4]["retry_index"], "0")
        self.assertEqual(calls[1][0], "store")
        self.assertEqual(calls[1][2], "vlm.rate_limit.value")
        self.assertEqual(calls[1][3], 250.0)
        self.assertEqual(calls[1][4]["metric"], "effective_rpm")
        self.assertEqual(calls[1][4]["job_id"], "job-1")

    def test_emit_download_metrics_use_counter_and_store(self):
        calls = []

        class FakeClient:
            def __init__(self, prefix):
                self.prefix = prefix

            def emit_rate_counter(self, name, value, tags):
                calls.append(("counter", self.prefix, name, value, tags))

            def emit_store(self, name, value, tags):
                calls.append(("store", self.prefix, name, value, tags))

        fake_metrics = types.SimpleNamespace(Client=FakeClient)
        sys.modules["bytedance"] = types.SimpleNamespace(metrics=fake_metrics)
        metrics_utils.set_metrics_context(job_id="job-1")

        metrics_utils.emit_download_qps(
            op_name="download_file_mapper",
            scheme="http",
            status="success",
            save_mode="memory",
        )
        metrics_utils.emit_download_bytes(
            op_name="download_file_mapper",
            scheme="http",
            byte_count=12,
            save_mode="memory",
        )
        metrics_utils.emit_download_latency_ms(
            op_name="download_file_mapper",
            scheme="http",
            status="success",
            latency_ms=34.5,
            save_mode="memory",
        )

        self.assertEqual([call[2] for call in calls], ["download.qps", "download.bytes", "download.latency_ms"])
        self.assertEqual(calls[0][0], "counter")
        self.assertEqual(calls[0][3], 1)
        self.assertEqual(calls[0][4]["op_name"], "download_file_mapper")
        self.assertEqual(calls[0][4]["scheme"], "http")
        self.assertEqual(calls[0][4]["status"], "success")
        self.assertEqual(calls[0][4]["save_mode"], "memory")
        self.assertEqual(calls[1][0], "store")
        self.assertEqual(calls[1][3], 12.0)
        self.assertEqual(calls[2][0], "store")
        self.assertEqual(calls[2][3], 34.5)

    @patch("data_juicer.utils.metrics_utils.incr_task_kv")
    def test_emit_download_event_updates_counter_and_runtime_stats(self, incr_mock):
        calls = []

        class FakeClient:
            def __init__(self, prefix):
                self.prefix = prefix

            def emit_rate_counter(self, name, value, tags):
                calls.append(("counter", self.prefix, name, value, tags))

        fake_metrics = types.SimpleNamespace(Client=FakeClient)
        sys.modules["bytedance"] = types.SimpleNamespace(metrics=fake_metrics)
        metrics_utils.set_metrics_context(job_id="job-1")

        metrics_utils.emit_download_event(
            op_name="download_file_mapper",
            scheme="http",
            save_mode="memory",
            event="retry",
            reason="timeout",
            attempt=2,
            max_attempts=3,
        )

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][2], "download.event")
        self.assertEqual(calls[0][3], 1)
        self.assertEqual(calls[0][4]["op_name"], "download_file_mapper")
        self.assertEqual(calls[0][4]["scheme"], "http")
        self.assertEqual(calls[0][4]["save_mode"], "memory")
        self.assertEqual(calls[0][4]["event"], "retry")
        self.assertEqual(calls[0][4]["reason"], "timeout")
        self.assertEqual(calls[0][4]["attempt"], "2")
        self.assertEqual(calls[0][4]["max_attempts"], "3")
        incr_mock.assert_has_calls(
            [
                call("download.retry_count", 1, namespace="runtime_stats", wait=False),
                call("download.download_file_mapper.retry_count", 1, namespace="runtime_stats", wait=False),
            ],
            any_order=True,
        )

    @patch("data_juicer.utils.metrics_utils._emit_rate_counter")
    @patch("data_juicer.utils.metrics_utils.incr_task_kv")
    def test_qps_metrics_update_runtime_operation_counts(self, incr_mock, _emit_mock):
        metrics_utils.emit_rpc_qps(op_name="video_url_rpc_mapper", target="svc", method="M", status="success")
        metrics_utils.emit_rpc_qps(op_name="video_url_rpc_mapper", target="svc", method="M", status="error")
        metrics_utils.emit_vlm_qps(op_name="vlm_api_response_mapper", target="host", method="/v1", status="success")
        metrics_utils.record_runtime_operation_counts(
            "download",
            op_name="download_file_mapper",
            total=10,
            success=9,
            failed=1,
        )

        incr_mock.assert_has_calls(
            [
                call("rpc.total_count", 1, namespace="runtime_stats", wait=False),
                call("rpc.video_url_rpc_mapper.total_count", 1, namespace="runtime_stats", wait=False),
                call("rpc.success_count", 1, namespace="runtime_stats", wait=False),
                call("rpc.video_url_rpc_mapper.success_count", 1, namespace="runtime_stats", wait=False),
                call("rpc.failed_count", 1, namespace="runtime_stats", wait=False),
                call("rpc.video_url_rpc_mapper.failed_count", 1, namespace="runtime_stats", wait=False),
                call("download.total_count", 10, namespace="runtime_stats", wait=False),
                call("download.download_file_mapper.total_count", 10, namespace="runtime_stats", wait=False),
                call("download.success_count", 9, namespace="runtime_stats", wait=False),
                call("download.download_file_mapper.success_count", 9, namespace="runtime_stats", wait=False),
                call("download.failed_count", 1, namespace="runtime_stats", wait=False),
                call("download.download_file_mapper.failed_count", 1, namespace="runtime_stats", wait=False),
                call("vlm.success_count", 1, namespace="runtime_stats", wait=False),
                call("vlm.vlm_api_response_mapper.success_count", 1, namespace="runtime_stats", wait=False),
            ],
            any_order=True,
        )

    def test_emit_dedup_rows_uses_counter_value_and_stable_tags(self):
        calls = []

        class FakeClient:
            def __init__(self, prefix):
                self.prefix = prefix

            def emit_rate_counter(self, name, value, tags):
                calls.append((self.prefix, name, value, tags))

        fake_metrics = types.SimpleNamespace(Client=FakeClient)
        sys.modules["bytedance"] = types.SimpleNamespace(metrics=fake_metrics)
        metrics_utils.set_metrics_context(job_id="job-1", ray_address="ray://cluster")

        metrics_utils.emit_dedup_rows(
            op_name="ray_field_dedup_pipeline",
            field_key="md5",
            event="duplicate",
            count=7,
            extra_tags={"backend": "ray_actor"},
        )

        self.assertEqual(len(calls), 1)
        prefix, name, value, tags = calls[0]
        self.assertEqual(prefix, "ad.ai.data_forge")
        self.assertEqual(name, "dedup.rows")
        self.assertEqual(value, 7)
        self.assertEqual(tags["job_id"], "job-1")
        self.assertEqual(tags["ray_address"], "ray://cluster")
        self.assertEqual(tags["op_name"], "ray_field_dedup_pipeline")
        self.assertEqual(tags["field_key"], "md5")
        self.assertEqual(tags["event"], "duplicate")
        self.assertEqual(tags["backend"], "ray_actor")

    def test_missing_metrics_sdk_warns_once_and_drops_events(self):
        with patch("data_juicer.utils.metrics_utils.logger.warning") as warning_mock:
            with patch.dict(sys.modules, {"bytedance": None}):
                metrics_utils.emit_vlm_qps(op_name="op", target="host", method="/v1", status="success")
                metrics_utils.emit_vlm_qps(op_name="op", target="host", method="/v1", status="error")

        warning_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
