import os
import sys
import types
import unittest
from unittest.mock import patch

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

    def test_missing_metrics_sdk_warns_once_and_drops_events(self):
        with patch("data_juicer.utils.metrics_utils.logger.warning") as warning_mock:
            with patch.dict(sys.modules, {"bytedance": None}):
                metrics_utils.emit_vlm_qps(op_name="op", target="host", method="/v1", status="success")
                metrics_utils.emit_vlm_qps(op_name="op", target="host", method="/v1", status="error")

        warning_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
