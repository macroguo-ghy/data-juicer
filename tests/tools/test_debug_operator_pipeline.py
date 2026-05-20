import base64
import datetime as dt
import json
import os
import shutil
import sys
import tempfile
import types
import unittest
import yaml
from unittest.mock import patch


class DebugOperatorPipelineUnitTest(unittest.TestCase):
    def test_debug_config_validation_errors(self):
        from jsonargparse import Namespace
        from tools.debug_operator_pipeline import (
            DebugConfigError,
            _get_debug_cfg,
            _normalize_bytes_output,
            _normalize_debug_output,
        )

        with self.assertRaisesRegex(DebugConfigError, "`debug` must be a mapping"):
            _get_debug_cfg(Namespace(debug=True))
        with self.assertRaisesRegex(DebugConfigError, "`debug.enabled` must be true"):
            _get_debug_cfg(Namespace(debug={}))
        with self.assertRaisesRegex(DebugConfigError, "`debug.output` must be a mapping"):
            _normalize_debug_output({})
        with self.assertRaisesRegex(DebugConfigError, "`debug.output.path` is required"):
            _normalize_debug_output({"output": {}})
        with self.assertRaisesRegex(DebugConfigError, "only supports `jsonl`"):
            _normalize_debug_output({"output": {"path": "x", "type": "parquet"}})
        with self.assertRaisesRegex(DebugConfigError, "`debug.bytes_output` must be a mapping"):
            _normalize_bytes_output({"bytes_output": "full"})
        with self.assertRaisesRegex(DebugConfigError, "mode"):
            _normalize_bytes_output({"bytes_output": {"mode": "raw"}})
        with self.assertRaisesRegex(DebugConfigError, "preview_bytes"):
            _normalize_bytes_output({"bytes_output": {"preview_bytes": -1}})

    def test_sample_json_and_bytes_wrapper_decode(self):
        from tools.debug_operator_pipeline import _load_debug_sample

        encoded = base64.b64encode(b"image-bytes").decode("ascii")
        sample = _load_debug_sample(
            {
                "sample_json": json.dumps(
                    {
                        "text": "hello",
                        "image_bytes": {
                            "__dj_bytes__": {
                                "encoding": "base64",
                                "data": encoded,
                            }
                        },
                    }
                ),
                "decode_fields": {"image_bytes": "bytes"},
            }
        )

        self.assertEqual(sample["text"], "hello")
        self.assertEqual(sample["image_bytes"], b"image-bytes")

    def test_bytes_wrapper_supports_data_url_and_reports_bad_shapes(self):
        from tools.debug_operator_pipeline import DebugConfigError, _load_debug_sample

        encoded = base64.b64encode(b"abc").decode("ascii")
        sample = _load_debug_sample(
            {
                "sample": {
                    "items": [
                        {
                            "__dj_bytes__": {
                                "encoding": "data_url",
                                "data": f"data:application/octet-stream;base64,{encoded}",
                            }
                        }
                    ]
                },
                "decode_fields": {"items": "bytes"},
            }
        )
        self.assertEqual(sample["items"], [b"abc"])

        bad_cases = [
            ({"__dj_bytes__": "abc"}, "must contain a mapping"),
            ({"__dj_bytes__": {"encoding": "base64", "data": 1}}, "data` must be a string"),
            ({"__dj_bytes__": {"encoding": "data_url", "data": encoded}}, "data_url"),
            ({"__dj_bytes__": {"encoding": "hex", "data": encoded}}, "Unsupported bytes encoding"),
            ("abc", "must use `__dj_bytes__`"),
        ]
        for value, message in bad_cases:
            with self.subTest(value=value):
                with self.assertRaisesRegex(DebugConfigError, message):
                    _load_debug_sample({"sample": {"payload": value}, "decode_fields": {"payload": "bytes"}})

    def test_sample_and_sample_json_are_mutually_exclusive(self):
        from tools.debug_operator_pipeline import DebugConfigError, _load_debug_sample

        with self.assertRaisesRegex(DebugConfigError, "mutually exclusive"):
            _load_debug_sample({"sample": {"text": "a"}, "sample_json": '{"text":"b"}'})

    def test_sample_validation_errors(self):
        from tools.debug_operator_pipeline import DebugConfigError, _load_debug_sample

        cases = [
            ({"sample_json": "{"}, "not valid JSON"),
            ({}, "One of `debug.sample` or `debug.sample_json` is required"),
            ({"sample": ["not", "object"]}, "must be a JSON object"),
            ({"sample": {}, "decode_fields": ["payload"]}, "`debug.decode_fields` must be a mapping"),
            ({"sample": {"payload": {}}, "decode_fields": {"payload": "str"}}, "only supports value `bytes`"),
            ({"sample": {}, "decode_fields": {"payload": "bytes"}}, "references missing field"),
        ]
        for debug_cfg, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(DebugConfigError, message):
                    _load_debug_sample(debug_cfg)

    def test_decode_fields_only_support_top_level_fields(self):
        from tools.debug_operator_pipeline import DebugConfigError, _load_debug_sample

        with self.assertRaisesRegex(DebugConfigError, "top-level"):
            _load_debug_sample({"sample": {"nested": {"image": {}}}, "decode_fields": {"nested.image": "bytes"}})

    def test_to_jsonable_debug_value_summarizes_and_full_encodes_bytes(self):
        from tools.debug_operator_pipeline import to_jsonable_debug_value

        summary = to_jsonable_debug_value(
            {"payload": b"abcdef", "token": "secret"},
            bytes_cfg={"mode": "summary", "preview_bytes": 3},
            redact_fields={"token"},
        )
        self.assertEqual(summary["token"], "<redacted>")
        self.assertEqual(summary["payload"]["__dj_bytes_summary__"]["length"], 6)
        self.assertTrue(summary["payload"]["__dj_bytes_summary__"]["truncated"])

        full = to_jsonable_debug_value(
            {"payload": b"abcdef"},
            bytes_cfg={"mode": "full_base64", "preview_bytes": 3},
            redact_fields=set(),
        )
        payload = full["payload"]["__dj_bytes__"]
        self.assertEqual(payload["encoding"], "base64")
        self.assertEqual(base64.b64decode(payload["data"]), b"abcdef")

    def test_unserializable_values_fall_back_to_repr(self):
        from tools.debug_operator_pipeline import to_jsonable_debug_value

        value = to_jsonable_debug_value(
            {"obj": object()},
            bytes_cfg={"mode": "summary", "preview_bytes": 0},
            redact_fields=set(),
        )

        self.assertIn("__dj_repr__", value["obj"])

    def test_jsonable_converts_common_non_json_values(self):
        import numpy as np
        import pyarrow as pa
        from tools.debug_operator_pipeline import to_jsonable_debug_value

        converted = to_jsonable_debug_value(
            {
                "nan": float("nan"),
                "inf": float("inf"),
                "when": dt.datetime(2026, 5, 19, 1, 2, 3),
                "seq": (np.int64(3), np.array([1, 2]), pa.scalar("x")),
                "arr": pa.array([1, 2]),
            },
            bytes_cfg={"mode": "summary", "preview_bytes": 0},
            redact_fields=set(),
        )

        self.assertIsNone(converted["nan"])
        self.assertIsNone(converted["inf"])
        self.assertEqual(converted["when"], "2026-05-19T01:02:03")
        self.assertEqual(converted["seq"], [3, [1, 2], "x"])
        self.assertEqual(converted["arr"], [1, 2])

    def test_schema_and_small_helpers(self):
        from jsonargparse import Namespace
        from tools.debug_operator_pipeline import (
            DebugConfigError,
            _build_redact_fields,
            _force_fail_fast_ops,
            _format_output_path,
            _redact_op_config,
            _schema_to_jsonable,
            _summary_event,
            _validate_indices,
            _validate_ray_only,
        )

        class FakeSchema:
            names = ["text"]
            types = ["string"]

        class FakeDataset:
            def __init__(self, schema):
                self._schema = schema

            def schema(self, fetch_if_missing=False):
                if fetch_if_missing is not False:
                    raise AssertionError("unexpected schema call")
                return self._schema

        class TypeErrorDataset:
            def schema(self, *args, **kwargs):
                if "fetch_if_missing" in kwargs:
                    raise TypeError("old signature")
                return FakeSchema()

        class ErrorDataset:
            def schema(self, fetch_if_missing=False):
                raise RuntimeError("schema unavailable")

        self.assertEqual(_schema_to_jsonable(FakeDataset(FakeSchema())), {"text": "string"})
        self.assertEqual(_schema_to_jsonable(TypeErrorDataset()), {"text": "string"})
        self.assertIsNone(_schema_to_jsonable(FakeDataset(object())))
        self.assertIsNone(_schema_to_jsonable(ErrorDataset()))

        with self.assertRaisesRegex(DebugConfigError, "redact_fields"):
            _build_redact_fields({"redact_fields": "token"})
        self.assertIn("custom", _build_redact_fields({"redact_fields": ["Custom"]}))

        self.assertEqual(
            _redact_op_config({"api_key": "x", "nested": [{"token_value": "y"}, {"keep": "z"}]}),
            {"api_key": "<redacted>", "nested": [{"token_value": "<redacted>"}, {"keep": "z"}]},
        )
        self.assertEqual(
            _format_output_path(
                "/tmp/{job_id}/{debug_run_id}/{timestamp}.jsonl",
                cfg=Namespace(job_id="job"),
                debug_run_id="run",
                timestamp="ts",
            ),
            "/tmp/job/run/ts.jsonl",
        )
        self.assertEqual(
            _force_fail_fast_ops([{}, [], {"op": None}, {"other": {"skip_op_error": True}}, {"raw": "x"}]),
            [{}, [], {"op": {"skip_op_error": False}}, {"other": {"skip_op_error": False}}, {"raw": "x"}],
        )
        self.assertEqual(_validate_indices({}, [{"op": {}}]), (0, 0))
        for debug_cfg, process, message in [
            ({}, [], "at least one"),
            ({"start_index": -1}, [{"op": {}}], "start_index"),
            ({"end_index": "1"}, [{"op": {}}], "end_index"),
            ({"start_index": 1, "end_index": 0}, [{"op": {}}, {"op": {}}], "<="),
            ({"end_index": 2}, [{"op": {}}], "out of range"),
        ]:
            with self.subTest(message=message):
                with self.assertRaisesRegex(DebugConfigError, message):
                    _validate_indices(debug_cfg, process)
        with self.assertRaisesRegex(DebugConfigError, "executor_type: ray"):
            _validate_ray_only(Namespace(executor_type="default"))
        self.assertEqual(
            _summary_event({"debug_run_id": "run"}, status="failed", executed_ops=0, error={"error_type": "ValueError"})[
                "error_type"
            ],
            "ValueError",
        )

    def test_snapshot_empty_dataset_and_failure_summary(self):
        from jsonargparse import Namespace
        from tools.debug_operator_pipeline import _snapshot_dataset, _write_failure_summary

        class EmptyRayDataset:
            def materialize(self):
                return self

            def take(self, limit):
                self.limit = limit
                return []

            def schema(self, fetch_if_missing=False):
                class Schema:
                    names = ["text"]
                    types = ["string"]

                return Schema()

        class Holder:
            data = EmptyRayDataset()

        data, row_count, schema = _snapshot_dataset(
            Holder(), bytes_cfg={"mode": "summary", "preview_bytes": 0}, redact_fields=set()
        )
        self.assertIsNone(data)
        self.assertEqual(row_count, 0)
        self.assertEqual(schema, {"text": "string"})

        cfg = Namespace(
            debug={
                "enabled": True,
                "run_id": "run",
                "output": {"path": "/tmp/{job_id}/{debug_run_id}.jsonl"},
            },
            job_id="job",
        )
        events, output_path = _write_failure_summary(cfg, ValueError("bad"), include_traceback=False)
        self.assertEqual(output_path, "/tmp/job/run.jsonl")
        self.assertEqual(events[0]["status"], "failed")
        self.assertEqual(events[0]["error_type"], "ValueError")

    def test_persist_uses_copy_local_to_uri(self):
        from jsonargparse import Namespace
        from tools import debug_operator_pipeline

        tmp_dir = tempfile.mkdtemp(prefix="dj_debug_persist_")
        try:
            cfg = Namespace(work_dir=tmp_dir)
            events = [{"debug_run_id": "run1", "event": "summary", "status": "success"}]
            with patch.object(debug_operator_pipeline, "copy_local_to_uri") as mock_copy:
                local_path = debug_operator_pipeline._persist_events(
                    cfg,
                    events,
                    "hdfs://cluster/tmp/trace.jsonl",
                    {"filesystem": "webhdfs", "webhdfs": {"host": "localhost"}},
                )

            self.assertTrue(os.path.exists(local_path))
            mock_copy.assert_called_once_with(
                local_path,
                "hdfs://cluster/tmp/trace.jsonl",
                filesystem="webhdfs",
                storage_options={"host": "localhost"},
            )
        finally:
            shutil.rmtree(tmp_dir)

    def test_run_handles_validation_pipeline_and_persist_errors(self):
        from jsonargparse import Namespace
        from tools import debug_operator_pipeline

        invalid_cfg = Namespace(debug={"enabled": True, "output": {}})
        with patch.object(debug_operator_pipeline, "init_configs", return_value=invalid_cfg):
            self.assertEqual(debug_operator_pipeline.run(["--config", "x.yaml"]), 2)

        cfg = Namespace(debug={"enabled": True, "output": {"path": "/tmp/out.jsonl"}}, work_dir=self.tmp_dir if hasattr(self, "tmp_dir") else None)
        with patch.object(debug_operator_pipeline, "init_configs", return_value=cfg):
            with patch.object(debug_operator_pipeline, "run_debug_pipeline", side_effect=debug_operator_pipeline.DebugConfigError("bad")):
                with patch.object(debug_operator_pipeline, "_persist_events", return_value="/tmp/local.jsonl") as persist:
                    self.assertEqual(debug_operator_pipeline.run(["--config", "x.yaml"]), 0)
                    self.assertEqual(persist.call_args.args[1][0]["status"], "failed")

        with patch.object(debug_operator_pipeline, "init_configs", return_value=cfg):
            with patch.object(debug_operator_pipeline, "run_debug_pipeline", return_value=([{"debug_run_id": "run"}], "/tmp/out.jsonl")):
                with patch.object(debug_operator_pipeline, "_persist_events", side_effect=OSError("no hdfs")):
                    self.assertEqual(debug_operator_pipeline.run(["--config", "x.yaml"]), 2)

    def test_main_forwards_wrapper_config_and_extra_args(self):
        from tools import debug_operator_pipeline

        with patch.object(debug_operator_pipeline, "run", return_value=0) as mock_run:
            with patch("sys.argv", ["debug_operator_pipeline.py", "--config", "debug.yaml", "--ray_address", "local"]):
                with self.assertRaises(SystemExit) as ctx:
                    debug_operator_pipeline.main()

        self.assertEqual(ctx.exception.code, 0)
        mock_run.assert_called_once_with(["--config", "debug.yaml", "--ray_address", "local"])


class DebugOperatorPipelineBase64Test(unittest.TestCase):
    def test_decode_base64_config_accepts_whitespace_and_missing_padding(self):
        from tools.debug_operator_pipeline_base64 import decode_base64_config

        self.assertEqual(decode_base64_config("ZGVidWc6IHt9Cg"), "debug: {}\n")
        self.assertEqual(decode_base64_config("ZGVid Wc6IHt9Cg==\n"), "debug: {}\n")

    def test_get_debug_run_loads_default_entrypoint(self):
        from tools.debug_operator_pipeline import run
        from tools.debug_operator_pipeline_base64 import get_debug_operator_pipeline_run

        self.assertIs(get_debug_operator_pipeline_run(), run)

    def test_get_debug_run_falls_back_to_package_entrypoint(self):
        from tools.debug_operator_pipeline_base64 import get_debug_operator_pipeline_run

        fake_module = types.ModuleType("data_juicer.tools.debug_operator_pipeline")
        fake_module.run = object()
        original_import = __import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "tools.debug_operator_pipeline":
                raise ImportError("missing local tools entrypoint")
            return original_import(name, globals, locals, fromlist, level)

        with patch.dict(sys.modules, {"data_juicer.tools.debug_operator_pipeline": fake_module}):
            with patch("builtins.__import__", side_effect=fake_import):
                self.assertIs(get_debug_operator_pipeline_run(), fake_module.run)

    def test_main_decodes_config_and_forwards_args(self):
        from tools import debug_operator_pipeline_base64

        calls = []

        def fake_run(args):
            calls.append(args)
            with open(args[1], encoding="utf-8") as fin:
                self.assertEqual(fin.read(), "debug: {}\n")
            self.assertEqual(args[2:], ["--ray_address", "local"])
            return 0

        encoded = base64.b64encode(b"debug: {}\n").decode("ascii")
        with patch.object(debug_operator_pipeline_base64, "get_debug_operator_pipeline_run", return_value=fake_run):
            with patch("sys.argv", ["debug_operator_pipeline_base64.py", "--config-base64", encoded, "--ray_address", "local"]):
                with self.assertRaises(SystemExit) as ctx:
                    debug_operator_pipeline_base64.main()

        self.assertEqual(ctx.exception.code, 0)
        self.assertEqual(calls[0][0], "--config")

    def test_main_rejects_config_argument(self):
        from tools import debug_operator_pipeline_base64

        encoded = base64.b64encode(b"debug: {}\n").decode("ascii")
        with patch("sys.argv", ["debug_operator_pipeline_base64.py", "--config-base64", encoded, "--config", "x.yaml"]):
            with self.assertRaises(SystemExit):
                debug_operator_pipeline_base64.main()

        with patch("sys.argv", ["debug_operator_pipeline_base64.py", "--config-base64", encoded, "--config=x.yaml"]):
            with self.assertRaises(SystemExit):
                debug_operator_pipeline_base64.main()

    def test_main_rejects_missing_base64_argument(self):
        from tools import debug_operator_pipeline_base64

        with patch.dict(os.environ, {}, clear=True):
            with patch("sys.argv", ["debug_operator_pipeline_base64.py"]):
                with self.assertRaises(SystemExit):
                    debug_operator_pipeline_base64.main()


class DebugOperatorPipelineLocalE2ETest(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp(prefix="dj_debug_e2e_")

    def tearDown(self):
        try:
            import ray

            ray.shutdown()
        except Exception:
            pass
        shutil.rmtree(self.tmp_dir)

    def _write_config(self, config):
        path = os.path.join(self.tmp_dir, "config.yaml")
        with open(path, "w", encoding="utf-8") as fout:
            yaml.safe_dump(config, fout, allow_unicode=True)
        return path

    def _read_events(self, output_path):
        with open(output_path, encoding="utf-8") as fin:
            return [json.loads(line) for line in fin]

    def test_local_ray_e2e_writes_input_steps_and_summary(self):
        from tools.debug_operator_pipeline import run

        output_path = os.path.join(self.tmp_dir, "trace.jsonl")
        config_path = self._write_config(
            {
                "executor_type": "ray",
                "ray_address": "local",
                "work_dir": os.path.join(self.tmp_dir, "work"),
                "export_path": os.path.join(self.tmp_dir, "unused.jsonl"),
                "job_id": "debug-e2e",
                "debug": {
                    "enabled": True,
                    "sample_json": '{"text": "  hello\\tworld  ", "token": "hide-me"}',
                    "output": {"path": output_path, "type": "jsonl"},
                    "redact_fields": ["token"],
                },
                "process": [
                    {"whitespace_normalization_mapper": {}},
                    {"text_length_filter": {"min_len": 1}},
                ],
            }
        )

        self.assertEqual(run(["--config", config_path]), 0)

        events = self._read_events(output_path)
        self.assertEqual([event["event"] for event in events], ["input", "op_step", "op_step", "summary"])
        self.assertEqual(events[0]["data"]["token"], "<redacted>")
        self.assertEqual(events[1]["op_name"], "whitespace_normalization_mapper")
        self.assertEqual(events[1]["data"]["text"], "hello world")
        self.assertEqual(events[2]["op_name"], "text_length_filter")
        self.assertEqual(events[2]["row_count"], 1)
        self.assertIn("__dj__stats__", events[2]["data"])
        self.assertEqual(events[-1]["status"], "success")

    def test_filter_drop_stops_following_ops(self):
        from tools.debug_operator_pipeline import run

        output_path = os.path.join(self.tmp_dir, "drop_trace.jsonl")
        config_path = self._write_config(
            {
                "executor_type": "ray",
                "ray_address": "local",
                "work_dir": os.path.join(self.tmp_dir, "drop_work"),
                "export_path": os.path.join(self.tmp_dir, "drop_unused.jsonl"),
                "debug": {
                    "enabled": True,
                    "sample": {"text": "tiny"},
                    "output": {"path": output_path, "type": "jsonl"},
                },
                "process": [
                    {"text_length_filter": {"min_len": 100}},
                    {"whitespace_normalization_mapper": {}},
                ],
            }
        )

        self.assertEqual(run(["--config", config_path]), 0)

        events = self._read_events(output_path)
        self.assertEqual([event["event"] for event in events], ["input", "op_step", "summary"])
        self.assertTrue(events[1]["dropped"])
        self.assertEqual(events[1]["row_count"], 0)
        self.assertEqual(events[-1]["status"], "dropped")

    def test_operator_failure_writes_failed_summary_and_exits_zero(self):
        from tools.debug_operator_pipeline import run

        output_path = os.path.join(self.tmp_dir, "failed_trace.jsonl")
        config_path = self._write_config(
            {
                "executor_type": "ray",
                "ray_address": "local",
                "work_dir": os.path.join(self.tmp_dir, "failed_work"),
                "export_path": os.path.join(self.tmp_dir, "failed_unused.jsonl"),
                "skip_op_error": False,
                "debug": {
                    "enabled": True,
                    "sample": {"not_text": "missing required text field"},
                    "output": {"path": output_path, "type": "jsonl"},
                },
                "process": [
                    {"whitespace_normalization_mapper": {}},
                    {"text_length_filter": {"min_len": 1}},
                ],
            }
        )

        self.assertEqual(run(["--config", config_path]), 0)

        events = self._read_events(output_path)
        self.assertEqual([event["event"] for event in events], ["input", "op_step", "summary"])
        self.assertEqual(events[1]["status"], "failed")
        self.assertIn("error_type", events[1])
        self.assertEqual(events[-1]["status"], "failed")

    def test_start_and_end_index_selects_sub_chain(self):
        from tools.debug_operator_pipeline import run

        output_path = os.path.join(self.tmp_dir, "range_trace.jsonl")
        config_path = self._write_config(
            {
                "executor_type": "ray",
                "ray_address": "local",
                "work_dir": os.path.join(self.tmp_dir, "range_work"),
                "export_path": os.path.join(self.tmp_dir, "range_unused.jsonl"),
                "debug": {
                    "enabled": True,
                    "sample": {"text": "  hello  "},
                    "output": {"path": output_path, "type": "jsonl"},
                    "start_index": 1,
                    "end_index": 1,
                },
                "process": [
                    {"whitespace_normalization_mapper": {}},
                    {"text_length_filter": {"min_len": 1}},
                ],
            }
        )

        self.assertEqual(run(["--config", config_path]), 0)

        events = self._read_events(output_path)
        self.assertEqual([event["event"] for event in events], ["input", "op_step", "summary"])
        self.assertEqual(events[1]["op_index"], 1)
        self.assertEqual(events[1]["op_name"], "text_length_filter")


if __name__ == "__main__":
    unittest.main()
