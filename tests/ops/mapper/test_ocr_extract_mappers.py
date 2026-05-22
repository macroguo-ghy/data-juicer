import asyncio
import base64
import json
import os
import sys
import tempfile
import types
import unittest
import urllib.error
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
from data_juicer.config.config import init_configs
from data_juicer.core.io_utils import build_arrow_schema_from_config
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.schema.aligned_list_field_flatten_mapper import (
    AlignedListFieldFlattenMapper,
)
from data_juicer.ops.mapper.image.image_ocr_mapper import (
    GDPR_TOKEN_FALLBACK_ENV,
    GDPR_TOKEN_ENV,
    GDPR_TOKEN_PATH_ENV,
    ImageOcrMapper,
    OcrResponseStatusError,
    _RayJobOcrRateLimiter,
    _build_gdpr_auth_middleware,
    _ocr_rate_limiter_actor_name,
    _serialize_ocr_response,
    _thrift_obj_to_dict,
)
from data_juicer.ops.mapper.text.ocr_answer_category_mapper import (
    OcrAnswerCategoryMapper,
    parse_answer_categories,
)
from data_juicer.ops.mapper.text.ocr_text_richness_mapper import (
    OcrTextRichnessMapper,
    calculate_text_richness_score,
)
from data_juicer.ops.mapper.qa.vlm_api_response_mapper import (
    VlmApiResponseMapper,
    _RayJobVlmRateLimiter,
    _VlmApiHttpError,
)
from data_juicer.ops.op_env import OPEnvManager

pa.register_extension_type = _register_extension_type


def _patch_yaml_loader_tags():
    from jsonargparse import _loaders_dumpers
    from yaml.resolver import BaseResolver, Resolver

    def construct_unresolved_tag(loader, node):
        node_id = getattr(node, "id", None)
        if node_id == "mapping":
            return {
                loader.construct_object(key_node, deep=True): loader.construct_object(value_node, deep=True)
                for key_node, value_node in node.value
            }
        if node_id == "sequence":
            return [loader.construct_object(child, deep=True) for child in node.value]
        if node_id == "scalar":
            value = node.value
            normalized = value.lower()
            if normalized == "true":
                return True
            if normalized == "false":
                return False
            if normalized in {"null", "none", "~"}:
                return None
            try:
                return int(value)
            except ValueError:
                pass
            try:
                return float(value)
            except ValueError:
                return value
        raise TypeError(f"Unsupported YAML node type: {type(node)}")

    BaseResolver.DEFAULT_MAPPING_TAG = "tag:yaml.org,2002:map"
    BaseResolver.DEFAULT_SEQUENCE_TAG = "tag:yaml.org,2002:seq"
    BaseResolver.DEFAULT_SCALAR_TAG = "tag:yaml.org,2002:str"
    Resolver.DEFAULT_MAPPING_TAG = "tag:yaml.org,2002:map"
    Resolver.DEFAULT_SEQUENCE_TAG = "tag:yaml.org,2002:seq"
    Resolver.DEFAULT_SCALAR_TAG = "tag:yaml.org,2002:str"

    loader_cls = _loaders_dumpers.get_yaml_default_loader()
    loader_cls.DEFAULT_MAPPING_TAG = "tag:yaml.org,2002:map"
    loader_cls.DEFAULT_SEQUENCE_TAG = "tag:yaml.org,2002:seq"
    loader_cls.DEFAULT_SCALAR_TAG = "tag:yaml.org,2002:str"
    loader_cls.add_constructor(None, construct_unresolved_tag)


def _ocr_payload(text="hello", area_ratio=0.3):
    return {
        "ocr_area_ratio": area_ratio,
        "words": [
            {
                "text": text,
                "det_points_relative": [
                    {"x": 0, "y": 0},
                    {"x": 10, "y": 0},
                    {"x": 10, "y": 20},
                    {"x": 0, "y": 20},
                ],
            }
        ],
    }


class ImageOcrMapperTest(unittest.TestCase):
    def test_thrift_response_serialization_adds_area_ratio(self):
        class Point:
            def __init__(self, x, y):
                self.x = x
                self.y = y

        class Word:
            thrift_spec = {1: (None, "text"), 2: (None, "det_points_abs")}

            def __init__(self):
                self.text = "hello"
                self.det_points_abs = [Point(0, 0), Point(10, 0), Point(10, 20), Point(0, 20)]

        class Result:
            thrift_spec = {1: (None, "words"), 2: (None, "extra")}

            def __init__(self):
                self.words = [Word()]
                self.extra = {"width": 20, "height": 20}

        class Response:
            results = [Result()]

        payload = json.loads(_serialize_ocr_response(Response())[0])

        self.assertEqual(payload["words"][0]["text"], "hello")
        self.assertEqual(payload["ocr_area_ratio"], 0.5)
        self.assertEqual(_thrift_obj_to_dict({"x": [Point(1, 2)]})["x"][0]["x"], 1)

    def test_process_batched_handles_dict_and_arrow_schema(self):
        op = ImageOcrMapper(auto_op_parallelism=False, num_proc=1)
        op.batch_get_ocr = lambda value: [f"ocr:{len(ImageOcrMapper._as_bytes_list(value))}"]

        dict_output = op.process_batched({"id": ["a"], "images": [[b"1", b"2"]]})
        self.assertEqual(dict_output, {"id": ["a"], "images": [[b"1", b"2"]], "ocr_result": [["ocr:2"]]})

        table = pa.Table.from_pylist(
            [{"id": "b", "images": [b"3"]}],
            schema=pa.schema([pa.field("id", pa.string()), pa.field("images", pa.list_(pa.binary()))]),
        )
        arrow_output = op.process_batched(table)

        self.assertEqual(arrow_output.schema.field("ocr_result").type, pa.list_(pa.string()))
        self.assertEqual(arrow_output.to_pylist()[0]["ocr_result"], ["ocr:1"])
        self.assertEqual(op.process_batched({}), {"ocr_result": []})
        self.assertEqual(op._rows_to_arrow_table([{"ocr_result": ["x"]}], None).to_pylist(), [{"ocr_result": ["x"]}])

    def test_constructor_and_state_validation(self):
        with self.assertRaisesRegex(ValueError, "split_size"):
            ImageOcrMapper(split_size=0)
        with self.assertRaisesRegex(ValueError, "split_size must be no more than 16"):
            ImageOcrMapper(split_size=17)
        with self.assertRaisesRegex(ValueError, "qps"):
            ImageOcrMapper(qps=0)
        with self.assertRaisesRegex(ValueError, "status_retry_attempts"):
            ImageOcrMapper(status_retry_attempts=-1)
        with self.assertRaisesRegex(ValueError, "status_retry_backoff_seconds"):
            ImageOcrMapper(status_retry_backoff_seconds=-0.1)

        op = ImageOcrMapper(auto_op_parallelism=False, num_proc=1)
        op._client = object()
        op._api_thrift = object()
        state = op.__getstate__()

        self.assertIsNone(state["_client"])
        self.assertIsNone(state["_api_thrift"])

    def test_batch_get_ocr_splits_values_and_fails_closed(self):
        op = ImageOcrMapper(split_size=2, auto_op_parallelism=False, num_proc=1)
        calls = []

        def fake_get_ocr(values):
            calls.append(values)
            return [f"batch:{len(values)}"]

        op.get_ocr = fake_get_ocr

        self.assertEqual(
            op.batch_get_ocr([b"a", bytearray(b"b"), memoryview(b"c"), "https://example.com/a.png"]),
            ["batch:2", "batch:2"],
        )
        self.assertEqual([len(call) for call in calls], [2, 2])

        op.get_ocr = lambda values: None
        self.assertEqual(op.batch_get_ocr([b"a"]), [])
        self.assertEqual(op.batch_get_ocr(None), [])

        class ArrowScalar:
            def as_py(self):
                return bytearray(b"x")

        class ArrayValue:
            def tolist(self):
                return [b"y", None, [memoryview(b"z")]]

        self.assertEqual(ImageOcrMapper._as_bytes_list(ArrowScalar()), [b"x"])
        self.assertEqual(ImageOcrMapper._as_bytes_list(ArrayValue()), [b"y", b"z"])
        self.assertEqual(ImageOcrMapper._as_bytes_list("not-bytes"), [])
        self.assertEqual(ImageOcrMapper._as_image_input_list(ArrowScalar()), [b"x"])
        self.assertEqual(ImageOcrMapper._as_image_input_list(ArrayValue()), [b"y", b"z"])
        self.assertEqual(
            ImageOcrMapper._as_image_input_list(
                [
                    {"data": bytearray(b"d")},
                    {"binary": memoryview(b"e")},
                    {"url": "https://example.com/b.png"},
                    {"tos_bucket": "b", "tos_obj": "o"},
                    {"url": "local-path-is-ignored"},
                    "local-path-is-ignored",
                ]
            ),
            [
                {"data": b"d"},
                {"data": b"e"},
                {"url": "https://example.com/b.png"},
                {"tos_bucket": "b", "tos_obj": "o"},
            ],
        )

    def test_get_ocr_uses_client_and_throttle_even_on_error(self):
        op = ImageOcrMapper(day_interval_seconds=4.0, auto_op_parallelism=False, num_proc=1)

        class FakeClient:
            def __init__(self):
                self.last_req = None

            def PredictImages(self, req):
                self.last_req = req
                return types.SimpleNamespace(results=[])

        class FakeBase:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class FakeImageInfo:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class FakeImagesOcrRequest:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        fake_client = FakeClient()
        fake_thrift = types.SimpleNamespace(
            base=types.SimpleNamespace(Base=FakeBase),
            ImageInfo=FakeImageInfo,
            ImagesOcrRequest=FakeImagesOcrRequest,
        )
        op._get_client_and_thrift = lambda: (fake_client, fake_thrift)
        with patch("data_juicer.ops.mapper.image.image_ocr_mapper.time.time", side_effect=[10.0, 11.5]), patch(
            "data_juicer.ops.mapper.image.image_ocr_mapper.time.sleep"
        ) as sleep_mock:
            self.assertEqual(op.get_ocr([b"a"]), [])
            self.assertEqual(fake_client.last_req.images[0].data, b"a")
            sleep_mock.assert_called_once_with(2.5)

        op._get_client_and_thrift = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
        with patch("data_juicer.ops.mapper.image.image_ocr_mapper.time.time", side_effect=[20.0, 25.0]), patch(
            "data_juicer.ops.mapper.image.image_ocr_mapper.time.sleep"
        ) as sleep_mock:
            self.assertIsNone(op.get_ocr([b"a"]))
            sleep_mock.assert_not_called()

        op._client = None
        op._api_thrift = None
        op._create_client_and_thrift = lambda: ("client", "thrift")
        op._get_client_and_thrift = ImageOcrMapper._get_client_and_thrift.__get__(op, ImageOcrMapper)
        self.assertEqual(op._get_client_and_thrift(), ("client", "thrift"))
        self.assertEqual(op._get_client_and_thrift(), ("client", "thrift"))

    def test_get_ocr_uses_qps_limiter_without_interval_sleep(self):
        op = ImageOcrMapper(
            qps=2,
            day_interval_seconds=4.0,
            auto_op_parallelism=False,
            num_proc=1,
        )
        order = []

        def apply_rate_limit():
            order.append("limit")

        def call_ocr_rpc(image_inputs):
            order.append(("rpc", list(image_inputs)))
            return []

        op._apply_rate_limit = apply_rate_limit
        op._call_ocr_rpc = call_ocr_rpc

        with patch("data_juicer.ops.mapper.image.image_ocr_mapper.time.sleep") as sleep_mock:
            self.assertEqual(op.get_ocr([b"a"]), [])

        self.assertEqual(order, ["limit", ("rpc", [b"a"])])
        sleep_mock.assert_not_called()

    def test_ocr_rpc_qps_metrics_are_emitted_for_success_and_status_error(self):
        op = ImageOcrMapper(
            psm="ocr.psm",
            cluster="boe",
            rpc_method="PredictImages",
            auto_op_parallelism=False,
            num_proc=1,
        )

        class FakeBase:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class FakeImageInfo:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class FakeImagesOcrRequest:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class FakeBaseResp:
            def __init__(self, status_code):
                self.StatusCode = status_code
                self.StatusMessage = "bad"

        class FakeResponse:
            def __init__(self, status_code):
                self.BaseResp = FakeBaseResp(status_code)
                self.results = []

        class FakeClient:
            def __init__(self):
                self.responses = [FakeResponse(0), FakeResponse(1)]

            def PredictImages(self, req):
                return self.responses.pop(0)

        fake_thrift = types.SimpleNamespace(
            base=types.SimpleNamespace(Base=FakeBase),
            ImageInfo=FakeImageInfo,
            ImagesOcrRequest=FakeImagesOcrRequest,
        )
        fake_client = FakeClient()
        op._get_client_and_thrift = lambda: (fake_client, fake_thrift)

        with patch("data_juicer.ops.mapper.image.image_ocr_mapper.emit_rpc_qps") as emit_mock:
            self.assertEqual(op._call_ocr_rpc([b"a"]), [])
            with self.assertRaises(OcrResponseStatusError):
                op._call_ocr_rpc([b"b"])

        self.assertEqual(emit_mock.call_count, 2)
        success_tags = emit_mock.call_args_list[0].kwargs
        error_tags = emit_mock.call_args_list[1].kwargs
        self.assertEqual(success_tags["op_name"], "image_ocr_mapper")
        self.assertEqual(success_tags["target"], "sd://ocr.psm?cluster=boe")
        self.assertEqual(success_tags["method"], "PredictImages")
        self.assertEqual(success_tags["status"], "success")
        self.assertEqual(error_tags["status"], "error")

    def test_get_ocr_logs_rpc_failure_as_single_line_without_traceback(self):
        op = ImageOcrMapper(auto_op_parallelism=False, num_proc=1)

        def call_ocr_rpc(image_inputs):
            raise RuntimeError("first line\nsecond line")

        op._call_ocr_rpc_with_rate_limit = call_ocr_rpc

        with patch("data_juicer.ops.mapper.image.image_ocr_mapper.logger.error") as error_mock:
            with patch("data_juicer.ops.mapper.image.image_ocr_mapper.logger.exception") as exception_mock:
                self.assertIsNone(op.get_ocr([b"a"]))

        exception_mock.assert_not_called()
        error_mock.assert_called_once()
        message, pid, attempts, image_count, error = error_mock.call_args.args
        self.assertIn("error={}", message)
        self.assertIsInstance(pid, int)
        self.assertEqual(attempts, 1)
        self.assertEqual(image_count, 1)
        self.assertEqual(error, "RuntimeError: first line\\nsecond line")
        self.assertNotIn("\n", error)

    def test_get_ocr_retries_status_errors_then_succeeds(self):
        op = ImageOcrMapper(
            day_interval_seconds=0.0,
            night_interval_seconds=0.0,
            auto_op_parallelism=False,
            num_proc=1,
        )
        responses = [
            OcrResponseStatusError("status-1"),
            OcrResponseStatusError("status-2"),
            ["ok"],
        ]
        calls = []

        def call_ocr_rpc_with_rate_limit(image_inputs):
            calls.append(list(image_inputs))
            response = responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response

        op._call_ocr_rpc_with_rate_limit = call_ocr_rpc_with_rate_limit

        with patch("data_juicer.ops.mapper.image.image_ocr_mapper.time.sleep") as sleep_mock:
            self.assertEqual(op.get_ocr([b"a"]), ["ok"])

        self.assertEqual(calls, [[b"a"], [b"a"], [b"a"]])
        self.assertEqual([call.args[0] for call in sleep_mock.call_args_list], [0.5, 1.0])

    def test_get_ocr_returns_none_after_status_retry_exhaustion(self):
        op = ImageOcrMapper(
            day_interval_seconds=0.0,
            night_interval_seconds=0.0,
            auto_op_parallelism=False,
            num_proc=1,
        )
        calls = []

        def call_ocr_rpc_with_rate_limit(image_inputs):
            calls.append(list(image_inputs))
            raise OcrResponseStatusError("still bad")

        op._call_ocr_rpc_with_rate_limit = call_ocr_rpc_with_rate_limit

        with patch("data_juicer.ops.mapper.image.image_ocr_mapper.time.sleep") as sleep_mock:
            with patch("data_juicer.ops.mapper.image.image_ocr_mapper.logger.error") as error_mock:
                with patch("data_juicer.ops.mapper.image.image_ocr_mapper.logger.exception") as exception_mock:
                    self.assertIsNone(op.get_ocr([b"a", b"b"]))

        self.assertEqual(calls, [[b"a", b"b"], [b"a", b"b"], [b"a", b"b"]])
        self.assertEqual([call.args[0] for call in sleep_mock.call_args_list], [0.5, 1.0])
        exception_mock.assert_not_called()
        error_mock.assert_called_once()
        message, pid, attempts, image_count, error = error_mock.call_args.args
        self.assertIn("attempts={}", message)
        self.assertIsInstance(pid, int)
        self.assertEqual(attempts, 3)
        self.assertEqual(image_count, 2)
        self.assertEqual(error, "OcrResponseStatusError: still bad")

    def test_get_ocr_does_not_status_retry_regular_runtime_error(self):
        op = ImageOcrMapper(
            day_interval_seconds=0.0,
            night_interval_seconds=0.0,
            auto_op_parallelism=False,
            num_proc=1,
        )
        calls = []

        def call_ocr_rpc_with_rate_limit(image_inputs):
            calls.append(list(image_inputs))
            raise RuntimeError("not a response status")

        op._call_ocr_rpc_with_rate_limit = call_ocr_rpc_with_rate_limit

        with patch("data_juicer.ops.mapper.image.image_ocr_mapper.time.sleep") as sleep_mock:
            with patch("data_juicer.ops.mapper.image.image_ocr_mapper.logger.error") as error_mock:
                self.assertIsNone(op.get_ocr([b"a"]))

        self.assertEqual(calls, [[b"a"]])
        sleep_mock.assert_not_called()
        message, _, attempts, image_count, error = error_mock.call_args.args
        self.assertIn("attempts={}", message)
        self.assertEqual(attempts, 1)
        self.assertEqual(image_count, 1)
        self.assertEqual(error, "RuntimeError: not a response status")

    def test_get_ocr_retries_connection_failure_with_qps_limiter_per_attempt(self):
        op = ImageOcrMapper(qps=2, auto_op_parallelism=False, num_proc=1)
        order = []
        responses = [ConnectionRefusedError("Could not connect"), []]

        def apply_rate_limit():
            order.append("limit")

        def call_ocr_rpc(image_inputs):
            order.append(("rpc", list(image_inputs)))
            response = responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response

        op._apply_rate_limit = apply_rate_limit
        op._call_ocr_rpc = call_ocr_rpc

        with patch("data_juicer.ops.mapper.image.image_ocr_mapper.time.sleep") as sleep_mock:
            self.assertEqual(op.get_ocr([b"a"]), [])

        self.assertEqual(order, ["limit", ("rpc", [b"a"]), "limit", ("rpc", [b"a"])])
        sleep_mock.assert_called_once_with(0.1)

    def test_local_qps_rate_limiter_smooths_requests(self):
        op = ImageOcrMapper(qps=2, auto_op_parallelism=False, num_proc=1)
        clock = {"now": 0.0}
        sleeps = []

        def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        with patch("data_juicer.ops.mapper.image.image_ocr_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("data_juicer.ops.mapper.image.image_ocr_mapper.time.sleep", side_effect=fake_sleep):
                op._apply_rate_limit()
                op._apply_rate_limit()

        self.assertEqual(sleeps, [0.5])

    def test_ray_job_ocr_rate_limiter_smooths_requests(self):
        limiter = _RayJobOcrRateLimiter()
        clock = {"now": 0.0}
        sleeps = []

        async def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        with patch("data_juicer.ops.mapper.image.image_ocr_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("data_juicer.ops.mapper.image.image_ocr_mapper.asyncio.sleep", side_effect=fake_sleep):
                asyncio.run(limiter.acquire("ocr", 2))
                asyncio.run(limiter.acquire("ocr", 2))

        self.assertEqual(sleeps, [0.5])

    def test_get_ocr_retries_connection_failure_once(self):
        op = ImageOcrMapper(
            day_interval_seconds=0.0,
            night_interval_seconds=0.0,
            auto_op_parallelism=False,
            num_proc=1,
        )

        class FakeBase:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class FakeImageInfo:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class FakeImagesOcrRequest:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        fake_thrift = types.SimpleNamespace(
            base=types.SimpleNamespace(Base=FakeBase),
            ImageInfo=FakeImageInfo,
            ImagesOcrRequest=FakeImagesOcrRequest,
        )

        class FailingClient:
            def PredictImages(self, req):
                raise ConnectionRefusedError("Could not connect to OCR service")

        class SuccessClient:
            def __init__(self):
                self.last_req = None

            def PredictImages(self, req):
                self.last_req = req
                return types.SimpleNamespace(results=[])

        success_client = SuccessClient()
        created_clients = [FailingClient(), success_client]
        create_calls = []

        def create_client_and_thrift():
            create_calls.append(len(create_calls))
            return created_clients.pop(0), fake_thrift

        op._create_client_and_thrift = create_client_and_thrift
        op._get_client_and_thrift = ImageOcrMapper._get_client_and_thrift.__get__(op, ImageOcrMapper)

        with patch("data_juicer.ops.mapper.image.image_ocr_mapper.time.time", side_effect=[10.0, 10.2]), patch(
            "data_juicer.ops.mapper.image.image_ocr_mapper.time.sleep"
        ) as sleep_mock:
            self.assertEqual(op.get_ocr([b"a"]), [])

        sleep_mock.assert_called_once_with(0.1)
        self.assertEqual(len(create_calls), 2)
        self.assertEqual(success_client.last_req.images[0].data, b"a")

    def test_create_client_builds_request_and_serializes_response(self):
        created = {}

        class FakeBase:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class FakeImageInfo:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class FakeImagesOcrRequest:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        fake_thrift = types.SimpleNamespace(
            OcrService=object(),
            base=types.SimpleNamespace(Base=FakeBase),
            ImageInfo=FakeImageInfo,
            ImagesOcrRequest=FakeImagesOcrRequest,
        )

        class FakeEulerClient:
            def __init__(self, service_cls, target, **kwargs):
                self.service_cls = service_cls
                self.target = target
                self.kwargs = kwargs
                self.middlewares = []
                self.last_req = None
                created["client"] = self

            def use(self, middleware):
                self.middlewares.append(middleware)

            def PredictImages(self, req):
                self.last_req = req
                return types.SimpleNamespace(results=[])

        fake_euler = types.ModuleType("euler")
        fake_euler.Client = FakeEulerClient
        fake_base_compat_middleware = types.ModuleType("euler.base_compat_middleware")
        fake_base_compat_middleware.client_middleware = object()
        fake_euler.base_compat_middleware = fake_base_compat_middleware

        op = ImageOcrMapper(
            psm="psm",
            cluster="cluster",
            timeout=3,
            caller="caller",
            source_cluster="source",
            dag="dag",
        )
        with patch.dict(sys.modules, {"euler": fake_euler, "euler.base_compat_middleware": fake_base_compat_middleware}):
            with patch.dict(os.environ, {GDPR_TOKEN_ENV: " token-1 \n"}, clear=False):
                with patch("data_juicer.ops.mapper.image.image_ocr_mapper._load_lab_ocr_thrift", return_value=fake_thrift):
                    client, api_thrift = op._create_client_and_thrift()

        req = op._build_req(
            api_thrift,
            [
                bytearray(b"a"),
                "https://example.com/a.png",
                {"tos_bucket": "bucket", "tos_obj": "obj"},
            ]
        )
        self.assertIs(client, created["client"])
        self.assertEqual(client.target, "sd://psm?cluster=cluster")
        self.assertEqual(client.kwargs["timeout"], 3)
        self.assertEqual(client.kwargs["transport"], "ttheader")
        self.assertEqual(client.kwargs["protocol"], "binary")
        self.assertEqual(len(client.middlewares), 3)
        self.assertEqual(req.images[0].data, b"a")
        self.assertEqual(req.images[1].url, "https://example.com/a.png")
        self.assertEqual(req.images[2].tos_bucket, "bucket")
        self.assertEqual(req.images[2].tos_obj, "obj")
        self.assertEqual(req.extra, {"dag": "dag"})
        self.assertEqual(req.Base.Caller, "caller")
        self.assertEqual(req.Base.extra, {"cluster": "source"})
        op._client = client
        op._api_thrift = api_thrift
        self.assertEqual(op.get_ocr([b"a"]), [])
        self.assertEqual(client.last_req.images[0].data, b"a")
        with self.assertRaisesRegex(ValueError, "images size must be no more than 16"):
            op._build_req(api_thrift, [b"a"] * 17)

        class FakeContext:
            def __init__(self):
                self.persistent = {}

            def next(self, *args, **kwargs):
                return "next-called"

        ctx = FakeContext()
        ctx.local = {}
        self.assertEqual(client.middlewares[0](ctx), "next-called")
        self.assertEqual(ctx.persistent["cluster"], "cluster")
        self.assertEqual(client.middlewares[2](ctx, req), "next-called")
        self.assertEqual(req.Base.extra["gdpr-token"], "token-1")
        self.assertEqual(ctx.local["gdpr_token"], "token-1")

        op._client = None
        op._api_thrift = None
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as token_file:
            token_file.write(" token-2 \n")
            token_file.flush()
            with patch.dict(sys.modules, {"euler": fake_euler, "euler.base_compat_middleware": fake_base_compat_middleware}):
                with patch.dict(
                    os.environ,
                    {
                        GDPR_TOKEN_ENV: " ",
                        GDPR_TOKEN_PATH_ENV: token_file.name,
                        GDPR_TOKEN_FALLBACK_ENV: " token-should-not-be-used ",
                    },
                    clear=False,
                ):
                    with patch("data_juicer.ops.mapper.image.image_ocr_mapper._load_lab_ocr_thrift", return_value=fake_thrift):
                        token_path_client, _ = op._create_client_and_thrift()
        token_path_req = op._build_req(api_thrift, [b"a"])
        token_path_ctx = FakeContext()
        token_path_ctx.local = {}
        self.assertEqual(token_path_client.middlewares[2](token_path_ctx, token_path_req), "next-called")
        self.assertEqual(token_path_req.Base.extra["gdpr-token"], "token-2")

        op._client = None
        op._api_thrift = None
        with patch.dict(sys.modules, {"euler": fake_euler, "euler.base_compat_middleware": fake_base_compat_middleware}):
            with patch.dict(
                os.environ,
                {
                    GDPR_TOKEN_ENV: " ",
                    GDPR_TOKEN_PATH_ENV: "/path/does/not/exist",
                    GDPR_TOKEN_FALLBACK_ENV: " token-3 ",
                },
                clear=False,
            ):
                with patch("data_juicer.ops.mapper.image.image_ocr_mapper._load_lab_ocr_thrift", return_value=fake_thrift):
                    fallback_client, _ = op._create_client_and_thrift()
        fallback_req = op._build_req(api_thrift, [b"a"])
        fallback_ctx = FakeContext()
        fallback_ctx.local = {}
        self.assertEqual(fallback_client.middlewares[2](fallback_ctx, fallback_req), "next-called")
        self.assertEqual(fallback_req.Base.extra["gdpr-token"], "token-3")

        with self.assertRaisesRegex(RuntimeError, "status"):
            op._check_ocr_response(types.SimpleNamespace(results=[types.SimpleNamespace(status="bad")]))
        with self.assertRaisesRegex(RuntimeError, "status_code=1"):
            op._check_ocr_response(
                types.SimpleNamespace(
                    BaseResp=types.SimpleNamespace(StatusCode=1, StatusMessage="bad"),
                    results=[],
                )
            )
        op._check_ocr_response(types.SimpleNamespace(results=[types.SimpleNamespace(status=None)]))
        self.assertEqual(op._base_resp_status_code(object()), 0)

    def test_create_client_requires_euler_runtime(self):
        op = ImageOcrMapper()
        with patch.dict(sys.modules, {"euler": None}), self.assertRaisesRegex(RuntimeError, "Euler RPC runtime"):
            op._create_client_and_thrift()

        fake_base_compat_middleware = types.ModuleType("euler.base_compat_middleware")
        fake_base_compat_middleware.client_middleware = object()
        fake_euler = types.ModuleType("euler")
        fake_euler.Client = object
        fake_euler.base_compat_middleware = fake_base_compat_middleware

        with patch.dict(sys.modules, {"euler": fake_euler, "euler.base_compat_middleware": fake_base_compat_middleware}):
            with patch.dict(
                os.environ,
                {GDPR_TOKEN_ENV: "", GDPR_TOKEN_PATH_ENV: "", GDPR_TOKEN_FALLBACK_ENV: ""},
                clear=False,
            ):
                with self.assertRaisesRegex(RuntimeError, GDPR_TOKEN_FALLBACK_ENV):
                    op._create_client_and_thrift()

    def test_gdpr_middleware_supports_extra_and_extra_uppercase(self):
        class FakeContext:
            def __init__(self):
                self.local = {}

            def next(self, *args, **kwargs):
                return "next-called"

        lower_base = types.SimpleNamespace(extra={})
        upper_base = types.SimpleNamespace(Extra={})
        middleware = _build_gdpr_auth_middleware("token-2")
        ctx = FakeContext()

        self.assertEqual(middleware(ctx, types.SimpleNamespace(Base=lower_base)), "next-called")
        self.assertEqual(lower_base.extra["gdpr-token"], "token-2")
        self.assertEqual(middleware(ctx, req=types.SimpleNamespace(Base=upper_base)), "next-called")
        self.assertEqual(upper_base.Extra["gdpr-token"], "token-2")
        self.assertEqual(middleware(ctx, types.SimpleNamespace()), "next-called")

    def test_ray_run_repartitions_and_forwards_resource_knobs(self):
        class FakeRayDataset:
            def __init__(self):
                self.repartition_kwargs = None
                self.map_kwargs = None

            def repartition(self, **kwargs):
                self.repartition_kwargs = kwargs
                return self

            def map_batches(self, fn, **kwargs):
                self.fn = fn
                self.map_kwargs = kwargs
                return "mapped"

        op = ImageOcrMapper(
            batch_size=1,
            num_proc=64,
            num_cpus=1,
            num_gpus=0,
            memory=1024,
            runtime_env={"env_vars": {"X": "1"}},
            repartition_num_blocks=128,
            auto_op_parallelism=False,
        )
        dataset = FakeRayDataset()

        self.assertEqual(op.run(dataset), "mapped")
        self.assertEqual(dataset.repartition_kwargs, {"num_blocks": 128, "shuffle": False})
        self.assertEqual(dataset.map_kwargs["batch_format"], "pyarrow")
        self.assertEqual(dataset.map_kwargs["batch_size"], 1)
        self.assertEqual(dataset.map_kwargs["concurrency"], 64)
        self.assertEqual(dataset.map_kwargs["num_cpus"], 1)
        self.assertEqual(dataset.map_kwargs["num_gpus"], 0)
        self.assertEqual(dataset.map_kwargs["memory"], 1024)
        self.assertEqual(dataset.map_kwargs["runtime_env"], {"env_vars": {"X": "1"}})

    def test_ray_run_initializes_global_ocr_qps_limiter_and_acquires_before_request(self):
        order = []

        class FakeActorMethod:
            def __init__(self, name):
                self.name = name
                self.calls = []

            def remote(self, *args):
                self.calls.append(args)
                order.append(self.name)
                return f"{self.name}-ref"

        class FakeActor:
            def __init__(self):
                self.register = FakeActorMethod("register")
                self.acquire = FakeActorMethod("acquire")

        class FakeRemoteActorClass:
            def __init__(self, actor):
                self.actor = actor
                self.options_kwargs = None

            def options(self, **kwargs):
                self.options_kwargs = kwargs
                return self

            def remote(self):
                return self.actor

        class FakeRuntimeContext:
            def get_job_id(self):
                return "job-1"

        class FakeRay:
            def __init__(self):
                self.actor = FakeActor()
                self.remote_actor_class = FakeRemoteActorClass(self.actor)
                self.get_refs = []

            def is_initialized(self):
                return True

            def get_runtime_context(self):
                return FakeRuntimeContext()

            def get_actor(self, name):
                raise ValueError(name)

            def remote(self, cls):
                self.remote_cls = cls
                return self.remote_actor_class

            def get(self, ref):
                self.get_refs.append(ref)
                return None

        class FakeRayDataset:
            def map_batches(self, fn, **kwargs):
                self.fn = fn
                self.map_kwargs = kwargs
                return "mapped"

        op = ImageOcrMapper(
            psm="psm",
            cluster="cluster",
            rpc_method="PredictImages",
            qps=200,
            auto_op_parallelism=False,
            num_proc=2,
        )
        op._call_ocr_rpc = lambda image_inputs: order.append("rpc") or []
        fake_ray = FakeRay()
        dataset = FakeRayDataset()
        limiter_key = "psm:cluster:PredictImages"
        actor_name = _ocr_rate_limiter_actor_name("job-1", limiter_key)

        with patch("data_juicer.ops.mapper.image.image_ocr_mapper._try_import_ray", return_value=fake_ray):
            self.assertEqual(op.run(dataset), "mapped")
            self.assertEqual(op._rate_limiter_actor_name, actor_name)
            self.assertEqual(fake_ray.remote_actor_class.options_kwargs, {"name": actor_name, "num_cpus": 0})
            self.assertEqual(fake_ray.actor.register.calls, [(limiter_key, 200)])
            self.assertEqual(op.get_ocr([b"a"]), [])

        self.assertEqual(fake_ray.actor.acquire.calls, [(limiter_key, 200)])
        self.assertEqual(order[-2:], ["acquire", "rpc"])

    def test_nested_run_uses_nested_dataset_map(self):
        class FakeNestedDataset:
            def map(self, fn, **kwargs):
                self.fn = fn
                self.kwargs = kwargs
                return "nested"

        op = ImageOcrMapper(batch_size=3, num_proc=5, auto_op_parallelism=False)
        dataset = FakeNestedDataset()

        with patch("data_juicer.ops.mapper.image.image_ocr_mapper.NestedDataset", FakeNestedDataset):
            self.assertEqual(op.run(dataset), "nested")
        self.assertEqual(dataset.kwargs["batched"], True)
        self.assertEqual(dataset.kwargs["batch_size"], 3)
        self.assertEqual(dataset.kwargs["num_proc"], 5)
        self.assertEqual(dataset.kwargs["desc"], "image_ocr_mapper_process")

    def test_constructor_validates_repartition_num_blocks(self):
        with self.assertRaisesRegex(ValueError, "repartition_num_blocks"):
            ImageOcrMapper(repartition_num_blocks=0)

    def test_process_batched_logs_first_worker_batch_once(self):
        op = ImageOcrMapper(auto_op_parallelism=False, num_proc=1)
        op.batch_get_ocr = lambda value: ["ok"]

        with patch("data_juicer.ops.mapper.image.image_ocr_mapper.logger.info") as log_info:
            op.process_batched({"id": ["a"], "images": [[b"1"]]})
            op.process_batched({"id": ["b"], "images": [[b"2"]]})

        first_batch_logs = [call for call in log_info.call_args_list if "first worker batch" in call.args[0]]
        self.assertEqual(len(first_batch_logs), 1)


class AlignedListFieldFlattenMapperTest(unittest.TestCase):
    def test_constructor_validates_field_keys(self):
        with self.assertRaisesRegex(ValueError, "field_keys"):
            AlignedListFieldFlattenMapper(field_keys=[])

    def test_arrow_block_flattens_aligned_fields_and_preserves_binary_list_schema(self):
        op = AlignedListFieldFlattenMapper(
            field_keys=["images", "ocr_result"],
            wrap_value_keys=["images"],
            index_key="image_index",
            id_key="id",
            passthrough_types={"md5": "string"},
            auto_op_parallelism=False,
            num_proc=1,
        )
        schema = pa.schema(
            [
                pa.field("id", pa.string()),
                pa.field("images", pa.list_(pa.binary())),
                pa.field("ocr_result", pa.list_(pa.string())),
                pa.field("md5", pa.null()),
            ]
        )
        table = pa.Table.from_pylist(
            [{"id": "row", "images": [b"a", b"b"], "ocr_result": ["ra", "rb"], "md5": None}],
            schema=schema,
        )

        output = op.process_batched(table)

        self.assertEqual(output.num_rows, 2)
        self.assertEqual(output.schema.field("images").type, pa.list_(pa.binary()))
        self.assertEqual(output.schema.field("ocr_result").type, pa.string())
        self.assertEqual(output.schema.field("image_index").type, pa.int64())
        self.assertEqual(output.schema.field("md5").type, pa.string())
        self.assertEqual(
            output.to_pylist(),
            [
                {"id": "row-0", "images": [b"a"], "ocr_result": "ra", "md5": None, "image_index": 0},
                {"id": "row-1", "images": [b"b"], "ocr_result": "rb", "md5": None, "image_index": 1},
            ],
        )

    def test_empty_and_mismatched_arrow_blocks_keep_stable_schema(self):
        op = AlignedListFieldFlattenMapper(
            field_keys=["images", "ocr_result"],
            wrap_value_keys=["images"],
            index_key="image_index",
            auto_op_parallelism=False,
            num_proc=1,
        )
        schema = pa.schema(
            [
                pa.field("images", pa.list_(pa.binary())),
                pa.field("ocr_result", pa.list_(pa.string())),
            ]
        )
        empty = pa.Table.from_arrays(
            [pa.array([], type=pa.list_(pa.binary())), pa.array([], type=pa.list_(pa.string()))],
            schema=schema,
        )
        mismatch = pa.Table.from_pylist([{"images": [b"a"], "ocr_result": ["ra", "rb"]}], schema=schema)

        empty_output = op.process_batched(empty)
        mismatch_output = op.process_batched(mismatch)

        self.assertEqual(empty_output.num_rows, 0)
        self.assertEqual(empty_output.schema.field("images").type, pa.list_(pa.binary()))
        self.assertEqual(empty_output.schema.field("ocr_result").type, pa.string())
        self.assertEqual(mismatch_output.num_rows, 0)
        self.assertEqual(mismatch_output.schema.field("images").type, pa.list_(pa.binary()))

    def test_dict_batch_empty_rows_and_mismatch_truncation_paths(self):
        op = AlignedListFieldFlattenMapper(
            field_keys=["images", "ocr_result"],
            output_field_keys={"ocr_result": "ocr_json"},
            drop_empty=False,
            drop_mismatch=False,
            id_key="id",
            id_format="{id}:{index}",
            auto_op_parallelism=False,
            num_proc=1,
        )

        empty = op.process_batched({"id": [], "images": [], "ocr_result": []})
        rows = op.process_single({"id": "row", "images": [b"a", b"b"], "ocr_result": ["ra"]})
        kept_empty = op.process_single({"id": "empty", "images": [], "ocr_result": []})

        self.assertEqual(empty, {"id": [], "images": [], "ocr_result": [], "ocr_json": []})
        self.assertEqual(rows[0]["id"], "row:0")
        self.assertEqual(rows[0]["ocr_json"], "ra")
        self.assertEqual(kept_empty[0]["ocr_json"], None)

    def test_as_list_handles_arrow_tolist_and_scalar_values(self):
        class ArrowScalar:
            def as_py(self):
                return "x"

        class ArrayValue:
            def tolist(self):
                return ("a", "b")

        self.assertEqual(AlignedListFieldFlattenMapper._as_list(ArrowScalar()), ["x"])
        self.assertEqual(AlignedListFieldFlattenMapper._as_list(ArrayValue()), ["a", "b"])
        self.assertEqual(AlignedListFieldFlattenMapper._as_list(b"bytes"), [b"bytes"])

    def test_run_dispatches_to_ray_map_batches(self):
        class FakeRayDataset:
            def map_batches(self, fn, *, batch_format, batch_size):
                self.fn = fn
                self.batch_format = batch_format
                self.batch_size = batch_size
                return "mapped"

        op = AlignedListFieldFlattenMapper(field_keys=["images", "ocr_result"], batch_size=7)
        dataset = FakeRayDataset()

        self.assertEqual(op.run(dataset), "mapped")
        self.assertEqual(dataset.batch_format, "pyarrow")
        self.assertEqual(dataset.batch_size, 7)


class OcrTextRichnessMapperTest(unittest.TestCase):
    def test_score_and_bbox_are_extracted_from_ocr_json(self):
        op = OcrTextRichnessMapper(char_max=10, area_max_ratio=0.5, auto_op_parallelism=False, num_proc=1)
        row = op.process_single({"ocr_result": json.dumps(_ocr_payload(text="hello", area_ratio=0.5))})

        self.assertAlmostEqual(row["char_score"], 0.5)
        self.assertAlmostEqual(row["area_score"], 1.0)
        self.assertAlmostEqual(row["text_richness_score"], 5 * (0.5 * 1.0) ** 0.5)
        self.assertEqual(row["ocr_text"], ["hello"])
        self.assertEqual(row["ocr_bbox"], [[0.0, 0.0, 10.0, 20.0]])

    def test_empty_bad_and_all_null_arrow_blocks_have_stable_output_schema(self):
        op = OcrTextRichnessMapper(auto_op_parallelism=False, num_proc=1)
        schema = pa.schema([pa.field("ocr_result", pa.string()), pa.field("images", pa.list_(pa.binary()))])
        table = pa.Table.from_pylist(
            [
                {"ocr_result": None, "images": None},
                {"ocr_result": "not-json", "images": [b"a"]},
                {"ocr_result": json.dumps({"words": []}), "images": []},
            ],
            schema=schema,
        )

        output = op.process_batched(table)

        self.assertEqual(output.schema.field("images").type, pa.list_(pa.binary()))
        self.assertEqual(output.schema.field("text_richness_score").type, pa.float64())
        self.assertEqual(output.schema.field("ocr_text").type, pa.list_(pa.string()))
        self.assertEqual(output.schema.field("ocr_bbox").type, pa.list_(pa.list_(pa.float64())))
        for row in output.to_pylist():
            self.assertEqual(row["text_richness_score"], 0.0)
            self.assertEqual(row["ocr_text"], [])
            self.assertEqual(row["ocr_bbox"], [])

    def test_calculate_text_richness_score_handles_invalid_shapes(self):
        self.assertEqual(calculate_text_richness_score({}), (0.0, 0.0, 0.0))
        self.assertEqual(calculate_text_richness_score({"words": [{"text": ""}]}), (0.0, 0.0, 0.0))
        self.assertEqual(calculate_text_richness_score({"words": "bad"}), (0.0, 0.0, 0.0))
        self.assertEqual(
            calculate_text_richness_score({"words": [{"text": "x"}], "ocr_area_ratio": "bad"}),
            (0.0, 1 / 300, 0.0),
        )
        self.assertEqual(
            calculate_text_richness_score({"words": [{"text": "x"}]}, char_max=0, area_max_ratio=0),
            (5.0, 1.0, 1.0),
        )

    def test_dict_batch_and_run_dispatch_paths(self):
        class FakeRayDataset:
            def map_batches(self, fn, *, batch_format, batch_size):
                self.fn = fn
                self.batch_format = batch_format
                self.batch_size = batch_size
                return "mapped"

        op = OcrTextRichnessMapper(batch_size=9, auto_op_parallelism=False, num_proc=1)
        rows = op.process_batched({"ocr_result": [json.dumps(_ocr_payload("x"))]})
        empty = op.process_batched({})
        dataset = FakeRayDataset()

        self.assertEqual(rows["ocr_text"], [["x"]])
        self.assertEqual(empty["text_richness_score"], [])
        self.assertEqual(op.run(dataset), "mapped")
        self.assertEqual(dataset.batch_format, "pyarrow")
        self.assertEqual(dataset.batch_size, 9)

    def test_ocr_parser_accepts_bytes_lists_dicts_and_ignores_bad_points(self):
        op = OcrTextRichnessMapper(auto_op_parallelism=False, num_proc=1)
        payload = _ocr_payload("x")
        payload["words"][0]["det_points_relative"] = [{"x": "bad", "y": 1}, "bad-point"]

        from data_juicer.ops.mapper.text import ocr_text_richness_mapper as module

        self.assertEqual(module._parse_ocr_payload([json.dumps(_ocr_payload("x"))])["words"][0]["text"], "x")
        self.assertEqual(module._parse_ocr_payload(json.dumps([])), {})
        self.assertEqual(module._parse_ocr_payload(json.dumps(payload).encode())["words"][0]["text"], "x")
        self.assertEqual(module._parse_ocr_payload(payload), payload)
        self.assertEqual(op.process_single({"ocr_result": payload})["ocr_bbox"], [[]])


class VlmApiResponseMapperTest(unittest.TestCase):
    def test_image_bytes_are_sent_as_openai_compatible_multimodal_payload(self):
        calls = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append((url, payload, headers))
                return {"choices": [{"message": {"content": "{\"labels\": []}"}}]}

        op = CapturingMapper(
            image_key="images",
            output_key="ocr_answer",
            prompt_template="classify",
            model="seed-test",
            base_url="https://seed.example/v1",
            api_key="token",
            auto_op_parallelism=False,
            num_proc=1,
        )

        row = op.process_single({"images": [b"image-bytes"]})

        self.assertEqual(row["ocr_answer"], "{\"labels\": []}")
        url, payload, headers = calls[0]
        self.assertEqual(url, "https://seed.example/v1/chat/completions")
        self.assertEqual(headers["Authorization"], "Bearer token")
        self.assertEqual(payload["model"], "seed-test")
        content = payload["messages"][0]["content"]
        self.assertEqual(content[0], {"type": "text", "text": "classify"})
        encoded = content[1]["image_url"]["url"].split(",", 1)[1]
        self.assertEqual(base64.b64decode(encoded), b"image-bytes")

    def test_vlm_qps_metrics_are_emitted_for_success_and_error_without_model_tag(self):
        class CapturingMapper(VlmApiResponseMapper):
            def __init__(self, responses, **kwargs):
                super().__init__(**kwargs)
                self.responses = list(responses)

            def _post_json(self, url, payload, headers):
                response = self.responses.pop(0)
                if isinstance(response, Exception):
                    raise response
                return response

        op = CapturingMapper(
            [
                {"choices": [{"message": {"content": "ok"}}]},
                RuntimeError("vlm failed"),
            ],
            image_key="images",
            prompt_template="classify",
            model="seed-test",
            base_url="https://seed.example/v1",
            auto_op_parallelism=False,
            num_proc=1,
        )

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.emit_vlm_qps") as emit_mock:
            self.assertEqual(op.process_single({"images": [b"image-bytes"]})["ocr_answer"], "ok")
            self.assertEqual(op.process_single({"images": [b"image-bytes"]})["ocr_answer"], "")

        self.assertEqual(emit_mock.call_count, 2)
        success_tags = emit_mock.call_args_list[0].kwargs
        error_tags = emit_mock.call_args_list[1].kwargs
        self.assertEqual(success_tags["op_name"], "vlm_api_response_mapper")
        self.assertEqual(success_tags["target"], "seed.example")
        self.assertEqual(success_tags["method"], "/v1/chat/completions")
        self.assertEqual(success_tags["status"], "success")
        self.assertNotIn("model", success_tags)
        self.assertEqual(error_tags["status"], "error")

    def test_endpoint_pool_rotates_urls_and_keeps_endpoint_specific_headers(self):
        calls = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append((url, headers.get("Authorization")))
                return {"choices": [{"message": {"content": "ok"}}]}

        op = CapturingMapper(
            prompt_template="classify",
            model="seed-test",
            endpoint_pool=[
                {"base_url": "https://seed-a.example/v1", "api_key": "token-a"},
                {"base_url": "https://seed-b.example/v1", "api_key": "token-b", "weight": 2},
            ],
            auto_op_parallelism=False,
            num_proc=1,
        )

        for _ in range(4):
            self.assertEqual(op.process_single({"images": [b"image-bytes"]})["ocr_answer"], "ok")

        self.assertEqual(
            calls,
            [
                ("https://seed-a.example/v1/chat/completions", "Bearer token-a"),
                ("https://seed-b.example/v1/chat/completions", "Bearer token-b"),
                ("https://seed-b.example/v1/chat/completions", "Bearer token-b"),
                ("https://seed-a.example/v1/chat/completions", "Bearer token-a"),
            ],
        )

    def test_endpoint_pool_rejects_mixed_api_formats_without_explicit_api_format(self):
        with self.assertRaisesRegex(ValueError, "must not mix chat and responses"):
            VlmApiResponseMapper(
                model="m",
                endpoint_pool=[
                    {"base_url": "https://seed-a.example/v1", "endpoint": "/chat/completions"},
                    {"base_url": "https://seed-b.example/v1", "endpoint": "/responses"},
                ],
            )

    def test_endpoint_limiter_key_defaults_to_full_url_with_port_and_can_be_overridden(self):
        op = VlmApiResponseMapper(
            model="m",
            endpoint_pool=[
                "http://[2605:340:cd51:603::1]:8001/v1",
                {
                    "base_url": "http://[2605:340:cd51:603::1]:8002/v1",
                    "limiter_key": "serving-b",
                },
            ],
        )

        self.assertEqual(
            [op._endpoint_limiter_key(endpoint_config) for endpoint_config in op._endpoint_configs],
            [
                "http://[2605:340:cd51:603::1]:8001/v1/chat/completions",
                "serving-b",
            ],
        )

    def test_multiple_image_keys_are_recursively_expanded(self):
        calls = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append((url, payload, headers))
                return {"choices": [{"message": {"content": "ok"}}]}

        op = CapturingMapper(
            image_keys=["image_a", "image_b"],
            prompt_template="p",
            model="m",
            base_url="https://seed.example/v1",
            image_mime_type="image/jpeg",
            image_detail="high",
            auto_op_parallelism=False,
            num_proc=1,
        )

        row = op.process_single(
            {
                "image_a": [b"a"],
                "image_b": {"nested": [bytearray(b"b"), [memoryview(b"c")]]},
            }
        )

        self.assertEqual(row["ocr_answer"], "ok")
        content = calls[0][1]["messages"][0]["content"]
        image_parts = content[1:]
        self.assertEqual(len(image_parts), 3)
        self.assertTrue(image_parts[0]["image_url"]["url"].startswith("data:image/jpeg;base64,"))
        self.assertEqual(image_parts[0]["image_url"]["detail"], "high")
        self.assertEqual(base64.b64decode(image_parts[2]["image_url"]["url"].split(",", 1)[1]), b"c")

    def test_url_strings_are_used_directly_as_image_urls(self):
        calls = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append(payload)
                return {"choices": [{"message": {"content": "ok"}}]}

        data_url = "data:image/png;base64,abc"
        http_url = "https://example.com/image.png"
        op = CapturingMapper(prompt_template="p", model="m", base_url="https://seed.example/v1")

        op.process_single({"images": [http_url, data_url, "local-path-is-ignored"]})

        image_urls = [part["image_url"]["url"] for part in calls[0]["messages"][0]["content"][1:]]
        self.assertEqual(image_urls, [http_url, data_url])

    def test_custom_image_content_template_formats_image_parts(self):
        calls = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append(payload)
                return {"choices": [{"message": {"content": "ok"}}]}

        op = CapturingMapper(
            prompt_template="p",
            model="m",
            base_url="https://seed.example/v1",
            image_mime_type="image/jpeg",
            image_detail="low",
            image_content_template={
                "type": "input_image",
                "image_url": "{url}",
                "source": {
                    "type": "base64",
                    "media_type": "{mime_type}",
                    "data": "{base64}",
                    "detail": "{detail}",
                },
            },
        )

        op.process_single({"images": [b"jpeg-bytes"]})

        image_part = calls[0]["messages"][0]["content"][1]
        self.assertEqual(image_part["type"], "input_image")
        self.assertTrue(image_part["image_url"].startswith("data:image/jpeg;base64,"))
        self.assertEqual(image_part["source"]["media_type"], "image/jpeg")
        self.assertEqual(base64.b64decode(image_part["source"]["data"]), b"jpeg-bytes")
        self.assertEqual(image_part["source"]["detail"], "low")

    def test_prompt_template_uses_dollar_brace_fields_and_keeps_json_literals(self):
        calls = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append(payload)
                return {"choices": [{"message": {"content": "ok"}}]}

        system_op = CapturingMapper(
            system_prompt='return {"labels": []}',
            model="m",
            base_url="https://seed.example/v1",
        )
        system_op.process_single({"images": [b"a"], "labels": "unused"})
        self.assertEqual(calls[-1]["messages"][0], {"role": "system", "content": 'return {"labels": []}'})

        templated_op = CapturingMapper(
            prompt_template='return {"labels": []}; classify ${title}',
            model="m",
            base_url="https://seed.example/v1",
            error_key="error",
        )
        self.assertEqual(templated_op.process_single({"images": [b"a"], "title": "invoice"})["ocr_answer"], "ok")
        self.assertEqual(
            calls[-1]["messages"][0]["content"][0]["text"],
            'return {"labels": []}; classify invoice',
        )
        failed = templated_op.process_single({"images": [b"a"]})
        self.assertEqual(failed["ocr_answer"], "")
        self.assertIn("title", failed["error"])

    def test_responses_api_payload_uses_input_and_input_image_parts(self):
        calls = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append((url, payload, headers))
                return {"output": [{"content": [{"text": "custom response"}]}]}

        op = CapturingMapper(
            prompt_template="p",
            system_prompt="system",
            model="default-model",
            base_url="https://seed.example/v1/responses",
            temperature=0.0,
            extra_body={"temperature": 0.7, "model": "override-model", "metadata": {"task": "ocr"}},
            extra_headers={"X-Trace": "trace-id"},
            auto_op_parallelism=False,
            num_proc=1,
        )

        row = op.process_single({"images": [b"a"]})

        self.assertEqual(row["ocr_answer"], "custom response")
        url, payload, headers = calls[0]
        self.assertEqual(url, "https://seed.example/v1/responses")
        self.assertEqual(payload["model"], "override-model")
        self.assertEqual(payload["temperature"], 0.7)
        self.assertEqual(payload["metadata"], {"task": "ocr"})
        self.assertNotIn("messages", payload)
        self.assertEqual(payload["instructions"], "system")
        self.assertEqual(payload["input"][0]["role"], "user")
        content = payload["input"][0]["content"]
        self.assertEqual(content[0]["type"], "input_image")
        self.assertTrue(content[0]["image_url"].startswith("data:image/png;base64,"))
        self.assertEqual(content[1], {"type": "input_text", "text": "p"})
        self.assertEqual(headers["X-Trace"], "trace-id")

    def test_responses_api_prompt_template_can_add_text_context(self):
        calls = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append(payload)
                return {"output": [{"content": [{"text": "ok"}]}]}

        op = CapturingMapper(
            prompt_template="请结合补充说明回答：${hint}",
            model="m",
            base_url="https://seed.example/v1/responses",
        )

        row = op.process_single({"images": ["https://example.com/image.png"], "hint": "图片来自模型支持矩阵"})

        self.assertEqual(row["ocr_answer"], "ok")
        content = calls[0]["input"][0]["content"]
        self.assertEqual(content[0]["type"], "input_image")
        self.assertEqual(content[1], {"type": "input_text", "text": "请结合补充说明回答：图片来自模型支持矩阵"})

    def test_responses_api_accepts_url_images_and_maps_max_tokens(self):
        calls = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append((url, payload, headers))
                return {"output": [{"content": [{"text": "ok"}]}]}

        image_url = "https://example.com/image.png"
        op = CapturingMapper(
            prompt_template="describe",
            model="m",
            base_url="https://seed.example/v1",
            endpoint="/responses",
            max_tokens=32,
            image_detail="low",
        )

        row = op.process_single({"images": [image_url]})

        self.assertEqual(row["ocr_answer"], "ok")
        self.assertEqual(calls[0][0], "https://seed.example/v1/responses")
        self.assertEqual(calls[0][1]["max_output_tokens"], 32)
        self.assertNotIn("max_tokens", calls[0][1])
        image_part = calls[0][1]["input"][0]["content"][0]
        self.assertEqual(image_part, {"type": "input_image", "image_url": image_url, "detail": "low"})

    def test_responses_api_maps_first_class_response_parameters(self):
        calls = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append(payload)
                return {"output": [{"content": [{"text": "ok"}]}]}

        op = CapturingMapper(
            prompt_template="describe",
            model="m",
            base_url="https://seed.example/v1/responses",
            top_p=0.8,
            text={"format": {"type": "json_object"}},
            store=False,
            reasoning={"effort": "low"},
            thinking={"type": "disabled"},
            previous_response_id="resp_123",
        )

        row = op.process_single({"images": ["https://example.com/image.png"]})

        self.assertEqual(row["ocr_answer"], "ok")
        payload = calls[0]
        self.assertEqual(payload["top_p"], 0.8)
        self.assertEqual(payload["text"], {"format": {"type": "json_object"}})
        self.assertIs(payload["store"], False)
        self.assertEqual(payload["reasoning"], {"effort": "low"})
        self.assertEqual(payload["thinking"], {"type": "disabled"})
        self.assertEqual(payload["previous_response_id"], "resp_123")

    def test_responses_api_extracts_message_text_after_reasoning_items(self):
        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                return {
                    "output": [
                        {"type": "reasoning", "summary": [{"type": "summary_text", "text": "thinking"}]},
                        {
                            "type": "message",
                            "content": [
                                {"type": "output_text", "text": "final"},
                            ],
                        },
                    ]
                }

        op = CapturingMapper(
            prompt_template="describe",
            model="m",
            base_url="https://seed.example/v1/responses",
        )

        self.assertEqual(op.process_single({"images": [b"a"]})["ocr_answer"], "final")

    def test_batch_paths_error_handling_prompt_template_and_content_list(self):
        calls = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append((url, payload, headers))
                return {"choices": [{"message": {"content": [{"text": "a"}, {"text": "b"}, {"bad": "c"}]}}]}

        op = CapturingMapper(
            prompt_template="prompt-from-config",
            model="seed-config",
            base_url="https://seed.example/v1",
            max_tokens=32,
            auto_op_parallelism=False,
            num_proc=1,
        )
        output = op.process_batched({"images": [[bytearray(b"abc")]]})

        self.assertEqual(output["ocr_answer"], ["ab"])
        self.assertEqual(calls[0][1]["model"], "seed-config")
        self.assertEqual(calls[0][1]["max_tokens"], 32)
        self.assertEqual(calls[0][1]["messages"][0]["content"][0]["text"], "prompt-from-config")
        self.assertNotIn("Authorization", calls[0][2])

        empty = op.process_batched({})
        self.assertEqual(empty, {"ocr_answer": []})

    def test_chat_api_maps_first_class_chat_parameters(self):
        calls = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append(payload)
                return {"choices": [{"message": {"content": "ok"}}]}

        op = CapturingMapper(
            prompt_template="describe",
            model="m",
            base_url="https://seed.example/v1/chat/completions",
            top_p=0.6,
            thinking={"type": "disabled"},
            response_format={"type": "json_object"},
            stop=["END"],
            frequency_penalty=0.1,
            presence_penalty=0.2,
        )

        row = op.process_single({"images": ["https://example.com/image.png"]})

        self.assertEqual(row["ocr_answer"], "ok")
        payload = calls[0]
        self.assertEqual(payload["top_p"], 0.6)
        self.assertEqual(payload["thinking"], {"type": "disabled"})
        self.assertEqual(payload["response_format"], {"type": "json_object"})
        self.assertEqual(payload["stop"], ["END"])
        self.assertEqual(payload["frequency_penalty"], 0.1)
        self.assertEqual(payload["presence_penalty"], 0.2)

    def test_chat_api_uses_messages_template_with_recursive_field_rendering(self):
        calls = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append(payload)
                return {"choices": [{"message": {"content": "ok"}}]}

        op = CapturingMapper(
            messages_template=[
                {"role": "system", "content": "fixed system"},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "name=${user.name}; metadata=${metadata}"},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": "${image_bytes}",
                                "detail": "high",
                            },
                        },
                    ],
                },
            ],
            model="m",
            base_url="https://seed.example/v1/chat/completions",
        )

        row = op.process_single(
            {
                "user.name": "Alice",
                "metadata": {"类别": ["截图"], "score": 1},
                "image_bytes": b"abc",
            }
        )

        self.assertEqual(row["ocr_answer"], "ok")
        messages = calls[0]["messages"]
        self.assertEqual(messages[0], {"role": "system", "content": "fixed system"})
        self.assertEqual(messages[1]["content"][0]["text"], 'name=Alice; metadata={"类别": ["截图"], "score": 1}')
        self.assertEqual(messages[1]["content"][1]["image_url"]["url"], "data:image/png;base64,YWJj")

    def test_responses_api_uses_input_template_with_list_and_string_forms(self):
        calls = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append(payload)
                return {"output": [{"content": [{"text": "ok"}]}]}

        list_op = CapturingMapper(
            input_template=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "question=${question}; metadata=${metadata}"},
                        {"type": "input_image", "image_url": "${image_bytes}", "detail": "high"},
                    ],
                }
            ],
            model="m",
            base_url="https://seed.example/v1/responses",
        )
        self.assertEqual(
            list_op.process_single({"question": "what", "metadata": {"a": [1]}, "image_bytes": b"abc"})[
                "ocr_answer"
            ],
            "ok",
        )
        self.assertEqual(calls[0]["input"][0]["content"][0]["text"], 'question=what; metadata={"a": [1]}')
        self.assertEqual(calls[0]["input"][0]["content"][1]["image_url"], "data:image/png;base64,YWJj")

        string_op = CapturingMapper(
            input_template="question=${question}",
            model="m",
            base_url="https://seed.example/v1/responses",
        )
        self.assertEqual(string_op.process_single({"question": "what"})["ocr_answer"], "ok")
        self.assertEqual(calls[1]["input"], "question=what")

    def test_template_validation_and_missing_field_errors(self):
        with self.assertRaisesRegex(ValueError, "messages_template can only be used"):
            VlmApiResponseMapper(
                messages_template=[{"role": "user", "content": "x"}],
                model="m",
                base_url="https://seed.example/v1/responses",
            )
        with self.assertRaisesRegex(ValueError, "input_template can only be used"):
            VlmApiResponseMapper(
                input_template="x",
                model="m",
                base_url="https://seed.example/v1/chat/completions",
            )
        with self.assertRaisesRegex(ValueError, "messages_template cannot be combined"):
            VlmApiResponseMapper(
                messages_template=[{"role": "user", "content": "x"}],
                prompt_template="x",
                model="m",
                base_url="https://seed.example/v1/chat/completions",
            )
        with self.assertRaisesRegex(ValueError, "input_template must be a non-empty"):
            VlmApiResponseMapper(input_template=[], model="m", base_url="https://seed.example/v1/responses")

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                return {"choices": [{"message": {"content": "ok"}}]}

        failed = CapturingMapper(
            messages_template=[{"role": "user", "content": "${missing}"}],
            model="m",
            base_url="https://seed.example/v1/chat/completions",
            error_key="error",
        ).process_single({})
        self.assertEqual(failed["ocr_answer"], "")
        self.assertIn("missing", failed["error"])

    def test_ray_job_rate_limiter_shares_rpm_per_model(self):
        limiter = _RayJobVlmRateLimiter()
        clock = {"now": 0.0}
        sleeps = []

        async def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        async def scenario():
            await limiter.acquire("model-a", rpm=1, tpm=None, estimated_tokens=0)
            await limiter.acquire("model-a", rpm=1, tpm=None, estimated_tokens=0)

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.asyncio.sleep", side_effect=fake_sleep):
                asyncio.run(scenario())

        self.assertEqual(sleeps, [60.0])

    def test_ray_job_rate_limiter_keeps_models_independent(self):
        limiter = _RayJobVlmRateLimiter()
        sleeps = []

        async def fake_sleep(seconds):
            sleeps.append(seconds)

        async def scenario():
            await limiter.acquire("model-a", rpm=1, tpm=None, estimated_tokens=0)
            await limiter.acquire("model-b", rpm=1, tpm=None, estimated_tokens=0)

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.asyncio.sleep", side_effect=fake_sleep):
            asyncio.run(scenario())

        self.assertEqual(sleeps, [])

    def test_ray_job_rate_limiter_balances_endpoint_pool_globally(self):
        limiter = _RayJobVlmRateLimiter()

        async def scenario():
            return [
                await limiter.acquire_endpoint(
                    "pool-a",
                    ["https://seed-a.example/v1/chat/completions", "https://seed-b.example/v1/chat/completions"],
                    [1, 2],
                    "model-a",
                    [None, None],
                    [None, None],
                    estimated_tokens=0,
                )
                for _ in range(4)
            ]

        self.assertEqual(asyncio.run(scenario()), [0, 1, 1, 0])

    def test_ray_job_rate_limiter_keeps_endpoints_independent_for_same_model(self):
        limiter = _RayJobVlmRateLimiter()
        sleeps = []

        async def fake_sleep(seconds):
            sleeps.append(seconds)

        async def scenario():
            await limiter.acquire("model-a", rpm=1, tpm=None, estimated_tokens=0, limiter_key="endpoint-a")
            await limiter.acquire("model-a", rpm=1, tpm=None, estimated_tokens=0, limiter_key="endpoint-b")

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.asyncio.sleep", side_effect=fake_sleep):
            asyncio.run(scenario())

        self.assertEqual(sleeps, [])

    def test_ray_job_rate_limiter_smooths_bursts_within_window(self):
        limiter = _RayJobVlmRateLimiter()
        clock = {"now": 0.0}
        sleeps = []

        async def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        async def scenario():
            await limiter.acquire("model-a", rpm=60, tpm=None, estimated_tokens=0)
            await limiter.acquire("model-a", rpm=60, tpm=None, estimated_tokens=0)

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.asyncio.sleep", side_effect=fake_sleep):
                asyncio.run(scenario())

        self.assertEqual(sleeps, [1.0])

    def test_ray_job_rate_limiter_waits_for_tpm_window(self):
        limiter = _RayJobVlmRateLimiter()
        clock = {"now": 0.0}
        sleeps = []

        async def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        async def scenario():
            await limiter.acquire("model-a", rpm=None, tpm=10, estimated_tokens=6)
            await limiter.acquire("model-a", rpm=None, tpm=10, estimated_tokens=6)

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.asyncio.sleep", side_effect=fake_sleep):
                asyncio.run(scenario())

        self.assertEqual(sleeps, [60.0])

    def test_ray_job_rate_limiter_uses_more_conservative_limit_for_same_model(self):
        limiter = _RayJobVlmRateLimiter()
        clock = {"now": 0.0}
        sleeps = []

        async def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        async def scenario():
            await limiter.acquire("model-a", rpm=10, tpm=None, estimated_tokens=0)
            await limiter.acquire("model-a", rpm=1, tpm=None, estimated_tokens=0)

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.asyncio.sleep", side_effect=fake_sleep):
                asyncio.run(scenario())

        self.assertEqual(sleeps, [60.0])

    def test_local_rpm_and_tpm_rate_limits_wait_before_request(self):
        calls = []
        clock = {"now": 0.0}
        sleeps = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append((clock["now"], payload))
                return {"choices": [{"message": {"content": "ok"}}]}

        def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        op = CapturingMapper(
            prompt_template="classify",
            model="m",
            base_url="https://seed.example/v1",
            rpm=1,
            tpm=10,
            estimated_tokens_per_request=6,
        )

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.sleep", side_effect=fake_sleep):
                self.assertEqual(op.process_single({"images": [b"a"]})["ocr_answer"], "ok")
                self.assertEqual(op.process_single({"images": [b"b"]})["ocr_answer"], "ok")

        self.assertEqual([call[0] for call in calls], [0.0, 60.0])
        self.assertEqual(sleeps, [60.0])

    def test_endpoint_pool_local_rate_limits_are_endpoint_specific(self):
        calls = []
        clock = {"now": 0.0}
        sleeps = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append((url, clock["now"]))
                return {"choices": [{"message": {"content": "ok"}}]}

        def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        op = CapturingMapper(
            prompt_template="classify",
            model="m",
            endpoint_pool=[
                {"base_url": "https://seed-a.example/v1", "rpm": 1, "tpm": 10},
                {"base_url": "https://seed-b.example/v1", "rpm": 1, "tpm": 10},
            ],
            estimated_tokens_per_request=6,
        )

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.sleep", side_effect=fake_sleep):
                for _ in range(4):
                    self.assertEqual(op.process_single({"images": [b"a"]})["ocr_answer"], "ok")

        self.assertEqual(
            calls,
            [
                ("https://seed-a.example/v1/chat/completions", 0.0),
                ("https://seed-b.example/v1/chat/completions", 0.0),
                ("https://seed-a.example/v1/chat/completions", 60.0),
                ("https://seed-b.example/v1/chat/completions", 60.0),
            ],
        )
        self.assertEqual(sleeps, [60.0])

    def test_endpoint_pool_local_adaptive_penalty_is_endpoint_specific_and_recovers(self):
        clock = {"now": 0.0}

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                if "seed-a" in url:
                    raise _VlmApiHttpError(429, "too many requests")
                return {"choices": [{"message": {"content": "ok"}}]}

        op = CapturingMapper(
            prompt_template="classify",
            model="m",
            endpoint_pool=[
                {"base_url": "https://seed-a.example/v1", "rpm": 60, "tpm": 100},
                {"base_url": "https://seed-b.example/v1", "rpm": 60, "tpm": 100},
            ],
            estimated_tokens_per_request=10,
            adaptive_rate_limit=True,
            rate_limit_retry_attempts=0,
            error_key="error",
        )

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            row_a = op.process_single({"images": [b"a"]})
            row_b = op.process_single({"images": [b"b"]})
            state_a = op._endpoint_local_rate_limit_state(op._endpoint_configs[0])
            state_b = op._endpoint_local_rate_limit_state(op._endpoint_configs[1])
            self.assertEqual(state_a["adaptive_effective_limits"], {"rpm": 30.0, "tpm": 50.0})
            self.assertEqual(state_b["adaptive_effective_limits"], {"rpm": 60.0, "tpm": 100.0})
            clock["now"] = 300.0
            self.assertEqual(op._current_endpoint_local_rate_limits(op._endpoint_configs[0], state_a), (36.0, 60.0))

        self.assertEqual(row_a["ocr_answer"], "")
        self.assertIn("HTTP 429", row_a["error"])
        self.assertEqual(row_b["ocr_answer"], "ok")

    def test_local_rate_limiter_smooths_bursts_within_window(self):
        calls = []
        clock = {"now": 0.0}
        sleeps = []

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                calls.append(clock["now"])
                return {"choices": [{"message": {"content": "ok"}}]}

        def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        op = CapturingMapper(
            prompt_template="classify",
            model="m",
            base_url="https://seed.example/v1",
            rpm=60,
        )

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.sleep", side_effect=fake_sleep):
                self.assertEqual(op.process_single({"images": [b"a"]})["ocr_answer"], "ok")
                self.assertEqual(op.process_single({"images": [b"b"]})["ocr_answer"], "ok")

        self.assertEqual(calls, [0.0, 1.0])
        self.assertEqual(sleeps, [1.0])

    def test_default_rate_limit_retry_retries_429_once_without_adaptive_penalty(self):
        clock = {"now": 0.0}
        calls = []
        sleeps = []

        class CapturingMapper(VlmApiResponseMapper):
            def __init__(self, responses, request_durations, **kwargs):
                super().__init__(**kwargs)
                self.responses = list(responses)
                self.request_durations = list(request_durations)

            def _post_json(self, url, payload, headers):
                calls.append(clock["now"])
                clock["now"] += self.request_durations.pop(0)
                response = self.responses.pop(0)
                if isinstance(response, Exception):
                    raise response
                return response

        def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        op = CapturingMapper(
            [
                _VlmApiHttpError(429, "too many requests"),
                {"choices": [{"message": {"content": "ok"}}]},
            ],
            [2.0, 3.0],
            prompt_template="classify",
            model="m",
            base_url="https://seed.example/v1",
            rpm=60,
            tpm=100,
            estimated_tokens_per_request=10,
            error_key="error",
        )

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.sleep", side_effect=fake_sleep):
                row = op.process_single({"images": [b"a"]})

        self.assertEqual(row["ocr_answer"], "ok")
        self.assertEqual(row["error"], "")
        self.assertEqual(calls, [0.0, 6.0])
        self.assertEqual(sleeps, [2.0, 2.0])
        self.assertEqual(op._current_local_rate_limits(), (60, 100))
        self.assertIsNone(op._adaptive_last_limited_at)

    def test_rate_limit_retry_attempts_zero_keeps_429_as_row_error(self):
        class FailingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                raise _VlmApiHttpError(429, "too many requests")

        op = FailingMapper(
            prompt_template="classify",
            model="m",
            base_url="https://seed.example/v1",
            rpm=60,
            tpm=100,
            estimated_tokens_per_request=10,
            error_key="error",
            rate_limit_retry_attempts=0,
        )

        row = op.process_single({"images": [b"a"]})

        self.assertEqual(row["ocr_answer"], "")
        self.assertIn("HTTP 429", row["error"])
        self.assertEqual(op._current_local_rate_limits(), (60, 100))
        self.assertIsNone(op._adaptive_last_limited_at)

    def test_rate_limit_retry_backoff_scales_with_vlm_request_duration(self):
        clock = {"now": 0.0}
        calls = []
        sleeps = []

        class CapturingMapper(VlmApiResponseMapper):
            def __init__(self, responses, request_durations, **kwargs):
                super().__init__(**kwargs)
                self.responses = list(responses)
                self.request_durations = list(request_durations)

            def _post_json(self, url, payload, headers):
                calls.append(clock["now"])
                clock["now"] += self.request_durations.pop(0)
                response = self.responses.pop(0)
                if isinstance(response, Exception):
                    raise response
                return response

        def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        op = CapturingMapper(
            [
                _VlmApiHttpError(429, "first limit"),
                _VlmApiHttpError(429, "second limit"),
                {"choices": [{"message": {"content": "ok"}}]},
            ],
            [2.0, 3.0, 4.0],
            prompt_template="classify",
            model="m",
            base_url="https://seed.example/v1",
            rate_limit_retry_attempts=2,
        )

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.sleep", side_effect=fake_sleep):
                row = op.process_single({"images": [b"a"]})

        self.assertEqual(row["ocr_answer"], "ok")
        self.assertEqual(calls, [0.0, 4.0, 13.0])
        self.assertEqual(sleeps, [2.0, 6.0])

    def test_adaptive_rate_limit_halves_limits_after_http_429_and_slows_later_requests(self):
        clock = {"now": 0.0}
        calls = []
        sleeps = []

        class CapturingMapper(VlmApiResponseMapper):
            def __init__(self, responses, **kwargs):
                super().__init__(**kwargs)
                self.responses = list(responses)

            def _post_json(self, url, payload, headers):
                calls.append(clock["now"])
                response = self.responses.pop(0)
                if isinstance(response, Exception):
                    raise response
                return response

        def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        op = CapturingMapper(
            [
                _VlmApiHttpError(429, "too many requests"),
                {"choices": [{"message": {"content": "ok-1"}}]},
                {"choices": [{"message": {"content": "ok-2"}}]},
            ],
            prompt_template="classify",
            model="m",
            base_url="https://seed.example/v1",
            rpm=60,
            tpm=100,
            estimated_tokens_per_request=10,
            adaptive_rate_limit=True,
            error_key="error",
            rate_limit_retry_attempts=0,
        )

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.sleep", side_effect=fake_sleep):
                self.assertEqual(op.process_single({"images": [b"a"]})["ocr_answer"], "")
                self.assertEqual(op._current_local_rate_limits(), (30.0, 50.0))
                self.assertEqual(op.process_single({"images": [b"b"]})["ocr_answer"], "ok-1")
                self.assertEqual(op.process_single({"images": [b"c"]})["ocr_answer"], "ok-2")

        self.assertEqual(calls, [0.0, 6.0, 18.0])
        self.assertEqual(sleeps, [6.0, 12.0])

    def test_adaptive_rate_limit_ignores_http_500_and_non_http_errors(self):
        class FailingMapper(VlmApiResponseMapper):
            def __init__(self, error, **kwargs):
                super().__init__(**kwargs)
                self.error = error

            def _post_json(self, url, payload, headers):
                raise self.error

        for error in [_VlmApiHttpError(500, "server error"), RuntimeError("network error")]:
            with self.subTest(error=error):
                op = FailingMapper(
                    error,
                    prompt_template="classify",
                    model="m",
                    base_url="https://seed.example/v1",
                    rpm=60,
                    tpm=100,
                    estimated_tokens_per_request=10,
                    adaptive_rate_limit=True,
                    error_key="error",
                )
                row = op.process_single({"images": [b"a"]})

                self.assertEqual(row["ocr_answer"], "")
                self.assertEqual(op._current_local_rate_limits(), (60.0, 100.0))
                self.assertIsNone(op._adaptive_last_limited_at)

    def test_ray_job_adaptive_rate_limiter_shares_state_per_model_only(self):
        limiter = _RayJobVlmRateLimiter()
        clock = {"now": 0.0}

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            limiter.penalize("model-a", rpm=100, tpm=1000)
            self.assertEqual(limiter.effective_limits("model-a", rpm=100, tpm=1000), {"rpm": 50.0, "tpm": 500.0})
            self.assertEqual(limiter.effective_limits("model-a", rpm=100, tpm=1000), {"rpm": 50.0, "tpm": 500.0})
            self.assertEqual(limiter.effective_limits("model-b", rpm=100, tpm=1000), {"rpm": 100.0, "tpm": 1000.0})

    def test_ray_job_rate_limiter_snapshot_reports_configured_effective_and_window_state(self):
        limiter = _RayJobVlmRateLimiter()
        clock = {"now": 0.0}
        events = []
        values = []

        async def scenario():
            await limiter.acquire(
                "model-a",
                rpm=100,
                tpm=1000,
                estimated_tokens=25,
                adaptive_rate_limit=True,
                limiter_key="seed.example",
            )

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.logger.info") as log_info:
                with patch(
                    "data_juicer.ops.mapper.qa.vlm_api_response_mapper.emit_vlm_rate_limit_event",
                    side_effect=lambda **kwargs: events.append(kwargs),
                ):
                    with patch(
                        "data_juicer.ops.mapper.qa.vlm_api_response_mapper.emit_vlm_rate_limit_value",
                        side_effect=lambda **kwargs: values.append(kwargs),
                    ):
                        asyncio.run(scenario())
                        penalty = limiter.penalize(
                            "model-a",
                            rpm=100,
                            tpm=1000,
                            target="seed.example",
                            method="/v1/chat/completions",
                            limiter_key="seed.example",
                        )
                        snapshot = limiter.snapshot()

        self.assertEqual(penalty["configured"], {"rpm": 100.0, "tpm": 1000.0})
        self.assertEqual(penalty["old_effective"], {"rpm": 100.0, "tpm": 1000.0})
        self.assertEqual(snapshot["model-a@@seed.example"]["effective"], {"rpm": 50.0, "tpm": 500.0})
        self.assertEqual(snapshot["model-a@@seed.example"]["window"]["requests"], 1)
        self.assertEqual(snapshot["model-a@@seed.example"]["window"]["tokens"], 25)
        self.assertTrue(any(call.args[0].startswith("VlmApiResponseMapper adaptive rate limit") for call in log_info.call_args_list))
        self.assertEqual(events[0]["event"], "penalty")
        self.assertEqual(events[0]["extra_tags"]["limiter_key"], "seed.example")
        self.assertIn("effective_rpm", {item["metric"] for item in values})
        self.assertIn("configured_tpm", {item["metric"] for item in values})

    def test_adaptive_rate_limit_recovers_after_quiet_period_without_exceeding_configured_limit(self):
        limiter = _RayJobVlmRateLimiter()
        clock = {"now": 0.0}

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.time.monotonic", side_effect=lambda: clock["now"]):
            limiter.penalize("model-a", rpm=100, tpm=None)
            self.assertEqual(limiter.effective_limits("model-a", rpm=100, tpm=None)["rpm"], 50.0)
            clock["now"] = 299.0
            self.assertEqual(limiter.effective_limits("model-a", rpm=100, tpm=None)["rpm"], 50.0)
            clock["now"] = 300.0
            self.assertEqual(limiter.effective_limits("model-a", rpm=100, tpm=None)["rpm"], 60.0)
            clock["now"] = 600.0
            self.assertEqual(limiter.effective_limits("model-a", rpm=100, tpm=None)["rpm"], 72.0)
            clock["now"] = 3600.0
            self.assertEqual(limiter.effective_limits("model-a", rpm=100, tpm=None)["rpm"], 100.0)

    def test_local_adaptive_rate_limit_logs_and_emits_effective_limit_metrics(self):
        class FailingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                raise _VlmApiHttpError(429, "too many requests")

        events = []
        values = []
        op = FailingMapper(
            prompt_template="classify",
            model="m",
            base_url="https://seed.example/v1",
            rpm=60,
            tpm=100,
            estimated_tokens_per_request=10,
            adaptive_rate_limit=True,
            rate_limit_retry_attempts=0,
            error_key="error",
        )

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.logger.info") as log_info:
            with patch(
                "data_juicer.ops.mapper.qa.vlm_api_response_mapper.emit_vlm_rate_limit_event",
                side_effect=lambda **kwargs: events.append(kwargs),
            ):
                with patch(
                    "data_juicer.ops.mapper.qa.vlm_api_response_mapper.emit_vlm_rate_limit_value",
                    side_effect=lambda **kwargs: values.append(kwargs),
                ):
                    row = op.process_single({"images": [b"a"]})

        self.assertEqual(row["ocr_answer"], "")
        self.assertIn("HTTP 429", row["error"])
        self.assertEqual(op._current_local_rate_limits(), (30.0, 50.0))
        self.assertTrue(any("limiter_key={}" in call.args[0] for call in log_info.call_args_list))
        self.assertEqual([event["event"] for event in events], ["429", "penalty", "exhausted"])
        metrics = {item["metric"]: item["value"] for item in values}
        self.assertEqual(metrics["effective_rpm"], 30.0)
        self.assertEqual(metrics["effective_tpm"], 50.0)

    def test_vlm_adaptive_rate_limit_config_loads_real_op(self):
        ops = load_ops(
            [
                {
                    "vlm_api_response_mapper": {
                        "prompt_template": "classify",
                        "model": "m",
                        "base_url": "https://seed.example/v1",
                        "rpm": 60,
                        "adaptive_rate_limit": True,
                        "rate_limit_retry_attempts": 2,
                    }
                }
            ],
            OPEnvManager(min_common_dep_num_to_combine=0),
        )

        self.assertEqual(len(ops), 1)
        self.assertIsInstance(ops[0], VlmApiResponseMapper)
        self.assertTrue(ops[0].adaptive_rate_limit)
        self.assertEqual(ops[0].rate_limit_retry_attempts, 2)

    def test_vlm_endpoint_pool_config_loads_real_op(self):
        ops = load_ops(
            [
                {
                    "vlm_api_response_mapper": {
                        "prompt_template": "classify",
                        "model": "m",
                        "endpoint_pool": [
                            {"base_url": "https://seed-a.example/v1"},
                            {"base_url": "https://seed-b.example/v1", "weight": 2},
                        ],
                    }
                }
            ],
            OPEnvManager(min_common_dep_num_to_combine=0),
        )

        self.assertEqual(len(ops), 1)
        self.assertIsInstance(ops[0], VlmApiResponseMapper)
        self.assertEqual(
            [ops[0]._api_url(endpoint_config) for endpoint_config in ops[0]._endpoint_configs],
            [
                "https://seed-a.example/v1/chat/completions",
                "https://seed-b.example/v1/chat/completions",
            ],
        )

    def test_ray_run_initializes_global_rate_limiter_and_acquires_before_request(self):
        order = []

        class FakeActorMethod:
            def __init__(self, name):
                self.name = name
                self.calls = []

            def remote(self, *args):
                self.calls.append(args)
                order.append(self.name)
                return f"{self.name}-ref"

        class FakeActor:
            def __init__(self):
                self.register = FakeActorMethod("register")
                self.acquire = FakeActorMethod("acquire")

        class FakeRemoteActorClass:
            def __init__(self, actor):
                self.actor = actor
                self.options_kwargs = None

            def options(self, **kwargs):
                self.options_kwargs = kwargs
                return self

            def remote(self):
                return self.actor

        class FakeRuntimeContext:
            def get_job_id(self):
                return "job-1"

        class FakeRay:
            def __init__(self):
                self.actor = FakeActor()
                self.remote_actor_class = FakeRemoteActorClass(self.actor)
                self.get_refs = []

            def is_initialized(self):
                return True

            def get_runtime_context(self):
                return FakeRuntimeContext()

            def get_actor(self, name):
                raise ValueError(name)

            def remote(self, cls):
                self.remote_cls = cls
                return self.remote_actor_class

            def get(self, ref):
                self.get_refs.append(ref)
                return None

        class FakeRayDataset:
            def map_batches(self, fn, **kwargs):
                self.fn = fn
                self.map_kwargs = kwargs
                return "mapped"

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                order.append("post")
                return {"choices": [{"message": {"content": "ok"}}]}

        fake_ray = FakeRay()
        op = CapturingMapper(
            prompt_template="abcd",
            model="model-a",
            base_url="https://seed.example/v1",
            rpm=2,
            max_tokens=7,
            batch_size=3,
        )
        dataset = FakeRayDataset()

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper._try_import_ray", return_value=fake_ray):
            self.assertEqual(op.run(dataset), "mapped")
            self.assertEqual(op._rate_limiter_actor_name, "dj_vlm_rate_limiter_job_1")
            self.assertEqual(
                fake_ray.remote_actor_class.options_kwargs,
                {"name": "dj_vlm_rate_limiter_job_1", "num_cpus": 0},
            )
            self.assertEqual(
                fake_ray.actor.register.calls,
                [("model-a", 2, None, "https://seed.example/v1/chat/completions")],
            )
            self.assertEqual(op.process_single({"images": [b"a", b"b"]})["ocr_answer"], "ok")

        self.assertEqual(
            fake_ray.actor.acquire.calls,
            [("model-a", 2, None, 0, False, "https://seed.example/v1/chat/completions")],
        )
        self.assertEqual(order[-2:], ["acquire", "post"])

    def test_ray_endpoint_pool_uses_global_actor_to_choose_endpoint(self):
        order = []

        class FakeActorMethod:
            def __init__(self, name):
                self.name = name
                self.calls = []

            def remote(self, *args):
                self.calls.append(args)
                order.append(self.name)
                return f"{self.name}-ref"

        class FakeActor:
            def __init__(self):
                self.register = FakeActorMethod("register")
                self.acquire = FakeActorMethod("acquire")
                self.acquire_endpoint = FakeActorMethod("acquire_endpoint")

        class FakeRemoteActorClass:
            def __init__(self, actor):
                self.actor = actor

            def options(self, **kwargs):
                return self

            def remote(self):
                return self.actor

        class FakeRuntimeContext:
            def get_job_id(self):
                return "job-1"

        class FakeRay:
            def __init__(self):
                self.actor = FakeActor()

            def is_initialized(self):
                return True

            def get_runtime_context(self):
                return FakeRuntimeContext()

            def get_actor(self, name):
                raise ValueError(name)

            def remote(self, cls):
                return FakeRemoteActorClass(self.actor)

            def get(self, ref):
                return 1 if ref == "acquire_endpoint-ref" else None

        class FakeRayDataset:
            def map_batches(self, fn, **kwargs):
                return "mapped"

        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                order.append(url)
                return {"choices": [{"message": {"content": "ok"}}]}

        fake_ray = FakeRay()
        op = CapturingMapper(
            prompt_template="abcd",
            model="model-a",
            endpoint_pool=[
                "https://seed-a.example/v1",
                {"base_url": "https://seed-b.example/v1", "weight": 2, "tpm": 100},
            ],
            batch_size=3,
        )

        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper._try_import_ray", return_value=fake_ray):
            self.assertEqual(op.run(FakeRayDataset()), "mapped")
            self.assertEqual(op.process_single({"images": [b"a"]})["ocr_answer"], "ok")

        self.assertEqual(
            fake_ray.actor.acquire_endpoint.calls,
            [
                (
                    "https://seed-a.example/v1/chat/completions|https://seed-b.example/v1/chat/completions",
                    [
                        "https://seed-a.example/v1/chat/completions",
                        "https://seed-b.example/v1/chat/completions",
                    ],
                    [1, 2],
                    "model-a",
                    [None, None],
                    [None, 100],
                    5121,
                    False,
                )
            ],
        )
        self.assertEqual(order[-1], "https://seed-b.example/v1/chat/completions")

    def test_tpm_estimate_skips_base64_payload_and_counts_internal_image_fallback(self):
        op = VlmApiResponseMapper(
            prompt_template="abcd",
            model="m",
            base_url="https://seed.example/v1",
            max_tokens=7,
        )
        payload = op._request_payload({"images": [b"a", b"b"]})

        self.assertEqual(op._estimate_request_tokens(payload, {"images": [b"a", b"b"]}), 10248)

    def test_rate_limit_validation(self):
        for kwargs in [
            {"rpm": 0},
            {"tpm": 0},
            {"estimated_tokens_per_request": 0},
            {"rate_limit_retry_attempts": -1},
        ]:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, next(iter(kwargs))):
                    VlmApiResponseMapper(**kwargs)
        self.assertEqual(VlmApiResponseMapper().rate_limit_retry_attempts, 1)

    def test_removed_image_token_rate_limit_knobs_are_rejected(self):
        for kwargs in [
            {"image_tokens_per_image": 5120},
            {"image_token_divisor": 1764},
            {"max_image_tokens": 5120},
        ]:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(TypeError, "estimated_tokens_per_request"):
                    VlmApiResponseMapper(**kwargs)

    def test_response_path_raw_error_and_fail_on_error_paths(self):
        class CapturingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                return {"choices": [{"message": {"content": "ok"}}]}

        op = CapturingMapper(
            prompt_template="p",
            model="m",
            base_url="https://seed.example/v1",
            raw_response_key="raw",
            error_key="error",
        )

        row = op.process_single({"images": [b"a"]})
        self.assertEqual(row["ocr_answer"], "ok")
        self.assertEqual(json.loads(row["raw"])["choices"][0]["message"]["content"], "ok")
        self.assertEqual(row["error"], "")

        missing_path_op = CapturingMapper(
            prompt_template="p",
            model="m",
            base_url="https://seed.example/v1",
            response_path="choices.1.message.content",
            raw_response_key="raw",
            error_key="error",
        )
        failed = missing_path_op.process_single({"images": [b"a"]})
        self.assertEqual(failed["ocr_answer"], "")
        self.assertEqual(json.loads(failed["raw"])["choices"][0]["message"]["content"], "ok")
        self.assertIn("choices.1.message.content", failed["error"])

        with self.assertRaisesRegex(KeyError, "choices.1.message.content"):
            CapturingMapper(
                prompt_template="p",
                model="m",
                base_url="https://seed.example/v1",
                response_path="choices.1.message.content",
                fail_on_error=True,
            ).process_single({"images": [b"a"]})

    def test_arrow_batch_empty_choices_missing_config_and_fail_on_error_paths(self):
        class EmptyChoicesMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                return {"choices": []}

        op = EmptyChoicesMapper(
            prompt_template="p",
            model="m",
            base_url="https://seed.example/v1/chat/completions",
            auto_op_parallelism=False,
            num_proc=1,
        )
        schema = pa.schema([pa.field("images", pa.list_(pa.binary()))])
        table = pa.Table.from_pylist([{"images": [b"a"]}], schema=schema)
        output = op.process_batched(table)

        self.assertEqual(output.to_pylist()[0]["ocr_answer"], "")
        self.assertEqual(output.schema.field("images").type, pa.list_(pa.binary()))

        self.assertEqual(VlmApiResponseMapper(prompt_template="p").process_single({"images": []})["ocr_answer"], "")
        with self.assertRaisesRegex(ValueError, "image bytes"):
            VlmApiResponseMapper(prompt_template="p", fail_on_error=True).process_single({"images": []})
        with self.assertRaisesRegex(RuntimeError, "base_url"):
            VlmApiResponseMapper(prompt_template="p", model="m")._api_url()
        with self.assertRaisesRegex(RuntimeError, "model"):
            VlmApiResponseMapper(prompt_template="p", base_url="https://seed.example/v1")._model()

    def test_invalid_api_format_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "api_format"):
            VlmApiResponseMapper(api_format="bad")

    def test_arrow_batch_keeps_output_schema_stable_with_raw_and_error_columns(self):
        class AlternatingMapper(VlmApiResponseMapper):
            def _post_json(self, url, payload, headers):
                prompt = payload["messages"][-1]["content"][0]["text"]
                if prompt == "fail":
                    raise RuntimeError("request failed")
                return {"choices": [{"message": {"content": [{"text": prompt}]}}]}

        op = AlternatingMapper(
            prompt_template="${title}",
            model="m",
            base_url="https://seed.example/v1",
            raw_response_key="raw_response",
            error_key="error",
            auto_op_parallelism=False,
            num_proc=1,
        )
        schema = pa.schema(
            [
                pa.field("title", pa.string()),
                pa.field("images", pa.list_(pa.binary())),
            ]
        )
        table = pa.Table.from_pylist(
            [
                {"title": "ok", "images": [b"a"]},
                {"title": "fail", "images": [b"b"]},
                {"title": "ok2", "images": [b"c"]},
            ],
            schema=schema,
        )

        output = op.process_batched(table)

        self.assertEqual(output.schema.field("title").type, pa.string())
        self.assertEqual(output.schema.field("images").type, pa.list_(pa.binary()))
        self.assertEqual(output.schema.field("ocr_answer").type, pa.string())
        self.assertEqual(output.schema.field("raw_response").type, pa.string())
        self.assertEqual(output.schema.field("error").type, pa.string())
        rows = output.to_pylist()
        self.assertEqual([row["ocr_answer"] for row in rows], ["ok", "", "ok2"])
        self.assertIn("request failed", rows[1]["error"])

    def test_post_json_success_and_http_error_are_wrapped(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'{"choices": []}'

        op = VlmApiResponseMapper(prompt_template="p")
        with patch("urllib.request.urlopen", return_value=FakeResponse()):
            self.assertEqual(op._post_json("https://seed.example/v1", {"a": 1}, {}), {"choices": []})

        class FakeHttpError(urllib.error.HTTPError):
            def read(self):
                return b"bad"

        with patch(
            "urllib.request.urlopen",
            side_effect=FakeHttpError("https://seed.example/v1", 500, "bad", hdrs=None, fp=None),
        ):
            with self.assertRaisesRegex(RuntimeError, "HTTP 500"):
                op._post_json("https://seed.example/v1", {"a": 1}, {})

    def test_run_dispatches_to_ray_map_batches(self):
        class FakeRayDataset:
            def __init__(self):
                self.repartition_kwargs = None
                self.map_kwargs = None

            def repartition(self, **kwargs):
                self.repartition_kwargs = kwargs
                return self

            def map_batches(self, fn, **kwargs):
                self.fn = fn
                self.map_kwargs = kwargs
                return "mapped"

        op = VlmApiResponseMapper(
            prompt_template="p",
            batch_size=3,
            num_proc=64,
            num_cpus=1,
            num_gpus=0,
            memory=1024,
            runtime_env={"env_vars": {"X": "1"}},
            repartition_num_blocks=128,
            auto_op_parallelism=False,
        )
        dataset = FakeRayDataset()

        self.assertEqual(op.run(dataset), "mapped")
        self.assertEqual(dataset.repartition_kwargs, {"num_blocks": 128, "shuffle": False})
        self.assertEqual(dataset.map_kwargs["batch_format"], "pyarrow")
        self.assertEqual(dataset.map_kwargs["batch_size"], 3)
        self.assertEqual(dataset.map_kwargs["concurrency"], 64)
        self.assertEqual(dataset.map_kwargs["num_cpus"], 1)
        self.assertEqual(dataset.map_kwargs["num_gpus"], 0)
        self.assertEqual(dataset.map_kwargs["memory"], 1024)
        self.assertEqual(dataset.map_kwargs["runtime_env"], {"env_vars": {"X": "1"}})

    def test_constructor_validates_vlm_repartition_num_blocks(self):
        with self.assertRaisesRegex(ValueError, "repartition_num_blocks"):
            VlmApiResponseMapper(prompt_template="p", repartition_num_blocks=0)

    def test_process_batched_logs_first_worker_batch_once(self):
        class CapturingMapper(VlmApiResponseMapper):
            def _api_response(self, sample):
                return {"choices": [{"message": {"content": "ok"}}]}

        op = CapturingMapper(prompt_template="p", model="m", base_url="https://seed.example/v1")
        with patch("data_juicer.ops.mapper.qa.vlm_api_response_mapper.logger.info") as log_info:
            op.process_batched({"id": ["a"], "images": [[b"1"]]})
            op.process_batched({"id": ["b"], "images": [[b"2"]]})

        first_batch_logs = [call for call in log_info.call_args_list if "first worker batch" in call.args[0]]
        self.assertEqual(len(first_batch_logs), 1)


class OcrAnswerCategoryMapperTest(unittest.TestCase):
    def test_parse_answer_supports_labels_label_alias_and_messages(self):
        answer = {
            "labels": ["数值计算与推理", "指令式区域定位与KIE"],
            "qa": {
                "数值计算与推理": {"question1": "哪项更大？", "answer1": "左侧"},
                "指令式区域定位与KIE": {"question": "姓名？", "answer": "张三"},
            },
        }

        categories, type2messages = parse_answer_categories(json.dumps(answer, ensure_ascii=False))

        self.assertEqual(categories, ["数值计算与校验", "指令式区域定位与KIE"])
        self.assertEqual(type2messages["数值计算与校验"][0]["content"], "哪项更大？")
        self.assertEqual(type2messages["指令式区域定位与KIE"][1]["content"], "张三")

        categories, _ = parse_answer_categories({"label": "图表语义理解"})
        self.assertEqual(categories, ["图表语义理解"])
        self.assertEqual(parse_answer_categories(None), ([], {}))
        self.assertEqual(parse_answer_categories('{"label": "版面结构解析"}'.encode())[0], ["版面结构解析"])
        self.assertEqual(parse_answer_categories("[]"), ([], {}))
        self.assertEqual(parse_answer_categories({"labels": {"数值计算与校验": False}})[0], [])

    def test_arrow_batch_expands_multi_label_and_defaults_simple_extract(self):
        op = OcrAnswerCategoryMapper(auto_op_parallelism=False, num_proc=1)
        table = pa.Table.from_pylist(
            [
                {
                    "id": "multi",
                    "ocr_answer": json.dumps(
                        {"labels": ["版面结构解析", "长文档理解"], "qa": {"长文档理解": {"question": "Q", "answer": "A"}}},
                        ensure_ascii=False,
                    ),
                },
                {"id": "none", "ocr_answer": "bad-json"},
            ],
            schema=pa.schema([pa.field("id", pa.string()), pa.field("ocr_answer", pa.string())]),
        )

        output = op.process_batched(table)

        self.assertEqual(output.num_rows, 3)
        self.assertEqual(output.schema.field("messages").type, pa.list_(pa.struct([("role", pa.string()), ("content", pa.string())])))
        rows = output.to_pylist()
        self.assertEqual([row["ocr_type_en"] for row in rows], ["layout_analysis", "long_document_understanding", "simple_extract"])
        self.assertEqual(rows[1]["messages"], [{"role": "user", "content": "Q"}, {"role": "assistant", "content": "A"}])

    def test_dict_batch_empty_and_run_dispatch_paths(self):
        class FakeRayDataset:
            def map_batches(self, fn, *, batch_format, batch_size):
                self.fn = fn
                self.batch_format = batch_format
                self.batch_size = batch_size
                return "mapped"

        op = OcrAnswerCategoryMapper(batch_size=5, auto_op_parallelism=False, num_proc=1)
        rows = op.process_batched({"ocr_answer": [json.dumps({"label": "图表语义理解"}, ensure_ascii=False)]})
        empty = op.process_batched({})
        dataset = FakeRayDataset()

        self.assertEqual(rows["ocr_type_en"], ["chart_understanding"])
        self.assertEqual(empty["ocr_type"], [])
        self.assertEqual(op.run(dataset), "mapped")
        self.assertEqual(dataset.batch_format, "pyarrow")
        self.assertEqual(dataset.batch_size, 5)


class OcrExtractConfigTest(unittest.TestCase):
    def test_main_and_sample_configs_load_real_ops(self):
        config_dir = os.path.join(os.getcwd(), "demos/bytedance/ocr_extract")
        if not os.path.isdir(config_dir):
            self.skipTest("ByteDance OCR extract demo configs are not included")
        expected = {
            "third_site_ocr_seed_main.yaml": [
                "ImageOcrMapper",
                "AlignedListFieldFlattenMapper",
                "OcrTextRichnessMapper",
                "NumericProbabilitySamplingFilter",
                "VlmApiResponseMapper",
                "OcrAnswerCategoryMapper",
            ],
            "third_site_ocr_seed_main_demo1.yaml": [
                "ImageOcrMapper",
                "AlignedListFieldFlattenMapper",
                "OcrTextRichnessMapper",
                "NumericProbabilitySamplingFilter",
                "VlmApiResponseMapper",
                "OcrAnswerCategoryMapper",
            ],
            "third_site_ocr_seed_sample.yaml": [
                "RayGroupSamplePipeline",
                "VlmApiResponseMapper",
                "OcrAnswerCategoryMapper",
            ],
            "third_site_ocr_seed_main_demo1_sample_label.yaml": [
                "RayGroupSamplePipeline",
                "VlmApiResponseMapper",
                "VlmApiResponseMapper",
                "VlmApiResponseMapper",
            ],
        }

        _patch_yaml_loader_tags()
        for name, expected_classes in expected.items():
            config_path = os.path.join(config_dir, name)
            if not os.path.exists(config_path):
                self.skipTest("ByteDance OCR extract demo configs are not included")
            cfg = init_configs(
                args=["--config", config_path, "--ray_address", "local"],
                load_configs_only=True,
            )
            ops = load_ops(cfg.process, OPEnvManager(min_common_dep_num_to_combine=0))
            self.assertEqual([op.__class__.__name__ for op in ops], expected_classes)
            if name in {"third_site_ocr_seed_main.yaml", "third_site_ocr_seed_main_demo1.yaml"}:
                self.assertEqual(cfg.export["target"], "magnus")
                expected_table = (
                    "ai_data_forge.ccu.third_site_ocr_seed_main_demo1"
                    if name == "third_site_ocr_seed_main_demo1.yaml"
                    else "ai_data_forge.ccu.product_comment_ocr_main"
                )
                self.assertEqual(cfg.export["table_name"], expected_table)
                self.assertEqual(cfg.export["operation"], "OVERWRITE")
                self.assertEqual(cfg.export["magnus_conf"]["concurrency"], 8)
                self.assertEqual(cfg.export["magnus_conf"]["ray_remote_args"]["num_cpus"], 1)
                self.assertEqual(
                    str(cfg.export["magnus_conf"]["write_options"]["magnus.ray.write.disable_repartition"]).lower(),
                    "true",
                )
                self.assertEqual(
                    str(cfg.export["magnus_conf"]["write_options"]["magnus.ray.write.disable_sort"]).lower(),
                    "true",
                )
                if name == "third_site_ocr_seed_main.yaml":
                    self.assertEqual(cfg.dataset["configs"][0]["override_num_blocks"], 600)
                    self.assertEqual(cfg.process[0]["image_ocr_mapper"]["caller"], "ad.ai.data_forge_merlin")
                    self.assertEqual(
                        cfg.process[0]["image_ocr_mapper"]["expected_caller_psm"],
                        "ad.ai.data_forge_merlin",
                    )
                    with open(os.path.join(config_dir, name), encoding="utf-8") as config_file:
                        config_text = config_file.read()
                    self.assertNotIn("day_interval_seconds", config_text)
                    self.assertNotIn("night_interval_seconds", config_text)
                    self.assertEqual(cfg.process[0]["image_ocr_mapper"]["qps"], 200)
                    self.assertEqual(cfg.process[0]["image_ocr_mapper"]["num_proc"], 256)
                    self.assertEqual(ops[0].caller, "ad.ai.data_forge_merlin")
                    self.assertEqual(ops[0].expected_caller_psm, "ad.ai.data_forge_merlin")
                    self.assertEqual(ops[0].qps, 200)
                    self.assertEqual(ops[0].num_proc, 256)
                    self.assertEqual(cfg.export["partition_columns"], ["p_date"])
                    self.assertEqual(str(cfg.export["partition_values"]["p_date"]), "20260428")
                    self.assertTrue(cfg.export["create_table_if_not_exists"])
                    self.assertEqual(ops[4].num_proc, 800)
                if name == "third_site_ocr_seed_main_demo1.yaml":
                    self.assertEqual(cfg.dataset["configs"][0]["override_num_blocks"], 256)
                    self.assertEqual(cfg.process[0]["image_ocr_mapper"]["day_interval_seconds"], 1.0)
                    self.assertEqual(cfg.process[0]["image_ocr_mapper"]["num_proc"], 128)
                    self.assertEqual(ops[0].day_interval_seconds, 1.0)
                    self.assertEqual(ops[0].num_proc, 128)
                    self.assertIsNone(ops[0].repartition_num_blocks)
                    self.assertEqual(ops[4].repartition_num_blocks, 160)
                if name == "third_site_ocr_seed_main_demo1.yaml":
                    self.assertIsNone(ops[4].system_prompt)
                    self.assertTrue(ops[4].prompt_template)
                    self.assertEqual(ops[4].error_key, "vlm_error")
                    self.assertIsNone(ops[4].max_tokens)
                    self.assertEqual(ops[4].rpm, 500)
                    self.assertEqual(ops[4].tpm, 1000000)
                    self.assertEqual(ops[4].estimated_tokens_per_request, 3500)
                    schema_fields = {field["name"]: field["type"] for field in cfg.export["schema"]["fields"]}
                    self.assertEqual(schema_fields["vlm_error"], "string")
                if name == "third_site_ocr_seed_main.yaml":
                    self.assertIsNone(ops[4].system_prompt)
                    self.assertTrue(ops[4].prompt_template)
                    self.assertEqual(ops[4].error_key, "vlm_error")
                    self.assertIsNone(ops[4].max_tokens)
                    self.assertEqual(ops[4].rpm, 2500)
                    self.assertEqual(ops[4].tpm, 5000000)
                    self.assertEqual(ops[4].estimated_tokens_per_request, 3500)
                    schema_fields = {field["name"]: field["type"] for field in cfg.export["schema"]["fields"]}
                    self.assertEqual(schema_fields["vlm_error"], "string")
            elif name == "third_site_ocr_seed_sample.yaml":
                ds_config = cfg.dataset["configs"][0]
                self.assertEqual(ds_config["source"], "magnus")
                self.assertEqual(ds_config["table_name"], "ai_data_forge.ccu.third_site_ocr_seed_main")
                self.assertEqual(cfg.export["target"], "magnus")
                self.assertEqual(cfg.export["table_name"], "ai_data_forge.ccu.third_site_ocr_seed_sample")
                self.assertEqual(cfg.export["operation"], "OVERWRITE")
                self.assertEqual(cfg.export["magnus_conf"]["concurrency"], 8)
                self.assertEqual(cfg.export["magnus_conf"]["ray_remote_args"]["num_cpus"], 1)
                self.assertEqual(ops[2].answer_key, "sample_ocr_answer")
                self.assertEqual(ops[2].type_key, "sample_ocr_type")
                self.assertEqual(ops[2].type_en_key, "sample_ocr_type_en")
                self.assertEqual(ops[2].messages_key, "sample_messages")
            else:
                ds_config = cfg.dataset["configs"][0]
                self.assertEqual(ds_config["source"], "magnus")
                self.assertEqual(ds_config["table_name"], "ai_data_forge.ccu.third_site_ocr_seed_main_demo1")
                self.assertEqual(cfg.export["target"], "magnus")
                self.assertEqual(
                    cfg.export["table_name"],
                    "ai_data_forge.ccu.third_site_ocr_seed_main_demo1_sample_labels",
                )
                self.assertEqual(cfg.export["operation"], "OVERWRITE")
                self.assertEqual(cfg.export["partition_columns"], ["p_date"])
                self.assertEqual(str(cfg.export["partition_values"]["p_date"]), "20260509")
                self.assertTrue(cfg.export["create_table_if_not_exists"])
                schema = build_arrow_schema_from_config(cfg.export["schema"])
                self.assertIn("p_date", schema.names)
                for field_name in [
                    "category_classification",
                    "category_classification_error",
                    "difficulty_evaluation",
                    "difficulty_evaluation_error",
                    "quality_inspection",
                    "quality_inspection_error",
                ]:
                    self.assertIn(field_name, schema.names)
                self.assertEqual(cfg.export["magnus_conf"]["concurrency"], 8)
                self.assertEqual(cfg.export["magnus_conf"]["ray_remote_args"]["num_cpus"], 1)
                self.assertEqual(ops[0].group_field_key, "ocr_type_en")
                self.assertEqual(ops[0].select_num_per_group, 150)
                self.assertEqual(
                    [ops[index].output_key for index in (1, 2, 3)],
                    ["category_classification", "difficulty_evaluation", "quality_inspection"],
                )
                self.assertEqual(
                    [ops[index].error_key for index in (1, 2, 3)],
                    [
                        "category_classification_error",
                        "difficulty_evaluation_error",
                        "quality_inspection_error",
                    ],
                )
                self.assertIn("expert classifier", ops[1].system_prompt)
                self.assertIn("expert evaluator", ops[2].system_prompt)
                self.assertIn("data quality assessor", ops[3].system_prompt)
                self.assertIn("${ocr_answer}", cfg.process[1]["vlm_api_response_mapper"]["prompt_template"])
                for index in (1, 2, 3):
                    self.assertEqual(ops[index].num_proc, 128)
                    self.assertEqual(ops[index].rpm, 500)
                    self.assertEqual(ops[index].tpm, 1000000)
                self.assertEqual(ops[1].repartition_num_blocks, 160)
                self.assertIsNone(ops[2].repartition_num_blocks)
                self.assertIsNone(ops[3].repartition_num_blocks)


if __name__ == "__main__":
    unittest.main()
