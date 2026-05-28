import hashlib
import json
import os
import sys
import tempfile
import types
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import tomli
from PIL import Image

_register_extension_type = pa.register_extension_type


def _register_extension_type_once(extension_type):
    try:
        _register_extension_type(extension_type)
    except pa.ArrowKeyError:
        if not extension_type.extension_name.startswith("datasets.features.features."):
            raise


pa.register_extension_type = _register_extension_type_once
from data_juicer.ops.mapper.schema.aweme_pack_url_mapper import (
    GDPR_TOKEN_EXTRA_KEY,
    GDPR_TOKEN_ENV,
    AwemePackUrlMapper,
    _build_target,
    _build_override_gdpr_auth_middleware,
    _ensure_requester_env,
)
from data_juicer.ops.mapper.io.download_file_mapper import DownloadFileMapper
from data_juicer.ops.mapper.schema.ecom_comment_schema_prepare_mapper import (
    EcomCommentSchemaPrepareMapper,
)
from data_juicer.ops.mapper.schema.image_bytes_exact_dedup_mapper import (
    ImageBytesExactDedupMapper,
)
from data_juicer.ops.mapper.schema.image_bytes_prune_mapper import ImageBytesPruneMapper
from data_juicer.ops.mapper.schema.image_schema_finalize_mapper import ImageSchemaFinalizeMapper
from data_juicer.ops.mapper.schema.json_extra_update_mapper import JsonExtraUpdateMapper

pa.register_extension_type = _register_extension_type


def _image_bytes(seed: int = 0, size=(120, 120)) -> bytes:
    image = Image.new("RGB", size)
    pixels = []
    for y in range(size[1]):
        for x in range(size[0]):
            pixels.append(
                (
                    (x * 37 + y * 17 + seed) % 256,
                    (x * 13 + y * 29 + seed * 3) % 256,
                    (x * 7 + y * 19 + seed * 11) % 256,
                )
            )
    image.putdata(pixels)
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def _expected_image_md5(images: list[bytes]) -> str:
    sample_md5 = hashlib.md5()
    seen = set()
    for image in sorted(images):
        image_md5 = hashlib.md5(image).hexdigest()
        if image_md5 in seen:
            continue
        seen.add(image_md5)
        sample_md5.update(image)
    return sample_md5.hexdigest()


def _to_batch(sample):
    return {key: [value] for key, value in sample.items()}


def _from_batch(batch):
    return {key: values[0] for key, values in batch.items()}


class _FakeUrlRpc:
    def __init__(self, mapping):
        self.mapping = mapping
        self.calls = []

    def __call__(self, uri):
        self.calls.append(uri)
        value = self.mapping[uri]
        if isinstance(value, Exception):
            raise value
        return value


class _FakeBase:
    def __init__(self, Caller="", Extra=None):
        self.Caller = Caller
        self.Extra = Extra


class _FakeBaseResp:
    def __init__(self, StatusCode=0, StatusMessage=""):
        self.StatusCode = StatusCode
        self.StatusMessage = StatusMessage


class _FakePackImageUrlRequest:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _FakePackImageUrlResponse:
    def __init__(self, uri="", url_list=None, status_code=0):
        self.uri = uri
        self.url_list = list(url_list or [])
        self.BaseResp = _FakeBaseResp(StatusCode=status_code)


class _FakeAwemePackUrlThrift:
    class base_thrift:
        Base = _FakeBase

    PackUrlService = object()
    PackImageUrlRequest = _FakePackImageUrlRequest


class _FakePackUrlClient:
    def __init__(self, mapping):
        self.mapping = mapping
        self.calls = []

    def PackImage(self, req):
        self.calls.append(req)
        value = self.mapping[req.uri]
        if isinstance(value, Exception):
            raise value
        if isinstance(value, tuple):
            url_list, status_code = value
            return _FakePackImageUrlResponse(req.uri, url_list, status_code=status_code)
        return _FakePackImageUrlResponse(req.uri, value)


class EcomCommentSchemaPrepareMapperTest(unittest.TestCase):
    def _op(self, **kwargs):
        defaults = {
            "extra_keys": [
                "comment_id",
                "product_id",
                "content",
                "comment_pic_url",
                "cmmt_img_uri",
                "with_pic",
            ],
            "passthrough_keys": ["p_date"],
            "passthrough_types": {"p_date": "string"},
            "auto_op_parallelism": False,
            "num_proc": 1,
        }
        defaults.update(kwargs)
        return EcomCommentSchemaPrepareMapper(**defaults)

    def test_prepare_image_rows_without_resolving_or_downloading(self):
        row = self._op().process_single(
            {
                "comment_id": 7,
                "product_id": 11,
                "content": " comment text ",
                "comment_pic_url": ["unused"],
                "cmmt_img_uri": ["uri-a", {"uri": "uri-b"}],
                "with_pic": 1,
                "p_date": "20260428",
            }
        )[0]

        self.assertEqual(row["id"], "comment_id-7")
        self.assertEqual(row["source"], "ecom_comment_with_pic_raw_data")
        self.assertEqual(row["texts"], [" comment text "])
        self.assertEqual(row["image_uris"], ["uri-a", "uri-b"])
        self.assertEqual(row["image_urls"], [])
        self.assertEqual(row["image_bytes"], [])
        self.assertEqual(row["valid_image_count"], 0)
        self.assertEqual(row["type"], "image")
        self.assertIsNone(row["md5"])
        self.assertEqual(row["p_date"], "20260428")
        self.assertEqual(json.loads(row["extra"])["comment_id"], 7)

    def test_prepare_text_rows_use_content_md5_and_empty_uri_list_stays_image(self):
        op = self._op()
        content = "text only comment"

        text_row = op.process_single(
            {
                "comment_id": 8,
                "product_id": 12,
                "content": content,
                "cmmt_img_uri": None,
                "with_pic": 0,
                "p_date": "20260428",
            }
        )[0]
        empty_uri_row = op.process_single(
            {
                "comment_id": 9,
                "content": "has non-null empty uri",
                "cmmt_img_uri": [],
                "p_date": "20260428",
            }
        )[0]

        self.assertEqual(text_row["source"], "ecom_comment_no_pic_raw_data")
        self.assertEqual(text_row["type"], "text")
        self.assertEqual(text_row["image_uris"], [])
        self.assertEqual(text_row["md5"], hashlib.md5(content.encode()).hexdigest())
        self.assertEqual(empty_uri_row["type"], "image")
        self.assertEqual(empty_uri_row["image_uris"], [])
        self.assertIsNone(empty_uri_row["md5"])
        self.assertEqual(op.process_single({"comment_id": 1, "content": " ", "cmmt_img_uri": None}), [])

    def test_constructor_validation_and_helper_parsing(self):
        for kwargs, message in [
            ({"id_field": ""}, "id_field"),
            ({"text_field": ""}, "text_field"),
            ({"uri_field": ""}, "uri_field"),
            ({"passthrough_types": {"p_date": "unknown"}}, "Unsupported"),
        ]:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    EcomCommentSchemaPrepareMapper(
                        auto_op_parallelism=False,
                        num_proc=1,
                        **kwargs,
                    )

        self.assertEqual(EcomCommentSchemaPrepareMapper._uri_items(b"uri"), ["uri"])
        self.assertEqual(EcomCommentSchemaPrepareMapper._uri_items('["a", "b"]'), ["a", "b"])
        self.assertEqual(EcomCommentSchemaPrepareMapper._uri_items("{bad json"), ["{bad json"])
        self.assertEqual(EcomCommentSchemaPrepareMapper._uri_items({"uri": "u"}), ["u"])
        self.assertEqual(EcomCommentSchemaPrepareMapper._uri_items({"bad": "u"}), [])
        self.assertEqual(EcomCommentSchemaPrepareMapper._uri_items(3), ["3"])
        self.assertIsNone(EcomCommentSchemaPrepareMapper._text_value(None))
        self.assertEqual(EcomCommentSchemaPrepareMapper._text_value(b"text"), "text")
        self.assertIsNone(EcomCommentSchemaPrepareMapper._text_value(" "))

    def test_process_batched_returns_stable_arrow_schema_for_mixed_rows(self):
        table = pa.table(
            {
                "comment_id": pa.array([1, 2, 3], type=pa.int64()),
                "product_id": pa.array([11, 12, 13], type=pa.int64()),
                "content": pa.array(["image row", "text row", ""], type=pa.string()),
                "comment_pic_url": pa.array([["unused"], None, None], type=pa.list_(pa.string())),
                "cmmt_img_uri": pa.array([["uri-a"], None, None], type=pa.list_(pa.string())),
                "with_pic": pa.array([1, 0, 0], type=pa.int64()),
                "p_date": pa.array(["20260428", "20260428", "20260428"], type=pa.string()),
            }
        )

        output = self._op().process_batched(table)

        self.assertEqual(output.num_rows, 2)
        self.assertEqual(output.schema.field("image_uris").type, pa.list_(pa.string()))
        self.assertEqual(output.schema.field("image_urls").type, pa.list_(pa.string()))
        self.assertEqual(output.schema.field("image_bytes").type, pa.list_(pa.binary()))
        self.assertEqual(output.schema.field("valid_image_count").type, pa.int64())
        self.assertEqual(output.schema.field("p_date").type, pa.string())
        self.assertEqual(output.column("type").to_pylist(), ["image", "text"])

        empty_output = self._op().process_batched(table.slice(2, 1))
        self.assertEqual(empty_output.num_rows, 0)
        self.assertEqual(empty_output.schema.field("image_bytes").type, pa.list_(pa.binary()))

    def test_run_paths_dict_batches_and_arrow_type_inference(self):
        class FakeNestedDataset:
            column_names = ["comment_id", "content", "cmmt_img_uri"]

            def __init__(self):
                self.kwargs = None

            def map(self, *args, **kwargs):
                self.kwargs = kwargs
                return "nested-mapped"

        class FakeRayDataset:
            def __init__(self):
                self.kwargs = None

            def map_batches(self, *args, **kwargs):
                self.kwargs = kwargs
                return "ray-mapped"

        class ScalarValue:
            def as_py(self):
                return "scalar-extra"

        class ArrayValue:
            def tolist(self):
                return ["uri-from-array"]

        op = self._op(extra_keys=["scalar"])
        with patch(
            "data_juicer.ops.mapper.schema.ecom_comment_schema_prepare_mapper.NestedDataset",
            FakeNestedDataset,
        ):
            nested = FakeNestedDataset()
            self.assertEqual(op.run(nested), "nested-mapped")
            self.assertEqual(nested.kwargs["remove_columns"], FakeNestedDataset.column_names)

        ray_dataset = FakeRayDataset()
        self.assertEqual(op.run(ray_dataset), "ray-mapped")
        self.assertEqual(ray_dataset.kwargs["batch_format"], "pyarrow")
        self.assertEqual(op.process_batched({})["image_uris"], [])
        self.assertEqual(
            op.process_batched({"comment_id": [1], "content": [""], "cmmt_img_uri": [None]})["id"],
            [],
        )

        wrapped_row = op.process_single(
            {
                "comment_id": 2,
                "content": b"wrapped text",
                "cmmt_img_uri": ArrayValue(),
                "scalar": ScalarValue(),
            }
        )[0]
        self.assertEqual(wrapped_row["texts"], ["wrapped text"])
        self.assertEqual(wrapped_row["image_uris"], ["uri-from-array"])
        self.assertEqual(json.loads(wrapped_row["extra"])["scalar"], "scalar-extra")
        self.assertEqual(op._arrow_type_for_key("unknown", [None], None), pa.string())
        self.assertEqual(op._arrow_type_for_key("number", [1], None), pa.int64())
        self.assertEqual(op._parse_arrow_type(pa.int32()), pa.int32())


class AwemePackUrlMapperTest(unittest.TestCase):
    def test_resolves_each_uri_and_skips_rpc_failures(self):
        op = AwemePackUrlMapper(image_expire_second=123, auto_op_parallelism=False, num_proc=1)
        client = _FakePackUrlClient(
            {
                "uri-a": ["https://img/a"],
                "uri-b": ["https://img/b", {"url": "https://img/c"}],
                "uri-fail": RuntimeError("rpc failed"),
                "uri-bad-status": (["https://img/bad"], 1),
            }
        )
        op._client = client
        op._api_thrift = _FakeAwemePackUrlThrift

        sample = op.process_single({"image_uris": ["uri-a", "uri-fail", "uri-bad-status", "uri-b"]})

        self.assertEqual(sample["image_urls"], ["https://img/a", "https://img/b", "https://img/c"])
        self.assertEqual([req.uri for req in client.calls], ["uri-a", "uri-fail", "uri-bad-status", "uri-b"])
        self.assertEqual(client.calls[0].image_expire_second, 123)
        self.assertEqual(client.calls[0].Base.Caller, "ad.ai.data_forge_merlin")
        self.assertEqual(client.calls[0].Base.Extra, {"cluster": "default"})

    def test_pack_image_rpc_qps_metrics_cover_success_status_error_and_exception(self):
        op = AwemePackUrlMapper(
            target_psm="aweme.pack.url",
            target_cluster="boe",
            auto_op_parallelism=False,
            num_proc=1,
        )
        op._client = _FakePackUrlClient(
            {
                "uri-a": ["https://img/a"],
                "uri-bad-status": (["https://img/bad"], 1),
                "uri-fail": RuntimeError("rpc failed"),
            }
        )
        op._api_thrift = _FakeAwemePackUrlThrift

        with patch("data_juicer.ops.mapper.schema.aweme_pack_url_mapper.emit_rpc_qps") as emit_mock:
            self.assertEqual(op._pack_image("uri-a"), ["https://img/a"])
            self.assertEqual(op._pack_image("uri-bad-status"), [])
            with self.assertRaisesRegex(RuntimeError, "rpc failed"):
                op._pack_image("uri-fail")

        self.assertEqual([call.kwargs["status"] for call in emit_mock.call_args_list], ["success", "error", "error"])
        for call in emit_mock.call_args_list:
            self.assertEqual(call.kwargs["op_name"], "aweme_pack_url_mapper")
            self.assertEqual(call.kwargs["target"], "sd://aweme.pack.url?cluster=boe")
            self.assertEqual(call.kwargs["method"], "PackImage")

    def test_pack_image_uses_qps_limiter_before_each_rpc(self):
        op = AwemePackUrlMapper(qps=2, auto_op_parallelism=False, num_proc=1)
        op._client = _FakePackUrlClient({"uri-a": ["url-a"], "uri-b": ["url-b"]})
        op._api_thrift = _FakeAwemePackUrlThrift
        limiter_acquires = []
        op._rpc_qps_limiter = types.SimpleNamespace(acquire=lambda: limiter_acquires.append("acquire"))

        self.assertEqual(op._resolve_urls(["uri-a", "uri-b"]), ["url-a", "url-b"])

        self.assertEqual(limiter_acquires, ["acquire", "acquire"])
        with self.assertRaisesRegex(ValueError, "qps"):
            AwemePackUrlMapper(qps=0, auto_op_parallelism=False, num_proc=1)

    def test_empty_uris_do_not_create_rpc_and_batch_processing_adds_urls(self):
        op = AwemePackUrlMapper(auto_op_parallelism=False, num_proc=1)

        with patch.object(op, "_get_client_and_thrift", side_effect=AssertionError("rpc should not be created")):
            self.assertEqual(op.process_single({"image_uris": []})["image_urls"], [])

        op._client = _FakePackUrlClient({"a": ["url-a"]})
        op._api_thrift = _FakeAwemePackUrlThrift
        batch = op.process_batched({"image_uris": [["a"], []]})
        self.assertEqual(batch["image_urls"], [["url-a"], []])

    def test_constructor_state_and_parser_helpers(self):
        with self.assertRaisesRegex(ValueError, "uri_field"):
            AwemePackUrlMapper(uri_field="", auto_op_parallelism=False, num_proc=1)
        with self.assertRaisesRegex(ValueError, "url_field"):
            AwemePackUrlMapper(url_field="", auto_op_parallelism=False, num_proc=1)

        op = AwemePackUrlMapper(image_expire_second=123, auto_op_parallelism=False, num_proc=1)
        self.assertIsNone(op.__getstate__()["_client"])
        self.assertIsNone(op.__getstate__()["_api_thrift"])

        self.assertEqual(AwemePackUrlMapper._uri_items('["a", {"uri": "b"}]'), ["a", "b"])
        self.assertEqual(AwemePackUrlMapper._uri_items(None), [])
        self.assertEqual(AwemePackUrlMapper._uri_items(b"byte-uri"), ["byte-uri"])
        self.assertEqual(AwemePackUrlMapper._uri_items(" "), [])
        self.assertEqual(AwemePackUrlMapper._uri_items(3), ["3"])
        self.assertEqual(AwemePackUrlMapper._url_items([" ", "x"]), ["x"])
        self.assertEqual(AwemePackUrlMapper._uri_items({"bad": "x"}), [])
        self.assertEqual(AwemePackUrlMapper._uri_items({"src": "s"}), ["s"])
        self.assertEqual(AwemePackUrlMapper._parse_structured_string('"plain"'), "plain")
        self.assertEqual(AwemePackUrlMapper._base_resp_status_code(object()), 0)
        self.assertEqual(_build_target("aweme.pack.url", "default"), "sd://aweme.pack.url?cluster=default")

        class ScalarValue:
            def as_py(self):
                return "scalar-uri"

        class ArrayValue:
            def tolist(self):
                return ["array-uri"]

        self.assertEqual(AwemePackUrlMapper._uri_items(ScalarValue()), ["scalar-uri"])
        self.assertEqual(AwemePackUrlMapper._uri_items(ArrayValue()), ["array-uri"])

    def test_gdpr_override_middleware_injects_env_token_and_requester_env(self):
        class FakeBaseCompatMiddleware:
            class base_thrift:
                Base = _FakeBase

        class FakeContext:
            def __init__(self):
                self.local = {}

            def next(self, *args, **kwargs):
                return "next-called"

        class FakeReq:
            Base = None

        middleware = _build_override_gdpr_auth_middleware(FakeBaseCompatMiddleware, "token-1")
        ctx = FakeContext()
        req = FakeReq()

        self.assertEqual(middleware(ctx, req), "next-called")
        self.assertEqual(req.Base.Extra[GDPR_TOKEN_EXTRA_KEY], "token-1")
        self.assertEqual(ctx.local["gdpr_token"], "token-1")

        class ReqWithoutBase:
            pass

        self.assertEqual(middleware(ctx, ReqWithoutBase()), "next-called")
        with self.assertRaisesRegex(RuntimeError, "base_thrift"):
            _build_override_gdpr_auth_middleware(object(), "token-1")

        _ensure_requester_env("ad.ai.data_forge", "default")
        self.assertEqual(os.environ["TCE_PSM"], "ad.ai.data_forge")
        self.assertEqual(os.environ["TCE_CLUSTER"], "default")

    def test_create_client_uses_euler_client_and_gdpr_override(self):
        fake_base_compat_middleware = types.ModuleType("euler.base_compat_middleware")
        fake_base_compat_middleware.base_thrift = types.SimpleNamespace(Base=_FakeBase)
        fake_base_compat_middleware.client_middleware = object()

        class FakeEulerClient:
            instances = []

            def __init__(self, service, target, timeout, transport, protocol):
                self.service = service
                self.target = target
                self.timeout = timeout
                self.transport = transport
                self.protocol = protocol
                self.middlewares = []
                FakeEulerClient.instances.append(self)

            def use(self, middleware):
                self.middlewares.append(middleware)

        fake_euler = types.ModuleType("euler")
        fake_euler.Client = FakeEulerClient
        fake_euler.base_compat_middleware = fake_base_compat_middleware

        op = AwemePackUrlMapper(
            source_psm="ad.ai.data_forge",
            source_cluster="default",
            target_psm="aweme.pack.url",
            target_cluster="default",
            timeout=7.0,
            auto_op_parallelism=False,
            num_proc=1,
        )
        with patch.dict(sys.modules, {"euler": fake_euler, "euler.base_compat_middleware": fake_base_compat_middleware}):
            with patch.dict(os.environ, {GDPR_TOKEN_ENV: "token-2"}, clear=False):
                with patch(
                    "data_juicer.ops.mapper.schema.aweme_pack_url_mapper._load_aweme_pack_url_thrift",
                    return_value=_FakeAwemePackUrlThrift,
                ):
                    client, api_thrift = op._get_client_and_thrift()

        self.assertIs(api_thrift, _FakeAwemePackUrlThrift)
        self.assertIs(client, FakeEulerClient.instances[0])
        self.assertEqual(client.service, _FakeAwemePackUrlThrift.PackUrlService)
        self.assertEqual(client.target, "sd://aweme.pack.url?cluster=default")
        self.assertEqual(client.timeout, 7.0)
        self.assertEqual(client.transport, "ttheader")
        self.assertEqual(client.protocol, "binary")
        self.assertEqual(client.middlewares[1], fake_base_compat_middleware.client_middleware)

        class FakeContext:
            def __init__(self):
                self.local = {}
                self.persistent = {}

            def next(self, *args, **kwargs):
                return "next-called"

        ctx = FakeContext()
        self.assertEqual(client.middlewares[0](ctx), "next-called")
        self.assertEqual(ctx.persistent["cluster"], "default")

        req = _FakePackImageUrlRequest(Base=None)
        self.assertEqual(client.middlewares[2](ctx, req), "next-called")
        self.assertEqual(req.Base.Extra[GDPR_TOKEN_EXTRA_KEY], "token-2")

    def test_create_client_requires_runtime_and_token(self):
        op = AwemePackUrlMapper(auto_op_parallelism=False, num_proc=1)
        with patch.dict(sys.modules, {"euler": None}):
            with self.assertRaisesRegex(RuntimeError, "Euler RPC runtime"):
                op._create_client_and_thrift()

        fake_base_compat_middleware = types.ModuleType("euler.base_compat_middleware")
        fake_base_compat_middleware.base_thrift = types.SimpleNamespace(Base=_FakeBase)
        fake_base_compat_middleware.client_middleware = object()
        fake_euler = types.ModuleType("euler")
        fake_euler.Client = object
        fake_euler.base_compat_middleware = fake_base_compat_middleware

        with patch.dict(sys.modules, {"euler": fake_euler, "euler.base_compat_middleware": fake_base_compat_middleware}):
            with patch.dict(os.environ, {GDPR_TOKEN_ENV: ""}, clear=False):
                with self.assertRaisesRegex(RuntimeError, GDPR_TOKEN_ENV):
                    op._create_client_and_thrift()

    def test_euler_packages_are_base_dependencies(self):
        self.assertIsNone(AwemePackUrlMapper._requirements)

        pyproject_path = Path(__file__).resolve().parents[3] / "pyproject.toml"
        with pyproject_path.open("rb") as fin:
            dependencies = tomli.load(fin)["project"]["dependencies"]

        self.assertIn("bytedeuler", dependencies)
        self.assertIn("thriftpy2", dependencies)


class JsonExtraUpdateMapperTest(unittest.TestCase):
    def test_updates_extra_from_field_mappings_and_skips_empty_values(self):
        op = JsonExtraUpdateMapper(
            field_mappings={"image_urls": "valid_urls", "missing": "missing"},
            auto_op_parallelism=False,
            num_proc=1,
        )

        sample = op.process_single({"extra": '{"comment_id": 1}', "image_urls": ["url-a"], "missing": []})

        self.assertEqual(json.loads(sample["extra"]), {"comment_id": 1, "valid_urls": ["url-a"]})

    def test_skip_empty_false_and_extra_parsing_fallbacks(self):
        class ScalarValue:
            def as_py(self):
                return {"a": 1}

        class ArrayValue:
            def tolist(self):
                return ["a", ""]

        op = JsonExtraUpdateMapper(
            field_mappings={"image_urls": "valid_urls"},
            skip_empty=False,
            auto_op_parallelism=False,
            num_proc=1,
        )

        sample = op.process_single({"extra": "not json", "image_urls": []})

        self.assertEqual(json.loads(sample["extra"]), {"valid_urls": []})
        self.assertEqual(JsonExtraUpdateMapper._extra_to_dict(None), {})
        self.assertEqual(JsonExtraUpdateMapper._extra_to_dict(""), {})
        self.assertEqual(JsonExtraUpdateMapper._extra_to_dict({"a": 1}), {"a": 1})
        self.assertEqual(JsonExtraUpdateMapper._extra_to_dict(ScalarValue()), {"a": 1})
        self.assertEqual(JsonExtraUpdateMapper._extra_to_dict("[1]"), {})
        self.assertEqual(JsonExtraUpdateMapper._extra_to_dict(3), {})
        self.assertEqual(JsonExtraUpdateMapper._jsonable(ArrayValue()), ["a", ""])
        self.assertTrue(JsonExtraUpdateMapper._is_empty(None))
        self.assertTrue(JsonExtraUpdateMapper._is_empty(" "))
        self.assertTrue(JsonExtraUpdateMapper._is_empty(b""))
        self.assertTrue(JsonExtraUpdateMapper._is_empty({"a": []}))
        self.assertTrue(JsonExtraUpdateMapper._is_empty(["", []]))
        self.assertFalse(JsonExtraUpdateMapper._is_empty(0))
        with self.assertRaisesRegex(ValueError, "extra_key"):
            JsonExtraUpdateMapper(extra_key="", auto_op_parallelism=False, num_proc=1)


class EcomCommentComposedMapperTest(unittest.TestCase):
    def test_composed_image_and_text_rows_preserve_notebook_branch_rules(self):
        image = _image_bytes(seed=3)
        with tempfile.TemporaryDirectory() as temp_dir:
            image_path = os.path.join(temp_dir, "img.png")
            with open(image_path, "wb") as fout:
                fout.write(image)

            prepare = EcomCommentSchemaPrepareMapper(
                extra_keys=["comment_id", "content", "cmmt_img_uri"],
                passthrough_keys=["p_date"],
                auto_op_parallelism=False,
                num_proc=1,
            )
            pack = AwemePackUrlMapper(auto_op_parallelism=False, num_proc=1)
            pack._client = _FakePackUrlClient({"uri-a": [image_path], "uri-dup": [image_path]})
            pack._api_thrift = _FakeAwemePackUrlThrift
            download = DownloadFileMapper(
                download_field="image_urls",
                save_field="image_bytes",
                resume_download=True,
                auto_op_parallelism=False,
                num_proc=1,
            )
            prune = ImageBytesPruneMapper(
                image_key="image_urls",
                image_bytes_key="image_bytes",
                auto_op_parallelism=False,
                num_proc=1,
            )
            prune.is_valid_image_bytes = lambda img_bytes: bool(img_bytes)
            dedup = ImageBytesExactDedupMapper(
                image_key="image_urls",
                image_bytes_key="image_bytes",
                preserve_existing_md5_on_empty=True,
                auto_op_parallelism=False,
                num_proc=1,
            )
            extra = JsonExtraUpdateMapper(
                field_mappings={"image_urls": "valid_urls"},
                auto_op_parallelism=False,
                num_proc=1,
            )
            finalize = ImageSchemaFinalizeMapper(
                image_bytes_key="image_bytes",
                type_key="type",
                passthrough_keys=["p_date"],
                auto_op_parallelism=False,
                num_proc=1,
            )

            image_row = prepare.process_single(
                {
                    "comment_id": 7,
                    "content": "image comment",
                    "cmmt_img_uri": ["uri-a", "uri-dup"],
                    "p_date": "20260428",
                }
            )[0]
            image_row = pack.process_single(image_row)
            image_row = _from_batch(download.process_batched(_to_batch(image_row)))
            image_row = prune.process_single(image_row)
            image_row = dedup.process_single(image_row)
            image_row = extra.process_single(image_row)
            image_row = finalize.process_single(image_row)

            text_row = prepare.process_single(
                {
                    "comment_id": 8,
                    "content": "text comment",
                    "cmmt_img_uri": None,
                    "p_date": "20260428",
                }
            )[0]
            text_row = pack.process_single(text_row)
            text_row = _from_batch(download.process_batched(_to_batch(text_row)))
            text_row = prune.process_single(text_row)
            text_row = dedup.process_single(text_row)
            text_row = extra.process_single(text_row)
            text_row = finalize.process_single(text_row)

        self.assertEqual(image_row["type"], "image")
        self.assertEqual(image_row["images"], [image])
        self.assertEqual(image_row["md5"], _expected_image_md5([image, image]))
        self.assertEqual(json.loads(image_row["extra"])["valid_urls"], [image_path])
        self.assertEqual(text_row["type"], "text")
        self.assertEqual(text_row["images"], [])
        self.assertEqual(text_row["md5"], hashlib.md5("text comment".encode()).hexdigest())
        self.assertNotIn("valid_urls", json.loads(text_row["extra"]))


class ExistingMapperEnhancementTest(unittest.TestCase):
    def test_download_file_mapper_short_circuits_empty_url_batches(self):
        op = DownloadFileMapper(
            download_field="image_urls",
            save_field="image_bytes",
            auto_op_parallelism=False,
            num_proc=1,
        )

        with patch.object(op, "download_files_async", side_effect=AssertionError("download should not run")):
            output = op.process_batched({"image_urls": [[]]})

        self.assertEqual(output["image_bytes"], [[]])

    def test_image_bytes_exact_dedup_can_preserve_existing_md5_on_empty_bytes(self):
        preserve_op = ImageBytesExactDedupMapper(
            image_key="image_urls",
            image_bytes_key="image_bytes",
            preserve_existing_md5_on_empty=True,
            auto_op_parallelism=False,
            num_proc=1,
        )
        overwrite_op = ImageBytesExactDedupMapper(
            image_key="image_urls",
            image_bytes_key="image_bytes",
            auto_op_parallelism=False,
            num_proc=1,
        )

        old_md5 = "text-md5"
        self.assertEqual(
            preserve_op.process_single({"image_urls": [], "image_bytes": [], "md5": old_md5})["md5"],
            old_md5,
        )
        self.assertEqual(
            overwrite_op.process_single({"image_urls": [], "image_bytes": [], "md5": old_md5})["md5"],
            hashlib.md5().hexdigest(),
        )

        output = preserve_op.process_single(
            {
                "image_urls": ["url-b", "url-a", "url-a-dup"],
                "image_bytes": [b"b", b"a", b"a"],
            }
        )
        self.assertEqual(output["image_urls"], ["url-a", "url-b"])
        self.assertEqual(output["image_bytes"], [b"a", b"b"])
        self.assertEqual(output["valid_image_count"], 2)
        self.assertEqual(output["md5"], _expected_image_md5([b"b", b"a", b"a"]))
        self.assertEqual(ImageBytesExactDedupMapper._as_list(None), [])

    def test_image_schema_finalize_uses_optional_type_key(self):
        row = ImageSchemaFinalizeMapper(type_key="type").process_single(
            {
                "id": "comment_id-8",
                "source": "ecom_comment_no_pic_raw_data",
                "texts": ["text"],
                "image_bytes": [],
                "type": "text",
                "extra": {},
                "md5": "abc",
            }
        )

        self.assertEqual(row["type"], "text")
        self.assertEqual(ImageSchemaFinalizeMapper(type_key="missing").process_single({})["type"], "image")


if __name__ == "__main__":
    unittest.main()
