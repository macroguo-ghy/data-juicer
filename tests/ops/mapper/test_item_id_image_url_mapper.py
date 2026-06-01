import hashlib
import json
import os
import sys
import tempfile
import types
import unittest
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
from PIL import Image

_register_extension_type = pa.register_extension_type


def _register_extension_type_once(extension_type):
    try:
        _register_extension_type(extension_type)
    except pa.ArrowKeyError:
        if not extension_type.extension_name.startswith("datasets.features.features."):
            raise


pa.register_extension_type = _register_extension_type_once
from data_juicer.config.config import init_configs
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.schema.image_bytes_exact_dedup_mapper import ImageBytesExactDedupMapper
from data_juicer.ops.mapper.schema.image_schema_finalize_mapper import ImageSchemaFinalizeMapper
from data_juicer.ops.mapper.schema.item_id_image_url_mapper import (
    ItemIdImageUrlMapper,
    _normalize_image_ref,
)
from data_juicer.ops.mapper.schema.multi_source_image_url_mapper import MultiSourceImageUrlMapper

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


def _write_file(path: str, content: bytes):
    with open(path, "wb") as fout:
        fout.write(content)


def _expected_md5(images: list[bytes]) -> str:
    sample_md5 = hashlib.md5()
    seen = set()
    for image in sorted(images):
        image_md5 = hashlib.md5(image).hexdigest()
        if image_md5 in seen:
            continue
        seen.add(image_md5)
        sample_md5.update(image)
    return sample_md5.hexdigest()


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
                parsed_int = int(value)
            except ValueError:
                pass
            else:
                if str(parsed_int) == value:
                    return parsed_int
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


class FakeItemRpc:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, item_id):
        self.calls.append(item_id)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _op_with_clients(attr_rpc: FakeItemRpc, info_rpc: FakeItemRpc, **kwargs) -> ItemIdImageUrlMapper:
    defaults = {
        "auto_op_parallelism": False,
        "num_proc": 1,
    }
    defaults.update(kwargs)
    op = ItemIdImageUrlMapper(**defaults)
    op._create_rpc_clients = lambda: (attr_rpc, info_rpc)
    return op


class ItemIdImageUrlMapperTest(unittest.TestCase):
    def test_constructor_sets_defaults_and_state_does_not_serialize_rpc(self):
        op = ItemIdImageUrlMapper(auto_op_parallelism=False, num_proc=1)

        self.assertEqual(op.id_field, "item_id")
        self.assertEqual(op.output_url_field, "item_image_urls")
        self.assertEqual(op.image_key, "item_image_urls")

        op._rpc_clients = ("attr", "info")
        self.assertIsNone(op.__getstate__()["_rpc_clients"])

    def test_process_single_uses_item_info_rpc_first(self):
        attr_rpc = FakeItemRpc([["fallback"]])
        info_rpc = FakeItemRpc([["image-a", "image-b"]])
        op = _op_with_clients(attr_rpc, info_rpc)

        row = op.process_single({"item_id": 123, "cover_image_uri": "cover"})

        self.assertEqual(row["item_image_urls"], ["image-a", "image-b"])
        self.assertEqual(row["cover_image_uri"], "cover")
        self.assertEqual(info_rpc.calls, [123])
        self.assertEqual(attr_rpc.calls, [])

    def test_process_single_falls_back_to_item_attr_rpc(self):
        attr_rpc = FakeItemRpc([["image-a", "image-b"]])
        info_rpc = FakeItemRpc([RuntimeError("item-info failed")])
        op = _op_with_clients(attr_rpc, info_rpc)

        row = op.process_single({"item_id": "456"})

        self.assertEqual(row["item_image_urls"], ["image-a", "image-b"])
        self.assertEqual(info_rpc.calls, ["456"])
        self.assertEqual(attr_rpc.calls, ["456"])

    def test_item_rpc_qps_metrics_cover_fallback_calls(self):
        attr_rpc = FakeItemRpc([["image-a"]])
        info_rpc = FakeItemRpc([RuntimeError("item-info failed")])
        op = _op_with_clients(attr_rpc, info_rpc)

        with patch("data_juicer.ops.mapper.schema.item_id_image_url_mapper.emit_rpc_qps") as emit_mock:
            row = op.process_single({"item_id": "456"})

        self.assertEqual(row["item_image_urls"], ["image-a"])
        self.assertEqual([call.kwargs["method"] for call in emit_mock.call_args_list], ["ItemImageInfoRPC", "ItemImageAttrRPC"])
        self.assertEqual([call.kwargs["status"] for call in emit_mock.call_args_list], ["error", "success"])
        for call in emit_mock.call_args_list:
            self.assertEqual(call.kwargs["op_name"], "item_id_image_url_mapper")
            self.assertEqual(call.kwargs["target"], "item_id_image_url")

    def test_item_rpc_uses_qps_limiter_before_each_fallback_attempt(self):
        attr_rpc = FakeItemRpc([["image-a"]])
        info_rpc = FakeItemRpc([RuntimeError("item-info failed")])
        op = _op_with_clients(attr_rpc, info_rpc, qps=2)
        limiter_acquires = []
        op._rpc_qps_limiter = SimpleNamespace(acquire=lambda: limiter_acquires.append("acquire"))

        row = op.process_single({"item_id": "456"})

        self.assertEqual(row["item_image_urls"], ["image-a"])
        self.assertEqual(limiter_acquires, ["acquire", "acquire"])
        with self.assertRaisesRegex(ValueError, "qps"):
            ItemIdImageUrlMapper(qps=0)

    def test_ray_backend_hook_sets_up_shared_qps_limiter(self):
        op = ItemIdImageUrlMapper(qps=100, auto_op_parallelism=False, num_proc=1)
        setup_calls = []
        op._rpc_qps_limiter = SimpleNamespace(setup_ray_actor=lambda: setup_calls.append("setup"))

        op.prepare_backend_for_ray_tasks()

        self.assertEqual(setup_calls, ["setup"])

    def test_process_single_outputs_empty_list_when_rpc_empty_or_item_id_missing(self):
        empty_row = _op_with_clients(FakeItemRpc([None]), FakeItemRpc([[]])).process_single({"item_id": 1})
        missing_id_row = _op_with_clients(FakeItemRpc([["unused"]]), FakeItemRpc([["unused"]])).process_single(
            {"item_id": None}
        )

        self.assertEqual(empty_row["item_image_urls"], [])
        self.assertEqual(missing_id_row["item_image_urls"], [])

    def test_normalize_image_ref_handles_prefix_and_http_requirement(self):
        self.assertIsNone(_normalize_image_ref(None))
        self.assertEqual(_normalize_image_ref("https://a.test/1.png"), "https://a.test/1.png")
        self.assertEqual(_normalize_image_ref("/tos/path.png", "https://img.test"), "https://img.test/tos/path.png")
        self.assertEqual(_normalize_image_ref("tos/path.png"), "tos/path.png")
        self.assertIsNone(_normalize_image_ref("tos/path.png", require_http_url=True))

    def test_mapper_then_schema_pipeline_outputs_multimodal_row(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            image_a = _image_bytes(1)
            image_b = _image_bytes(2)
            cover = _image_bytes(3)
            image_a_path = os.path.join(temp_dir, "item_a.png")
            image_b_path = os.path.join(temp_dir, "item_b.png")
            cover_path = os.path.join(temp_dir, "cover.png")
            _write_file(image_a_path, image_a)
            _write_file(image_b_path, image_b)
            _write_file(cover_path, cover)

            mapper = _op_with_clients(FakeItemRpc([[cover_path]]), FakeItemRpc([[image_a_path, image_b_path]]))
            url_op = MultiSourceImageUrlMapper(
                source_specs=[
                    {
                        "name": "item_images",
                        "url_field": "item_image_urls",
                        "source": "ecom_video_item_raw_data",
                        "extra_url_key": "item_image_urls",
                        "extra_url_mode": "list",
                    }
                ],
                id_field="item_id",
                text_fields=[],
                extra_keys=["item_id", "item_title", "cover_image_uri"],
                passthrough_keys=["date", "app_name"],
                passthrough_types={"date": "string", "app_name": "string"},
                auto_op_parallelism=False,
                num_proc=1,
            )
            dedup_op = ImageBytesExactDedupMapper(
                image_key="image_urls",
                image_bytes_key="image_bytes",
                auto_op_parallelism=False,
                num_proc=1,
            )
            schema_op = ImageSchemaFinalizeMapper(
                passthrough_keys=["date", "app_name"],
                passthrough_types={"date": "string", "app_name": "string"},
                auto_op_parallelism=False,
                num_proc=1,
            )
            mapped = mapper.process_single(
                {
                    "item_id": 123,
                    "item_title": "video title",
                    "cover_image_uri": cover_path,
                    "date": "20260425",
                    "app_name": "aweme",
                }
            )
            rows = url_op.process_single(mapped)
            rows[0]["image_bytes"] = [image_a, image_b]
            deduped = dedup_op.process_single(rows[0])
            row = schema_op.process_single(deduped)

        self.assertEqual(row["id"], "item_id-123")
        self.assertEqual(row["source"], "ecom_video_item_raw_data")
        self.assertEqual(row["texts"], [])
        self.assertEqual(row["date"], "20260425")
        self.assertEqual(row["app_name"], "aweme")
        self.assertEqual(row["images"], sorted([image_a, image_b]))
        self.assertEqual(row["md5"], _expected_md5([image_a, image_b]))

        extra = json.loads(row["extra"])
        self.assertEqual(extra["cover_image_uri"], cover_path)
        self.assertEqual(extra["item_id"], 123)
        self.assertEqual(sorted(extra["item_image_urls"]), sorted([image_a_path, image_b_path]))
        self.assertNotIn(cover, row["images"])

    def test_create_rpc_clients_uses_internal_item_rpc_contract(self):
        class FakeItemAttrRPC:
            pass

        class FakeItemInfoRPC:
            pass

        class FakeUDFMixin:
            pass

        class GetItemDomainRequest:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class IdListRequest:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        euler_module = types.ModuleType("euler")
        euler_module.install_thrift_import_hook = lambda: None
        thrift_module = types.SimpleNamespace(
            GetItemDomainRequest=GetItemDomainRequest,
            IdListRequest=IdListRequest,
        )
        item_attr_module = types.ModuleType("aigc_common.rpc.item_attr.item_attr")
        item_attr_module.ItemAttrRPC = FakeItemAttrRPC
        item_info_module = types.ModuleType("aigc_common.rpc.item_info.item_info")
        item_info_module.ItemInfoRPC = FakeItemInfoRPC
        ugc_module = types.ModuleType("aigc_common.rpc.item_info.idl.ugc")
        ugc_module.item_info_service_thrift = thrift_module
        iudf_module = types.ModuleType("harryspark.iudf")
        iudf_module.UDFMixin = FakeUDFMixin
        modules = {
            "euler": euler_module,
            "aigc_common": types.ModuleType("aigc_common"),
            "aigc_common.rpc": types.ModuleType("aigc_common.rpc"),
            "aigc_common.rpc.item_attr": types.ModuleType("aigc_common.rpc.item_attr"),
            "aigc_common.rpc.item_attr.item_attr": item_attr_module,
            "aigc_common.rpc.item_info": types.ModuleType("aigc_common.rpc.item_info"),
            "aigc_common.rpc.item_info.idl": types.ModuleType("aigc_common.rpc.item_info.idl"),
            "aigc_common.rpc.item_info.idl.ugc": ugc_module,
            "aigc_common.rpc.item_info.item_info": item_info_module,
            "harryspark": types.ModuleType("harryspark"),
            "harryspark.iudf": iudf_module,
        }

        with patch.dict(sys.modules, modules):
            op = ItemIdImageUrlMapper(image_url_prefix="https://img.test")
            attr_rpc, info_rpc = op._create_rpc_clients()

        info_req = info_rpc.build_req("123")
        attr_req = attr_rpc.build_req("123")
        self.assertEqual(info_req.Ids, [123])
        self.assertEqual(info_req.Info, {"stats": "0"})
        self.assertEqual(attr_req.Ids, [123])
        self.assertEqual(attr_req.Fields, ["UserId"])
        self.assertEqual(attr_req.Info, {"with_deleted": "1"})

        info_resp = SimpleNamespace(
            BaseResp=SimpleNamespace(StatusCode=0),
            Items=[SimpleNamespace(Content=json.dumps({"images": [{"uri": "/a.png"}, {"uri": "https://cdn/b.png"}]}))],
        )
        self.assertEqual(info_rpc.process_resp(info_resp, 123), ["https://img.test/a.png", "https://cdn/b.png"])
        self.assertIsNone(info_rpc.process_resp(SimpleNamespace(BaseResp=SimpleNamespace(StatusCode=1)), 123))
        self.assertIsNone(info_rpc.process_resp(SimpleNamespace(BaseResp=SimpleNamespace(StatusCode=0), Items=[]), 123))

        attr_resp = SimpleNamespace(
            BaseResp=SimpleNamespace(StatusCode=0),
            ItemAttrList=[
                SimpleNamespace(
                    ItemAttrMap=json.dumps(
                        {
                            "original_images": json.dumps(
                                [
                                    {"idx": 2, "uri": "/b.png"},
                                    {"idx": 1, "uri": "/a.png"},
                                ]
                            )
                        }
                    )
                )
            ],
        )
        self.assertEqual(attr_rpc.process_resp(attr_resp), ["https://img.test/a.png", "https://img.test/b.png"])
        self.assertIsNone(attr_rpc.process_resp(SimpleNamespace(BaseResp=SimpleNamespace(StatusCode=1))))
        self.assertIsNone(attr_rpc.build_failed_result())
        self.assertIsNone(info_rpc.build_failed_result())


class EcomVideoItemConfigTest(unittest.TestCase):
    def test_ecom_video_item_hive_magnus_ocr_config_loads(self):
        _patch_yaml_loader_tags()
        path = os.path.join(
            os.getcwd(),
            "demos",
            "bytedance",
            "ecom_video_item_a_dragon",
            "configs",
            "ecom_video_item_hive_magnus_ocr.yaml",
        )

        cfg = init_configs(args=["--config", path, "--ray_address", "local"], load_configs_only=True)
        ops = load_ops(cfg.process)
        ds_config = cfg.dataset["configs"][0]
        schema_fields = {field["name"]: field["type"] for field in cfg.export["schema"]["fields"]}

        self.assertEqual(ds_config["source"], "hive")
        self.assertEqual(ds_config["table_name"], "ad_addrd_stats.ecom_video_item_sample_stats_daily_v2")
        self.assertIn("date = '<DATE>'", ds_config["filter"])
        self.assertIn("app_name = '<APP_NAME>'", ds_config["filter"])
        self.assertIn("item_id", ds_config["columns"])
        self.assertIn("cover_image_uri", ds_config["columns"])
        self.assertIn("date", ds_config["columns"])
        self.assertIn("app_name", ds_config["columns"])
        self.assertEqual(cfg.export["target"], "magnus")
        self.assertEqual(cfg.export["table_name"], "<CATALOG>.<DATABASE>.ecom_video_item_image_ocr")
        self.assertEqual(cfg.export["partition_columns"], ["date", "app_name"])
        self.assertEqual(cfg.export["partition_values"]["date"], "<DATE>")
        self.assertEqual(cfg.export["partition_values"]["app_name"], "<APP_NAME>")
        self.assertEqual(schema_fields["ocr_result"], "list<string>")
        self.assertEqual(schema_fields["date"], "string")
        self.assertEqual(schema_fields["app_name"], "string")
        self.assertEqual(
            [op.__class__.__name__ for op in ops],
            [
                "ItemIdImageUrlMapper",
                "MultiSourceImageUrlMapper",
                "DownloadFileMapper",
                "ImageBytesPruneMapper",
                "SpecifiedNumericFieldFilter",
                "ImageBytesExactDedupMapper",
                "RayDocumentDeduplicator",
                "ImageSchemaFinalizeMapper",
                "ImageOcrMapper",
                "SpecifiedFieldNonEmptyFilter",
            ],
        )
        self.assertEqual(ops[0].output_url_field, "item_image_urls")
        self.assertEqual(ops[1].text_fields, [])
        self.assertEqual(ops[1].source_specs[0].url_field, "item_image_urls")
        self.assertEqual(ops[1].source_specs[0].source, "ecom_video_item_raw_data")
        self.assertIn("cover_image_uri", ops[1].extra_keys)
        self.assertEqual(ops[1].passthrough_types["date"], pa.string())
        self.assertEqual(ops[1].passthrough_types["app_name"], pa.string())
        self.assertEqual(ops[4].min_value, 2)
        self.assertEqual(ops[7].passthrough_types["date"], pa.string())
        self.assertEqual(ops[7].passthrough_types["app_name"], pa.string())
        self.assertEqual(ops[8].ocr_result_key, "ocr_result")
        self.assertEqual(ops[9].field_key, "ocr_result")

    def test_ecom_video_item_video_hdfs_parquet_config_loads(self):
        _patch_yaml_loader_tags()
        path = os.path.join(
            os.getcwd(),
            "demos",
            "bytedance",
            "ecom_video_item_a_dragon",
            "configs",
            "ecom_video_item_video_hdfs_parquet.yaml",
        )

        with open(path, encoding="utf-8") as f:
            yaml_text = f.read()
        self.assertEqual(yaml_text.splitlines()[0].strip(), "# 电商视频-视频")

        cfg = init_configs(args=["--config", path, "--ray_address", "local"], load_configs_only=True)
        source_root = (
            "hdfs://haruna/default/default/ad_base/addrd_core/hive/ad_addrd_stats.db/"
            "ecom_video_item_sample_stats_daily_v2"
        )
        output_path = (
            "hdfs://haruna/ad_base/addrd_core/addrd_stats/lance/ai_data_forge.catalog/ccu/"
            "ecom_video_item_sample_stats_daily_v2"
        )

        self.assertEqual(cfg.executor_type, "ray")
        self.assertEqual(cfg.process, [])
        self.assertIn("TODO: add item_id_video_id_mapper", yaml_text)
        self.assertIn("TODO: add guldan_video_frames_mapper", yaml_text)
        self.assertIn("TODO: add a Ray branch/union pipeline", yaml_text)
        self.assertEqual([config["source"] for config in cfg.dataset["configs"]], ["hdfs"])
        self.assertEqual(
            [config["path"] for config in cfg.dataset["configs"]],
            [
                f"{source_root}/date=20260425",
            ],
        )
        self.assertEqual(cfg.export_path, output_path)
        self.assertEqual(cfg.export_type, "parquet")


class EcomPlayletConfigTest(unittest.TestCase):
    def test_ecom_playlet_hdfs_parquet_config_loads(self):
        _patch_yaml_loader_tags()
        path = os.path.join(
            os.getcwd(),
            "demos",
            "bytedance",
            "ecom_playlet_a_dragon",
            "configs",
            "ecom_playlet_hdfs_parquet.yaml",
        )

        with open(path, encoding="utf-8") as f:
            yaml_text = f.read()

        cfg = init_configs(args=["--config", path, "--ray_address", "local"], load_configs_only=True)
        source_path = "hdfs://haruna/home/byte_life_gen_ai/user/wangqianle/ecom_raw/playlet/20260204"
        output_path = "hdfs://haruna/ad_base/addrd_core/addrd_stats/lance/ai_data_forge.catalog/ccu/ecom_playlet"

        self.assertEqual(yaml_text.splitlines()[0].strip(), "# 短剧")
        self.assertIn("TODO: add ecom_playlet_schema_prepare_pipeline", yaml_text)
        self.assertIn("TODO: add guldan_video_frames_mapper", yaml_text)
        self.assertIn("TODO: add a Ray branch/union pipeline", yaml_text)
        self.assertEqual(cfg.executor_type, "ray")
        self.assertEqual(cfg.process, [])
        self.assertEqual(len(cfg.dataset["configs"]), 1)
        self.assertEqual(cfg.dataset["configs"][0]["source"], "hdfs")
        self.assertEqual(cfg.dataset["configs"][0]["path"], source_path)
        self.assertEqual(cfg.dataset["configs"][0]["format"], "parquet")
        self.assertEqual(cfg.export_path, output_path)
        self.assertEqual(cfg.export_type, "parquet")


if __name__ == "__main__":
    unittest.main()
