import hashlib
import json
import os
import tempfile
import types
import unittest
from unittest.mock import patch

import pyarrow as pa
import yaml

_register_extension_type = pa.register_extension_type


def _register_extension_type_once(extension_type):
    try:
        _register_extension_type(extension_type)
    except pa.ArrowKeyError:
        if not extension_type.extension_name.startswith("datasets.features.features."):
            raise


pa.register_extension_type = _register_extension_type_once

from data_juicer.config.config import init_configs
from data_juicer.core.data import NestedDataset
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.schema.bytes_exact_dedup_mapper import BytesExactDedupMapper
from data_juicer.ops.mapper.schema import bytes_exact_dedup_mapper as bytes_dedup_module
from data_juicer.ops.mapper.schema.field_assign_mapper import FieldAssignMapper
from data_juicer.ops.mapper.schema.field_drop_mapper import FieldDropMapper
from data_juicer.ops.mapper.schema.json_object_mapper import JsonObjectMapper
from data_juicer.ops.mapper.schema.video_url_rpc_mapper import (
    VideoUrlRpcMapper,
    sign_rpc_request,
)

pa.register_extension_type = _register_extension_type


class FieldAssignMapperTest(unittest.TestCase):
    def test_template_constant_copy_null_and_list_values(self):
        row = FieldAssignMapper(
            assignments={
                "id": {"template": "item_id-{item_id}", "type": "string"},
                "source": {"value": "ecom_video_raw_data", "type": "string"},
                "type": {"copy_from": "source_type", "type": "string"},
                "texts": {"value": None, "type": "string"},
                "urls": {"value": [], "type": "list<string>"},
                "videos": {"value": [], "type": "list<binary>"},
                "has_audio_in_video": {"value": True, "type": "bool"},
                "valid_video_count": {"value": 0, "type": "int64"},
            }
        ).process_single({"item_id": 123, "source_type": "video"})

        self.assertEqual(row["id"], "item_id-123")
        self.assertEqual(row["source"], "ecom_video_raw_data")
        self.assertEqual(row["type"], "video")
        self.assertIsNone(row["texts"])
        self.assertEqual(row["urls"], [])
        self.assertEqual(row["videos"], [])
        self.assertTrue(row["has_audio_in_video"])
        self.assertEqual(row["valid_video_count"], 0)

    def test_arrow_batch_keeps_all_null_list_and_binary_schema_stable(self):
        table = pa.table({"item_id": [1, 2], "source_type": ["video", "video"]})
        output = FieldAssignMapper(
            assignments={
                "texts": {"value": None, "type": "string"},
                "urls": {"value": [], "type": "list<string>"},
                "videos": {"value": [], "type": "list<binary>"},
                "duplicate_id_list": {"value": [], "type": "list<string>"},
            }
        ).process_batched(table)

        self.assertEqual(output.schema.field("texts").type, pa.string())
        self.assertEqual(output.schema.field("urls").type, pa.list_(pa.string()))
        self.assertEqual(output.schema.field("videos").type, pa.list_(pa.binary()))
        self.assertEqual(output.schema.field("duplicate_id_list").type, pa.list_(pa.string()))
        self.assertEqual(output.column("texts").to_pylist(), [None, None])

    def test_dict_batch_empty_and_missing_template_field(self):
        op = FieldAssignMapper(assignments={"id": {"template": "item_id-{missing}", "type": "string"}})

        self.assertEqual(op.process_batched({}), {"id": []})
        self.assertEqual(op.process_single({})["id"], "item_id-")


class JsonObjectMapperTest(unittest.TestCase):
    def test_include_all_excludes_configured_and_internal_keys(self):
        row = JsonObjectMapper(
            output_key="extra",
            include_all=True,
            exclude_keys=["date", "__dj__stats__", "__dj__source_file__"],
        ).process_single(
            {
                "item_id": 1,
                "date": "20260525",
                "__dj__stats__": {"x": 1},
                "__dj__source_file__": "part-0",
                "payload": {"a": [1]},
                "raw": b"abc",
            }
        )

        extra = json.loads(row["extra"])
        self.assertEqual(extra["item_id"], 1)
        self.assertEqual(extra["payload"], {"a": [1]})
        self.assertEqual(extra["raw"], "abc")
        self.assertNotIn("date", extra)
        self.assertNotIn("__dj__stats__", extra)

    def test_arrow_batch_jsonifies_scalar_bytes_list_and_dict(self):
        table = pa.table(
            {
                "id": pa.array(["a"]),
                "raw": pa.array([b"abc"], type=pa.binary()),
                "tags": pa.array([["x", "y"]], type=pa.list_(pa.string())),
                "nested": pa.array([{"score": 1}], type=pa.struct([("score", pa.int64())])),
                "date": pa.array(["20260525"]),
            }
        )

        output = JsonObjectMapper(output_key="extra", include_all=True, exclude_keys=["date"]).process_batched(table)

        self.assertEqual(output.schema.field("extra").type, pa.string())
        extra = json.loads(output.column("extra").to_pylist()[0])
        self.assertEqual(extra, {"id": "a", "raw": "abc", "tags": ["x", "y"], "nested": {"score": 1}})

    def test_include_keys_empty_batch_and_non_utf8_bytes(self):
        op = JsonObjectMapper(output_key="extra", include_keys=["raw", "ignored"])

        row = op.process_single({"raw": bytearray(b"\xff"), "ignored": memoryview(b"ok"), "skip": 1})
        extra = json.loads(row["extra"])
        self.assertEqual(extra["raw"], "/w==")
        self.assertEqual(extra["ignored"], "ok")
        self.assertEqual(op.process_batched({}), {"extra": []})


class FieldDropMapperTest(unittest.TestCase):
    def test_process_single_drops_configured_fields_and_keeps_others(self):
        row = FieldDropMapper(fields=["videos"]).process_single(
            {"id": "item_id-1", "urls": ["u1"], "videos": [b"video"]}
        )

        self.assertEqual(row, {"id": "item_id-1", "urls": ["u1"]})

    def test_dict_batch_drops_configured_fields(self):
        batch = {
            "id": ["item_id-1"],
            "urls": [["u1"]],
            "videos": [[b"video"]],
            "valid_video_count": [1],
        }

        output = FieldDropMapper(fields=["videos"]).process_batched(batch)

        self.assertEqual(
            output,
            {
                "id": ["item_id-1"],
                "urls": [["u1"]],
                "valid_video_count": [1],
            },
        )

    def test_arrow_batch_removes_binary_list_column_without_materializing_rows(self):
        table = pa.Table.from_pylist(
            [
                {
                    "id": "item_id-1",
                    "urls": ["u1"],
                    "videos": [b"video"],
                    "valid_video_count": 1,
                }
            ],
            schema=pa.schema(
                [
                    pa.field("id", pa.string()),
                    pa.field("urls", pa.list_(pa.string())),
                    pa.field("videos", pa.list_(pa.binary())),
                    pa.field("valid_video_count", pa.int64()),
                ]
            ),
        )

        output = FieldDropMapper(fields=["videos"]).process_batched(table)

        self.assertEqual(output.schema.names, ["id", "urls", "valid_video_count"])
        self.assertEqual(output.schema.field("urls").type, pa.list_(pa.string()))
        self.assertEqual(output.column("urls").to_pylist(), [["u1"]])
        self.assertEqual(output.column("valid_video_count").to_pylist(), [1])

    def test_missing_fields_are_noop(self):
        op = FieldDropMapper(fields=["missing"])

        self.assertEqual(op.process_single({"id": "item_id-1"}), {"id": "item_id-1"})
        self.assertEqual(op.process_batched({"id": ["item_id-1"]}), {"id": ["item_id-1"]})

        table = pa.table({"id": ["item_id-1"]})
        self.assertEqual(op.process_batched(table), table)


class BytesExactDedupMapperTest(unittest.TestCase):
    def test_deduplicates_aligned_url_and_bytes_lists_and_sets_md5(self):
        output = BytesExactDedupMapper(
            url_key="urls",
            bytes_key="videos",
            md5_key="md5",
            valid_count_key="valid_video_count",
        ).process_single({"urls": ["u2", "u1", "dup"], "videos": [b"b", b"a", b"a"]})

        sample_md5 = hashlib.md5()
        sample_md5.update(b"a")
        sample_md5.update(b"b")
        self.assertEqual(output["urls"], ["u1", "u2"])
        self.assertEqual(output["videos"], [b"a", b"b"])
        self.assertEqual(output["valid_video_count"], 2)
        self.assertEqual(output["md5"], sample_md5.hexdigest())

    def test_empty_bytes_sets_zero_count_and_null_md5(self):
        output = BytesExactDedupMapper(
            url_key="urls",
            bytes_key="videos",
            md5_key="md5",
            valid_count_key="valid_video_count",
        ).process_single({"urls": ["u1"], "videos": [None], "md5": "old"})

        self.assertEqual(output["urls"], [])
        self.assertEqual(output["videos"], [])
        self.assertEqual(output["valid_video_count"], 0)
        self.assertIsNone(output["md5"])

    def test_condition_false_leaves_row_unchanged(self):
        row = {"item_duration": 90, "urls": ["u1"], "videos": [b"a"], "md5": "old", "valid_video_count": 1}

        output = BytesExactDedupMapper(
            condition="item_duration <= 60",
            url_key="urls",
            bytes_key="videos",
            md5_key="md5",
            valid_count_key="valid_video_count",
        ).process_single(dict(row))

        self.assertEqual(output, row)

    def test_arrow_batch_keeps_binary_list_schema(self):
        table = pa.Table.from_pylist(
            [{"item_duration": 1, "urls": ["u"], "videos": [b"a"], "md5": None, "valid_video_count": 0}],
            schema=pa.schema(
                [
                    pa.field("item_duration", pa.int64()),
                    pa.field("urls", pa.list_(pa.string())),
                    pa.field("videos", pa.list_(pa.binary())),
                    pa.field("md5", pa.string()),
                    pa.field("valid_video_count", pa.int64()),
                ]
            ),
        )

        output = BytesExactDedupMapper(
            condition="item_duration <= 60",
            url_key="urls",
            bytes_key="videos",
            md5_key="md5",
            valid_count_key="valid_video_count",
        ).process_batched(table)

        self.assertEqual(output.schema.field("videos").type, pa.list_(pa.binary()))
        self.assertEqual(output.schema.field("urls").type, pa.list_(pa.string()))

    def test_dict_batch_empty_scalar_and_memoryview_helpers(self):
        op = BytesExactDedupMapper(url_key="urls", bytes_key="videos")

        self.assertEqual(op.process_batched({}), {"urls": [], "videos": [], "md5": [], "valid_video_count": []})
        urls, binary_values = bytes_dedup_module.dedup_aligned_bytes("u", memoryview(b"a"))
        self.assertEqual((urls, binary_values), (["u"], [b"a"]))
        self.assertIsNone(bytes_dedup_module._to_bytes("not-bytes"))


class VideoUrlRpcMapperTest(unittest.TestCase):
    def test_sign_rpc_request_contains_ak_deadline_and_hmac(self):
        signature = sign_rpc_request("ak", "sk", "MGetPlayInfosV2", "caller", ttl=60, now=100)

        self.assertTrue(signature.startswith("VARCH1-HMAC-SHA1:ak:160:"))

    def test_build_request_expands_ak_sk_from_environment(self):
        mapper = VideoUrlRpcMapper(ak="${VIDEOARCH_AK}", sk="${VIDEOARCH_SK}")
        api_thrift = types.SimpleNamespace(
            MGetPlayInfosV2Request=lambda: types.SimpleNamespace(),
            FilterParams=lambda **kwargs: types.SimpleNamespace(**kwargs),
            UrlParams=lambda **kwargs: types.SimpleNamespace(**kwargs),
            Identity=lambda **kwargs: types.SimpleNamespace(**kwargs),
            VideoDefinition=types.SimpleNamespace(V720P="720p"),
        )

        with patch.dict("os.environ", {"VIDEOARCH_AK": "ak-env", "VIDEOARCH_SK": "sk-env"}), patch(
            "data_juicer.ops.mapper.schema.video_url_rpc_mapper.sign_rpc_request",
            return_value="signed",
        ) as sign:
            req = mapper._build_request(api_thrift, "vid-a")

        self.assertEqual(req.Identity.IdentityInfo, "signed")
        self.assertEqual(sign.call_args.args[:2], ("ak-env", "sk-env"))

    def test_builds_720p_signed_request_and_extracts_url(self):
        created = []

        class FakeClient:
            def __init__(self, *_args, **_kwargs):
                self.calls = []
                created.append(self)

            def use(self, _middleware):
                return None

            def MGetPlayInfosV2(self, req):
                self.calls.append(req)
                meta = types.SimpleNamespace(Width=1280, Definition="720p", EncodedType="transcoded")
                video = types.SimpleNamespace(MainUrl="http://video-720", VideoMeta=meta)
                play_info = types.SimpleNamespace(Status=10, OriginalVideoInfo=None, VideoInfos=[video])
                return types.SimpleNamespace(VideoInfos={"vid-a": play_info})

        fake_euler = types.SimpleNamespace(Client=FakeClient)

        with patch.dict("sys.modules", {"euler": fake_euler}), patch(
            "data_juicer.ops.mapper.schema.video_url_rpc_mapper.sign_rpc_request",
            wraps=sign_rpc_request,
        ) as sign:
            mapper = VideoUrlRpcMapper(
                vid_key="vid",
                output_key="urls",
                quality_preference="720p",
                ak="ak",
                sk="sk",
                psm="toutiao.videoarch.smart_player",
                cluster="aweme",
            )
            output = mapper.process_single({"vid": "vid-a"})

        req = created[0].calls[0]
        self.assertEqual(output["urls"], ["http://video-720"])
        self.assertEqual(req.VIDs, ["vid-a"])
        self.assertEqual(req.FilterParams.NeedDefinition, mapper._api_thrift.VideoDefinition.V720P)
        self.assertIn("VARCH1-HMAC-SHA1:ak:", req.Identity.IdentityInfo)
        self.assertEqual(sign.call_args.args[3], "ad.ai.data_forge_merlin")

    def test_condition_false_does_not_create_client(self):
        with patch("data_juicer.ops.mapper.schema.video_url_rpc_mapper.VideoUrlRpcMapper._create_client_and_thrift") as create:
            output = VideoUrlRpcMapper(
                condition="item_duration <= 60",
                vid_key="vid",
                output_key="urls",
            ).process_single({"vid": "vid-a", "item_duration": 90})

        self.assertEqual(output["urls"], [])
        create.assert_not_called()

    def test_retry_failure_returns_empty_list(self):
        class FakeClient:
            def __init__(self, *_args, **_kwargs):
                self.calls = 0

            def use(self, _middleware):
                return None

            def MGetPlayInfosV2(self, _req):
                self.calls += 1
                raise RuntimeError("boom")

        fake_euler = types.SimpleNamespace(Client=FakeClient)

        with patch.dict("sys.modules", {"euler": fake_euler}):
            mapper = VideoUrlRpcMapper(vid_key="vid", output_key="urls", retry_times=2)
            output = mapper.process_single({"vid": "vid-a"})

        self.assertEqual(output["urls"], [])
        self.assertEqual(mapper._client.calls, 2)

    def test_rpc_metrics_emit_success_and_error_statuses(self):
        class FakeClient:
            def __init__(self, *_args, **_kwargs):
                self.calls = 0

            def use(self, _middleware):
                return None

            def MGetPlayInfosV2(self, _req):
                self.calls += 1
                if self.calls == 1:
                    meta = types.SimpleNamespace(Width=1280, Definition="720p", EncodedType="transcoded")
                    video = types.SimpleNamespace(MainUrl="http://video-720", VideoMeta=meta)
                    play_info = types.SimpleNamespace(Status=10, OriginalVideoInfo=None, VideoInfos=[video])
                    return types.SimpleNamespace(VideoInfos={"vid-ok": play_info})
                if self.calls == 2:
                    return types.SimpleNamespace(VideoInfos={})
                raise RuntimeError("boom")

        fake_euler = types.SimpleNamespace(Client=FakeClient)

        with patch.dict("sys.modules", {"euler": fake_euler}), patch(
            "data_juicer.ops.mapper.schema.video_url_rpc_mapper.emit_rpc_qps",
        ) as emit:
            mapper = VideoUrlRpcMapper(
                vid_key="vid",
                output_key="urls",
                retry_times=1,
                psm="toutiao.videoarch.smart_player",
                cluster="aweme",
            )
            self.assertEqual(mapper.process_single({"vid": "vid-ok"})["urls"], ["http://video-720"])
            self.assertEqual(mapper.process_single({"vid": "vid-empty"})["urls"], [])
            self.assertEqual(mapper.process_single({"vid": "vid-error"})["urls"], [])
            self.assertEqual(mapper.process_single({"vid": " "})["urls"], [])

        statuses = [call.kwargs["status"] for call in emit.call_args_list]
        self.assertEqual(statuses, ["success", "success", "error"])
        self.assertTrue(
            all(call.kwargs["op_name"] == "video_url_rpc_mapper" for call in emit.call_args_list)
        )
        self.assertTrue(
            all(
                call.kwargs["target"] == "sd://toutiao.videoarch.smart_player?cluster=aweme"
                for call in emit.call_args_list
            )
        )
        self.assertTrue(all(call.kwargs["method"] == "MGetPlayInfosV2" for call in emit.call_args_list))

    def test_batch_logs_summary_without_sensitive_values(self):
        rows = {
            "vid": ["vid-ok", "", "vid-error"],
            "item_duration": [10, 20, 30],
        }

        with patch.object(VideoUrlRpcMapper, "_resolve_url_once", side_effect=[["http://video"], RuntimeError("secret")]), patch(
            "data_juicer.ops.mapper.schema.video_url_rpc_mapper.emit_rpc_qps",
        ), patch("data_juicer.ops.mapper.schema.video_url_rpc_mapper.logger") as logger:
            mapper = VideoUrlRpcMapper(
                vid_key="vid",
                output_key="urls",
                condition="item_duration <= 60",
                retry_times=1,
            )
            output = mapper.process_batched(rows)

        self.assertEqual(output["urls"], [["http://video"], [], []])
        messages = [call.args[0] for call in logger.info.call_args_list]
        self.assertTrue(any("VideoUrlRpcMapper first worker batch" in message for message in messages))
        self.assertTrue(any("VideoUrlRpcMapper batch summary" in message for message in messages))
        logged_args = " ".join(str(arg) for call in logger.info.call_args_list for arg in call.args)
        self.assertNotIn("vid-ok", logged_args)
        self.assertNotIn("secret", logged_args)

    def test_state_empty_vid_batch_and_runtime_import_error_paths(self):
        mapper = VideoUrlRpcMapper(vid_key="vid", output_key="urls")
        mapper._client = object()
        mapper._api_thrift = object()
        state = mapper.__getstate__()

        self.assertIsNone(state["_client"])
        self.assertIsNone(state["_api_thrift"])
        self.assertEqual(mapper.process_single({"vid": " "})["urls"], [])
        self.assertEqual(mapper.process_batched({}), {"urls": []})

        table = pa.table({"vid": [""]})
        output = mapper.process_batched(table)
        self.assertEqual(output.schema.field("urls").type, pa.list_(pa.string()))
        self.assertEqual(output.column("urls").to_pylist(), [[]])

        with patch.dict("sys.modules", {"euler": None}):
            with self.assertRaisesRegex(RuntimeError, "Euler RPC runtime"):
                VideoUrlRpcMapper()._create_client_and_thrift()

    def test_video_info_selection_edges(self):
        original_meta = types.SimpleNamespace(Width=1920, Definition="ori", EncodedType="original")
        original = types.SimpleNamespace(MainUrl="ori", VideoMeta=original_meta)
        low = types.SimpleNamespace(
            MainUrl="low",
            VideoMeta=types.SimpleNamespace(Width=360, Definition="360p", EncodedType="transcoded"),
        )
        high = types.SimpleNamespace(
            MainUrl="high",
            VideoMeta=types.SimpleNamespace(Width=1280, Definition="720p", EncodedType="transcoded"),
        )

        high_mapper = VideoUrlRpcMapper(quality_preference="high")
        low_mapper = VideoUrlRpcMapper(quality_preference="low")
        exact_mapper = VideoUrlRpcMapper(quality_preference="540p")
        no_ori_mapper = VideoUrlRpcMapper(quality_preference="high", need_ori=False)

        self.assertIsNone(high_mapper._get_video_info(types.SimpleNamespace(VideoInfos={}), "missing"))
        self.assertIsNone(
            high_mapper._get_video_info(
                types.SimpleNamespace(VideoInfos={"vid": types.SimpleNamespace(Status=1, VideoInfos=[])}),
                "vid",
            )
        )
        self.assertIs(
            high_mapper._get_video_info(
                types.SimpleNamespace(
                    VideoInfos={
                        "vid": types.SimpleNamespace(Status=10, OriginalVideoInfo=original, VideoInfos=[low, high])
                    }
                ),
                "vid",
            ),
            original,
        )
        self.assertIs(
            low_mapper._get_video_info(
                types.SimpleNamespace(
                    VideoInfos={
                        "vid": types.SimpleNamespace(Status=10, OriginalVideoInfo=original, VideoInfos=[high, low])
                    }
                ),
                "vid",
            ),
            low,
        )
        self.assertIs(
            exact_mapper._get_video_info(
                types.SimpleNamespace(
                    VideoInfos={
                        "vid": types.SimpleNamespace(Status=10, OriginalVideoInfo=None, VideoInfos=[low, high])
                    }
                ),
                "vid",
            ),
            high,
        )
        self.assertIsNone(
            no_ori_mapper._get_video_info(
                types.SimpleNamespace(
                    VideoInfos={
                        "vid": types.SimpleNamespace(Status=10, OriginalVideoInfo=None, VideoInfos=[original])
                    }
                ),
                "vid",
            )
        )


class EcomVideoConfigLoadTest(unittest.TestCase):
    def test_new_video_ops_load_from_config_shape(self):
        config = {
            "process": [
                {"json_object_mapper": {"output_key": "extra", "include_all": True, "exclude_keys": ["date"]}},
                {
                    "field_assign_mapper": {
                        "assignments": {
                            "id": {"template": "item_id-{item_id}", "type": "string"},
                            "urls": {"value": [], "type": "list<string>"},
                            "videos": {"value": [], "type": "list<binary>"},
                        }
                    }
                },
                {"video_url_rpc_mapper": {"condition": "item_duration <= 60", "vid_key": "vid"}},
                {"bytes_exact_dedup_mapper": {"url_key": "urls", "bytes_key": "videos"}},
                {"field_drop_mapper": {"fields": ["videos"]}},
            ]
        }
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as handle:
            yaml.safe_dump(config, handle)
            path = handle.name
        self.addCleanup(lambda: os.path.exists(path) and os.unlink(path))

        cfg = init_configs(args=["--config", path], load_configs_only=True)

        ops = load_ops(cfg.process)

        self.assertEqual([op.__class__.__name__ for op in ops], [
            "JsonObjectMapper",
            "FieldAssignMapper",
            "VideoUrlRpcMapper",
            "BytesExactDedupMapper",
            "FieldDropMapper",
        ])

    def test_ecom_video_hdfs_parquet_configs_use_targets_and_new_ops(self):
        config_names = [
            "ecom_video_item_video_hdfs_parquet.yaml",
            "ecom_video_item_video_hdfs_parquet_demo_test.yaml",
        ]
        for config_name in config_names:
            with self.subTest(config_name=config_name):
                path = os.path.join(
                    os.getcwd(),
                    "demos",
                    "bytedance",
                    "ecom_video_item_a_dragon",
                    "configs",
                    config_name,
                )

                cfg = init_configs(args=["--config", path], load_configs_only=True)
                ops = load_ops(cfg.process)

                self.assertFalse(cfg.ray_data_checkpoint.enabled)
                self.assertEqual(
                    [op.__class__.__name__ for op in ops],
                    [
                        "JsonObjectMapper",
                        "FieldAssignMapper",
                        "VideoUrlRpcMapper",
                        "DownloadFileMapper",
                        "BytesExactDedupMapper",
                        "FieldDropMapper",
                        "RayFieldDeduplicator",
                        "DownloadFileMapper",
                        "BytesExactDedupMapper",
                    ],
                )
                self.assertEqual(ops[2].condition, "item_duration <= 60")
                self.assertEqual(ops[2].vid_key, "vid")
                self.assertEqual(ops[2].quality_preference, "720p")
                self.assertTrue(ops[3].resume_download)
                self.assertEqual(ops[4].bytes_key, "videos")
                self.assertEqual(ops[5].fields, ["videos"])
                self.assertEqual(ops[6].condition, "item_duration <= 60 and valid_video_count > 0")
                self.assertEqual(ops[6].representative_policy, "min_id")
                self.assertTrue(ops[7].resume_download)
                self.assertEqual(ops[7].download_field, "urls")
                self.assertEqual(ops[7].save_field, "videos")
                self.assertEqual(ops[8].condition, "item_duration <= 60")
                self.assertEqual(ops[8].bytes_key, "videos")
                targets = cfg.export["targets"] if isinstance(cfg.export, dict) else cfg.export.targets
                self.assertEqual(len(targets), 2)
                self.assertEqual(targets[0]["target"] if isinstance(targets[0], dict) else targets[0].target, "hdfs")
                self.assertEqual(targets[0]["type"] if isinstance(targets[0], dict) else targets[0].type, "parquet")
                first_filter = targets[0]["filter_condition"] if isinstance(targets[0], dict) else targets[0].filter_condition
                second_filter = targets[1]["filter_condition"] if isinstance(targets[1], dict) else targets[1].filter_condition
                self.assertIn("valid_video_count > 0", first_filter)
                self.assertEqual(second_filter, "item_duration > 60")


class RayFieldDeduplicatorEcomVideoTest(unittest.TestCase):
    def test_nested_dataset_keeps_min_id_and_records_removed_duplicate_ids(self):
        from data_juicer.ops.pipeline.ray_field_deduplicator import RayFieldDeduplicator

        dataset = NestedDataset.from_list(
            [
                {"id": "item_id-2", "item_duration": 10, "md5": "same", "valid_video_count": 1},
                {"id": "item_id-1", "item_duration": 10, "md5": "same", "valid_video_count": 1},
                {"id": "item_id-3", "item_duration": 90, "md5": "same", "valid_video_count": 0},
                {"id": "item_id-4", "item_duration": 10, "md5": None, "valid_video_count": 1},
            ]
        )

        rows = RayFieldDeduplicator(
            condition="item_duration <= 60 and valid_video_count > 0",
            field_key="md5",
            id_key="id",
            duplicate_ids_key="duplicate_id_list",
            duplicate_ids_mode="removed",
            representative_policy="min_id",
        ).run(dataset).to_list()

        self.assertEqual(
            rows,
            [
                {
                    "id": "item_id-1",
                    "item_duration": 10,
                    "md5": "same",
                    "valid_video_count": 1,
                    "duplicate_id_list": ["item_id-2"],
                },
                {
                    "id": "item_id-3",
                    "item_duration": 90,
                    "md5": "same",
                    "valid_video_count": 0,
                    "duplicate_id_list": [],
                },
                {
                    "id": "item_id-4",
                    "item_duration": 10,
                    "md5": None,
                    "valid_video_count": 1,
                    "duplicate_id_list": [],
                },
            ],
        )

    def test_pyarrow_group_min_id_helper_preserves_schema_and_duplicate_ids(self):
        from data_juicer.ops.pipeline.ray_field_deduplicator import RayFieldDeduplicator

        table = pa.Table.from_pylist(
            [
                {"id": "b", "md5": "same", "duplicate_id_list": []},
                {"id": "a", "md5": "same", "duplicate_id_list": []},
            ],
            schema=pa.schema(
                [
                    pa.field("id", pa.string()),
                    pa.field("md5", pa.string()),
                    pa.field("duplicate_id_list", pa.list_(pa.string())),
                ]
            ),
        )
        with_hash = RayFieldDeduplicator._append_hash_batch(
            table,
            field_key="md5",
            hash_key=RayFieldDeduplicator._HASH_KEY,
            condition="",
        )

        output = RayFieldDeduplicator._take_group_representative(
            with_hash,
            hash_key=RayFieldDeduplicator._HASH_KEY,
            id_key="id",
            duplicate_ids_key="duplicate_id_list",
            duplicate_ids_mode="removed",
            representative_policy="min_id",
        )

        self.assertEqual(output.to_pylist(), [{"id": "a", "md5": "same", "duplicate_id_list": ["b"]}])
        self.assertEqual(output.schema, table.schema)

    def test_pyarrow_group_helper_slices_representative_without_rebuilding_null_fields(self):
        from data_juicer.ops.pipeline.ray_field_deduplicator import RayFieldDeduplicator

        schema = pa.schema(
            [
                pa.field("id", pa.string()),
                pa.field("md5", pa.string()),
                pa.field("texts", pa.string(), nullable=False),
                pa.field("duplicate_id_list", pa.list_(pa.string())),
            ]
        )
        table = pa.Table.from_arrays(
            [
                pa.array(["b", "a"], type=pa.string()),
                pa.array(["same", "same"], type=pa.string()),
                pa.array([None, None], type=pa.string()),
                pa.array([[], []], type=pa.list_(pa.string())),
            ],
            schema=schema,
        )
        with_hash = RayFieldDeduplicator._append_hash_batch(
            table,
            field_key="md5",
            hash_key=RayFieldDeduplicator._HASH_KEY,
            condition="",
        )

        output = RayFieldDeduplicator._take_group_representative(
            with_hash,
            hash_key=RayFieldDeduplicator._HASH_KEY,
            id_key="id",
            duplicate_ids_key="duplicate_id_list",
            duplicate_ids_mode="removed",
            representative_policy="min_id",
        )

        self.assertEqual(
            output.to_pylist(),
            [{"id": "a", "md5": "same", "texts": None, "duplicate_id_list": ["b"]}],
        )
        self.assertEqual(output.schema, table.schema)


if __name__ == "__main__":
    unittest.main()
