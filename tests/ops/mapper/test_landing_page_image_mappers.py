import hashlib
import json
import os
import tempfile
import unittest
from io import BytesIO
from unittest.mock import patch

import numpy as np
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
from data_juicer.core.data import NestedDataset
from data_juicer.ops.filter.specified_numeric_field_filter import SpecifiedNumericFieldFilter
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.io.download_file_mapper import DownloadFileMapper
from data_juicer.ops.mapper.schema.image_bytes_exact_dedup_mapper import ImageBytesExactDedupMapper
from data_juicer.ops.mapper.schema.image_bytes_prune_mapper import ImageBytesPruneMapper
from data_juicer.ops.mapper.schema.landing_page_image_schema_finalize_mapper import (
    LandingPageImageSchemaFinalizeMapper,
)
from data_juicer.ops.mapper.schema.landing_page_image_url_mapper import LandingPageImageUrlMapper
from data_juicer.ops.mapper.schema.list_field_flatten_mapper import ListFieldFlattenMapper
from data_juicer.utils.constant import HashKeys

pa.register_extension_type = _register_extension_type

LANDING_PAGE_PASSTHROUGH_KEYS = [
    "site_id",
    "local_stat_time_day",
    "p_date",
    "external_url",
    "thumbnail",
    "page_public_data",
    "preload_resources",
    "site_type",
    "is_highlight",
    "cost",
    "send_count",
    "show_count",
    "click_count",
    "convert_count",
    "ad_id",
    "campaign_id",
    "advertiser_id",
    "customer_id",
    "company_id",
    "first_industry_id",
    "first_industry_name",
    "second_industry_id",
    "second_industry_name",
    "third_industry_id",
    "ad_first_industry_id",
    "ad_first_industry_name",
    "ad_second_industry_id",
    "ad_second_industry_name",
    "ad_third_industry_id",
    "ad_third_industry_name",
]


def _image_bytes(seed: int = 0, size=(120, 120)) -> bytes:
    rng = np.random.default_rng(seed)
    array = rng.integers(0, 256, size=(size[1], size[0], 3), dtype=np.uint8)
    buffer = BytesIO()
    Image.fromarray(array, mode="RGB").save(buffer, format="PNG")
    return buffer.getvalue()


class LandingPageImageUrlMapperTest(unittest.TestCase):
    def test_thumbnail_single_url_is_extracted(self):
        op = LandingPageImageUrlMapper(
            image_source="thumbnail",
            extra_keys=["site_id", "local_stat_time_day", "external_url"],
        )
        sample = op.process_single(
            {
                "site_id": 123,
                "local_stat_time_day": "20260308",
                "external_url": "https://example.com",
                "thumbnail": " https://example.com/a.png ",
            }
        )

        self.assertEqual(sample["images"], ["https://example.com/a.png"])
        self.assertEqual(sample["__dj_landing_page_id"], "site_id-123")
        self.assertEqual(sample["__dj_landing_page_image_source"], "thumbnail")
        self.assertNotIn("__dj_landing_page_extra", sample)

    def test_preload_resources_supports_list_dict_list_str_and_json(self):
        op = LandingPageImageUrlMapper(image_source="preloads", extra_keys=["site_id"])

        dict_list = op.process_single(
            {
                "site_id": 123,
                "preload_resources": [
                    {"url": "https://example.com/a.png"},
                    {"url": " "},
                    "https://example.com/b.png",
                ],
            }
        )
        json_list = op.process_single(
            {
                "site_id": 123,
                "preload_resources": json.dumps(
                    [
                        {"url": "https://example.com/c.png"},
                        "https://example.com/d.png",
                    ]
                ),
            }
        )
        single_string = op.process_single({"site_id": 123, "preload_resources": "https://example.com/e.png"})

        self.assertEqual(dict_list["images"], ["https://example.com/a.png", "https://example.com/b.png"])
        self.assertEqual(json_list["images"], ["https://example.com/c.png", "https://example.com/d.png"])
        self.assertEqual(single_string["images"], ["https://example.com/e.png"])
        self.assertNotIn("__dj_landing_page_extra", dict_list)

    def test_extra_keys_are_accepted_but_not_serialized(self):
        op = LandingPageImageUrlMapper(extra_keys=["site_id", "cost"], extra_cache_key="legacy_extra")
        sample = op.process_single({"site_id": 123, "cost": None, "thumbnail": "https://example.com/a.png"})

        self.assertEqual(op.extra_keys, ["site_id", "cost"])
        self.assertNotIn("legacy_extra", sample)
        self.assertNotIn("__dj_landing_page_extra", sample)

    def test_passthrough_types_normalize_object_and_null_columns(self):
        op = LandingPageImageUrlMapper(
            passthrough_types={
                "preload_resources": "string",
                "cost": "int64",
                "is_highlight": "int64",
            },
        )

        sample = op.process_single(
            {
                "site_id": 123,
                "thumbnail": "https://example.com/a.png",
                "preload_resources": [{"url": "https://example.com/preload.png"}],
                "cost": "",
                "is_highlight": True,
            }
        )

        self.assertEqual(json.loads(sample["preload_resources"]), [{"url": "https://example.com/preload.png"}])
        self.assertIsNone(sample["cost"])
        self.assertEqual(sample["is_highlight"], 1)
        self.assertNotIn("__dj_landing_page_extra", sample)

    def test_arrow_batches_are_processed_without_single_row_ray_batches(self):
        op = LandingPageImageUrlMapper(
            image_source="preloads",
            passthrough_types={
                "cost": "int64",
                "is_highlight": "int64",
            },
        )
        table = pa.Table.from_pylist(
            [
                {
                    "site_id": 123,
                    "preload_resources": json.dumps([{"url": "https://example.com/a.png"}]),
                    "cost": "10",
                    "is_highlight": "1",
                },
                {
                    "site_id": 456,
                    "preload_resources": " ",
                    "cost": "",
                    "is_highlight": "0",
                },
            ]
        )

        batch = op.process_batched(table)

        self.assertTrue(op.is_batched_op())
        self.assertEqual(batch["images"], [["https://example.com/a.png"], []])
        self.assertEqual(batch["__dj_landing_page_id"], ["site_id-123", "site_id-456"])
        self.assertEqual(batch["__dj_landing_page_image_source"], ["preloads", "preloads"])
        self.assertEqual(batch["cost"], [10, None])
        self.assertEqual(batch["is_highlight"], [1, 0])
        self.assertNotIn("__dj_landing_page_extra", batch)


class ImageBytesMapperTest(unittest.TestCase):
    def test_invalid_images_are_pruned_per_image(self):
        valid = _image_bytes(1)
        too_small = _image_bytes(2, size=(20, 20))
        op = ImageBytesPruneMapper()

        sample = op.process_single(
            {
                "images": ["valid", "failed", "invalid", "small"],
                "image_bytes": [valid, None, b"not-an-image", too_small],
            }
        )

        self.assertEqual(sample["images"], ["valid"])
        self.assertEqual(sample["image_bytes"], [valid])
        self.assertEqual(sample["valid_image_count"], 1)

    def test_empty_url_or_invalid_image_leaves_zero_valid_count(self):
        op = ImageBytesPruneMapper()

        sample = op.process_single({"images": [], "image_bytes": []})
        self.assertEqual(sample["valid_image_count"], 0)

        sample = op.process_single({"images": ["invalid"], "image_bytes": [b"not-an-image"]})
        self.assertEqual(sample["images"], [])
        self.assertEqual(sample["image_bytes"], [])
        self.assertEqual(sample["valid_image_count"], 0)

    def test_duplicate_images_are_removed_and_md5_is_stable(self):
        img1 = _image_bytes(1)
        img2 = _image_bytes(2)
        op = ImageBytesExactDedupMapper()
        sample = op.process_single(
            {
                "images": ["url-b", "url-a", "url-a-duplicate"],
                "image_bytes": [img2, img1, img1],
            }
        )

        expected_md5 = hashlib.md5()
        for img in sorted([img1, img2]):
            expected_md5.update(img)

        self.assertEqual(sample["image_bytes"], sorted([img1, img2]))
        self.assertEqual(sample["images"], ["url-a", "url-b"])
        self.assertEqual(sample["valid_image_count"], 2)
        self.assertEqual(sample["md5"], expected_md5.hexdigest())


class LandingPageImageSchemaFinalizeMapperTest(unittest.TestCase):
    def test_finalize_outputs_dj_schema_and_drops_temporary_fields(self):
        img = _image_bytes(1)
        sample = {
            "__dj_landing_page_id": "site_id-123",
            "__dj_landing_page_image_source": "preloads",
            "images": ["https://example.com/a.png"],
            "image_bytes": [img],
            "md5": "abc",
            HashKeys.is_unique: True,
            "raw_field": "drop-me",
        }

        row = LandingPageImageSchemaFinalizeMapper().process_single(sample)

        self.assertEqual(
            sorted(row.keys()),
            [
                "audios",
                "has_audio_in_video",
                "id",
                "images",
                "md5",
                "source",
                "texts",
                "type",
                "videos",
            ],
        )
        self.assertEqual(row["id"], "site_id-123")
        self.assertEqual(row["source"], "site_creative_preloads_raw_data")
        self.assertEqual(row["texts"], [])
        self.assertEqual(row["images"], [img])
        self.assertEqual(row["audios"], [])
        self.assertEqual(row["videos"], [])
        self.assertNotIn("extra", row)

    def test_finalize_keeps_configured_passthrough_fields(self):
        sample = {
            "__dj_landing_page_id": "site_id-123",
            "__dj_landing_page_image_source": "thumbnail",
            "images": ["https://example.com/a.png"],
            "image_bytes": [_image_bytes(1)],
            "md5": "abc",
            "site_id": 123,
            "local_stat_time_day": "20260308",
            "p_date": "20260421",
        }

        row = LandingPageImageSchemaFinalizeMapper(
            passthrough_keys=["site_id", "local_stat_time_day", "p_date"]
        ).process_single(sample)

        self.assertEqual(row["site_id"], 123)
        self.assertEqual(row["local_stat_time_day"], "20260308")
        self.assertEqual(row["p_date"], "20260421")

    def test_finalize_ignores_legacy_extra_values(self):
        img = _image_bytes(1)
        op = LandingPageImageSchemaFinalizeMapper()

        row = op.process_single(
            {
                "__dj_landing_page_id": "site_id-1",
                "__dj_landing_page_image_source": "thumbnail",
                "__dj_landing_page_extra": {"site_id": 2},
                "image_bytes": [img],
                "md5": "abc",
            }
        )

        self.assertNotIn("extra", row)

    def test_finalize_empty_batches_keep_output_shape(self):
        op = LandingPageImageSchemaFinalizeMapper(passthrough_keys=["site_id"])

        dict_output = op.process_batched({"__dj_landing_page_id": [], "site_id": []})
        arrow_output = op.process_batched(pa.table({}))

        self.assertEqual(dict_output["id"], [])
        self.assertEqual(dict_output["site_id"], [])
        self.assertIsInstance(arrow_output, pa.Table)
        self.assertEqual(arrow_output.num_rows, 0)
        self.assertEqual(arrow_output.schema.field("images").type, pa.list_(pa.binary()))

    def test_passthrough_type_inference_without_input_schema(self):
        typed_output = LandingPageImageSchemaFinalizeMapper(passthrough_keys=["score"])._rows_to_arrow_table(
            [{"id": "row-1", "score": 7}],
            input_schema=None,
        )
        null_output = LandingPageImageSchemaFinalizeMapper(passthrough_keys=["optional_note"])._rows_to_arrow_table(
            [{"id": "row-1", "optional_note": None}],
            input_schema=None,
        )

        self.assertEqual(typed_output.schema.field("score").type, pa.int64())
        self.assertEqual(null_output.schema.field("optional_note").type, pa.string())

    def test_passthrough_type_config_accepts_pyarrow_type_and_rejects_unknown_type(self):
        op = LandingPageImageSchemaFinalizeMapper(
            passthrough_keys=["score"],
            passthrough_types={"score": pa.int32()},
        )

        self.assertEqual(op.passthrough_types["score"], pa.int32())
        with self.assertRaises(ValueError):
            LandingPageImageSchemaFinalizeMapper(passthrough_types={"score": "decimal128"})

    def test_finalize_pyarrow_batch_has_stable_explicit_schema(self):
        img = _image_bytes(1)
        input_table = pa.Table.from_pylist(
            [
                {
                    "__dj_landing_page_id": "site_id-1",
                    "__dj_landing_page_image_source": "thumbnail",
                    "images": ["https://example.com/a.png"],
                    "image_bytes": [img],
                    "md5": "abc",
                    "site_id": 1,
                    "cost": None,
                },
                {
                    "__dj_landing_page_id": "site_id-2",
                    "__dj_landing_page_image_source": "thumbnail",
                    "images": ["https://example.com/b.png"],
                    "image_bytes": [img],
                    "md5": "def",
                    "site_id": 2,
                    "cost": 10,
                },
            ]
        )

        output_table = LandingPageImageSchemaFinalizeMapper(
            passthrough_keys=["site_id", "cost"],
            passthrough_types={"site_id": "int64", "cost": "int64"},
        ).process_batched(input_table)

        self.assertIsInstance(output_table, pa.Table)
        self.assertEqual(output_table.schema.field("id").type, pa.string())
        self.assertEqual(output_table.schema.field("texts").type, pa.list_(pa.string()))
        self.assertEqual(output_table.schema.field("images").type, pa.list_(pa.binary()))
        self.assertEqual(output_table.schema.field("audios").type, pa.list_(pa.binary()))
        self.assertEqual(output_table.schema.field("videos").type, pa.list_(pa.binary()))
        self.assertEqual(output_table.schema.field("has_audio_in_video").type, pa.bool_())
        self.assertEqual(output_table.schema.field("site_id").type, input_table.schema.field("site_id").type)
        self.assertEqual(output_table.schema.field("cost").type, input_table.schema.field("cost").type)
        self.assertEqual(output_table.column("texts").to_pylist(), [[], []])
        self.assertEqual(output_table.column("audios").to_pylist(), [[], []])
        self.assertEqual(output_table.column("videos").to_pylist(), [[], []])

    def test_finalize_pyarrow_batch_wraps_scalar_binary_image_bytes(self):
        img = _image_bytes(1)
        input_table = pa.Table.from_arrays(
            [
                pa.array(["site_id-1"], type=pa.string()),
                pa.array(["thumbnail"], type=pa.string()),
                pa.array(["abc"], type=pa.string()),
                pa.array([img], type=pa.binary()),
            ],
            names=[
                "__dj_landing_page_id",
                "__dj_landing_page_image_source",
                "md5",
                "image_bytes",
            ],
        )

        output_table = LandingPageImageSchemaFinalizeMapper().process_batched(input_table)

        self.assertEqual(output_table.schema.field("images").type, pa.list_(pa.binary()))
        self.assertEqual(output_table.column("images").to_pylist(), [[img]])

    def test_finalize_pyarrow_batch_accepts_uint8_list_image_bytes(self):
        img = _image_bytes(1)
        input_table = pa.Table.from_arrays(
            [
                pa.array(["site_id-1"], type=pa.string()),
                pa.array(["thumbnail"], type=pa.string()),
                pa.array(["abc"], type=pa.string()),
                pa.array([list(img)], type=pa.list_(pa.uint8())),
            ],
            names=[
                "__dj_landing_page_id",
                "__dj_landing_page_image_source",
                "md5",
                "image_bytes",
            ],
        )

        output_table = LandingPageImageSchemaFinalizeMapper().process_batched(input_table)

        self.assertEqual(output_table.schema.field("images").type, pa.list_(pa.binary()))
        self.assertEqual(output_table.column("images").to_pylist(), [[img]])

    def test_passthrough_types_override_null_only_input_block_schema(self):
        null_only_block = pa.Table.from_pylist(
            [
                {
                    "__dj_landing_page_id": "site_id-1",
                    "__dj_landing_page_image_source": "thumbnail",
                    "images": ["https://example.com/a.png"],
                    "image_bytes": [_image_bytes(1)],
                    "md5": "abc",
                    "cost": None,
                }
            ]
        )
        value_block = pa.Table.from_pylist(
            [
                {
                    "__dj_landing_page_id": "site_id-2",
                    "__dj_landing_page_image_source": "thumbnail",
                    "images": ["https://example.com/b.png"],
                    "image_bytes": [_image_bytes(1)],
                    "md5": "def",
                    "cost": 10,
                }
            ]
        )

        op = LandingPageImageSchemaFinalizeMapper(
            passthrough_keys=["cost"],
            passthrough_types={"cost": "int64"},
        )
        null_output = op.process_batched(null_only_block)
        value_output = op.process_batched(value_block)

        self.assertEqual(null_only_block.schema.field("cost").type, pa.null())
        self.assertEqual(null_output.schema.field("cost").type, pa.int64())
        self.assertEqual(value_output.schema.field("cost").type, pa.int64())
        self.assertEqual(null_output.column("cost").to_pylist(), [None])
        self.assertEqual(value_output.column("cost").to_pylist(), [10])

    def test_passthrough_types_coerce_object_values_before_arrow_array(self):
        output_table = LandingPageImageSchemaFinalizeMapper(
            passthrough_keys=["preload_resources", "cost", "is_highlight"],
            passthrough_types={
                "preload_resources": "string",
                "cost": "int64",
                "is_highlight": "int64",
            },
        )._rows_to_arrow_table(
            [
                {
                    "id": "site_id-1",
                    "preload_resources": [{"url": "https://example.com/a.png"}],
                    "cost": "",
                    "is_highlight": True,
                }
            ],
            input_schema=None,
        )

        self.assertEqual(output_table.schema.field("preload_resources").type, pa.string())
        self.assertEqual(output_table.schema.field("cost").type, pa.int64())
        self.assertEqual(output_table.schema.field("is_highlight").type, pa.int64())
        self.assertEqual(
            json.loads(output_table.column("preload_resources").to_pylist()[0]),
            [{"url": "https://example.com/a.png"}],
        )
        self.assertEqual(output_table.column("cost").to_pylist(), [None])
        self.assertEqual(output_table.column("is_highlight").to_pylist(), [1])

    def test_finalize_empty_pyarrow_batch_keeps_explicit_schema(self):
        input_table = pa.Table.from_pylist(
            [
                {
                    "__dj_landing_page_id": "site_id-1",
                    "__dj_landing_page_image_source": "thumbnail",
                    "images": ["https://example.com/a.png"],
                    "image_bytes": [_image_bytes(1)],
                    "md5": "abc",
                    "site_id": 1,
                }
            ]
        ).slice(0, 0)

        output_table = LandingPageImageSchemaFinalizeMapper(passthrough_keys=["site_id"]).process_batched(input_table)

        self.assertEqual(output_table.num_rows, 0)
        self.assertEqual(output_table.schema.field("texts").type, pa.list_(pa.string()))
        self.assertEqual(output_table.schema.field("images").type, pa.list_(pa.binary()))
        self.assertEqual(output_table.schema.field("site_id").type, input_table.schema.field("site_id").type)


class LandingPageConfigTest(unittest.TestCase):
    def test_demo_configs_load_and_register_composed_ops_without_tqs_request(self):
        config_dir = os.path.join(os.getcwd(), "demos", "bytedance", "process_landing_page_on_ray", "configs")
        if not os.path.isdir(config_dir):
            self.skipTest("ByteDance landing page demo configs are not included")
        expected_ops = [
            "LandingPageImageUrlMapper",
            "DownloadFileMapper",
            "ImageBytesPruneMapper",
            "SpecifiedNumericFieldFilter",
            "ImageBytesExactDedupMapper",
            "RayDocumentDeduplicator",
            "LandingPageImageSchemaFinalizeMapper",
        ]
        expected_sources = {
            "thumbnail.yaml": (
                "thumbnail",
                "thumbnail",
                "ai_data_forge.ccu.landing_page_thumbnail",
                2,
                2,
                50,
                64,
                20000,
                512,
                None,
            ),
            "preloads.yaml": (
                "preloads",
                "preload_resources",
                "ai_data_forge.ccu.landing_page_chengzi",
                3,
                4,
                100,
                8,
                128,
                64,
                1,
            ),
        }

        for name, (
            image_source,
            image_column,
            export_table,
            download_retry_times,
            download_max_concurrent,
            download_batch_size,
            dedup_set_num,
            override_num_blocks,
            concurrency,
            ray_remote_num_cpus,
        ) in expected_sources.items():
            path = os.path.join(config_dir, name)
            with patch("data_juicer.core.data.load_strategy.run_tqs_query") as mock_tqs:
                cfg = init_configs(args=["--config", path, "--ray_address", "local"], load_configs_only=True)

            ops = load_ops(cfg.process)
            ds_config = cfg.dataset["configs"][0]
            self.assertEqual([op.__class__.__name__ for op in ops], expected_ops)
            self.assertEqual(ops[0].image_source, image_source)
            self.assertEqual(ops[1].retry_times, download_retry_times)
            self.assertEqual(ops[1].batch_size, download_batch_size)
            self.assertEqual(ops[1].max_concurrent, download_max_concurrent)
            self.assertEqual(ops[5].backend._dedup_set_num_config, dedup_set_num)
            self.assertEqual(ops[5].text_key, "md5")
            self.assertEqual(ops[6].passthrough_keys, LANDING_PAGE_PASSTHROUGH_KEYS)
            self.assertEqual(ops[6].passthrough_types["site_id"], pa.int64())
            self.assertEqual(ops[6].passthrough_types["local_stat_time_day"], pa.string())
            self.assertEqual(ops[6].passthrough_types["p_date"], pa.string())
            self.assertEqual(ops[6].passthrough_types["external_url"], pa.string())
            self.assertEqual(ops[6].passthrough_types["preload_resources"], pa.string())
            self.assertEqual(ops[6].passthrough_types["ad_id"], pa.int64())
            self.assertEqual(ops[6].passthrough_types["customer_id"], pa.string())
            self.assertEqual(ds_config["type"], "remote")
            self.assertEqual(ds_config["source"], "hive")
            self.assertEqual(ds_config["table_name"], "ad_addrd_stats.site_creative_center_df_stats_daily_sample_v2_chengzi")
            self.assertIn(image_column, ds_config["columns"])
            self.assertIn("thumbnail", ds_config["columns"])
            self.assertIn("preload_resources", ds_config["columns"])
            self.assertIn("page_public_data", ds_config["columns"])
            self.assertIn("p_date = '20260423'", ds_config["filter"])
            self.assertEqual(ds_config["override_num_blocks"], override_num_blocks)
            self.assertEqual(ds_config["concurrency"], concurrency)
            if ray_remote_num_cpus is None:
                self.assertNotIn("ray_remote_args", ds_config)
            else:
                self.assertEqual(ds_config["ray_remote_args"]["num_cpus"], ray_remote_num_cpus)
            self.assertNotIn("query", ds_config)
            self.assertNotIn("output_uri", ds_config)
            self.assertEqual(cfg.export["target"], "magnus")
            self.assertEqual(cfg.export["table_name"], export_table)
            export_field_names = [field["name"] for field in cfg.export["schema"]["fields"]]
            self.assertNotIn("extra", export_field_names)
            for field_name in LANDING_PAGE_PASSTHROUGH_KEYS:
                self.assertIn(field_name, export_field_names)
            self.assertEqual(cfg.export["operation"], "OVERWRITE")
            self.assertEqual(cfg.export["partition_columns"], ["p_date"])
            self.assertEqual(cfg.export["partition_values"]["p_date"], "20260423")
            self.assertEqual(cfg.export["magnus_conf"]["concurrency"], 8)
            self.assertEqual(cfg.export["magnus_conf"]["ray_remote_args"]["num_cpus"], 1)
            self.assertEqual(cfg.export["magnus_conf"]["write_options"]["write.format.default"], "lance")
            self.assertEqual(cfg.export["magnus_conf"]["write_options"]["magnus.ray.write.disable_repartition"], "true")
            self.assertEqual(cfg.export["magnus_conf"]["write_options"]["magnus.ray.write.disable_sort"], "true")
            mock_tqs.assert_not_called()

    def test_preloads_demo_config_only_sets_effective_ray_batch_sizes(self):
        path = os.path.join(
            os.getcwd(),
            "demos",
            "bytedance",
            "process_landing_page_on_ray",
            "configs",
            "preloads_demo.yaml",
        )
        if not os.path.exists(path):
            self.skipTest("ByteDance landing page demo configs are not included")

        with patch("data_juicer.core.data.load_strategy.run_tqs_query") as mock_tqs:
            cfg = init_configs(args=["--config", path, "--ray_address", "local"], load_configs_only=True)

        ops = load_ops(cfg.process)
        ds_config = cfg.dataset["configs"][0]
        process_configs = {}
        for op_config in cfg.process:
            op_name, op_kwargs = next(iter(op_config.items()))
            process_configs[op_name] = op_kwargs or {}

        self.assertEqual(ds_config["override_num_blocks"], 1024)
        self.assertEqual(ds_config["concurrency"], 32)
        self.assertNotIn("cast_columns", ds_config)
        self.assertIn("is_highlight", ds_config["columns"])
        self.assertEqual(ds_config["columns"]["is_highlight"], "BIGINT")
        export_field_names = [field["name"] for field in cfg.export["schema"]["fields"]]
        self.assertNotIn("extra", export_field_names)
        self.assertIn("is_highlight", export_field_names)
        self.assertTrue(cfg.export["create_table_if_not_exists"])
        self.assertEqual(ds_config["columns"]["customer_id"], "BIGINT")
        self.assertEqual(ds_config["columns"]["preload_resources"], "STRING")
        self.assertEqual(cfg.export["magnus_conf"]["concurrency"], 8)
        self.assertEqual(cfg.export["magnus_conf"]["write_options"]["write.format.default"], "lance")
        ineffective_batch_size_ops = [
            "list_field_flatten_mapper",
            "image_bytes_prune_mapper",
            "specified_numeric_field_filter",
            "image_bytes_exact_dedup_mapper",
            "ray_document_deduplicator",
        ]
        for op_name in ineffective_batch_size_ops:
            self.assertNotIn("batch_size", process_configs[op_name])

        self.assertEqual(process_configs["landing_page_image_url_mapper"]["batch_size"], 5000)
        self.assertEqual(process_configs["download_file_mapper"]["batch_size"], 8)
        self.assertEqual(process_configs["download_file_mapper"]["max_concurrent"], 1)
        self.assertEqual(process_configs["ray_document_deduplicator"]["dedup_set_num"], 24)
        self.assertEqual(process_configs["list_field_flatten_mapper"]["field_key"], "images")
        self.assertEqual(process_configs["list_field_flatten_mapper"]["id_key"], "__dj_landing_page_id")
        self.assertEqual(process_configs["list_field_flatten_mapper"]["id_index_separator"], "-")
        self.assertIsInstance(ops[0], LandingPageImageUrlMapper)
        self.assertTrue(ops[0].is_batched_op())
        self.assertEqual(ops[0].batch_size, 5000)
        self.assertIsInstance(ops[1], ListFieldFlattenMapper)
        self.assertEqual(ops[1].field_key, "images")
        self.assertEqual(ops[1].id_key, "__dj_landing_page_id")
        self.assertEqual(process_configs["landing_page_image_schema_finalize_mapper"]["batch_size"], 32)
        self.assertEqual(process_configs["landing_page_image_url_mapper"]["passthrough_types"]["cost"], "int64")
        self.assertEqual(process_configs["landing_page_image_url_mapper"]["passthrough_types"]["is_highlight"], "int64")
        self.assertEqual(process_configs["list_field_flatten_mapper"]["passthrough_types"]["cost"], "int64")
        self.assertEqual(process_configs["list_field_flatten_mapper"]["passthrough_types"]["is_highlight"], "int64")
        self.assertIn("is_highlight", process_configs["landing_page_image_schema_finalize_mapper"]["passthrough_keys"])
        self.assertEqual(
            process_configs["landing_page_image_schema_finalize_mapper"]["passthrough_types"]["is_highlight"],
            "int64",
        )
        expected_num_proc_by_op = {
            "landing_page_image_url_mapper": 192,
            "list_field_flatten_mapper": 128,
            "download_file_mapper": 256,
            "image_bytes_prune_mapper": 192,
            "specified_numeric_field_filter": 128,
            "image_bytes_exact_dedup_mapper": 192,
            "ray_document_deduplicator": 192,
            "landing_page_image_schema_finalize_mapper": 128,
        }
        for op_name, op_kwargs in process_configs.items():
            self.assertEqual(op_kwargs["num_proc"], expected_num_proc_by_op[op_name])
        mock_tqs.assert_not_called()

    def test_preloads_demo_tqs_100_config_loads_for_magnus_probe(self):
        path = os.path.join(
            os.getcwd(),
            "demos",
            "bytedance",
            "process_landing_page_on_ray",
            "configs",
            "preloads_demo_tqs_100.yaml",
        )
        if not os.path.exists(path):
            self.skipTest("ByteDance landing page demo configs are not included")

        with patch("data_juicer.core.data.load_strategy.run_tqs_query") as mock_tqs:
            cfg = init_configs(args=["--config", path, "--ray_address", "local"], load_configs_only=True)

        ops = load_ops(cfg.process)
        ds_config = cfg.dataset["configs"][0]

        self.assertEqual(ds_config["source"], "tqs")
        self.assertEqual(ds_config["read_mode"], "client_result")
        self.assertEqual(ds_config["max_result_rows"], 1000)
        self.assertIn("FROM ad_addrd_stats.site_creative_center_df_stats_daily_sample_v2_chengzi", ds_config["query"])
        self.assertIn("preload_resources is not null", ds_config["query"])
        self.assertNotIn("table_name", ds_config)
        self.assertNotIn("columns", ds_config)
        self.assertEqual(ops[0].image_source, "preloads")
        self.assertEqual(cfg.export["target"], "magnus")
        self.assertEqual(cfg.export["table_name"], "ai_data_forge.ccu.landing_page_chengzi")
        self.assertEqual(cfg.export["operation"], "OVERWRITE")
        self.assertTrue(cfg.export["create_table_if_not_exists"])
        self.assertEqual(cfg.export["partition_values"]["p_date"], "20260423")
        self.assertEqual(cfg.export["magnus_conf"]["write_options"]["write.format.default"], "lance")
        self.assertTrue(cfg.ray_data_checkpoint.enabled)
        self.assertEqual(
            cfg.ray_data_checkpoint.dir,
            "hdfs://haruna/ad_base/addrd_core/addrd_stats/ray_checkpoint/tqs_100_1",
        )
        self.assertTrue(cfg.ray_data_checkpoint.delete_no_checkpoint_files)
        mock_tqs.assert_not_called()

    def test_preloads_demo_tqs_100_export_max_rows_config_loads(self):
        path = os.path.join(
            os.getcwd(),
            "demos",
            "bytedance",
            "process_landing_page_on_ray",
            "configs",
            "preloads_demo_tqs_100_export_max_rows.yaml",
        )
        if not os.path.exists(path):
            self.skipTest("ByteDance landing page demo configs are not included")

        with patch("data_juicer.core.data.load_strategy.run_tqs_query") as mock_tqs:
            cfg = init_configs(args=["--config", path, "--ray_address", "local"], load_configs_only=True)

        ops = load_ops(cfg.process)
        ds_config = cfg.dataset["configs"][0]

        self.assertEqual(ds_config["source"], "tqs")
        self.assertEqual(ds_config["max_result_rows"], 1000)
        self.assertEqual(ops[0].image_source, "preloads")
        self.assertEqual(cfg.export["target"], "magnus")
        self.assertEqual(
            cfg.export["table_name"],
            "ai_data_forge.ccu.landing_page_chengzi_export_max_rows_e2e",
        )
        self.assertEqual(cfg.export["operation"], "OVERWRITE")
        self.assertEqual(cfg.export["max_rows"], 10)
        self.assertEqual(cfg.export["max_rows_mode"], "limit")
        self.assertTrue(cfg.export["create_table_if_not_exists"])
        self.assertEqual(cfg.export["partition_values"]["p_date"], "20260423")
        self.assertEqual(cfg.export["magnus_conf"]["write_options"]["write.format.default"], "lance")
        mock_tqs.assert_not_called()

    def test_hive_read_only_probe_config_has_no_process_ops(self):
        path = os.path.join(
            os.getcwd(),
            "demos",
            "bytedance",
            "process_landing_page_on_ray",
            "configs",
            "hive_read_only_probe.yaml",
        )
        if not os.path.exists(path):
            self.skipTest("ByteDance landing page demo configs are not included")

        with patch("data_juicer.core.data.load_strategy.run_tqs_query") as mock_tqs:
            cfg = init_configs(args=["--config", path, "--ray_address", "local"], load_configs_only=True)

        ops = load_ops(cfg.process)
        ds_config = cfg.dataset["configs"][0]

        self.assertEqual(ops, [])
        self.assertEqual(cfg.process, [])
        self.assertEqual(ds_config["type"], "remote")
        self.assertEqual(ds_config["source"], "hive")
        self.assertEqual(ds_config["table_name"], "ad_addrd_stats.site_creative_center_df_stats_daily_sample_v2_chengzi")
        self.assertEqual(ds_config["override_num_blocks"], 256)
        self.assertEqual(ds_config["concurrency"], 64)
        self.assertEqual(ds_config["ray_remote_args"]["num_cpus"], 1)
        self.assertNotIn("cast_columns", ds_config)
        self.assertEqual(ds_config["columns"]["preload_resources"], "STRING")
        self.assertIn("p_date = '20260423'", ds_config["filter"])
        self.assertIn("preload_resources IS NOT NULL", ds_config["filter"])
        mock_tqs.assert_not_called()

    def test_merged_images_config_loads_two_sources_into_one_output_table(self):
        path = os.path.join(
            os.getcwd(),
            "demos",
            "bytedance",
            "process_landing_page_on_ray",
            "configs",
            "images.yaml",
        )
        if not os.path.exists(path):
            self.skipTest("ByteDance landing page demo configs are not included")
        expected_ops = [
            "MultiSourceImageUrlMapper",
            "DownloadFileMapper",
            "ImageBytesPruneMapper",
            "SpecifiedNumericFieldFilter",
            "ImageBytesExactDedupMapper",
            "PythonLambdaMapper",
            "RayDocumentDeduplicator",
            "ImageSchemaFinalizeMapper",
        ]

        with patch("data_juicer.core.data.load_strategy.run_tqs_query") as mock_tqs:
            cfg = init_configs(args=["--config", path, "--ray_address", "local"], load_configs_only=True)

        ops = load_ops(cfg.process)
        ds_config = cfg.dataset["configs"][0]
        self.assertEqual([op.__class__.__name__ for op in ops], expected_ops)
        self.assertEqual(cfg.export["target"], "magnus")
        self.assertEqual(cfg.export["table_name"], "ai_data_forge.ccu.site_creative_center_df_stats_daily_sample_v2_chengzi")
        self.assertEqual(ds_config["table_name"], "ad_addrd_stats.site_creative_center_df_stats_daily_sample_v2_chengzi")
        self.assertIn("thumbnail", ds_config["columns"])
        self.assertIn("preload_resources", ds_config["columns"])
        self.assertIn("thumbnail IS NOT NULL OR preload_resources IS NOT NULL", ds_config["filter"])
        self.assertEqual(ops[0].output_url_key, "images")
        self.assertEqual(ops[0].id_field, "site_id")
        self.assertEqual([spec.name for spec in ops[0].source_specs], ["thumbnail", "preloads"])
        self.assertEqual(
            [spec.source for spec in ops[0].source_specs],
            ["site_creative_thumbnail_raw_data", "site_creative_preloads_raw_data"],
        )
        self.assertEqual(ops[0].source_specs[0].extra_url_key, "thumbnail_url")
        self.assertEqual(ops[0].source_specs[1].extra_url_key, "preload_urls")
        self.assertEqual(ops[0].passthrough_keys, LANDING_PAGE_PASSTHROUGH_KEYS)
        self.assertEqual(ops[6].text_key, "__dj_source_md5")
        self.assertEqual(ops[7].passthrough_keys, LANDING_PAGE_PASSTHROUGH_KEYS)
        self.assertEqual(ops[7].passthrough_types["site_id"], pa.int64())
        self.assertEqual(ops[7].passthrough_types["preload_resources"], pa.string())
        export_field_names = [field["name"] for field in cfg.export["schema"]["fields"]]
        for field_name in ["source", "md5", *LANDING_PAGE_PASSTHROUGH_KEYS]:
            self.assertIn(field_name, export_field_names)
        self.assertEqual(cfg.export["operation"], "OVERWRITE")
        self.assertEqual(cfg.export["partition_columns"], ["p_date"])
        self.assertEqual(cfg.export["partition_values"]["p_date"], "20260423")
        self.assertEqual(cfg.export["magnus_conf"]["write_options"]["write.format.default"], "lance")
        mock_tqs.assert_not_called()

    def test_hive_sample_config_loads_without_tqs_loader(self):
        path = os.path.join(
            os.getcwd(),
            "demos",
            "bytedance",
            "process_landing_page_on_ray",
            "configs",
            "thumbnail_hive_sample_1_1000.yaml",
        )
        if not os.path.exists(path):
            self.skipTest("ByteDance landing page demo configs are not included")

        with patch("data_juicer.core.data.load_strategy.run_tqs_query") as mock_tqs:
            cfg = init_configs(args=["--config", path, "--ray_address", "local"], load_configs_only=True)

        ops = load_ops(cfg.process)
        ds_config = cfg.dataset["configs"][0]
        self.assertEqual(ds_config["source"], "hive")
        self.assertEqual(ds_config["table_name"], "ad_addrd_stats.site_creative_center_df_stats_daily_sample_v2_chengzi")
        self.assertIn("site_id % 1000 = 0", ds_config["filter"])
        self.assertEqual(ds_config["override_num_blocks"], 128)
        self.assertEqual(ds_config["concurrency"], 64)
        self.assertEqual(ops[0].extra_keys, [])
        self.assertEqual(ops[1].batch_size, 100)
        self.assertEqual(ops[1].max_concurrent, 4)
        self.assertEqual(ops[5].backend._dedup_set_num_config, 8)
        self.assertEqual(cfg.export["target"], "magnus")
        self.assertEqual(cfg.export["table_name"], "ai_data_forge.ccu.landing_page_thumbnail_sample_1_1000")
        export_field_names = [field["name"] for field in cfg.export["schema"]["fields"]]
        self.assertNotIn("extra", export_field_names)
        self.assertEqual(cfg.export["magnus_conf"]["concurrency"], 8)
        self.assertEqual(cfg.export["magnus_conf"]["ray_remote_args"]["num_cpus"], 1)
        self.assertEqual(cfg.export["magnus_conf"]["write_options"]["write.format.default"], "lance")
        self.assertEqual(cfg.export["magnus_conf"]["write_options"]["magnus.ray.write.disable_repartition"], "true")
        self.assertEqual(cfg.export["magnus_conf"]["write_options"]["magnus.ray.write.disable_sort"], "true")
        mock_tqs.assert_not_called()


class ListFieldFlattenMapperTest(unittest.TestCase):
    def test_rejects_empty_field_key(self):
        with self.assertRaises(ValueError):
            ListFieldFlattenMapper(field_key="")

    def test_process_batched_arrow_explodes_list_field_and_keeps_schema(self):
        input_table = pa.Table.from_pylist(
            [
                {
                    "__dj_landing_page_id": "site_id-1",
                    "images": ["https://example.com/a.png", "https://example.com/b.png"],
                    "site_id": 1,
                },
                {
                    "__dj_landing_page_id": "site_id-2",
                    "images": [],
                    "site_id": 2,
                },
            ]
        )

        output = ListFieldFlattenMapper(
            field_key="images",
            index_key="__dj_landing_page_image_index",
            id_key="__dj_landing_page_id",
            id_format="{id}-{index}",
        ).process_batched(input_table)

        self.assertIsInstance(output, pa.Table)
        self.assertEqual(output.num_rows, 2)
        self.assertEqual(output.schema.field("images").type, pa.list_(pa.string()))
        self.assertEqual(output.schema.field("__dj_landing_page_image_index").type, pa.int64())
        self.assertEqual(output.column("__dj_landing_page_id").to_pylist(), ["site_id-1-0", "site_id-1-1"])
        self.assertEqual(
            output.column("images").to_pylist(),
            [["https://example.com/a.png"], ["https://example.com/b.png"]],
        )
        self.assertEqual(output.column("site_id").to_pylist(), [1, 1])
        self.assertEqual(output.column("__dj_landing_page_image_index").to_pylist(), [0, 1])

    def test_process_batched_dict_handles_scalar_empty_and_format_id(self):
        output = ListFieldFlattenMapper(
            field_key="images",
            output_field_key="image",
            wrap_value=False,
            drop_empty=False,
            index_key="image_index",
            id_key="id",
            id_format="{id}:url:{index}",
        ).process_batched(
            {
                "id": ["row-1", "row-2"],
                "images": ["https://example.com/a.png", []],
            }
        )

        self.assertEqual(output["id"], ["row-1:url:0", "row-2"])
        self.assertEqual(output["images"], ["https://example.com/a.png", []])
        self.assertEqual(output["image"], ["https://example.com/a.png", []])
        self.assertEqual(output["image_index"], [0, None])

    def test_process_batched_dict_empty_inputs(self):
        self.assertEqual(ListFieldFlattenMapper(field_key="images").process_batched({}), {})
        output = ListFieldFlattenMapper(field_key="images").process_batched({"id": [], "images": []})
        self.assertEqual(output, {"id": [], "images": []})

    def test_process_batched_arrow_all_empty_keeps_list_schema(self):
        input_table = pa.Table.from_arrays(
            [
                pa.array(["site_id-1"], type=pa.string()),
                pa.array([[]], type=pa.list_(pa.string())),
            ],
            names=["__dj_landing_page_id", "images"],
        )

        output = ListFieldFlattenMapper(field_key="images").process_batched(input_table)

        self.assertEqual(output.num_rows, 0)
        self.assertEqual(output.schema.field("__dj_landing_page_id").type, pa.string())
        self.assertEqual(output.schema.field("images").type, pa.list_(pa.string()))

    def test_passthrough_types_stabilize_mixed_input_block_schema(self):
        string_input = pa.Table.from_arrays(
            [
                pa.array(["site_id-1"], type=pa.string()),
                pa.array([["https://example.com/a.png"]], type=pa.list_(pa.string())),
                pa.array(["10"], type=pa.string()),
                pa.array(["1"], type=pa.string()),
            ],
            names=["__dj_landing_page_id", "images", "cost", "is_highlight"],
        )
        int_input = pa.Table.from_arrays(
            [
                pa.array(["site_id-2"], type=pa.string()),
                pa.array([["https://example.com/b.png"]], type=pa.list_(pa.string())),
                pa.array([20], type=pa.int64()),
                pa.array([0], type=pa.int64()),
            ],
            names=["__dj_landing_page_id", "images", "cost", "is_highlight"],
        )

        op = ListFieldFlattenMapper(
            field_key="images",
            index_key="__dj_landing_page_image_index",
            id_key="__dj_landing_page_id",
            passthrough_types={"cost": "int64", "is_highlight": "int64"},
        )
        string_output = op.process_batched(string_input)
        int_output = op.process_batched(int_input)

        self.assertEqual(string_output.schema.field("cost").type, pa.int64())
        self.assertEqual(string_output.schema.field("is_highlight").type, pa.int64())
        self.assertEqual(int_output.schema.field("cost").type, pa.int64())
        self.assertEqual(int_output.schema.field("is_highlight").type, pa.int64())
        self.assertEqual(pa.concat_tables([string_output, int_output]).column("cost").to_pylist(), [10, 20])

    def test_run_uses_ray_map_batches_for_arrow_dataset(self):
        class FakeRayDataset:
            def __init__(self, table):
                self.table = table

            def map_batches(self, fn, *, batch_format, batch_size):
                self.batch_format = batch_format
                self.batch_size = batch_size
                return fn(self.table)

        input_table = pa.Table.from_pylist([{"id": "row-1", "images": ["a", "b"]}])
        output = ListFieldFlattenMapper(field_key="images", batch_size=8).run(FakeRayDataset(input_table))

        self.assertEqual(output.column("images").to_pylist(), [["a"], ["b"]])


class LandingPageComposedPipelineSmokeTest(unittest.TestCase):
    def test_composed_ops_run_through_nested_dataset_process(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            img_path = os.path.join(temp_dir, "img.png")
            with open(img_path, "wb") as f:
                f.write(_image_bytes(1))

            dataset = NestedDataset.from_list(
                [
                    {
                        "site_id": 123,
                        "local_stat_time_day": "20260308",
                        "p_date": "20260421",
                        "thumbnail": img_path,
                    },
                    {
                        "site_id": 456,
                        "local_stat_time_day": "20260308",
                        "p_date": "20260421",
                        "thumbnail": "",
                    },
                ]
            )
            ops = [
                LandingPageImageUrlMapper(image_source="thumbnail", num_proc=1),
                DownloadFileMapper(download_field="images", save_field="image_bytes", num_proc=1),
                ImageBytesPruneMapper(num_proc=1),
                SpecifiedNumericFieldFilter(field_key="valid_image_count", min_value=1, num_proc=1),
                ImageBytesExactDedupMapper(num_proc=1),
                LandingPageImageSchemaFinalizeMapper(
                    passthrough_keys=["site_id", "local_stat_time_day", "p_date"], num_proc=1
                ),
            ]

            rows = dataset.process(ops, open_monitor=False).to_list()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], "site_id-123")
        self.assertEqual(rows[0]["source"], "site_creative_thumbnail_raw_data")
        self.assertEqual(rows[0]["type"], "image")
        self.assertEqual(rows[0]["site_id"], 123)
        self.assertEqual(rows[0]["local_stat_time_day"], "20260308")
        self.assertEqual(rows[0]["p_date"], "20260421")
        self.assertEqual(len(rows[0]["images"]), 1)
        self.assertEqual(
            sorted(rows[0].keys()),
            [
                "audios",
                "has_audio_in_video",
                "id",
                "images",
                "local_stat_time_day",
                "md5",
                "p_date",
                "site_id",
                "source",
                "texts",
                "type",
                "videos",
            ],
        )


class LandingPageRayArrowSchemaTest(unittest.TestCase):
    def test_url_mapper_does_not_create_extra_cache_column(self):
        import ray
        from jsonargparse import Namespace

        from data_juicer.core.data.ray_dataset import RayDataset

        ray.init(ignore_reinit_error=True, include_dashboard=False, num_cpus=1)
        try:
            dataset = RayDataset(
                ray.data.from_items(
                    [
                        {"site_id": 1, "cost": None, "thumbnail": "https://example.com/a.png"},
                        {"site_id": 2, "cost": 10, "thumbnail": "https://example.com/b.png"},
                    ],
                    override_num_blocks=2,
                ),
                cfg=Namespace(image_key="images", audio_key="audios", video_key="videos", auto_op_parallelism=False),
                auto_op_parallelism=False,
            )

            dataset.process([LandingPageImageUrlMapper(extra_keys=["site_id", "cost"], num_proc=1)])
            rows = dataset.data.take_all()
        finally:
            ray.shutdown()

        self.assertTrue(all("__dj_landing_page_extra" not in row for row in rows))

    def test_finalize_passthrough_types_allow_ray_concat_null_and_int_blocks(self):
        import ray

        ray.init(ignore_reinit_error=True, include_dashboard=False, num_cpus=1)
        try:
            dataset = ray.data.from_items(
                [
                    {
                        "__dj_landing_page_id": "site_id-1",
                        "__dj_landing_page_image_source": "thumbnail",
                        "images": ["https://example.com/a.png"],
                        "image_bytes": [_image_bytes(1)],
                        "md5": "abc",
                        "cost": None,
                    },
                    {
                        "__dj_landing_page_id": "site_id-2",
                        "__dj_landing_page_image_source": "thumbnail",
                        "images": ["https://example.com/b.png"],
                        "image_bytes": [_image_bytes(2)],
                        "md5": "def",
                        "cost": 10,
                    },
                ],
                override_num_blocks=2,
            )
            output = LandingPageImageSchemaFinalizeMapper(
                passthrough_keys=["cost"],
                passthrough_types={"cost": "int64"},
            ).run(dataset)
            rows = sorted(output.take_all(), key=lambda row: row["id"])
        finally:
            ray.shutdown()

        self.assertEqual([row["cost"] for row in rows], [None, 10])


if __name__ == "__main__":
    unittest.main()
