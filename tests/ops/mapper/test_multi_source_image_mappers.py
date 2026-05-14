import hashlib
import json
import os
import tempfile
import unittest
from io import BytesIO

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
from data_juicer.ops.deduplicator.ray_group_required_field_filter_pipeline import (
    RayGroupRequiredFieldFilterPipeline,
)
from data_juicer.ops.filter.specified_numeric_field_filter import SpecifiedNumericFieldFilter
from data_juicer.ops.load import load_ops
from data_juicer.ops.mapper.io.download_file_mapper import DownloadFileMapper
from data_juicer.ops.mapper.schema.image_bytes_exact_dedup_mapper import ImageBytesExactDedupMapper
from data_juicer.ops.mapper.schema.image_bytes_prune_mapper import ImageBytesPruneMapper
from data_juicer.ops.mapper.schema.image_schema_finalize_mapper import ImageSchemaFinalizeMapper
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


def _url_op(**kwargs) -> MultiSourceImageUrlMapper:
    defaults = {
        "source_specs": [
            {
                "name": "product",
                "url_field": "product_img_url",
                "source": "ecom_product_product_raw_data",
                "extra_url_key": "product_url",
                "extra_url_mode": "single",
                "max_urls": 1,
            },
            {
                "name": "main",
                "url_field": "main_pic",
                "source": "ecom_product_main_raw_data",
                "extra_url_key": "main_urls",
                "extra_url_mode": "list",
            },
            {
                "name": "info",
                "url_field": "info_pic",
                "source": "ecom_product_info_raw_data",
                "extra_url_key": "info_urls",
                "extra_url_mode": "list",
            },
        ],
        "id_field": "product_id",
        "extra_keys": [
            "product_id",
            "product_title",
            "product_img_url",
            "main_pic",
            "info_pic",
        ],
        "passthrough_keys": ["p_date"],
        "passthrough_types": {"p_date": "string"},
        "auto_op_parallelism": False,
        "num_proc": 1,
    }
    defaults.update(kwargs)
    return MultiSourceImageUrlMapper(**defaults)


class MultiSourceImageUrlMapperTest(unittest.TestCase):
    def test_constructor_validates_source_specs_and_passthrough_types(self):
        base_kwargs = {
            "source_specs": [{"name": "product", "url_field": "product_img_url", "source": "product"}],
            "id_field": "product_id",
            "auto_op_parallelism": False,
            "num_proc": 1,
        }

        with self.assertRaisesRegex(ValueError, "source_specs must be a non-empty list"):
            MultiSourceImageUrlMapper(
                source_specs=[],
                id_field="product_id",
                auto_op_parallelism=False,
                num_proc=1,
            )
        with self.assertRaisesRegex(ValueError, "id_field must be provided"):
            MultiSourceImageUrlMapper(
                source_specs=base_kwargs["source_specs"],
                id_field=None,
                auto_op_parallelism=False,
                num_proc=1,
            )
        with self.assertRaisesRegex(ValueError, "must contain `source`"):
            MultiSourceImageUrlMapper(
                source_specs=[{"name": "product", "url_field": "product_img_url"}],
                id_field="product_id",
                auto_op_parallelism=False,
                num_proc=1,
            )
        with self.assertRaisesRegex(ValueError, "extra_url_mode"):
            MultiSourceImageUrlMapper(
                **{**base_kwargs, "source_specs": [{**base_kwargs["source_specs"][0], "extra_url_mode": "bad"}]}
            )
        with self.assertRaisesRegex(ValueError, "max_urls must be positive"):
            MultiSourceImageUrlMapper(
                **{**base_kwargs, "source_specs": [{**base_kwargs["source_specs"][0], "max_urls": 0}]}
            )
        with self.assertRaisesRegex(ValueError, "Unsupported passthrough arrow type"):
            MultiSourceImageUrlMapper(**{**base_kwargs, "passthrough_types": {"p_date": "unknown"}})

        op = MultiSourceImageUrlMapper(
            **{
                **base_kwargs,
                "source_specs": [{**base_kwargs["source_specs"][0], "extra_url_key": "url", "max_urls": 1}],
                "passthrough_types": {"p_date": pa.string()},
            }
        )
        self.assertEqual(op.source_specs[0].extra_url_mode, "single")
        self.assertEqual(op.passthrough_types["p_date"], pa.string())

    def test_jsonable_extra_and_text_values_support_scalar_wrappers(self):
        class ScalarValue:
            def as_py(self):
                return "scalar-extra"

        class ArrayValue:
            def tolist(self):
                return ["list-extra"]

        rows = _url_op(text_fields=["title", "missing"], extra_keys=["scalar", "array"]).process_single(
            {
                "product_id": 123,
                "product_img_url": "https://img.test/product.png",
                "main_pic": "",
                "info_pic": "",
                "title": [b"hello", ["world"]],
                "scalar": ScalarValue(),
                "array": ArrayValue(),
            }
        )

        self.assertEqual(rows[0]["texts"], ["hello", "world"])
        self.assertEqual(json.loads(rows[0]["extra"])["scalar"], "scalar-extra")
        self.assertEqual(json.loads(rows[0]["extra"])["array"], ["list-extra"])

    def test_process_single_outputs_source_level_rows_with_extra_and_texts(self):
        rows = _url_op(text_fields=["product_title"]).process_single(
            {
                "product_id": 123,
                "product_title": "shirt",
                "product_img_url": "https://img.test/product.png",
                "main_pic": json.dumps([
                    "https://img.test/main-a.png",
                    {"url": "https://img.test/main-b.png"},
                ]),
                "info_pic": "https://img.test/info.png",
                "p_date": "20260424",
            }
        )

        rows_by_source = {row["source"]: row for row in rows}
        self.assertEqual(set(rows_by_source), {
            "ecom_product_product_raw_data",
            "ecom_product_main_raw_data",
            "ecom_product_info_raw_data",
        })
        for row in rows:
            self.assertEqual(row["id"], "product_id-123")
            self.assertEqual(row["texts"], ["shirt"])
            self.assertEqual(row["p_date"], "20260424")

        product_extra = json.loads(rows_by_source["ecom_product_product_raw_data"]["extra"])
        main_extra = json.loads(rows_by_source["ecom_product_main_raw_data"]["extra"])
        self.assertEqual(product_extra["product_url"], "https://img.test/product.png")
        self.assertEqual(main_extra["main_urls"], [
            "https://img.test/main-a.png",
            "https://img.test/main-b.png",
        ])
        self.assertEqual(rows_by_source["ecom_product_main_raw_data"]["image_urls"], [
            "https://img.test/main-a.png",
            "https://img.test/main-b.png",
        ])

    def test_missing_source_is_dropped_without_dropping_other_sources(self):
        rows = _url_op().process_single(
            {
                "product_id": 1,
                "product_img_url": "https://img.test/product.png",
                "main_pic": "",
                "info_pic": "",
            }
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source"], "ecom_product_product_raw_data")

    def test_url_parser_accepts_common_string_shapes(self):
        parse = MultiSourceImageUrlMapper._parse_urls

        class ScalarValue:
            def as_py(self):
                return b"https://a.test/0.png"

        class ArrayValue:
            def tolist(self):
                return [{"image_url": "https://a.test/8.png"}, {"href": "https://a.test/9.png"}]

        self.assertEqual(parse(None), [])
        self.assertEqual(parse(""), [])
        self.assertEqual(parse(ScalarValue()), ["https://a.test/0.png"])
        self.assertEqual(parse('[{"url": "https://a.test/1.png"}, "https://a.test/2.png"]'), [
            "https://a.test/1.png",
            "https://a.test/2.png",
        ])
        self.assertEqual(parse("['https://a.test/3.png', 'https://a.test/4.png']"), [
            "https://a.test/3.png",
            "https://a.test/4.png",
        ])
        self.assertEqual(parse("https://a.test/5.png;https://a.test/6.png"), [
            "https://a.test/5.png",
            "https://a.test/6.png",
        ])
        self.assertEqual(
            parse(
                '<p><img src="https://a.test/info-a.png?x=1&amp;y=2" />'
                "<span>ignore</span>"
                "<img alt='b' src='https://a.test/info-b.png'/></p>"
            ),
            [
                "https://a.test/info-a.png?x=1&y=2",
                "https://a.test/info-b.png",
            ],
        )
        self.assertEqual(parse({"url": "https://a.test/7.png"}), ["https://a.test/7.png"])
        self.assertEqual(parse(ArrayValue()), ["https://a.test/8.png", "https://a.test/9.png"])
        self.assertEqual(parse({"not_url": "https://a.test/ignored.png"}), [])
        self.assertEqual(parse(object()), [])
        self.assertEqual(parse('"https://a.test/quoted.png"'), ["https://a.test/quoted.png"])

    def test_process_batched_arrow_keeps_stable_schema_for_empty_output(self):
        op = _url_op()
        input_table = pa.Table.from_pylist(
            [
                {
                    "product_id": 1,
                    "product_img_url": "",
                    "main_pic": "",
                    "info_pic": "",
                    "p_date": None,
                }
            ]
        )

        output = op.process_batched(input_table)

        self.assertEqual(output.num_rows, 0)
        self.assertEqual(output.schema.field("image_urls").type, pa.list_(pa.string()))
        self.assertEqual(output.schema.field("texts").type, pa.list_(pa.string()))
        self.assertEqual(output.schema.field("p_date").type, pa.string())

    def test_passthrough_types_coerce_object_values_before_arrow_array(self):
        op = _url_op(
            source_specs=[
                {
                    "name": "preloads",
                    "url_field": "preload_resources",
                    "source": "site_creative_preloads_raw_data",
                    "extra_url_key": "preload_urls",
                    "extra_url_mode": "list",
                }
            ],
            passthrough_keys=["preload_resources", "cost", "is_highlight"],
            passthrough_types={
                "preload_resources": "string",
                "cost": "int64",
                "is_highlight": "int64",
            },
        )
        input_table = pa.Table.from_pylist(
            [
                {
                    "product_id": 1,
                    "preload_resources": [{"url": "https://img.test/a.png"}],
                    "cost": "",
                    "is_highlight": True,
                }
            ]
        )

        output = op.process_batched(input_table)

        self.assertEqual(output.schema.field("preload_resources").type, pa.string())
        self.assertEqual(output.schema.field("cost").type, pa.int64())
        self.assertEqual(output.schema.field("is_highlight").type, pa.int64())
        self.assertEqual(
            json.loads(output.column("preload_resources").to_pylist()[0]),
            [{"url": "https://img.test/a.png"}],
        )
        self.assertEqual(output.column("cost").to_pylist(), [None])
        self.assertEqual(output.column("is_highlight").to_pylist(), [1])

    def test_process_batched_dict_handles_empty_invalid_and_inferred_passthrough_schema(self):
        op = _url_op(passthrough_keys=["p_date", "score"], passthrough_types={})

        self.assertEqual(op.process_batched({})["image_urls"], [])
        invalid = op.process_batched(
            {
                "product_id": [1],
                "product_img_url": [""],
                "main_pic": [""],
                "info_pic": [""],
                "p_date": [None],
                "score": [None],
            }
        )
        self.assertEqual(invalid["id"], [])
        self.assertEqual(invalid["score"], [])

        typed = op._rows_to_arrow_table(
            [{"id": "1", "score": 3}],
            input_schema=pa.schema([pa.field("score", pa.int64())]),
        )
        inferred = op._rows_to_arrow_table(
            [{"id": "1", "score": 1.5}],
            input_schema=pa.schema([]),
        )
        null_inferred = op._rows_to_arrow_table([], input_schema=pa.schema([]))
        self.assertEqual(typed.schema.field("score").type, pa.int64())
        self.assertEqual(inferred.schema.field("score").type, pa.float64())
        self.assertEqual(null_inferred.schema.field("score").type, pa.string())

    def test_nested_dataset_run_expands_rows_and_removes_raw_columns(self):
        dataset = NestedDataset.from_list(
            [
                {
                    "product_id": 123,
                    "product_title": "shirt",
                    "product_img_url": "https://img.test/product.png",
                    "main_pic": "https://img.test/main.png",
                    "info_pic": "https://img.test/info.png",
                    "p_date": "20260424",
                }
            ]
        )

        rows = _url_op().run(dataset).to_list()

        self.assertEqual(len(rows), 3)
        self.assertEqual({row["source"] for row in rows}, {
            "ecom_product_product_raw_data",
            "ecom_product_main_raw_data",
            "ecom_product_info_raw_data",
        })
        self.assertNotIn("product_img_url", rows[0])
        self.assertEqual({row["p_date"] for row in rows}, {"20260424"})


class ImageSchemaFinalizeMapperTest(unittest.TestCase):
    def test_finalize_outputs_dj_schema_and_passthrough_fields(self):
        img = _image_bytes(1)
        row = ImageSchemaFinalizeMapper(passthrough_keys=["p_date"]).process_single(
            {
                "id": "product_id-123",
                "source": "ecom_product_product_raw_data",
                "texts": ["shirt"],
                "image_bytes": [img],
                "extra": json.dumps({"product_id": 123}),
                "md5": "abc",
                "p_date": "20260424",
                "raw_field": "drop-me",
            }
        )

        self.assertEqual(
            sorted(row.keys()),
            [
                "audios",
                "extra",
                "has_audio_in_video",
                "id",
                "images",
                "md5",
                "p_date",
                "source",
                "texts",
                "type",
                "videos",
            ],
        )
        self.assertEqual(row["id"], "product_id-123")
        self.assertEqual(row["source"], "ecom_product_product_raw_data")
        self.assertEqual(row["texts"], ["shirt"])
        self.assertEqual(row["images"], [img])
        self.assertEqual(row["audios"], [])
        self.assertEqual(row["videos"], [])
        self.assertEqual(row["type"], "image")
        self.assertEqual(json.loads(row["extra"]), {"product_id": 123})
        self.assertEqual(row["p_date"], "20260424")

    def test_finalize_pyarrow_batch_has_stable_explicit_schema(self):
        img = _image_bytes(1)
        input_table = pa.Table.from_pylist(
            [
                {
                    "id": "product_id-1",
                    "source": "product",
                    "texts": [],
                    "image_bytes": [img],
                    "extra": json.dumps({"cost": None}),
                    "md5": "abc",
                    "cost": None,
                },
                {
                    "id": "product_id-2",
                    "source": "product",
                    "texts": [],
                    "image_bytes": [img],
                    "extra": json.dumps({"cost": 10}),
                    "md5": "def",
                    "cost": 10,
                },
            ]
        )

        output_table = ImageSchemaFinalizeMapper(
            passthrough_keys=["cost"],
            passthrough_types={"cost": "int64"},
        ).process_batched(input_table)

        self.assertIsInstance(output_table, pa.Table)
        self.assertEqual(output_table.schema.field("id").type, pa.string())
        self.assertEqual(output_table.schema.field("texts").type, pa.list_(pa.string()))
        self.assertEqual(output_table.schema.field("images").type, pa.list_(pa.binary()))
        self.assertEqual(output_table.schema.field("audios").type, pa.list_(pa.binary()))
        self.assertEqual(output_table.schema.field("videos").type, pa.list_(pa.binary()))
        self.assertEqual(output_table.schema.field("has_audio_in_video").type, pa.bool_())
        self.assertEqual(output_table.schema.field("cost").type, pa.int64())
        self.assertEqual(output_table.column("texts").to_pylist(), [[], []])
        self.assertEqual(output_table.column("audios").to_pylist(), [[], []])
        self.assertEqual(output_table.column("videos").to_pylist(), [[], []])

    def test_finalize_coerces_passthrough_object_values_before_arrow_array(self):
        output_table = ImageSchemaFinalizeMapper(
            passthrough_keys=["preload_resources", "cost", "is_highlight"],
            passthrough_types={
                "preload_resources": "string",
                "cost": "int64",
                "is_highlight": "int64",
            },
        )._rows_to_arrow_table(
            [
                {
                    "id": "product_id-1",
                    "preload_resources": [{"url": "https://img.test/a.png"}],
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
            [{"url": "https://img.test/a.png"}],
        )
        self.assertEqual(output_table.column("cost").to_pylist(), [None])
        self.assertEqual(output_table.column("is_highlight").to_pylist(), [1])

    def test_finalize_process_batched_dict_handles_empty_and_inferred_schema(self):
        op = ImageSchemaFinalizeMapper(passthrough_keys=["score"], passthrough_types={})

        self.assertEqual(op.process_batched({})["id"], [])
        empty_rows = op.process_batched({"id": [], "score": []})
        self.assertEqual(empty_rows["id"], [])
        self.assertEqual(empty_rows["score"], [])

        typed = op._rows_to_arrow_table(
            [{"id": "1", "score": 3}],
            input_schema=pa.schema([pa.field("score", pa.int64())]),
        )
        inferred = op._rows_to_arrow_table(
            [{"id": "1", "score": 1.5}],
            input_schema=pa.schema([]),
        )
        null_inferred = op._rows_to_arrow_table([], input_schema=pa.schema([]))
        self.assertEqual(typed.schema.field("score").type, pa.int64())
        self.assertEqual(inferred.schema.field("score").type, pa.float64())
        self.assertEqual(null_inferred.schema.field("score").type, pa.string())

    def test_finalize_normalizes_wrapped_text_bytes_and_extra_values(self):
        class ScalarValue:
            def __init__(self, value):
                self.value = value

            def as_py(self):
                return self.value

        class ArrayValue:
            def __init__(self, value):
                self.value = value

            def tolist(self):
                return self.value

        op = ImageSchemaFinalizeMapper()

        self.assertEqual(op._as_string_list(None), [])
        self.assertEqual(op._as_string_list(ScalarValue(b"hello")), ["hello"])
        self.assertEqual(op._as_string_list(ArrayValue(["a", ["b"]])), ["a", "b"])
        self.assertEqual(op._as_bytes_list(None), [])
        self.assertEqual(op._as_bytes_list(ScalarValue(bytearray(b"a"))), [b"a"])
        self.assertEqual(op._as_bytes_list(ArrayValue([memoryview(b"b"), [b"c"]])), [b"b", b"c"])
        self.assertEqual(op._as_bytes_list("not-bytes"), [])
        self.assertEqual(op._extra_to_json(None), "{}")
        self.assertEqual(op._extra_to_json(ScalarValue({"a": 1})), '{"a": 1}')
        self.assertEqual(op._extra_to_json(ArrayValue({"b": 2})), '{"b": 2}')

    def test_passthrough_type_config_accepts_pyarrow_type_and_rejects_unknown_type(self):
        op = ImageSchemaFinalizeMapper(
            passthrough_keys=["score"],
            passthrough_types={"score": pa.int32()},
        )

        self.assertEqual(op.passthrough_types["score"], pa.int32())
        with self.assertRaises(ValueError):
            ImageSchemaFinalizeMapper(passthrough_types={"score": "decimal128"})


class RayGroupRequiredFieldFilterPipelineTest(unittest.TestCase):
    def _op(self):
        return RayGroupRequiredFieldFilterPipeline(
            group_key="id",
            field_key="source",
            required_values={
                "product": 1,
                "main": 1,
                "info": 1,
            },
            auto_op_parallelism=False,
            num_proc=1,
        )

    def test_constructor_validates_required_keys(self):
        with self.assertRaisesRegex(ValueError, "group_key"):
            RayGroupRequiredFieldFilterPipeline(group_key="", required_values={"product": 1})
        with self.assertRaisesRegex(ValueError, "field_key"):
            RayGroupRequiredFieldFilterPipeline(group_key="id", field_key="", required_values={"product": 1})
        with self.assertRaisesRegex(ValueError, "required_values"):
            RayGroupRequiredFieldFilterPipeline(group_key="id", required_values={})

    def test_pyarrow_group_is_dropped_when_any_required_source_has_no_valid_images(self):
        op = self._op()
        complete = pa.Table.from_pylist(
            [
                {"id": "1", "source": "product", "valid_image_count": 1},
                {"id": "1", "source": "main", "valid_image_count": 2},
                {"id": "1", "source": "info", "valid_image_count": 1},
            ]
        )
        incomplete = pa.Table.from_pylist(
            [
                {"id": "2", "source": "product", "valid_image_count": 1},
                {"id": "2", "source": "main", "valid_image_count": 0},
                {"id": "2", "source": "info", "valid_image_count": 1},
            ],
            schema=complete.schema,
        )

        self.assertEqual(op._filter_arrow_group(complete).num_rows, 3)
        dropped = op._filter_arrow_group(incomplete)
        self.assertEqual(dropped.num_rows, 0)
        self.assertEqual(dropped.schema, complete.schema)

    def test_nested_dataset_run_keeps_only_complete_source_groups(self):
        dataset = NestedDataset.from_list(
            [
                {"id": "1", "source": "product", "valid_image_count": 1},
                {"id": "1", "source": "main", "valid_image_count": 1},
                {"id": "1", "source": "info", "valid_image_count": 1},
                {"id": "2", "source": "product", "valid_image_count": 1},
                {"id": "2", "source": "main", "valid_image_count": 1},
            ]
        )

        rows = self._op().run(dataset).to_list()

        self.assertEqual([row["id"] for row in rows], ["1", "1", "1"])


class MultiSourceComposedPipelineSmokeTest(unittest.TestCase):
    def test_composed_ops_run_through_nested_dataset_process(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            img_path = os.path.join(temp_dir, "product.png")
            img = _image_bytes(1)
            _write_file(img_path, img)
            dataset = NestedDataset.from_list(
                [
                    {
                        "product_id": 123,
                        "product_title": "shirt",
                        "product_img_url": img_path,
                        "main_pic": "",
                        "info_pic": "",
                        "p_date": "20260424",
                        "image_bytes": [img],
                    }
                ]
            )
            in_process_op_kwargs = {"auto_op_parallelism": False, "num_proc": None}
            prune_op = ImageBytesPruneMapper(
                image_key="image_urls",
                image_bytes_key="image_bytes",
                **in_process_op_kwargs,
            )
            prune_op.is_valid_image_bytes = lambda image_bytes: bool(image_bytes)
            ops = [
                _url_op(passthrough_keys=["p_date", "image_bytes"], **in_process_op_kwargs),
                DownloadFileMapper(
                    download_field="image_urls",
                    save_field="image_bytes",
                    resume_download=True,
                    **in_process_op_kwargs,
                ),
                prune_op,
                SpecifiedNumericFieldFilter(
                    field_key="valid_image_count",
                    min_value=1,
                    **in_process_op_kwargs,
                ),
                ImageBytesExactDedupMapper(
                    image_key="image_urls",
                    image_bytes_key="image_bytes",
                    **in_process_op_kwargs,
                ),
                ImageSchemaFinalizeMapper(passthrough_keys=["p_date"], **in_process_op_kwargs),
            ]

            rows = dataset.process(ops, open_monitor=False).to_list()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], "product_id-123")
        self.assertEqual(rows[0]["source"], "ecom_product_product_raw_data")
        self.assertEqual(rows[0]["type"], "image")
        self.assertEqual(rows[0]["p_date"], "20260424")
        self.assertEqual(rows[0]["images"], [img])
        self.assertEqual(rows[0]["md5"], _expected_md5([img]))
        self.assertEqual(json.loads(rows[0]["extra"])["product_url"], img_path)


class EcomProductConfigTest(unittest.TestCase):
    def test_ecom_product_hive_magnus_config_loads(self):
        _patch_yaml_loader_tags()
        path = os.path.join(
            os.getcwd(),
            "demos",
            "bytedance",
            "ecom_product_process",
            "configs",
            "ecom_product_hive_magnus.yaml",
        )

        cfg = init_configs(args=["--config", path, "--ray_address", "local"], load_configs_only=True)
        ops = load_ops(cfg.process)
        ds_config = cfg.dataset["configs"][0]

        self.assertEqual(ds_config["source"], "hive")
        self.assertEqual(ds_config["table_name"], "ad_addrd_stats.product_core_attribute_sample_stats_daily_v2")
        self.assertIn("p_date = '<P_DATE>'", ds_config["filter"])
        self.assertIn("second_category_name_new IS NOT NULL", ds_config["filter"])
        self.assertIn("third_category_name_new IS NOT NULL", ds_config["filter"])
        self.assertEqual(ds_config["override_num_blocks"], 256)
        self.assertEqual(ds_config["concurrency"], 128)
        self.assertEqual(cfg.export["target"], "magnus")
        self.assertEqual(cfg.export["table_name"], "<CATALOG>.<DATABASE>.ecom_product_image_schema")
        self.assertEqual(cfg.export["partition_values"]["p_date"], "<P_DATE>")
        self.assertEqual(
            [op.__class__.__name__ for op in ops],
            [
                "RayFieldDedupPipeline",
                "MultiSourceImageUrlMapper",
                "DownloadFileMapper",
                "ImageBytesPruneMapper",
                "RayGroupRequiredFieldFilterPipeline",
                "ImageBytesExactDedupMapper",
                "GeneralFieldFilter",
                "PythonLambdaMapper",
                "RayDocumentDeduplicator",
                "ImageSchemaFinalizeMapper",
            ],
        )
        self.assertEqual(ops[0].field_key, "product_id")
        self.assertEqual(len(ops[1].source_specs), 3)
        self.assertEqual(ops[1].passthrough_types["p_date"], pa.string())
        self.assertEqual(ops[4].group_key, "id")
        self.assertEqual(ops[4].field_key, "source")
        self.assertEqual(
            ops[4].required_values,
            {
                "ecom_product_product_raw_data": 1,
                "ecom_product_main_raw_data": 1,
                "ecom_product_info_raw_data": 1,
            },
        )
        self.assertEqual(
            ops[6].filter_condition,
            "(source == 'ecom_product_product_raw_data' and valid_image_count >= 1) or "
            "(source != 'ecom_product_product_raw_data' and valid_image_count >= 2)",
        )
        self.assertEqual(ops[8].text_key, "__dj_source_md5")
        self.assertEqual(ops[9].passthrough_types["p_date"], pa.string())

    def test_product_comment_hive_magnus_config_loads(self):
        _patch_yaml_loader_tags()
        path = os.path.join(
            os.getcwd(),
            "demos",
            "bytedance",
            "ecom_review_a_dragon",
            "configs",
            "product_comment_hive_magnus.yaml",
        )

        cfg = init_configs(args=["--config", path, "--ray_address", "local"], load_configs_only=True)
        ops = load_ops(cfg.process)
        ds_config = cfg.dataset["configs"][0]
        schema_fields = {field["name"]: field["type"] for field in cfg.export["schema"]["fields"]}

        self.assertEqual(ds_config["source"], "hive")
        self.assertEqual(ds_config["table_name"], "ad_addrd_stats.product_comment_sample_v2_stats_daily")
        self.assertIn("p_date = '<P_DATE>'", ds_config["filter"])
        self.assertNotIn("with_pic = 1", ds_config["filter"])
        self.assertNotIn("comment_pic_url IS NOT NULL", ds_config["filter"])
        self.assertIn("comment_pic_url", ds_config["columns"])
        self.assertEqual(cfg.export["target"], "magnus")
        self.assertEqual(cfg.export["table_name"], "<CATALOG>.<DATABASE>.ecom_product_comment_image_schema")
        self.assertEqual(cfg.export["partition_values"]["p_date"], "<P_DATE>")
        self.assertNotIn("ocr_result", schema_fields)
        self.assertEqual(
            [op.__class__.__name__ for op in ops],
            [
                "SpecifiedFieldNonEmptyFilter",
                "RayFieldDedupPipeline",
                "EcomCommentSchemaPrepareMapper",
                "AwemePackUrlMapper",
                "DownloadFileMapper",
                "ImageBytesPruneMapper",
                "ImageBytesExactDedupMapper",
                "GeneralFieldFilter",
                "JsonExtraUpdateMapper",
                "ImageSchemaFinalizeMapper",
                "RayFieldDedupPipeline",
            ],
        )
        self.assertEqual(ops[0].field_key, "content")
        self.assertEqual(ops[1].field_key, "comment_id")
        self.assertEqual(ops[2].uri_field, "cmmt_img_uri")
        self.assertEqual(ops[2].with_pic_source, "ecom_comment_with_pic_raw_data")
        self.assertEqual(ops[2].no_pic_source, "ecom_comment_no_pic_raw_data")
        self.assertEqual(ops[3].uri_field, "image_uris")
        self.assertEqual(ops[3].url_field, "image_urls")
        self.assertEqual(ops[3].source_psm, "ad.ai.data_forge")
        self.assertEqual(ops[3].target_psm, "aweme.pack.url")
        self.assertEqual(ops[4].timeout, 10)
        self.assertEqual(ops[4].retry_times, 3)
        self.assertTrue(ops[4].resume_download)
        self.assertEqual(ops[5].valid_image_count_key, "valid_image_count")
        self.assertTrue(ops[6].preserve_existing_md5_on_empty)
        self.assertEqual(ops[7].filter_condition, "type == 'text' or valid_image_count >= 1")
        self.assertEqual(ops[8].field_mappings, {"image_urls": "valid_urls"})
        self.assertEqual(ops[9].type_key, "type")
        self.assertEqual(ops[9].passthrough_types["p_date"], pa.string())
        self.assertEqual(ops[10].field_key, "md5")


if __name__ == "__main__":
    unittest.main()
