import json
import sys
import types
import unittest
from types import SimpleNamespace
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
from data_juicer.core.data import NestedDataset
from data_juicer.ops.mapper.image.image_ocr_mapper import (
    ImageOcrMapper,
    _serialize_ocr_response,
    _thrift_obj_to_dict,
)

pa.register_extension_type = _register_extension_type


class FakeRpc:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, *, binary_list):
        self.calls.append(list(binary_list))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _op_with_rpc(fake_rpc: FakeRpc, **kwargs) -> ImageOcrMapper:
    defaults = {
        "split_size": 10,
        "day_interval_seconds": 0.0,
        "night_interval_seconds": 0.0,
        "auto_op_parallelism": False,
        "num_proc": 1,
    }
    defaults.update(kwargs)
    op = ImageOcrMapper(**defaults)
    op._create_rpc = lambda: fake_rpc
    return op


class ImageOcrMapperTest(unittest.TestCase):
    def test_constructor_validates_split_size(self):
        with self.assertRaisesRegex(ValueError, "split_size must be positive"):
            ImageOcrMapper(split_size=0)

    def test_process_batched_arrow_adds_ocr_and_preserves_schema(self):
        fake_rpc = FakeRpc([["ocr-a"], ["ocr-b"]])
        op = _op_with_rpc(fake_rpc, split_size=1)
        input_schema = pa.schema(
            [
                pa.field("id", pa.string()),
                pa.field("images", pa.list_(pa.binary())),
                pa.field("p_date", pa.string()),
            ]
        )
        table = pa.Table.from_pylist(
            [{"id": "comment_id-1", "images": [b"a", b"b"], "p_date": "20260424"}],
            schema=input_schema,
        )

        output = op.process_batched(table)

        self.assertEqual(output.num_rows, 1)
        self.assertEqual(output.schema.field("images").type, pa.list_(pa.binary()))
        self.assertEqual(output.schema.field("ocr_result").type, pa.list_(pa.string()))
        self.assertEqual(fake_rpc.calls, [[b"a"], [b"b"]])
        self.assertEqual(output.to_pylist()[0]["ocr_result"], ["ocr-a", "ocr-b"])
        self.assertEqual(output.to_pylist()[0]["p_date"], "20260424")

    def test_process_batched_keeps_rows_with_empty_ocr_result_for_failure_and_no_images(self):
        fake_rpc = FakeRpc([RuntimeError("ocr failed"), []])
        op = _op_with_rpc(fake_rpc)
        input_schema = pa.schema(
            [
                pa.field("id", pa.string()),
                pa.field("images", pa.list_(pa.binary())),
            ]
        )
        table = pa.Table.from_pylist(
            [
                {"id": "rpc-error", "images": [b"a"]},
                {"id": "empty-result", "images": [b"b"]},
                {"id": "none-images", "images": None},
                {"id": "empty-images", "images": []},
            ],
            schema=input_schema,
        )

        output = op.process_batched(table)

        self.assertEqual(output.num_rows, 4)
        self.assertEqual(fake_rpc.calls, [[b"a"], [b"b"]])
        self.assertEqual(
            output.to_pylist(),
            [
                {"id": "rpc-error", "images": [b"a"], "ocr_result": []},
                {"id": "empty-result", "images": [b"b"], "ocr_result": []},
                {"id": "none-images", "images": None, "ocr_result": []},
                {"id": "empty-images", "images": [], "ocr_result": []},
            ],
        )

    def test_split_failure_discards_partial_ocr_results(self):
        fake_rpc = FakeRpc([["ocr-a"], RuntimeError("ocr failed")])
        op = _op_with_rpc(fake_rpc, split_size=1)

        output = op.process_batched({"id": ["1"], "images": [[b"a", b"b"]]})

        self.assertEqual(fake_rpc.calls, [[b"a"], [b"b"]])
        self.assertEqual(output["ocr_result"], [[]])

    def test_nested_dataset_run_and_state_do_not_serialize_rpc(self):
        fake_rpc = FakeRpc([["ocr-a"]])
        op = _op_with_rpc(fake_rpc)
        op._rpc = fake_rpc
        state = op.__getstate__()
        self.assertIsNone(state["_rpc"])

        dataset = NestedDataset.from_list([{"id": "1", "images": [b"a"]}])
        rows = op.run(dataset).to_list()

        self.assertEqual(rows[0]["ocr_result"], ["ocr-a"])

    def test_run_uses_ray_map_batches_for_non_nested_dataset(self):
        class FakeRayDataset:
            def map_batches(self, fn, *, batch_format, batch_size):
                self.fn = fn
                self.batch_format = batch_format
                self.batch_size = batch_size
                return "mapped"

        fake_dataset = FakeRayDataset()
        op = _op_with_rpc(FakeRpc([]), batch_size=7)

        result = op.run(fake_dataset)

        self.assertEqual(result, "mapped")
        self.assertEqual(fake_dataset.batch_format, "pyarrow")
        self.assertEqual(fake_dataset.batch_size, 7)

    def test_process_batched_dict_handles_empty_rows(self):
        op = _op_with_rpc(FakeRpc([]))

        empty = op.process_batched({})
        no_rows = op.process_batched({"id": [], "images": []})

        self.assertEqual(empty, {"ocr_result": []})
        self.assertEqual(no_rows, {"id": [], "images": [], "ocr_result": []})

    def test_empty_arrow_batch_keeps_input_schema_and_adds_ocr_result(self):
        op = _op_with_rpc(FakeRpc([]))
        input_schema = pa.schema(
            [
                pa.field("id", pa.string()),
                pa.field("images", pa.list_(pa.binary())),
            ]
        )
        table = pa.Table.from_arrays(
            [
                pa.array([], type=pa.string()),
                pa.array([], type=pa.list_(pa.binary())),
            ],
            schema=input_schema,
        )

        output = op.process_batched(table)

        self.assertEqual(output.num_rows, 0)
        self.assertEqual(output.schema.names, ["id", "images", "ocr_result"])
        self.assertEqual(output.schema.field("ocr_result").type, pa.list_(pa.string()))

    def test_existing_ocr_result_field_is_replaced_in_arrow_output(self):
        op = _op_with_rpc(FakeRpc([["new"]]))
        input_schema = pa.schema(
            [
                pa.field("id", pa.string()),
                pa.field("images", pa.list_(pa.binary())),
                pa.field("ocr_result", pa.list_(pa.string())),
            ]
        )
        table = pa.Table.from_pylist(
            [{"id": "1", "images": [b"a"], "ocr_result": ["old"]}],
            schema=input_schema,
        )

        output = op.process_batched(table)

        self.assertEqual(output.schema.names, ["id", "images", "ocr_result"])
        self.assertEqual(output.to_pylist()[0]["ocr_result"], ["new"])

    def test_as_bytes_list_accepts_arrow_like_values_and_ignores_other_types(self):
        class ArrowScalar:
            def as_py(self):
                return bytearray(b"a")

        class ArrayValue:
            def tolist(self):
                return [memoryview(b"b"), None, "ignored"]

        self.assertEqual(ImageOcrMapper._as_bytes_list(None), [])
        self.assertEqual(ImageOcrMapper._as_bytes_list(ArrowScalar()), [b"a"])
        self.assertEqual(ImageOcrMapper._as_bytes_list(ArrayValue()), [b"b"])
        self.assertEqual(ImageOcrMapper._as_bytes_list("ignored"), [])

    def test_serialize_ocr_response_adds_area_ratio(self):
        word = SimpleNamespace(
            det_points_abs=[
                SimpleNamespace(x=0, y=0),
                SimpleNamespace(x=10, y=0),
                SimpleNamespace(x=10, y=10),
                SimpleNamespace(x=0, y=10),
            ]
        )
        response = SimpleNamespace(
            results=[
                SimpleNamespace(
                    status="",
                    extra={"width": "20", "height": "10"},
                    words=[word],
                )
            ]
        )

        serialized = _serialize_ocr_response(response)

        payload = json.loads(serialized[0])
        self.assertAlmostEqual(payload["ocr_area_ratio"], 0.5)
        self.assertEqual(payload["extra"], {"width": "20", "height": "10"})

    def test_serialize_ocr_response_handles_thrift_specs_bad_extra_and_empty_area(self):
        class ThriftResult:
            thrift_spec = {
                1: (None, "extra"),
                2: None,
                3: (None, "words"),
            }

            def __init__(self):
                self.extra = {"width": "bad", "height": "10"}
                self.words = [SimpleNamespace(det_points_abs=[])]
                self.ignored = "not in thrift spec"

        payload = json.loads(_serialize_ocr_response(SimpleNamespace(results=[ThriftResult()]))[0])

        self.assertEqual(payload["extra"], {"width": "bad", "height": "10"})
        self.assertEqual(payload["ocr_area_ratio"], 0.0)
        self.assertIsNone(_thrift_obj_to_dict(None))
        self.assertEqual(_thrift_obj_to_dict(object()).__class__, object)

    def test_create_rpc_uses_internal_contract_when_modules_available(self):
        class FakeImageInfo:
            def __init__(self, data):
                self.data = data

        class FakeImagesOcrRequest:
            pass

        class FakeImgOcrRPC:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.base_status_checked = False

            def additional_status_check(self, resp, req):
                self.base_status_checked = True
                return "ok"

        ocrx_thrift = SimpleNamespace(
            ImageInfo=FakeImageInfo,
            ImagesOcrRequest=FakeImagesOcrRequest,
        )
        modules = {
            "aigc_common": types.ModuleType("aigc_common"),
            "aigc_common.const": types.ModuleType("aigc_common.const"),
            "aigc_common.const.params": types.SimpleNamespace(CALLER_PSM="life.gen_ai.dorado_llm"),
            "aigc_common.rpc": types.ModuleType("aigc_common.rpc"),
            "aigc_common.rpc.lab_ocr": types.ModuleType("aigc_common.rpc.lab_ocr"),
            "aigc_common.rpc.lab_ocr.idl": types.ModuleType("aigc_common.rpc.lab_ocr.idl"),
            "aigc_common.rpc.lab_ocr.idl.ocrx": types.SimpleNamespace(ocrx_thrift=ocrx_thrift),
            "aigc_common.rpc.lab_ocr.img_ocr": types.SimpleNamespace(ImgOcrRPC=FakeImgOcrRPC),
        }

        with patch.dict(sys.modules, modules):
            op = ImageOcrMapper(day_interval_seconds=0.0, night_interval_seconds=0.0)
            rpc = op._create_rpc()
            req = rpc.build_req([b"a"])
            self.assertEqual(rpc.kwargs["psm"], "lab.ocrx.fusion_general")
            self.assertEqual(req.images[0].data, b"a")
            self.assertEqual(req.extra, {"dag": "text_tag"})
            self.assertEqual(rpc.additional_status_check(SimpleNamespace(results=[]), req), "ok")
            self.assertTrue(rpc.base_status_checked)
            with self.assertRaisesRegex(RuntimeError, "failed, status"):
                rpc.additional_status_check(SimpleNamespace(results=[SimpleNamespace(status="bad")]), req)
            self.assertIsNone(rpc.build_failed_result())
            self.assertEqual(rpc.process_resp(SimpleNamespace(results=[])), [])

        modules["aigc_common.const.params"] = types.SimpleNamespace(CALLER_PSM="wrong")
        with patch.dict(sys.modules, modules):
            with self.assertRaisesRegex(RuntimeError, "CALLER_PSM must be"):
                ImageOcrMapper()._create_rpc()


if __name__ == "__main__":
    unittest.main()
