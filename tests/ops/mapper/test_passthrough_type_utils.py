import json
import unittest

import pyarrow as pa

_register_extension_type = pa.register_extension_type


def _register_extension_type_once(extension_type):
    try:
        _register_extension_type(extension_type)
    except pa.ArrowKeyError:
        if not extension_type.extension_name.startswith("datasets.features.features."):
            raise


pa.register_extension_type = _register_extension_type_once
from data_juicer.ops.mapper.schema.passthrough_type_utils import (
    coerce_value_for_arrow_type,
    parse_arrow_type,
)

pa.register_extension_type = _register_extension_type


class PassthroughTypeUtilsTest(unittest.TestCase):
    def test_parse_arrow_type_accepts_alias_and_pyarrow_type(self):
        self.assertEqual(parse_arrow_type("string"), pa.string())
        self.assertEqual(parse_arrow_type("DOUBLE"), pa.float64())
        self.assertEqual(parse_arrow_type(pa.int32()), pa.int32())
        with self.assertRaisesRegex(ValueError, "Unsupported passthrough arrow type"):
            parse_arrow_type("decimal128")

    def test_string_null_like_checks_stay_on_short_strings(self):
        class LargePayload(str):
            def strip(self, *args, **kwargs):
                raise AssertionError("large payload should not be stripped for null-like checks")

        payload = LargePayload('{"items": ["https://example.com/a.png"], "padding": "%s"}' % ("x" * 1024))

        self.assertIsNone(coerce_value_for_arrow_type(None, pa.string()))
        self.assertIsNone(coerce_value_for_arrow_type(" nan ", pa.string()))
        self.assertEqual(coerce_value_for_arrow_type(payload, pa.string()), payload)

    def test_coerce_scalar_container_and_binary_values(self):
        self.assertEqual(coerce_value_for_arrow_type(pa.scalar("abc"), pa.string()), "abc")
        self.assertEqual(
            coerce_value_for_arrow_type({"url": "https://example.com/a.png"}, pa.string()),
            '{"url": "https://example.com/a.png"}',
        )
        self.assertEqual(json.loads(coerce_value_for_arrow_type(["a", "b"], pa.string())), ["a", "b"])
        self.assertEqual(coerce_value_for_arrow_type(memoryview(b"abc"), pa.binary()), b"abc")
        self.assertEqual(coerce_value_for_arrow_type("abc", pa.binary()), b"abc")

    def test_coerce_numeric_and_boolean_values(self):
        self.assertEqual(coerce_value_for_arrow_type(True, pa.int64()), 1)
        self.assertEqual(coerce_value_for_arrow_type("12.0", pa.int64()), 12)
        self.assertIsNone(coerce_value_for_arrow_type("not-int", pa.int64()))
        self.assertEqual(coerce_value_for_arrow_type(False, pa.float64()), 0.0)
        self.assertEqual(coerce_value_for_arrow_type("1.25", pa.float64()), 1.25)
        self.assertIsNone(coerce_value_for_arrow_type("not-float", pa.float64()))
        self.assertIs(coerce_value_for_arrow_type("yes", pa.bool_()), True)
        self.assertIs(coerce_value_for_arrow_type("0", pa.bool_()), False)
        self.assertIsNone(coerce_value_for_arrow_type("maybe", pa.bool_()))
        self.assertIs(coerce_value_for_arrow_type(0, pa.bool_()), False)

    def test_unknown_arrow_type_returns_original_value(self):
        value = object()
        self.assertIs(coerce_value_for_arrow_type(value, pa.list_(pa.string())), value)


if __name__ == "__main__":
    unittest.main()
