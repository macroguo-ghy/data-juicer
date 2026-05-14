from typing import Any

import pyarrow as pa

from data_juicer.ops.base_op import OPERATORS, Pipeline
from data_juicer.ops.mapper.schema.passthrough_type_utils import (
    coerce_value_for_arrow_type,
    parse_arrow_type,
)

OP_NAME = "landing_page_image_schema_finalize_mapper"

_BASE_OUTPUT_KEYS = [
    "id",
    "source",
    "texts",
    "images",
    "audios",
    "videos",
    "has_audio_in_video",
    "type",
    "md5",
]
_BASE_ARROW_TYPES = {
    "id": pa.string(),
    "source": pa.string(),
    "texts": pa.list_(pa.string()),
    "images": pa.list_(pa.binary()),
    "audios": pa.list_(pa.binary()),
    "videos": pa.list_(pa.binary()),
    "has_audio_in_video": pa.bool_(),
    "type": pa.string(),
    "md5": pa.string(),
}
_ARROW_TYPE_ALIASES = {
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float": pa.float32(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "double": pa.float64(),
    "str": pa.string(),
    "string": pa.string(),
    "binary": pa.binary(),
}


@OPERATORS.register_module(OP_NAME)
class LandingPageImageSchemaFinalizeMapper(Pipeline):
    """Finalize landing-page image rows into the Data-Juicer multimodal schema."""

    def __init__(
        self,
        image_key: str = "images",
        image_bytes_key: str = "image_bytes",
        md5_key: str = "md5",
        id_cache_key: str = "__dj_landing_page_id",
        source_cache_key: str = "__dj_landing_page_image_source",
        passthrough_keys: list[str] | None = None,
        passthrough_types: dict[str, str] | None = None,
        extra_cache_key: str | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.
        :param image_key: field that stores final valid image URLs.
        :param image_bytes_key: field that stores final image bytes.
        :param md5_key: field that stores sample-level md5.
        :param id_cache_key: temporary field containing the final id.
        :param source_cache_key: temporary field containing image_source.
        :param passthrough_keys: source sample fields to keep as top-level output columns.
        :param passthrough_types: pyarrow types for passthrough fields. This keeps
            Ray block schemas stable when a block contains only null values.
        :param extra_cache_key: deprecated, accepted for old configs but ignored.
        :param args: extra args.
        :param kwargs: extra args.
        """
        kwargs["image_key"] = image_key
        kwargs["image_bytes_key"] = image_bytes_key
        super().__init__(*args, **kwargs)
        self.md5_key = md5_key
        self.id_cache_key = id_cache_key
        self.source_cache_key = source_cache_key
        self.extra_cache_key = extra_cache_key
        self.passthrough_keys = list(passthrough_keys or [])
        self.passthrough_types = {
            key: self._parse_arrow_type(value) for key, value in (passthrough_types or {}).items()
        }

    def run(self, dataset, *, exporter=None, tracer=None):
        from data_juicer.core.data import NestedDataset

        if isinstance(dataset, NestedDataset):
            return dataset.map(
                self.process_batched,
                batched=True,
                batch_size=self.batch_size,
                num_proc=self.runtime_np(),
                remove_columns=list(dataset.column_names),
                desc=self._name + "_process",
            )

        return dataset.map_batches(
            self.process_batched,
            batch_format="pyarrow",
            batch_size=self.batch_size,
        )

    def process_single(self, sample):
        image_source = sample.get(self.source_cache_key, "thumbnail")
        row = {
            "id": sample.get(self.id_cache_key),
            "source": f"site_creative_{image_source}_raw_data",
            "texts": [],
            "images": self._as_binary_list(sample.get(self.image_bytes_key)),
            "audios": [],
            "videos": [],
            "has_audio_in_video": False,
            "type": "image",
            "md5": sample.get(self.md5_key),
        }
        for key in self.passthrough_keys:
            row[key] = self._passthrough_value(key, sample.get(key))
        return row

    @staticmethod
    def _as_binary_list(value: Any) -> list[bytes]:
        if value is None:
            return []
        if hasattr(value, "as_py"):
            value = value.as_py()
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray, memoryview)):
            value = value.tolist()
        if isinstance(value, (bytes, bytearray, memoryview)):
            return [bytes(value)]
        if isinstance(value, (list, tuple)):
            if LandingPageImageSchemaFinalizeMapper._is_uint8_sequence(value):
                return [bytes(value)]
            values = []
            for item in value:
                values.extend(LandingPageImageSchemaFinalizeMapper._as_binary_list(item))
            return values
        return []

    @staticmethod
    def _is_uint8_sequence(value: list[Any] | tuple[Any, ...]) -> bool:
        return bool(value) and all(
            isinstance(item, int) and not isinstance(item, bool) and 0 <= item <= 255 for item in value
        )

    def process_batched(self, samples):
        input_schema = None
        return_arrow = isinstance(samples, pa.Table)
        if isinstance(samples, pa.Table):
            input_schema = samples.schema
            samples = samples.to_pydict()

        rows = []
        keys = list(samples.keys())
        if not keys:
            if return_arrow:
                return self._rows_to_arrow_table([], input_schema)
            return {}

        for i in range(len(samples[keys[0]])):
            rows.append(self.process_single({key: samples[key][i] for key in keys}))

        if return_arrow:
            return self._rows_to_arrow_table(rows, input_schema)

        if not rows:
            return {key: [] for key in self._output_keys()}

        return {key: [row[key] for row in rows] for key in self._output_keys()}

    def _output_keys(self):
        keys = list(_BASE_OUTPUT_KEYS)
        keys.extend(key for key in self.passthrough_keys if key not in keys)
        return keys

    def _rows_to_arrow_table(self, rows, input_schema=None):
        arrays = []
        fields = []
        for key in self._output_keys():
            values = [self._passthrough_value(key, row.get(key)) for row in rows]
            arrow_type = self._arrow_type_for_key(key, values, input_schema)
            arrays.append(pa.array(values, type=arrow_type))
            fields.append(pa.field(key, arrow_type))
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    def _passthrough_value(self, key: str, value: Any) -> Any:
        if key in self.passthrough_types:
            return coerce_value_for_arrow_type(value, self.passthrough_types[key])
        return value

    def _arrow_type_for_key(self, key, values, input_schema=None):
        if key in _BASE_ARROW_TYPES:
            return _BASE_ARROW_TYPES[key]
        if key in self.passthrough_types:
            return self.passthrough_types[key]
        if input_schema is not None:
            field_index = input_schema.get_field_index(key)
            if field_index >= 0:
                return input_schema.field(field_index).type

        inferred = pa.array(values)
        if pa.types.is_null(inferred.type):
            return pa.string()
        return inferred.type

    @staticmethod
    def _parse_arrow_type(value):
        return parse_arrow_type(value)
