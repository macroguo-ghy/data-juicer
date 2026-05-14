import json
from typing import Any

import pyarrow as pa

from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.ops.mapper.schema.passthrough_type_utils import (
    coerce_value_for_arrow_type,
    parse_arrow_type,
)

OP_NAME = "landing_page_image_url_mapper"


@OPERATORS.register_module(OP_NAME)
class LandingPageImageUrlMapper(Mapper):
    """Extract landing-page image URLs and cache fields needed by the final schema mapper."""

    _batched_op = True

    def __init__(
        self,
        image_source: str = "thumbnail",
        id_field: str = "site_id",
        thumbnail_key: str = "thumbnail",
        preload_resources_key: str = "preload_resources",
        image_key: str = "images",
        source_cache_key: str = "__dj_landing_page_image_source",
        id_cache_key: str = "__dj_landing_page_id",
        passthrough_types: dict[str, str] | None = None,
        extra_keys: list[str] | None = None,
        extra_cache_key: str | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.
        :param image_source: which image source to convert, either "thumbnail" or "preloads".
        :param id_field: source field used to build the output sample id.
        :param thumbnail_key: field that stores the thumbnail URL.
        :param preload_resources_key: field that stores preload resource URLs or JSON.
        :param image_key: field to store extracted image URLs for downstream download.
        :param source_cache_key: temporary field to store image_source.
        :param id_cache_key: temporary field to store the final row id.
        :param passthrough_types: pyarrow types used to normalize existing
            source fields that should survive downstream Ray Arrow blocks.
        :param extra_keys: deprecated, accepted for old configs but ignored.
        :param extra_cache_key: deprecated, accepted for old configs but ignored.
        :param args: extra args.
        :param kwargs: extra args.
        """
        kwargs["image_key"] = image_key
        super().__init__(*args, **kwargs)
        if image_source not in {"thumbnail", "preloads"}:
            raise ValueError('image_source must be either "thumbnail" or "preloads"')
        self.image_source = image_source
        self.id_field = id_field
        self.thumbnail_key = thumbnail_key
        self.preload_resources_key = preload_resources_key
        self.extra_keys = list(extra_keys or [])
        self.extra_cache_key = extra_cache_key
        self.source_cache_key = source_cache_key
        self.id_cache_key = id_cache_key
        self.passthrough_types = {
            key: parse_arrow_type(value) for key, value in (passthrough_types or {}).items()
        }

    def process_single(self, sample):
        urls = self._extract_urls(sample)
        sample[self.image_key] = urls
        sample[self.id_cache_key] = f"{self.id_field}-{sample.get(self.id_field)}"
        sample[self.source_cache_key] = self.image_source
        for key, arrow_type in self.passthrough_types.items():
            if key in sample:
                sample[key] = coerce_value_for_arrow_type(sample.get(key), arrow_type)
        return sample

    def process_batched(self, samples):
        data = samples.to_pydict() if isinstance(samples, pa.Table) else samples
        if not data:
            return data

        row_count = len(next(iter(data.values())))
        if self.image_source == "thumbnail":
            source_values = self._values_for_key(data, self.thumbnail_key, row_count)
            data[self.image_key] = [self._parse_thumbnail(value) for value in source_values]
        else:
            source_values = self._values_for_key(data, self.preload_resources_key, row_count)
            data[self.image_key] = [self._parse_preload_resources(value) for value in source_values]

        id_values = self._values_for_key(data, self.id_field, row_count)
        data[self.id_cache_key] = [f"{self.id_field}-{value}" for value in id_values]
        data[self.source_cache_key] = [self.image_source] * row_count
        for key, arrow_type in self.passthrough_types.items():
            if key in data:
                data[key] = [coerce_value_for_arrow_type(value, arrow_type) for value in data[key]]
        return data

    def _extract_urls(self, sample: dict[str, Any]) -> list[str]:
        if self.image_source == "thumbnail":
            return self._parse_thumbnail(sample.get(self.thumbnail_key))
        return self._parse_preload_resources(sample.get(self.preload_resources_key))

    @staticmethod
    def _parse_thumbnail(url: Any) -> list[str]:
        return [url.strip()] if isinstance(url, str) and url.strip() else []

    @staticmethod
    def _values_for_key(data: dict[str, list[Any]], key: str, row_count: int) -> list[Any]:
        return data.get(key, [None] * row_count)

    @staticmethod
    def _parse_preload_resources(preloads: Any) -> list[str]:
        if preloads is None:
            return []
        if isinstance(preloads, str):
            preloads = preloads.strip()
            if not preloads:
                return []
            if preloads[0] not in "[{":
                return [preloads]
            try:
                preloads = json.loads(preloads)
            except (TypeError, json.JSONDecodeError):
                preloads = [preloads]

        if not isinstance(preloads, list):
            return []

        urls = []
        for item in preloads:
            url = item.get("url") if isinstance(item, dict) else item
            if isinstance(url, str) and url.strip():
                urls.append(url.strip())
        return urls
