from __future__ import annotations

import hashlib
import json
from typing import Any

from data_juicer.ops.base_op import OPERATORS, Mapper

OP_NAME = "prepare_record_key_mapper"
NEED_CTX = True
RECORD_KEY_FIELD = "__adc_record_key"
INTERNAL_FIELDS = {"ctx", RECORD_KEY_FIELD}


@OPERATORS.register_module(OP_NAME)
class PrepareRecordKeyMapper(Mapper):
    """Prepare a stable internal record key for downstream ADC status upserts."""

    def __init__(
        self,
        source_fields: list[str] | None = None,
        overwrite: bool = False,
        ctx: dict | None = None,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param source_fields: optional fields used to build the record key.
            If empty, all non-internal sample fields are used.
        :param overwrite: whether to overwrite an existing record key.
        :param ctx: platform context injected by backend when NEED_CTX is True.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        self.source_fields = list(source_fields or [])
        self.overwrite = overwrite
        self.ctx = ctx

    def process_single(self, sample: dict[str, Any]):
        if not self.overwrite and sample.get(RECORD_KEY_FIELD):
            return sample

        source = self._build_source(sample)
        sample[RECORD_KEY_FIELD] = self._stable_hash(source)
        return sample

    def _build_source(self, sample: dict[str, Any]):
        if self.source_fields:
            return {
                field: sample.get(field)
                for field in self.source_fields
            }
        return {
            key: value
            for key, value in sample.items()
            if key not in INTERNAL_FIELDS
        }

    @staticmethod
    def _stable_hash(value) -> str:
        payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
