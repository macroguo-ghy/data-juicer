from __future__ import annotations

import pyarrow as pa

from data_juicer.ops.base_op import OPERATORS, Mapper

OP_NAME = "field_drop_mapper"


@OPERATORS.register_module(OP_NAME)
class FieldDropMapper(Mapper):
    """Drop configured fields from samples or batches."""

    _batched_op = True

    def __init__(self, fields: list[str] | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields = list(fields or [])

    def process_single(self, sample):
        for field in self.fields:
            sample.pop(field, None)
        return sample

    def process_batched(self, samples):
        if isinstance(samples, pa.Table):
            output = samples
            for field in self.fields:
                field_index = output.schema.get_field_index(field)
                if field_index >= 0:
                    output = output.remove_column(field_index)
            return output

        for field in self.fields:
            samples.pop(field, None)
        return samples
