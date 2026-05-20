import time

from data_juicer.ops.base_op import OPERATORS, Mapper

OP_NAME = "ad_test_processing_timestamp_mapper"


@OPERATORS.register_module(OP_NAME)
class AdTestProcessingTimestampMapper(Mapper):
    """Add the current processing timestamp to each sample."""

    _batched_op = True

    def __init__(self, field_name: str = "processing_timestamp", *args, **kwargs):
        """
        Initialization method.

        :param field_name: field name used to store the Unix timestamp.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not field_name:
            raise ValueError("field_name must be provided")
        self.field_name = field_name

    def process_batched(self, samples):
        first_key = next(iter(samples.keys()))
        samples[self.field_name] = [time.time() for _ in range(len(samples[first_key]))]
        return samples
