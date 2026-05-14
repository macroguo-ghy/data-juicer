from data_juicer.ops.base_op import OPERATORS, Mapper
from data_juicer.utils.http_utils import HttpClient

OP_NAME = "ad_ai_data_center_http_mapper"
CONFIG_PAGE_KEY = "adAiDataCenterHttp"


@OPERATORS.register_module(OP_NAME)
class AdAiDataCenterHttpMapper(Mapper):
    """Call an HTTP endpoint with selected sample fields and write the result."""

    def __init__(
        self,
        endpoint: str,
        input_fields: list[str],
        output_field: str = "http_result",
        error_field: str = "http_error",
        method: str = "POST",
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
        *args,
        **kwargs,
    ):
        """
        Initialization method.

        :param endpoint: request URL.
        :param input_fields: sample fields copied into the JSON request body.
        :param output_field: field used to store successful response data or text.
        :param error_field: field used to store failed HTTP result.
        :param method: HTTP method.
        :param headers: HTTP request headers.
        :param timeout: HTTP timeout in seconds.
        :param args: extra args.
        :param kwargs: extra args.
        """
        super().__init__(*args, **kwargs)
        if not input_fields:
            raise ValueError("input_fields must be provided")
        if not output_field:
            raise ValueError("output_field must be provided")
        if not error_field:
            raise ValueError("error_field must be provided")

        self.input_fields = list(input_fields)
        self.output_field = output_field
        self.error_field = error_field
        self.client = HttpClient(
            endpoint=endpoint,
            method=method,
            headers=headers,
            timeout=timeout,
        )

    def process_single(self, sample):
        payload = {
            "inputs": {
                field: sample.get(field)
                for field in self.input_fields
            }
        }
        result = self.client.request(json_body=payload)
        if result["ok"]:
            sample[self.output_field] = (
                result["data"] if result["data"] is not None else result["text"]
            )
            sample.pop(self.error_field, None)
        else:
            sample[self.error_field] = result
            sample.pop(self.output_field, None)
        return sample
