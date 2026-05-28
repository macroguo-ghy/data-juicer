import asyncio
import unittest
from unittest.mock import patch

from data_juicer.ops.mapper.io.s3_download_file_mapper import S3DownloadFileMapper
from data_juicer.utils.unittest_utils import DataJuicerTestCaseBase


class S3DownloadFileMapperTest(DataJuicerTestCaseBase):

    def test_placeholder(self):
        # placeholder for test
        pass

    def test_s3_download_metrics_cover_success_failure_bytes_and_latency(self):
        op = S3DownloadFileMapper(
            download_field="files",
            save_field="bytes",
        )

        def fake_download_from_s3(url, _save_path=None, _return_content=False):
            if url.endswith("failed.png"):
                return "failed", "s3 failed", None, None
            return "success", None, b"s3-bytes", None

        op._download_from_s3 = fake_download_from_s3

        with patch("data_juicer.ops.mapper.io.s3_download_file_mapper.emit_download_qps") as emit_qps, patch(
            "data_juicer.ops.mapper.io.s3_download_file_mapper.emit_download_bytes"
        ) as emit_bytes, patch(
            "data_juicer.ops.mapper.io.s3_download_file_mapper.emit_download_latency_ms"
        ) as emit_latency:
            results = asyncio.run(
                op.download_files_async(
                    ["s3://bucket/ok.png", "s3://bucket/failed.png"],
                    [True, True],
                )
            )

        self.assertEqual([result[2] for result in results], ["success", "failed"])
        self.assertEqual([call.kwargs["status"] for call in emit_qps.call_args_list], ["success", "failed"])
        self.assertTrue(all(call.kwargs["op_name"] == "s3_download_file_mapper" for call in emit_qps.call_args_list))
        self.assertTrue(all(call.kwargs["scheme"] == "s3" for call in emit_qps.call_args_list))
        self.assertTrue(all(call.kwargs["save_mode"] == "memory" for call in emit_qps.call_args_list))
        self.assertEqual(emit_bytes.call_count, 1)
        self.assertEqual(emit_bytes.call_args.kwargs["byte_count"], len(b"s3-bytes"))
        self.assertEqual(emit_latency.call_count, 2)
        for call in emit_qps.call_args_list + emit_bytes.call_args_list + emit_latency.call_args_list:
            self.assertNotIn("url", call.kwargs)


if __name__ == '__main__':
    unittest.main()
