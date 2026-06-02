import unittest
import os
import os.path as osp
import shutil
import tempfile
import threading
import numpy as np
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from unittest.mock import patch

from data_juicer.core.data import NestedDataset as Dataset
from data_juicer.utils.mm_utils import load_image, load_image_byte
from data_juicer.utils.unittest_utils import DataJuicerTestCaseBase
from data_juicer.ops.mapper.io.download_file_mapper import DownloadFileMapper


class DownloadFileMapperTest(DataJuicerTestCaseBase):

    def setUp(self):
        super().setUp()

        self.temp_dir = tempfile.mkdtemp()
        self.data_path = osp.abspath(osp.join(osp.dirname(osp.realpath(__file__)), '..', 'data'))
        self.img1_path = osp.join(self.data_path, 'img1.png')
        self.img2_path = osp.join(self.data_path, 'img2.jpg')
        self.img3_path = osp.join(self.data_path, 'img3.jpg')

        # start HTTP server
        self.server_address = ('localhost', 0)  # 0 means random port
        self.httpd = HTTPServer(
            self.server_address,
            partial(SimpleHTTPRequestHandler, directory=self.data_path)
        )
        self.port = self.httpd.server_address[1]
        self.img1_url = f'http://localhost:{self.port}/{os.path.basename(self.img1_path)}'
        self.img2_url = f'http://localhost:{self.port}/{os.path.basename(self.img2_path)}'
        self.img3_url = f'http://localhost:{self.port}/{os.path.basename(self.img3_path)}'
        
        # start the server in a thread
        self.server_thread = threading.Thread(target=self.httpd.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        shutil.rmtree(self.temp_dir)

        super().tearDown()

    def _test_image_download(self, ds_list, save_field=None):
        op = DownloadFileMapper(
                save_dir=self.temp_dir,
                download_field='images',
                save_field=save_field)

        dataset = Dataset.from_list(ds_list)
        dataset = dataset.map(op.process, batch_size=2)
        
        res_list = dataset.to_list()
        res_list = sorted(res_list, key=lambda x: x['id'])

        self.assertEqual(len(ds_list), len(res_list))

        for i in range(len(ds_list)):
            source, res = ds_list[i], res_list[i]
            for j in range(len(source[op.image_key])):
                s_path, r_path = source[op.image_key][j], res[op.image_key][j]
                fname = os.path.basename(s_path)
                self.assertEqual(fname, os.path.basename(r_path))
                if s_path.startswith('http'):
                    self.assertEqual(os.path.dirname(r_path), self.temp_dir)
                else:
                    self.assertEqual(s_path, r_path)

                t_img = np.array(load_image(os.path.join(self.data_path, fname)))
                r_img = np.array(load_image(r_path))

                np.testing.assert_array_equal(t_img, r_img)

                if save_field:
                    self.assertEqual(
                        res[save_field][j],
                        load_image_byte(os.path.join(self.data_path, fname))
                    )

    def test_image_download(self):
        ds_list = [{
            'images': [self.img1_url],
            'id': 1
        }, {
            'images': [self.img2_url, self.img3_url],
            'id': 2
        }, {
            'images': [self.img1_url, self.img2_url, self.img3_url],
            'id': 3
        }]

        self._test_image_download(ds_list)
        
    def test_image_url_and_local_path(self):
        ds_list = [{
            'images': [self.img1_path],
            'id': 1
        }, {
            'images': [self.img2_path, self.img3_url],
            'id': 2
        }, {
            'images': [self.img1_path, self.img2_url, self.img3_path],
            'id': 3
        }]

        self._test_image_download(ds_list)
        
    def test_download_image_failed(self):
        ds_list = [{
            'images': self.img2_url + '_failed_test',
            'id': 1
        }, {
            'images': self.img3_url + '_failed_test',
            'id': 2
        }, {
            'images': self.img1_url,
            'id': 3
        }]

        op = DownloadFileMapper(
                save_dir=self.temp_dir,
                download_field='images')

        dataset = Dataset.from_list(ds_list)
        dataset = dataset.map(op.process, batch_size=2)
        
        res_list = dataset.to_list()
        res_list = sorted(res_list, key=lambda x: x['id'])

        self.assertEqual(len(ds_list), len(res_list))

        for i in range(len(ds_list)):
            source, res = ds_list[i], res_list[i]
            s_path, r_path = source[op.image_key], res[op.image_key]
            fname = os.path.basename(s_path)
            self.assertEqual(fname, os.path.basename(r_path))
            if s_path.startswith('http') and 'failed_test' not in s_path:
                self.assertEqual(os.path.dirname(r_path), self.temp_dir)
            else:
                self.assertEqual(s_path, r_path)

    def test_image_str_type(self):
        ds_list = [{
            'images': self.img2_path,
            'id': 1
        }, {
            'images': self.img3_path,
            'id': 2
        }, {
            'images': self.img1_url,
            'id': 3
        }]

        op = DownloadFileMapper(
                save_dir=self.temp_dir,
                download_field='images')

        dataset = Dataset.from_list(ds_list)
        dataset = dataset.map(op.process, batch_size=2)
        
        res_list = dataset.to_list()
        res_list = sorted(res_list, key=lambda x: x['id'])

        self.assertEqual(len(ds_list), len(res_list))

        for i in range(len(ds_list)):
            source, res = ds_list[i], res_list[i]
            s_path, r_path = source[op.image_key], res[op.image_key]
            fname = os.path.basename(s_path)
            self.assertEqual(fname, os.path.basename(r_path))
            if s_path.startswith('http'):
                self.assertEqual(os.path.dirname(r_path), self.temp_dir)
            else:
                self.assertEqual(s_path, r_path)

            t_img = np.array(load_image(os.path.join(self.data_path, fname)))
            r_img = np.array(load_image(r_path))

            np.testing.assert_array_equal(t_img, r_img)

    def test_image_with_only_save_field(self):
        ds_list = [{
            'images': [self.img1_url],
            'id': 1
        }, {
            'images': [self.img2_url, self.img3_url],
            'id': 2
        }, {
            'images': [self.img1_url, self.img2_url, self.img3_url],
            'id': 3
        }, {
            'images': [self.img2_url],
            'id': 4
        },
        ]

        save_field='image_bytes'

        op = DownloadFileMapper(
                save_dir=None,
                download_field='images',
                save_field=save_field)

        dataset = Dataset.from_list(ds_list)
        dataset = dataset.map(op.process, batch_size=2)
        
        res_list = dataset.to_list()
        res_list = sorted(res_list, key=lambda x: x['id'])

        self.assertEqual(len(ds_list), len(res_list))

        for i in range(len(ds_list)):
            source, res = ds_list[i], res_list[i]
            for j in range(len(source[op.image_key])):
                s_path, r_path = source[op.image_key][j], res[op.image_key][j]
                self.assertEqual(s_path, r_path)
                fname = os.path.basename(s_path)
                self.assertEqual(
                    res[save_field][j],
                    load_image_byte(os.path.join(self.data_path, fname))
                )

    def test_filter_non_url_keeps_only_remote_downloads(self):
        op = DownloadFileMapper(
            download_field="images",
            save_field="image_bytes",
            filter_non_url=True,
        )

        samples = {
            "images": [
                [self.img1_url, self.img2_path, "", None, "not-a-url"],
                ["ftp://example.com/img.png", self.img3_url],
            ],
        }

        output = op.process_batched(samples)

        self.assertEqual(output["images"], [[self.img1_url], [self.img3_url]])
        self.assertEqual(
            output["image_bytes"],
            [[load_image_byte(self.img1_path)], [load_image_byte(self.img3_path)]],
        )

    def test_retry_remote_download_after_first_failure(self):
        attempts = {"count": 0}

        async def flaky_download(*args, **kwargs):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise RuntimeError("temporary failure")
            return object(), b"ok"

        op = DownloadFileMapper(
            download_field="images",
            save_field="image_bytes",
            retry_times=2,
        )

        with patch("data_juicer.ops.mapper.io.download_file_mapper.download_file", side_effect=flaky_download):
            results = op.download_files_async(["http://example.com/a.png"], [True])

        self.assertEqual(attempts["count"], 2)
        self.assertEqual(results[0][2], "success")
        self.assertEqual(results[0][4], b"ok")

    def test_default_retry_times_preserves_failed_download_shape(self):
        attempts = {"count": 0}

        async def failing_download(*args, **kwargs):
            attempts["count"] += 1
            raise RuntimeError("download failed")

        op = DownloadFileMapper(
            download_field="images",
            save_field="image_bytes",
        )

        with patch("data_juicer.ops.mapper.io.download_file_mapper.download_file", side_effect=failing_download):
            results = op.download_files_async(["http://example.com/a.png"], [True])

        self.assertEqual(attempts["count"], 1)
        self.assertEqual(results[0][2], "failed")
        self.assertEqual(results[0][4], None)

    def test_download_metrics_cover_success_failure_bytes_and_latency(self):
        async def fake_download(_session, url, _save_path, **_kwargs):
            if url.endswith("failed.png"):
                raise RuntimeError("download failed")
            return object(), b"image-bytes"

        op = DownloadFileMapper(
            download_field="images",
            save_field="image_bytes",
            retry_times=1,
        )

        with patch("data_juicer.ops.mapper.io.download_file_mapper.download_file", side_effect=fake_download), patch(
            "data_juicer.ops.mapper.io.download_file_mapper.emit_download_qps"
        ) as emit_qps, patch(
            "data_juicer.ops.mapper.io.download_file_mapper.emit_download_bytes"
        ) as emit_bytes, patch(
            "data_juicer.ops.mapper.io.download_file_mapper.emit_download_latency_ms"
        ) as emit_latency, patch(
            "data_juicer.ops.mapper.io.download_file_mapper.record_runtime_operation_counts"
        ) as record_counts:
            results = op.download_files_async(
                ["http://example.com/ok.png", "http://example.com/failed.png"],
                [True, True],
            )
            save_field_contents, reconstructed_path, failed_count, failed_summary = op.download_nested_urls(
                [["http://example.com/ok.png"], ["http://example.com/failed.png"]],
                save_field_contents=[[None], [None]],
            )

        self.assertEqual([result[2] for result in results], ["success", "failed"])
        self.assertEqual([call.kwargs["status"] for call in emit_qps.call_args_list], ["success", "failed"] * 2)
        self.assertTrue(all(call.kwargs["op_name"] == "download_file_mapper" for call in emit_qps.call_args_list))
        self.assertTrue(all(call.kwargs["scheme"] == "http" for call in emit_qps.call_args_list))
        self.assertTrue(all(call.kwargs["save_mode"] == "memory" for call in emit_qps.call_args_list))
        self.assertEqual(emit_bytes.call_count, 2)
        self.assertTrue(all(call.kwargs["byte_count"] == len(b"image-bytes") for call in emit_bytes.call_args_list))
        self.assertEqual(emit_latency.call_count, 4)
        for call in emit_qps.call_args_list + emit_bytes.call_args_list + emit_latency.call_args_list:
            self.assertNotIn("url", call.kwargs)

        self.assertEqual(failed_count, 1)
        self.assertIsNone(reconstructed_path)
        self.assertEqual(len(save_field_contents), 2)
        self.assertTrue(failed_summary)
        record_counts.assert_called_once_with(
            "download",
            op_name="download_file_mapper",
            total=2,
            success=1,
            failed=1,
        )

    def test_failed_download_logs_are_aggregated_per_worker(self):
        async def failing_download(*args, **kwargs):
            raise RuntimeError("403, message='Forbidden', url='http://example.com/image.png'")

        op = DownloadFileMapper(
            download_field="images",
            save_field="image_bytes",
            max_concurrent=1,
        )

        with patch("data_juicer.ops.mapper.io.download_file_mapper.download_file", side_effect=failing_download), patch(
            "data_juicer.ops.mapper.io.download_file_mapper.logger.error"
        ) as log_error:
            op.process_batched({"images": [f"http://example.com/{idx}.png" for idx in range(99)]})
            log_error.assert_not_called()

            op.process_batched({"images": ["http://example.com/99.png"]})

        log_error.assert_called_once()
        message = log_error.call_args.args[0]
        self.assertIn("download_file_mapper failures reached 100 in this worker", message)
        self.assertIn("100 x 403, message='Forbidden'", message)
        self.assertNotIn("url=", message)

    def test_image_with_savefield_and_savedir(self):
        ds_list = [{
            'images': [self.img1_url],
            'id': 1
        }, {
            'images': [self.img2_path, self.img3_url],
            'id': 2
        }, {
            'images': [self.img1_url, self.img2_path, self.img3_url],
            'id': 3
        }
        ]
        
        self._test_image_download(ds_list, save_field='image_bytes')

    def test_image_with_savefield_and_resume(self):
        save_field='image_bytes'

        ds_list = [{
            'images': [self.img1_url],
            'id': 1,
            save_field: []
        }, {
            'images': [self.img2_url, self.img3_url],
            'id': 2,
            save_field: ['loaded', None]
        }, {
            'images': [self.img1_url, self.img2_path, self.img3_url],
            'id': 3
        }, {
            'images': [self.img2_url],
            'id': 4,
            save_field: ['loaded', None]  # will be fixed auto
        }]


        tgt_list = [{
            'images': [self.img1_url],
            'id': 1,
            save_field: [load_image_byte(self.img1_path)]
        }, {
            'images': [self.img2_url, self.img3_url],
            'id': 2,
            save_field: [b'loaded', load_image_byte(self.img3_path)],
        }, {
            'images': [self.img1_url, self.img2_path, self.img3_url],
            'id': 3,
            save_field: [
                load_image_byte(self.img1_path),
                load_image_byte(self.img2_path),
                load_image_byte(self.img3_path)]
        }, {
            'images': [self.img2_url],
            'id': 4,
            save_field: [load_image_byte(self.img2_path)]
        }]

        op = DownloadFileMapper(
                save_dir=None,
                download_field='images',
                save_field=save_field,
                resume_download=True)

        dataset = Dataset.from_list(ds_list)
        dataset = dataset.map(op.process, batch_size=2)
        
        res_list = dataset.to_list()
        res_list = sorted(res_list, key=lambda x: x['id'])

        self.assertEqual(len(ds_list), len(res_list))

        for i in range(len(ds_list)):
            self.assertListEqual(res_list[i][save_field], tgt_list[i][save_field])
            self.assertEqual(res_list[i]['id'], tgt_list[i]['id'])
            self.assertListEqual(res_list[i]['images'], tgt_list[i]['images'])

    def test_image_with_savefield_and_resume_and_savedir(self):

        def _to_tmp_path(img_path):
            return osp.join(self.temp_dir, osp.basename(img_path))

        ds_list = [{
            'images': [self.img1_url],
            'id': 1,
            'image_bytes': []
        }, {
            'images': [self.img2_url, self.img3_url],
            'id': 2,
            'image_bytes': ['loaded', None]
        }, {
            'images': [self.img1_url, self.img2_path, self.img3_url],
            'id': 3
        }, {
            'images': [self.img2_url],
            'id': 4,
            'image_bytes': ['loaded', None]  # will be fixed auto
        }]


        tgt_list = [{
            'images': [_to_tmp_path(self.img1_url)],
            'id': 1,
            'image_bytes': [load_image_byte(self.img1_path)]
        }, {
            'images': [_to_tmp_path(self.img2_url), _to_tmp_path(self.img3_url)],
            'id': 2,
            'image_bytes': [b'loaded', load_image_byte(self.img3_path)],
        }, {
            'images': [
                _to_tmp_path(self.img1_url),
                self.img2_path,
                _to_tmp_path(self.img3_url)],
            'id': 3,
            'image_bytes': [
                load_image_byte(self.img1_path),
                load_image_byte(self.img2_path),
                load_image_byte(self.img3_path)]
        }, {
            'images': [_to_tmp_path(self.img2_url)],
            'id': 4,
            'image_bytes': [load_image_byte(self.img2_path)]
        }]

        op = DownloadFileMapper(
                save_dir=self.temp_dir,
                download_field='images',
                save_field='image_bytes',
                resume_download=True)

        dataset = Dataset.from_list(ds_list)
        dataset = dataset.map(op.process, batch_size=2)
        
        res_list = dataset.to_list()
        res_list = sorted(res_list, key=lambda x: x['id'])

        self.assertEqual(len(ds_list), len(res_list))

        for i in range(len(ds_list)):
            self.assertListEqual(res_list[i]['image_bytes'], tgt_list[i]['image_bytes'])
            self.assertEqual(res_list[i]['id'], tgt_list[i]['id'])
            self.assertListEqual(res_list[i]['images'], tgt_list[i]['images'])


if __name__ == '__main__':
    unittest.main()
