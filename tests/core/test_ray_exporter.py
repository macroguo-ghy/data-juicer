import copy
import builtins
import os
import os.path as osp
import shutil
import unittest
from unittest.mock import MagicMock, patch

from pyarrow.fs import FileInfo, FileType, LocalFileSystem

from data_juicer.utils.unittest_utils import TEST_TAG, DataJuicerTestCaseBase
from data_juicer.core.ray_exporter import RayExporter
from data_juicer.utils.constant import Fields, HashKeys
from data_juicer.utils.mm_utils import load_images_byte


class TestRayExporterCheckpoint(unittest.TestCase):
    def test_checkpoint_export_uses_supplied_columns_without_fetch(self):
        class StrictRayDataset:
            def __init__(self):
                self.drop_columns = MagicMock(return_value=self)

            def columns(self):
                raise AssertionError("checkpoint mode must not fetch columns before export")

        dataset = StrictRayDataset()
        exporter = RayExporter(
            "/tmp/checkpoint_export.json",
            keep_stats_in_res_ds=False,
            keep_hashes_in_res_ds=False,
        )
        export_method = MagicMock()

        with patch("data_juicer.core.ray_exporter._is_ray_data_checkpoint_enabled", return_value=True):
            with patch.object(RayExporter, "_router", return_value={"json": export_method}):
                exporter.export(dataset, columns=["id", Fields.stats, HashKeys.hash])

        dataset.drop_columns.assert_called_once_with([Fields.stats, HashKeys.hash])
        export_method.assert_called_once()


class TestRayExporterHDFS(unittest.TestCase):
    def test_hdfs_export_resolves_filesystem_path_and_defaults_to_error_if_exists(self):
        class FakeDataset:
            def __init__(self):
                self.drop_columns = MagicMock(return_value=self)
                self.write_parquet = MagicMock()

            def columns(self):
                return ["text"]

        dataset = FakeDataset()
        fake_filesystem = MagicMock()
        fake_filesystem.get_file_info.return_value = FileInfo("/path/output_dir", FileType.NotFound)

        with patch(
            "data_juicer.core.ray_exporter.get_pyarrow_filesystem",
            return_value=(fake_filesystem, "/path/output_dir"),
        ) as mock_get_filesystem:
            exporter = RayExporter(
                "hdfs://cluster/path/output_dir",
                export_type="parquet",
                filesystem="pyarrow",
            )
            exporter.export(dataset)

        mock_get_filesystem.assert_called_once_with(
            "hdfs://cluster/path/output_dir",
            filesystem="pyarrow",
            storage_options=None,
        )
        dataset.write_parquet.assert_called_once()
        args, kwargs = dataset.write_parquet.call_args
        self.assertEqual(args[0], "/path/output_dir")
        self.assertIs(kwargs["filesystem"], fake_filesystem)
        self.assertNotIn("mode", kwargs)
        fake_filesystem.get_file_info.assert_called_once_with("/path/output_dir")

    def test_hdfs_mode_resolution_does_not_depend_on_ray_savemode_module(self):
        real_import = builtins.__import__

        def fail_savemode_import(name, *args, **kwargs):
            if name == "ray.data._internal.savemode":
                raise ModuleNotFoundError("No module named 'ray.data._internal.savemode'")
            return real_import(name, *args, **kwargs)

        fake_filesystem = MagicMock()
        fake_filesystem.get_file_info.return_value = FileInfo("/path/output_dir", FileType.NotFound)

        with (
            patch("builtins.__import__", side_effect=fail_savemode_import),
            patch(
                "data_juicer.core.ray_exporter.get_pyarrow_filesystem",
                return_value=(fake_filesystem, "/path/output_dir"),
            ),
        ):
            RayExporter(
                "hdfs://cluster/path/output_dir",
                export_type="parquet",
                filesystem="pyarrow",
                mode="error_if_exists",
            )

    def test_hdfs_jsonl_append_maps_mode_and_warns(self):
        class FakeDataset:
            def __init__(self):
                self.write_datasink = MagicMock()

            def columns(self):
                return ["text"]

        dataset = FakeDataset()
        fake_filesystem = LocalFileSystem()

        with (
            patch(
                "data_juicer.core.ray_exporter.get_pyarrow_filesystem",
                return_value=(fake_filesystem, "/path/output_jsonl_dir"),
            ),
            patch("data_juicer.core.ray_exporter.logger.warning") as mock_warning,
        ):
            exporter = RayExporter(
                "hdfs://cluster/path/output_jsonl_dir",
                export_type="jsonl",
                filesystem="pyarrow",
                mode="append",
            )
            exporter.export(dataset)

        dataset.write_datasink.assert_called_once()
        datasink = dataset.write_datasink.call_args.args[0]
        self.assertEqual(datasink.path, "/path/output_jsonl_dir")
        if getattr(datasink, "mode", None) is not None:
            self.assertEqual(datasink.mode.value, "append")
        mock_warning.assert_called_once()

    def test_hdfs_overwrite_deletes_existing_directory_before_write(self):
        fake_filesystem = MagicMock()
        fake_filesystem.get_file_info.return_value = FileInfo("/path/output_dir", FileType.Directory)

        with patch(
            "data_juicer.core.ray_exporter.get_pyarrow_filesystem",
            return_value=(fake_filesystem, "/path/output_dir"),
        ):
            RayExporter(
                "hdfs://cluster/path/output_dir",
                export_type="parquet",
                mode="overwrite",
            )

        fake_filesystem.delete_dir.assert_called_once_with("/path/output_dir")

    def test_hdfs_jsonl_datasink_drops_mode_for_older_ray_file_datasink(self):
        class FakeDataset:
            def write_datasink(self, *args, **kwargs):
                pass

        with patch("data_juicer.core.ray_exporter._JsonlDatasink") as mock_datasink:
            RayExporter.write_jsonl_datasink(
                FakeDataset(),
                "/path/output_jsonl_dir",
                {
                    "filesystem": LocalFileSystem(),
                    "mode": "append",
                    "num_rows_per_file": 100,
                },
            )

        _, kwargs = mock_datasink.call_args
        self.assertNotIn("mode", kwargs)

    def test_write_others_maps_max_rows_per_file_for_older_ray_parquet_signature(self):
        class FakeDataset:
            def __init__(self):
                self.received_kwargs = None

            def write_parquet(self, path, *, min_rows_per_file=None, concurrency=None, **arrow_parquet_args):
                self.received_kwargs = {
                    "path": path,
                    "min_rows_per_file": min_rows_per_file,
                    "concurrency": concurrency,
                    "arrow_parquet_args": arrow_parquet_args,
                }

        dataset = FakeDataset()
        RayExporter.write_others(
            dataset,
            "/path/output_dir",
            export_format="parquet",
            export_extra_args={"max_rows_per_file": 1000, "concurrency": 8},
        )

        self.assertEqual(dataset.received_kwargs["min_rows_per_file"], 1000)
        self.assertEqual(dataset.received_kwargs["concurrency"], 8)
        self.assertNotIn("max_rows_per_file", dataset.received_kwargs["arrow_parquet_args"])

    def test_hdfs_export_rejects_unknown_mode(self):
        with self.assertRaisesRegex(ValueError, "export.mode"):
            RayExporter(
                "hdfs://cluster/path/output_dir",
                export_type="parquet",
                mode="bad-mode",
            )

    def test_hdfs_error_if_exists_rejects_existing_path_before_write(self):
        fake_filesystem = MagicMock()
        fake_filesystem.get_file_info.return_value = FileInfo("/path/output_dir", FileType.Directory)

        with (
            patch(
                "data_juicer.core.ray_exporter.get_pyarrow_filesystem",
                return_value=(fake_filesystem, "/path/output_dir"),
            ),
            self.assertRaisesRegex(FileExistsError, "already exists"),
        ):
            RayExporter(
                "hdfs://cluster/path/output_dir",
                export_type="parquet",
                mode="error_if_exists",
            )


class TestRayExporter(DataJuicerTestCaseBase):

    def setUp(self):
        """Set up test data"""
        super().setUp()

        import ray
        from data_juicer.core.data.ray_dataset import RayDataset

        cur_dir = osp.dirname(osp.abspath(__file__))
        self.tmp_dir = f'{cur_dir}/tmp/{self.__class__.__name__}/{self._testMethodName}'
        os.makedirs(self.tmp_dir, exist_ok=True)

        self.data = [
            {'text': 'hello', Fields.stats: {'score': 1}, HashKeys.hash: 'a1'},
            {'text': 'world', Fields.stats: {'score': 2}, HashKeys.hash: 'b2'},
            {'text': 'test', Fields.stats: {'score': 3}, HashKeys.hash: 'c3'}
        ]
        self.dataset = RayDataset(ray.data.from_items(self.data))

    def tearDown(self):
        """Clean up temporary outputs"""

        self.dataset = None
        if osp.exists(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)

        super().tearDown()

    def _pop_raw_data_keys(self, keys):
        res = copy.deepcopy(self.data)
        for d_i in res:
            for k in keys:
                d_i.pop(k, None)

        return res

    @TEST_TAG('ray')
    def test_json_not_keep_stats_and_hashes(self):
        import ray

        out_path = osp.join(self.tmp_dir, 'outdata.json')
        ray_exporter = RayExporter(
            out_path,
            keep_stats_in_res_ds=False,
            keep_hashes_in_res_ds=False)
        ray_exporter.export(self.dataset.data)

        ds = ray.data.read_json(out_path)
        data_list = ds.take_all()

        self.assertListOfDictEqual(data_list, self._pop_raw_data_keys([Fields.stats, HashKeys.hash]))

    @TEST_TAG('ray')
    def test_jsonl_keep_stats_and_hashes(self):
        import ray

        out_path = osp.join(self.tmp_dir, 'outdata.jsonl')
        ray_exporter = RayExporter(
            out_path,
            keep_stats_in_res_ds=True,
            keep_hashes_in_res_ds=True)
        ray_exporter.export(self.dataset.data)

        ds = ray.data.read_json(out_path)
        data_list = ds.take_all()

        self.assertListOfDictEqual(data_list, self.data)

    @TEST_TAG('ray')
    def test_parquet_keep_stats(self):
        import ray

        out_path = osp.join(self.tmp_dir, 'outdata.parquet')
        ray_exporter = RayExporter(
            out_path,
            keep_stats_in_res_ds=True,
            keep_hashes_in_res_ds=False)
        ray_exporter.export(self.dataset.data)

        ds = ray.data.read_parquet(out_path)
        data_list = ds.take_all()

        self.assertListEqual(data_list, self._pop_raw_data_keys([HashKeys.hash]))

    @TEST_TAG('ray')
    def test_lance_keep_hashes(self):
        import ray

        out_path = osp.join(self.tmp_dir, 'outdata.lance')
        ray_exporter = RayExporter(
            out_path,
            keep_stats_in_res_ds=False,
            keep_hashes_in_res_ds=True)
        ray_exporter.export(self.dataset.data)

        ds = ray.data.read_lance(out_path)
        data_list = ds.take_all()

        self.assertListOfDictEqual(data_list, self._pop_raw_data_keys([Fields.stats]))

    @TEST_TAG('ray')
    def test_webdataset_multi_images(self):
        import io
        from PIL import Image
        import ray
        from data_juicer.core.data.ray_dataset import RayDataset

        data_dir = osp.abspath(osp.join(osp.dirname(osp.realpath(__file__)), '..', 'ops', 'data'))
        img1_path = osp.join(data_dir, 'img1.png')
        img2_path = osp.join(data_dir, 'img2.jpg')
        img3_path = osp.join(data_dir, 'img3.jpg')

        data = [
            {
                'json': {
                    'text': 'hello',
                    'images': [img1_path, img2_path]
                    },
                'jpgs': load_images_byte([img1_path, img2_path])},
            {
                'json': {
                    'text': 'world',
                    'images': [img2_path, img3_path]
                    },
                'jpgs': load_images_byte([img2_path, img3_path])},
            {
                'json': {
                    'text': 'test',
                    'images': [img1_path, img2_path, img3_path]
                    },
                'jpgs': load_images_byte([img1_path, img2_path, img3_path])}
        ]
        dataset = RayDataset(ray.data.from_items(data))
        out_path = osp.join(self.tmp_dir, 'outdata.webdataset')
        ray_exporter = RayExporter(out_path)
        ray_exporter.export(dataset.data)

        ds = RayDataset.read_webdataset(out_path)
        res_list = ds.take_all()
        
        self.assertEqual(len(res_list), len(data))
        res_list.sort(key=lambda x: x['json']['text'])
        data.sort(key=lambda x: x['json']['text'])

        for i in range(len(data)):
            self.assertDictEqual(res_list[i]['json'], data[i]['json'])
            self.assertEqual(
                res_list[i]['jpgs'],
                [Image.open(io.BytesIO(v)) for v in data[i]['jpgs']]
            )

    @TEST_TAG('ray')
    def test_webdataset_multi_videos_frames_bytes(self):
        import io
        from PIL import Image
        import ray
        from data_juicer.core.data.ray_dataset import RayDataset

        data_dir = osp.abspath(osp.join(osp.dirname(osp.realpath(__file__)), '..', 'ops', 'data'))
        img1_path = osp.join(data_dir, 'img1.png')
        img2_path = osp.join(data_dir, 'img2.jpg')
        img3_path = osp.join(data_dir, 'img3.jpg')

        data = [
            {
                'json': {
                    'text': 'hello',
                    'videos': ['video1.mp4', 'video2.mp4']
                    },
                'mp4s': [
                    load_images_byte([img1_path]),  # as video1 frames bytes
                    load_images_byte([img1_path, img2_path])   # as video2 frames path
                    ]
            },
            {
                'json': {
                    'text': 'world',
                    'videos': ['video1.mp4']
                    },
                'mp4s': [
                    load_images_byte([img2_path, img3_path])  # as video1 frames
                    ]
            }
        ]
        dataset = RayDataset(ray.data.from_items(data))
        out_path = osp.join(self.tmp_dir, 'outdata.webdataset')
        ray_exporter = RayExporter(out_path, export_type='webdataset')
        ray_exporter.export(dataset.data)

        ds = RayDataset.read_webdataset(out_path)
        res_list = ds.take_all()
        
        self.assertEqual(len(res_list), len(data))
        res_list.sort(key=lambda x: x['json']['text'])
        data.sort(key=lambda x: x['json']['text'])
        
        for i in range(len(data)):
            if len(data[i]['mp4s']) > 1:
                tgt_mp4s = [[Image.open(io.BytesIO(f_i)) for f_i in v_i] for v_i in data[i]['mp4s']]
            else:
                tgt_mp4s = [Image.open(io.BytesIO(f_i)) for f_i in data[i]['mp4s'][0]]
            self.assertDictEqual(res_list[i]['json'], data[i]['json'])
            self.assertEqual(res_list[i]['mp4s'], tgt_mp4s)

    @TEST_TAG('ray')
    def test_webdataset_multi_videos_frames_path(self):
        import io
        from PIL import Image
        import ray
        from data_juicer.core.data.ray_dataset import RayDataset

        data_dir = osp.abspath(osp.join(osp.dirname(osp.realpath(__file__)), '..', 'ops', 'data'))
        img1_path = osp.join(data_dir, 'img8.jpg')
        img2_path = osp.join(data_dir, 'img9.jpg')
        img3_path = osp.join(data_dir, 'img10.jpg')

        data = [
            {
                'json': {
                    'text': 'hello',
                    'videos': ['video1.mp4', 'video2.mp4']
                    },
                'mp4s': [
                    [img1_path],  # as video1 frames path
                    [img1_path, img2_path]   # as video2 frames path
                    ]
            },
            {
                'json': {
                    'text': 'world',
                    'videos': ['video1.mp4']
                    },
                'mp4s': [
                    [img2_path, img3_path]  # as video1 frames path
                    ]
            }
        ]
        dataset = RayDataset(ray.data.from_items(data))
        out_path = osp.join(self.tmp_dir, 'outdata.webdataset')
        ray_exporter = RayExporter(out_path, export_type='webdataset')
        ray_exporter.export(dataset.data)

        ds = RayDataset.read_webdataset(out_path)
        res_list = ds.take_all()
        
        self.assertEqual(len(res_list), len(data))
        res_list.sort(key=lambda x: x['json']['text'])
        data.sort(key=lambda x: x['json']['text'])
        
        for i in range(len(data)):
            if len(data[i]['mp4s']) > 1:
                tgt_mp4s = [[Image.open(f_i, formats=['jpeg']) for f_i in v_i] for v_i in data[i]['mp4s']]
            else:
                tgt_mp4s = [Image.open(f_i, formats=['jpeg']) for f_i in data[i]['mp4s'][0]]
            self.assertDictEqual(res_list[i]['json'], data[i]['json'])
            self.assertEqual(res_list[i]['mp4s'], tgt_mp4s)

    @TEST_TAG('ray')
    def test_webdataset_multi_audios_path(self):
        import ray
        from data_juicer.core.data.ray_dataset import RayDataset
        from data_juicer.utils.mm_utils import load_audio

        data_dir = osp.abspath(osp.join(osp.dirname(osp.realpath(__file__)), '..', 'ops', 'data'))
        audio1_path = osp.join(data_dir, 'audio1.wav')
        audio2_path = osp.join(data_dir, 'audio2.wav')
        audio3_path = osp.join(data_dir, 'audio3.ogg')

        data = [
            {
                'json': {
                    'text': 'hello',
                    },
                'mp3s': [audio1_path]
            },
            {
                'json': {
                    'text': 'world',
                    },
                'mp3s': [audio2_path, audio3_path]
            }
        ]
        dataset = RayDataset(ray.data.from_items(data))
        out_path = osp.join(self.tmp_dir, 'outdata.webdataset')
        ray_exporter = RayExporter(out_path, export_type='webdataset')
        ray_exporter.export(dataset.data)

        ds = RayDataset.read_webdataset(out_path)
        res_list = ds.take_all()
        
        self.assertEqual(len(res_list), len(data))

        res_list.sort(key=lambda x: x['json']['text'])
        data.sort(key=lambda x: x['json']['text'])
        
        for i in range(len(data)):
            if len(data[i]['mp3s']) <= 1:
                mp3s_list = [res_list[i]['mp3s']]
            else:
                mp3s_list = res_list[i]['mp3s']

            tgt_mp3s = [load_audio(f_i) for f_i in data[i]['mp3s']]
            
            self.assertDictEqual(res_list[i]['json'], data[i]['json'])

            for j in range(len(mp3s_list)):
                arr, sampling_rate = mp3s_list[j]
                tgt_arr, tgt_sampling_rate = tgt_mp3s[j]
                import numpy as np
                np.testing.assert_array_equal(arr, tgt_arr)
                self.assertEqual(sampling_rate, tgt_sampling_rate)


if __name__ == '__main__':
    unittest.main()
