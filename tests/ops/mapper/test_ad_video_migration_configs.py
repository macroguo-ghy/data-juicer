import os
import unittest

from data_juicer.config import init_configs
from data_juicer.ops.load import load_ops


class AdVideoConfigLoadTest(unittest.TestCase):
    def test_ad_video_hdfs_parquet_configs_are_split_by_duration(self):
        config_dir = os.path.join(
            os.getcwd(),
            "demos",
            "bytedance",
            "ad_video_a_dragon",
            "configs",
        )
        expected = {
            "ad_video_short_hdfs_parquet.yaml": {
                "filter": "video_duration <= 60",
                "output_path": (
                    "hdfs://haruna/ad_base/addrd_core/addrd_stats/hdfs/ccu/"
                    "ad_video/20260117_full_video_short_dedup"
                ),
                "op_classes": [
                    "StatelessFieldFilter",
                    "JsonObjectMapper",
                    "FieldAssignMapper",
                    "VideoUrlRpcMapper",
                    "DownloadFileMapper",
                    "BytesExactDedupMapper",
                    "RayFieldDedupPipeline",
                    "StatelessFieldFilter",
                ],
                "has_download": True,
                "override_num_blocks": 1024,
                "op_num_cpus": [1, 1, 1, 1, 5, 5, 5, 5],
                "qps": 100,
                "download_timeout": 30,
                "avoid_write_fusion": False,
                "min_rows_per_file": 1000,
                "max_rows_per_file": 1500,
            },
            "ad_video_long_hdfs_parquet.yaml": {
                "filter": "video_duration > 60",
                "output_path": (
                    "hdfs://haruna/ad_base/addrd_core/addrd_stats/hdfs/ccu/ad_video/20260117_video_long"
                ),
                "op_classes": [
                    "GeneralFieldFilter",
                    "JsonObjectMapper",
                    "FieldAssignMapper",
                ],
                "has_download": False,
                "override_num_blocks": 2048,
                "op_num_cpus": [1, 1, 1],
                "avoid_write_fusion": True,
                "min_rows_per_file": 5000,
                "max_rows_per_file": 10000,
            },
        }

        for config_name, tuning in expected.items():
            with self.subTest(config_name=config_name):
                path = os.path.join(config_dir, config_name)
                cfg = init_configs(args=["--config", path], load_configs_only=True)
                ops = load_ops(cfg.process)

                self.assertEqual(cfg.executor_type, "ray")
                self.assertEqual(cfg.project_name, config_name.removesuffix("_hdfs_parquet.yaml").replace("_", "-"))
                dataset_config = cfg.dataset["configs"][0]
                self.assertEqual(dataset_config["source"], "hdfs")
                self.assertEqual(
                    dataset_config["path"],
                    "hdfs://haruna/home/byte_life_gen_ai/user/wangqianle/ad_raw/video_v1/20260117_sampled",
                )
                self.assertEqual(dataset_config["format"], "parquet")
                self.assertEqual(dataset_config["override_num_blocks"], tuning["override_num_blocks"])
                self.assertEqual(dataset_config["concurrency"], 512)
                self.assertEqual(dataset_config["num_cpus"], 0.5)

                self.assertEqual([op.__class__.__name__ for op in ops], tuning["op_classes"])
                self.assertEqual([op.num_proc for op in ops], [512] * len(ops))
                self.assertEqual([op.num_cpus for op in ops], tuning["op_num_cpus"])
                self.assertEqual(ops[0].filter_condition, tuning["filter"])
                self.assertEqual(ops[1].output_key, "extra")
                self.assertTrue(ops[1].include_all)
                self.assertEqual(ops[1].exclude_keys, {"p_date"})
                self.assertEqual(ops[2].assignments["id"]["template"], "vid-{image_uri}")
                self.assertEqual(ops[2].assignments["source"]["value"], "ad_video_raw_data")

                if tuning["has_download"]:
                    self.assertEqual(ops[3].vid_key, "image_uri")
                    self.assertEqual(ops[3].output_key, "urls")
                    self.assertEqual(ops[3].quality_preference, "720p")
                    self.assertEqual(ops[3].max_vids_per_request, 20)
                    self.assertEqual(ops[3].qps, tuning["qps"])
                    self.assertEqual(ops[4].download_field, "urls")
                    self.assertEqual(ops[4].save_field, "videos")
                    self.assertEqual(ops[4].timeout, tuning["download_timeout"])
                    self.assertEqual(ops[4].retry_times, 3)
                    self.assertEqual(ops[4].max_concurrent, 1)
                    self.assertEqual(ops[5].bytes_key, "videos")
                    self.assertEqual(ops[6].field_key, "md5")
                    self.assertEqual(ops[6].id_key, "id")
                    self.assertEqual(ops[7].filter_condition, "valid_video_count > 0")

                self.assertEqual(cfg.export["target"], "hdfs")
                self.assertEqual(cfg.export["type"], "parquet")
                self.assertEqual(cfg.export["mode"], "overwrite")
                self.assertEqual(cfg.export["path"], tuning["output_path"])
                self.assertEqual(
                    cfg.notification_hooks[0]["custom_fields"]["output_url"],
                    tuning["output_path"],
                )
                self.assertEqual(cfg.export["filesystem"], "pyarrow")
                extra_args = cfg.export["extra_args"]
                self.assertEqual(extra_args["concurrency"], 64)
                if tuning["avoid_write_fusion"]:
                    self.assertTrue(extra_args["avoid_write_fusion"])
                else:
                    self.assertNotIn("avoid_write_fusion", extra_args)
                self.assertEqual(extra_args["min_rows_per_file"], tuning["min_rows_per_file"])
                self.assertEqual(extra_args["max_rows_per_file"], tuning["max_rows_per_file"])
                schema_columns = [field["name"] for field in cfg.export["schema"]["fields"]]
                self.assertEqual(
                    schema_columns,
                    [
                        "id",
                        "urls",
                        "videos",
                        "md5",
                        "extra",
                        "source",
                        "texts",
                        "audios",
                        "images",
                        "has_audio_in_video",
                        "type",
                    ],
                )


if __name__ == "__main__":
    unittest.main()
