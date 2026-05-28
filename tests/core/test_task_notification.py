import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from data_juicer.core.task_notification import (
    AdcLarkMessageNotificationHook,
    RuntimeStatsCollector,
    TaskNotificationManager,
    TaskProgressSnapshot,
    parse_notification_interval_seconds,
)


class FakeHttpClient:
    requests = []

    def __init__(
        self,
        endpoint,
        method="POST",
        headers=None,
        timeout=30.0,
        retry_attempts=0,
        retry_status_codes=None,
        retry_on_timeout=True,
    ):
        self.endpoint = endpoint
        self.method = method
        self.headers = headers or {}
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_status_codes = retry_status_codes
        self.retry_on_timeout = retry_on_timeout

    def request(self, *, params=None, json_body=None):
        self.__class__.requests.append({
            "endpoint": self.endpoint,
            "method": self.method,
            "headers": self.headers,
            "timeout": self.timeout,
            "retry_attempts": self.retry_attempts,
            "retry_status_codes": self.retry_status_codes,
            "retry_on_timeout": self.retry_on_timeout,
            "params": params,
            "json_body": json_body,
        })
        return {
            "ok": True,
            "status_code": 200,
            "data": {"code": 0, "data": {}},
            "text": None,
            "error": None,
        }


class FailingHttpClient(FakeHttpClient):
    def request(self, *, params=None, json_body=None):
        super().request(params=params, json_body=json_body)
        return {
            "ok": False,
            "status_code": 500,
            "data": None,
            "text": "failed",
            "error": {"message": "server failed"},
        }


class TaskNotificationTest(unittest.TestCase):
    def setUp(self):
        FakeHttpClient.requests = []
        FailingHttpClient.requests = []

    def _hook_cfg(self, **overrides):
        cfg = {
            "type": "adc_lark_message",
            "enabled": True,
            "interval": "30s",
            "on_success": True,
            "on_failure": True,
            "fail_on_error": False,
            "ctx": {
                "userAccount": "submitter",
                "apiBase": "https://ai-data-center.bytedance.net/api",
                "x-tt-env": "ppe_sirius3",
                "x-use-ppe": "1",
            },
            "template_id": "template-id",
            "custom_fields": {
                "biz": "ecom_video",
                "ray_ui_url": "https://ray.example.com",
                "driver_log_url": "https://log.example.com",
                "output_url": "https://output.example.com",
            },
            "custom_stats": [
                {"key": "dedup.duplicate_rows", "label": "duplicate rows"},
            ],
        }
        cfg.update(overrides)
        return cfg

    def _snapshot(self, **overrides):
        data = {
            "job_id": "job-1",
            "project_name": "project",
            "executor_type": "ray",
            "status": "running",
            "phase": "process",
            "start_time": 100.0,
            "elapsed_seconds": 2.5,
            "export_path": "hdfs://cluster/output",
            "output_rows": None,
            "output_bytes": None,
            "output_files": None,
            "custom_stats": {"dedup.duplicate_rows": 3},
            "error_summary": None,
        }
        data.update(overrides)
        return TaskProgressSnapshot(**data)

    def test_parse_notification_interval_seconds(self):
        self.assertEqual(parse_notification_interval_seconds("30s"), 30)
        self.assertEqual(parse_notification_interval_seconds("10min"), 600)
        self.assertEqual(parse_notification_interval_seconds("1h"), 3600)
        self.assertIsNone(parse_notification_interval_seconds(None))
        for value in ("0s", "abc", "10", "-1h"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "notification_hooks.*interval"):
                    parse_notification_interval_seconds(value)

    @patch("data_juicer.core.task_notification.HttpClient", FakeHttpClient)
    def test_adc_lark_message_sends_template_card_to_user(self):
        hook = AdcLarkMessageNotificationHook(self._hook_cfg())

        hook.send(self._snapshot(status="success", phase="finished"), event="success")

        self.assertEqual(len(FakeHttpClient.requests), 1)
        request = FakeHttpClient.requests[0]
        self.assertEqual(
            request["endpoint"],
            "https://ai-data-center.bytedance.net/api/openapi/lark/message/template-card/send-to-user",
        )
        self.assertEqual(request["headers"], {
            "Content-Type": "application/json",
            "User-Account": "submitter",
            "x-tt-env": "ppe_sirius3",
            "x-use-ppe": "1",
        })
        self.assertEqual(request["json_body"]["userEmailOrAccount"], "submitter")
        self.assertEqual(request["json_body"]["templateId"], "template-id")
        variables = request["json_body"]["templateVariable"]
        self.assertEqual(variables["status"], "success")
        self.assertEqual(variables["phase"], "finished")
        self.assertEqual(variables["user"], "submitter")
        self.assertEqual(variables["jobId"], "job-1")
        self.assertEqual(variables["job_id"], "job-1")
        self.assertEqual(variables["projectName"], "project")
        self.assertEqual(variables["project_name"], "project")
        self.assertEqual(variables["executorType"], "ray")
        self.assertEqual(variables["executor_type"], "ray")
        self.assertEqual(variables["exportPath"], "hdfs://cluster/output")
        self.assertEqual(variables["export_path"], "hdfs://cluster/output")
        self.assertEqual(variables["exportPathText"], "hdfs://cluster/output")
        self.assertEqual(variables["export_path_text"], "hdfs://cluster/output")
        self.assertEqual(variables["elapsedText"], "2.5s")
        self.assertEqual(variables["elapsed_text"], "2.5s")
        self.assertEqual(variables["outputFilesText"], "unknown")
        self.assertEqual(variables["output_files_text"], "unknown")
        self.assertEqual(variables["errorSummaryText"], "无")
        self.assertEqual(variables["error_summary_text"], "无")
        self.assertEqual(variables["statusText"], "成功")
        self.assertEqual(variables["status_text"], "成功")
        self.assertEqual(variables["phaseProgress"], "load ✓ -> process ✓ -> export ✓ -> finished ✓")
        self.assertEqual(variables["phase_progress"], "load ✓ -> process ✓ -> export ✓ -> finished ✓")
        self.assertEqual(variables["customFields"]["biz"], "ecom_video")
        self.assertEqual(variables["biz"], "ecom_video")
        self.assertEqual(variables["rayUiUrl"], {
            "url": "https://ray.example.com",
            "pc_url": "https://ray.example.com",
            "android_url": "https://ray.example.com",
            "ios_url": "https://ray.example.com",
        })
        self.assertEqual(variables["ray_ui_url"], variables["rayUiUrl"])
        self.assertEqual(variables["rayUiUrlText"], "https://ray.example.com")
        self.assertEqual(variables["ray_ui_url_text"], "https://ray.example.com")
        self.assertEqual(variables["driverLogUrl"], {
            "url": "https://log.example.com",
            "pc_url": "https://log.example.com",
            "android_url": "https://log.example.com",
            "ios_url": "https://log.example.com",
        })
        self.assertEqual(variables["driver_log_url"], variables["driverLogUrl"])
        self.assertEqual(variables["driverLogUrlText"], "https://log.example.com")
        self.assertEqual(variables["driver_log_url_text"], "https://log.example.com")
        self.assertEqual(variables["outputUrl"], {
            "url": "https://output.example.com",
            "pc_url": "https://output.example.com",
            "android_url": "https://output.example.com",
            "ios_url": "https://output.example.com",
        })
        self.assertEqual(variables["output_url"], variables["outputUrl"])
        self.assertEqual(variables["outputUrlText"], "https://output.example.com")
        self.assertEqual(variables["output_url_text"], "https://output.example.com")
        self.assertEqual(variables["customStatsMap"], {"dedup.duplicate_rows": 3})
        self.assertEqual(variables["dedup_duplicate_rows"], 3)
        self.assertEqual(variables["dedupDuplicateRows"], 3)
        self.assertEqual(variables["dedupDuplicateRowsText"], "3")
        self.assertEqual(variables["dedup_duplicate_rows_text"], "3")
        self.assertEqual(
            variables["customStats"],
            [{"key": "dedup.duplicate_rows", "label": "duplicate rows", "value": 3}],
        )

    @patch("data_juicer.core.task_notification.HttpClient", FailingHttpClient)
    def test_manager_suppresses_heartbeat_errors_but_terminal_can_fail(self):
        cfg = SimpleNamespace(
            notification_hooks=[self._hook_cfg(fail_on_error=True)],
            job_id="job-1",
            project_name="project",
            executor_type="ray",
            export_path="hdfs://cluster/output",
            work_dir="/unused",
        )
        manager = TaskNotificationManager(cfg, stats_collector=RuntimeStatsCollector())

        manager.send_heartbeat()

        with self.assertRaisesRegex(ValueError, "ADC lark message request failed"):
            manager.finish(success=True)

    @patch("data_juicer.core.task_notification.snapshot_task_kv")
    @patch("data_juicer.core.task_notification.incr_task_kv")
    def test_runtime_stats_collector_uses_task_actor_snapshot_only(self, incr_mock, snapshot_mock):
        snapshot_mock.return_value = {
            "dedup.eligible_rows": 5,
            "dedup.duplicate_rows": 2,
        }
        collector = RuntimeStatsCollector()

        collector.increment("dedup.duplicate_rows", 2)
        snapshot = collector.snapshot()

        incr_mock.assert_called_once_with("dedup.duplicate_rows", 2, namespace="runtime_stats", wait=False)
        snapshot_mock.assert_called_once_with(namespace="runtime_stats")
        self.assertEqual(snapshot["dedup.duplicate_rows"], 2)

    @patch("data_juicer.core.task_notification.HttpClient", FakeHttpClient)
    def test_manager_writes_snapshot_file_with_export_summary_and_custom_stats(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg = SimpleNamespace(
                notification_hooks=[self._hook_cfg(interval=None)],
                job_id="job-1",
                project_name="project",
                executor_type="ray",
                export_path="hdfs://cluster/output",
                work_dir=tmp_dir,
            )
            collector = RuntimeStatsCollector()
            collector.snapshot = MagicMock(return_value={"dedup.duplicate_rows": 4, "other": 9})
            manager = TaskNotificationManager(cfg, stats_collector=collector)
            manager.update_phase("export")

            manager.finish(
                success=True,
                export_summary={
                    "output_rows": 4,
                    "output_files": 2,
                    "output_bytes": 128,
                },
            )

            with open(os.path.join(tmp_dir, "notification_snapshot.json"), "r", encoding="utf-8") as file:
                snapshot = json.load(file)
            self.assertEqual(snapshot["status"], "success")
            self.assertEqual(snapshot["phase"], "finished")
            self.assertEqual(snapshot["output_rows"], 4)
            self.assertEqual(snapshot["custom_stats"], {"dedup.duplicate_rows": 4})

    def test_manager_snapshot_uses_live_export_summary_provider(self):
        cfg = SimpleNamespace(
            notification_hooks=[self._hook_cfg(interval="30s")],
            job_id="job-1",
            project_name="project",
            executor_type="ray",
            export_path="hdfs://cluster/output",
            work_dir="/unused",
        )
        collector = RuntimeStatsCollector()
        collector.snapshot = MagicMock(return_value={})
        manager = TaskNotificationManager(cfg, stats_collector=collector)
        manager.set_export_summary_provider(
            lambda: {
                "partial": True,
                "output_rows": 7,
                "output_files": 2,
                "output_bytes": 128,
            }
        )

        snapshot = manager.build_snapshot()

        self.assertEqual(snapshot.output_rows, 7)
        self.assertEqual(snapshot.output_files, 2)
        self.assertEqual(snapshot.output_bytes, 128)

    @patch("data_juicer.core.task_notification.HttpClient", FakeHttpClient)
    def test_manager_defaults_missing_configured_custom_stats_to_zero(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg = SimpleNamespace(
                notification_hooks=[self._hook_cfg(interval=None)],
                job_id="job-1",
                project_name="project",
                executor_type="ray",
                export_path="hdfs://cluster/output",
                work_dir=tmp_dir,
            )
            collector = RuntimeStatsCollector()
            collector.snapshot = MagicMock(return_value={
                "dedup.eligible_rows": 160,
                "dedup.unique_rows": 160,
            })
            manager = TaskNotificationManager(cfg, stats_collector=collector)

            manager.finish(success=True)

            with open(os.path.join(tmp_dir, "notification_snapshot.json"), "r", encoding="utf-8") as file:
                snapshot = json.load(file)
            self.assertEqual(snapshot["custom_stats"], {"dedup.duplicate_rows": 0})
            variables = FakeHttpClient.requests[0]["json_body"]["templateVariable"]
            self.assertEqual(variables["customStatsMap"], {"dedup.duplicate_rows": 0})
            self.assertEqual(variables["dedup_duplicate_rows"], 0)
            self.assertEqual(variables["dedup_duplicate_rows_text"], "0")
            self.assertEqual(
                variables["customStats"],
                [{"key": "dedup.duplicate_rows", "label": "duplicate rows", "value": 0}],
            )

    @patch("data_juicer.core.task_notification.HttpClient", FakeHttpClient)
    def test_manager_computes_ratio_custom_stats(self):
        cfg = SimpleNamespace(
            notification_hooks=[
                self._hook_cfg(
                    interval=None,
                    custom_stats=[
                        {"key": "rpc.video_url_rpc_mapper.failed_count", "label": "RPC 失败次数"},
                        {
                            "key": "rpc.video_url_rpc_mapper.failure_rate",
                            "label": "RPC 失败率",
                            "type": "ratio",
                            "numerator": "rpc.video_url_rpc_mapper.failed_count",
                            "denominator": "rpc.video_url_rpc_mapper.total_count",
                        },
                    ],
                )
            ],
            job_id="job-1",
            project_name="project",
            executor_type="ray",
            export_path="hdfs://cluster/output",
            work_dir="/unused",
        )
        collector = RuntimeStatsCollector()
        collector.snapshot = MagicMock(
            return_value={
                "rpc.video_url_rpc_mapper.total_count": 20,
                "rpc.video_url_rpc_mapper.failed_count": 3,
            }
        )
        manager = TaskNotificationManager(cfg, stats_collector=collector)

        manager.finish(success=True)

        variables = FakeHttpClient.requests[0]["json_body"]["templateVariable"]
        self.assertEqual(variables["customStatsMap"]["rpc.video_url_rpc_mapper.failed_count"], 3)
        self.assertEqual(variables["customStatsMap"]["rpc.video_url_rpc_mapper.failure_rate"], "15.00%")
        self.assertEqual(variables["rpc_video_url_rpc_mapper_failure_rate"], "15.00%")
        self.assertEqual(variables["rpc_video_url_rpc_mapper_failure_rate_text"], "15.00%")
        self.assertEqual(
            variables["customStats"],
            [
                {"key": "rpc.video_url_rpc_mapper.failed_count", "label": "RPC 失败次数", "value": 3},
                {
                    "key": "rpc.video_url_rpc_mapper.failure_rate",
                    "label": "RPC 失败率",
                    "value": "15.00%",
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()
