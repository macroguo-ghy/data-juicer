import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


MODULE_PATH = Path(__file__).resolve().parents[2] / "demos" / "bytedance" / "e2e_test" / "online_ray_job.py"
SPEC = importlib.util.spec_from_file_location("online_ray_job", MODULE_PATH)
online_ray_job = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(online_ray_job)


class OnlineRayJobTest(unittest.TestCase):
    def test_default_username_prefers_bytedcli_login_identity(self):
        completed = SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {
                    "data": {
                        "bytecloud_auth": {
                            "identity": {
                                "username": "guohongyu.7",
                                "email": "guohongyu.7@bytedance.com",
                            }
                        }
                    }
                }
            ),
        )

        with patch.object(online_ray_job.subprocess, "run", return_value=completed):
            with patch.dict(os.environ, {"USER": "bytedance"}, clear=False):
                self.assertEqual(online_ray_job.default_username(), "guohongyu.7")

    def test_default_username_falls_back_to_env_when_bytedcli_unavailable(self):
        with patch.object(
            online_ray_job.subprocess,
            "run",
            side_effect=online_ray_job.subprocess.TimeoutExpired("bytedcli", 5),
        ):
            with patch.dict(os.environ, {"BYTEDANCE_USERNAME": "env-user", "USER": "bytedance"}, clear=False):
                self.assertEqual(online_ray_job.default_username(), "env-user")

    def test_prepare_operator_yaml_replaces_tqs_placeholders_from_env(self):
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
            f.write(
                "project_name: tqs\n"
                "dataset:\n"
                "  configs:\n"
                "    - tqs_app_id: \"<YOUR_TQS_APP_ID>\"\n"
                "      tqs_app_key: \"<YOUR_TQS_APP_KEY>\"\n"
            )
            path = f.name

        args = SimpleNamespace(
            config=path,
            ark_api_key="",
            ark_api_key_env="ARK_API_KEY",
            allow_placeholder_api_key=False,
            tqs_app_id="",
            tqs_app_id_env="TQS_APP_ID",
            tqs_app_key="",
            tqs_app_key_env="TQS_APP_KEY",
            model="",
            job_id="job",
            work_dir_template="",
        )

        try:
            with patch.dict(os.environ, {"TQS_APP_ID": "app-id", "TQS_APP_KEY": "app-key"}, clear=False):
                yaml_text = online_ray_job.prepare_operator_yaml(args)
        finally:
            os.unlink(path)

        self.assertIn('tqs_app_id: "app-id"', yaml_text)
        self.assertIn('tqs_app_key: "app-key"', yaml_text)

    def test_prepare_operator_yaml_rejects_missing_tqs_placeholder_values(self):
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
            f.write('dataset:\n  configs:\n    - tqs_app_id: "<YOUR_TQS_APP_ID>"\n')
            path = f.name

        args = SimpleNamespace(
            config=path,
            ark_api_key="",
            ark_api_key_env="ARK_API_KEY",
            allow_placeholder_api_key=False,
            tqs_app_id="",
            tqs_app_id_env="TQS_APP_ID",
            tqs_app_key="",
            tqs_app_key_env="TQS_APP_KEY",
            model="",
            job_id="job",
            work_dir_template="",
        )

        try:
            with patch.dict(os.environ, {"TQS_APP_ID": "", "TQS_APP_KEY": ""}, clear=False):
                with self.assertRaises(SystemExit):
                    online_ray_job.prepare_operator_yaml(args)
        finally:
            os.unlink(path)

    def test_request_for_disk_redacts_tqs_credentials_in_operator_yaml(self):
        request = {
            "operator_yaml": (
                'api_key: "ark"\n'
                'tqs_app_id: "app-id"\n'
                'tqs_app_key: "app-key"\n'
                'project_name: "demo"\n'
            )
        }

        sanitized = online_ray_job.request_for_disk(request, save_sensitive_request=False)

        self.assertNotIn("ark", sanitized["operator_yaml"])
        self.assertNotIn("app-id", sanitized["operator_yaml"])
        self.assertNotIn("app-key", sanitized["operator_yaml"])
        self.assertEqual(sanitized["operator_yaml"].count('"<redacted>"'), 3)

    def test_call_rpc_dry_run_writes_sanitized_request(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            args = SimpleNamespace(dry_run=True, save_sensitive_request=False)
            request = {"operator_yaml": 'tqs_app_key: "app-key"\n'}

            response = online_ray_job.call_rpc(args, "LaunchMerlinFederalJob", request, Path(temp_dir))

            saved = json.loads((Path(temp_dir) / "LaunchMerlinFederalJob.request.json").read_text())
            self.assertEqual(response, {})
            self.assertNotIn("app-key", saved["operator_yaml"])


if __name__ == "__main__":
    unittest.main()
