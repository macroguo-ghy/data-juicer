import os
import unittest
from unittest.mock import MagicMock, patch

from jsonargparse import Namespace

from data_juicer.utils.constant import (
    METRICS_JOB_ID_ENV_VAR,
    METRICS_RAY_ADDRESS_ENV_VAR,
    RAY_JOB_ENV_VAR,
)
from data_juicer.utils import ray_utils


class RayUtilsTest(unittest.TestCase):
    def setUp(self):
        self._original_ray_job_env_value = os.environ.get(RAY_JOB_ENV_VAR)
        self._original_env_values = {
            key: os.environ.get(key)
            for key in [
                "CLASSPATH",
                "JAVA_HOME",
                "HADOOP_HOME",
                "HADOOP_CONF_DIR",
                "YARN_CONF_DIR",
                "HIVE_HOME",
                "HIVE_CONF_DIR",
                "ARROW_LIBHDFS_DIR",
                "HADOOP_COMMON_LIB_NATIVE_DIR",
                "LD_LIBRARY_PATH",
                METRICS_JOB_ID_ENV_VAR,
                METRICS_RAY_ADDRESS_ENV_VAR,
            ]
        }

    def tearDown(self):
        if self._original_ray_job_env_value is None:
            os.environ.pop(RAY_JOB_ENV_VAR, None)
        else:
            os.environ[RAY_JOB_ENV_VAR] = self._original_ray_job_env_value
        for key, value in self._original_env_values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    @patch("data_juicer.utils.ray_utils._get_hadoop_classpath")
    def test_ensure_hadoop_classpath_env_uses_hadoop_classpath_glob(self, mock_get_hadoop_classpath):
        os.environ.pop("CLASSPATH", None)
        os.environ["HIVE_CONF_DIR"] = "/opt/tiger/hive_deploy/conf"
        os.environ["HIVE_HOME"] = "/opt/tiger/hive_deploy"
        mock_get_hadoop_classpath.return_value = "/opt/tiger/yarn_deploy/hadoop/etc/hadoop:/hadoop/a.jar"

        ray_utils.ensure_hadoop_classpath_env()

        self.assertEqual(
            os.environ["CLASSPATH"],
            "/opt/tiger/yarn_deploy/hadoop/etc/hadoop:/hadoop/a.jar:"
            "/opt/tiger/hive_deploy/conf:/opt/tiger/hive_deploy/lib/*",
        )

    @patch("data_juicer.utils.ray_utils._get_hadoop_classpath")
    def test_initialize_ray_forwards_hadoop_hive_env_vars(self, mock_get_hadoop_classpath):
        mock_get_hadoop_classpath.return_value = "/hadoop/classpath"
        os.environ.pop("CLASSPATH", None)
        os.environ["HADOOP_HOME"] = "/opt/tiger/yarn_deploy/hadoop"
        os.environ["HADOOP_CONF_DIR"] = "/opt/tiger/yarn_deploy/hadoop/conf"
        os.environ["YARN_CONF_DIR"] = "/opt/tiger/yarn_deploy/hadoop/conf"
        os.environ["HIVE_HOME"] = "/opt/tiger/hive_deploy"
        os.environ["HIVE_CONF_DIR"] = "/opt/tiger/hive_deploy/conf"
        os.environ["ARROW_LIBHDFS_DIR"] = "/opt/tiger/yarn_deploy/hadoop/lib/native"
        ray = MagicMock()
        ray.is_initialized.return_value = False

        with patch.object(ray_utils, "ray", ray):
            ray_utils.initialize_ray(Namespace(ray_address="auto", custom_operator_paths=None))

        _, kwargs = ray.init.call_args
        env_vars = kwargs["runtime_env"]["env_vars"]
        self.assertEqual(env_vars["CLASSPATH"], "/hadoop/classpath:/opt/tiger/hive_deploy/conf:/opt/tiger/hive_deploy/lib/*")
        self.assertEqual(env_vars["HADOOP_CONF_DIR"], "/opt/tiger/yarn_deploy/hadoop/conf")
        self.assertEqual(env_vars["HIVE_CONF_DIR"], "/opt/tiger/hive_deploy/conf")
        self.assertEqual(env_vars["ARROW_LIBHDFS_DIR"], "/opt/tiger/yarn_deploy/hadoop/lib/native")
        self.assertNotIn("job_config", kwargs)

    @patch("data_juicer.utils.ray_utils._get_hadoop_classpath")
    def test_initialize_ray_forwards_metrics_context_env_vars(self, mock_get_hadoop_classpath):
        mock_get_hadoop_classpath.return_value = None
        os.environ[METRICS_JOB_ID_ENV_VAR] = "job-1"
        os.environ[METRICS_RAY_ADDRESS_ENV_VAR] = "ray://cluster"
        ray = MagicMock()
        ray.is_initialized.return_value = False

        with patch.object(ray_utils, "ray", ray):
            ray_utils.initialize_ray(Namespace(ray_address="ray://cluster", custom_operator_paths=None))

        _, kwargs = ray.init.call_args
        env_vars = kwargs["runtime_env"]["env_vars"]
        self.assertEqual(env_vars[METRICS_JOB_ID_ENV_VAR], "job-1")
        self.assertEqual(env_vars[METRICS_RAY_ADDRESS_ENV_VAR], "ray://cluster")


if __name__ == "__main__":
    unittest.main()
