import os
import sys
import types
import unittest
from types import SimpleNamespace
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
        self._original_ray_nodes_info = ray_utils._RAY_NODES_INFO
        ray_utils._RAY_NODES_INFO = None
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
                "PYTHONPATH",
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
        ray_utils._RAY_NODES_INFO = self._original_ray_nodes_info

    @patch("data_juicer.utils.ray_utils.shutil.which")
    def test_get_hadoop_command_falls_back_to_path_lookup(self, mock_which):
        os.environ.pop("HADOOP_HOME", None)
        mock_which.return_value = "/usr/bin/hadoop"

        self.assertEqual(ray_utils._get_hadoop_command(), "/usr/bin/hadoop")
        mock_which.assert_called_once_with("hadoop")

    @patch("data_juicer.utils.ray_utils.shutil.which")
    @patch("data_juicer.utils.ray_utils.os.access")
    @patch("data_juicer.utils.ray_utils.os.path.exists")
    def test_get_hadoop_command_prefers_hadoop_home_bin(self, mock_exists, mock_access, mock_which):
        os.environ["HADOOP_HOME"] = "/opt/hadoop"
        mock_exists.return_value = True
        mock_access.return_value = True

        self.assertEqual(ray_utils._get_hadoop_command(), "/opt/hadoop/bin/hadoop")
        mock_which.assert_not_called()

    @patch("data_juicer.utils.ray_utils._get_hadoop_command", return_value=None)
    def test_get_hadoop_classpath_returns_none_without_hadoop_command(self, mock_get_hadoop_command):
        self.assertIsNone(ray_utils._get_hadoop_classpath())

    @patch("data_juicer.utils.ray_utils.subprocess.check_output")
    @patch("data_juicer.utils.ray_utils._get_hadoop_command", return_value="/usr/bin/hadoop")
    def test_get_hadoop_classpath_reads_hadoop_glob(self, mock_get_hadoop_command, mock_check_output):
        mock_check_output.return_value = " /a:/b \n"

        self.assertEqual(ray_utils._get_hadoop_classpath(), "/a:/b")
        mock_check_output.assert_called_once_with(
            ["/usr/bin/hadoop", "classpath", "--glob"],
            stderr=ray_utils.subprocess.DEVNULL,
            text=True,
        )

    @patch("data_juicer.utils.ray_utils.subprocess.check_output", side_effect=RuntimeError("boom"))
    @patch("data_juicer.utils.ray_utils._get_hadoop_command", return_value="/usr/bin/hadoop")
    def test_get_hadoop_classpath_returns_none_on_command_error(self, mock_get_hadoop_command, mock_check_output):
        self.assertIsNone(ray_utils._get_hadoop_classpath())

    def test_ensure_hadoop_classpath_env_keeps_existing_classpath(self):
        os.environ["CLASSPATH"] = "/already/set"

        ray_utils.ensure_hadoop_classpath_env()

        self.assertEqual(os.environ["CLASSPATH"], "/already/set")

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

    def test_merge_path_entries_preserves_order_and_deduplicates(self):
        self.assertEqual(
            ray_utils._merge_path_entries("/repo", f"/repo{os.pathsep}/opt/extra", "", "/opt/extra"),
            f"/repo{os.pathsep}/opt/extra",
        )

    def test_is_ray_mode_reads_ray_job_env(self):
        os.environ[RAY_JOB_ENV_VAR] = "1"
        self.assertTrue(ray_utils.is_ray_mode())

        os.environ[RAY_JOB_ENV_VAR] = "0"
        self.assertFalse(ray_utils.is_ray_mode())

    @patch("data_juicer.utils.ray_utils._get_hadoop_classpath", return_value=None)
    def test_initialize_ray_returns_when_already_initialized_without_force(self, mock_get_hadoop_classpath):
        ray = MagicMock()
        ray.is_initialized.return_value = True

        with patch.object(ray_utils, "ray", ray):
            ray_utils.initialize_ray(Namespace(ray_address="auto", custom_operator_paths=None))

        ray.init.assert_not_called()

    @patch("data_juicer.utils.ray_utils._get_hadoop_classpath")
    @patch("data_juicer.utils.ray_utils._get_data_juicer_import_root")
    def test_initialize_ray_defaults_cfg_and_forwards_special_token(
        self, mock_get_data_juicer_import_root, mock_get_hadoop_classpath
    ):
        mock_get_hadoop_classpath.return_value = None
        mock_get_data_juicer_import_root.return_value = "/repo"
        os.environ["_DJ_SPECIAL_TOKEN_TEST"] = "secret"
        ray = MagicMock()
        ray.is_initialized.return_value = False

        try:
            with patch.object(ray_utils, "ray", ray):
                ray_utils.initialize_ray()
        finally:
            os.environ.pop("_DJ_SPECIAL_TOKEN_TEST", None)

        args, kwargs = ray.init.call_args
        self.assertEqual(args[0], "auto")
        self.assertEqual(kwargs["runtime_env"]["env_vars"]["_DJ_SPECIAL_TOKEN_TEST"], "secret")

    @patch("data_juicer.utils.ray_utils._get_hadoop_classpath")
    @patch("data_juicer.utils.ray_utils._get_data_juicer_import_root")
    def test_initialize_ray_forwards_data_juicer_root_in_pythonpath(
        self, mock_get_data_juicer_import_root, mock_get_hadoop_classpath
    ):
        mock_get_hadoop_classpath.return_value = None
        mock_get_data_juicer_import_root.return_value = "/opt/tiger/data-juicer"
        os.environ["PYTHONPATH"] = f"/opt/tiger/data-juicer{os.pathsep}/opt/extra"
        ray = MagicMock()
        ray.is_initialized.return_value = False

        with patch.object(ray_utils, "ray", ray):
            ray_utils.initialize_ray(Namespace(ray_address="auto", custom_operator_paths=None))

        _, kwargs = ray.init.call_args
        env_vars = kwargs["runtime_env"]["env_vars"]
        self.assertEqual(env_vars["PYTHONPATH"], f"/opt/tiger/data-juicer{os.pathsep}/opt/extra")

    @patch("data_juicer.utils.ray_utils.initialize_ray")
    def test_check_and_initialize_ray_only_runs_in_ray_mode(self, mock_initialize_ray):
        os.environ[RAY_JOB_ENV_VAR] = "0"
        self.assertFalse(ray_utils.check_and_initialize_ray())
        mock_initialize_ray.assert_not_called()

        os.environ[RAY_JOB_ENV_VAR] = "1"
        self.assertTrue(ray_utils.check_and_initialize_ray())
        mock_initialize_ray.assert_called_once_with(None)

    @patch("data_juicer.utils.ray_utils.initialize_ray", side_effect=RuntimeError("boom"))
    def test_check_and_initialize_ray_returns_false_on_init_error(self, mock_initialize_ray):
        os.environ[RAY_JOB_ENV_VAR] = "1"

        self.assertFalse(ray_utils.check_and_initialize_ray())

    @patch("data_juicer.utils.ray_utils.subprocess.check_output")
    @patch("data_juicer.utils.ray_utils.psutil.cpu_count", return_value=8)
    @patch("data_juicer.utils.ray_utils.psutil.virtual_memory")
    def test_collect_node_info_parses_gpu_memory(self, mock_virtual_memory, mock_cpu_count, mock_check_output):
        mock_virtual_memory.return_value = SimpleNamespace(available=2 * 1024**2)
        mock_check_output.return_value = b"100, 200\n300, 400\n"

        self.assertEqual(
            ray_utils._collect_node_info(),
            {
                "free_memory": 2,
                "cpu_count": 8,
                "gpu_count": 2,
                "gpus_memory": [200, 400],
                "free_gpus_memory": [100, 300],
            },
        )

    @patch("data_juicer.utils.ray_utils.subprocess.check_output", side_effect=RuntimeError("no gpu"))
    @patch("data_juicer.utils.ray_utils.psutil.cpu_count", return_value=4)
    @patch("data_juicer.utils.ray_utils.psutil.virtual_memory")
    def test_collect_node_info_handles_missing_gpu(self, mock_virtual_memory, mock_cpu_count, mock_check_output):
        mock_virtual_memory.return_value = SimpleNamespace(available=3 * 1024**2)

        self.assertEqual(
            ray_utils._collect_node_info(),
            {
                "free_memory": 3,
                "cpu_count": 4,
                "gpu_count": 0,
                "gpus_memory": [],
                "free_gpus_memory": [],
            },
        )

    @patch("data_juicer.utils.ray_utils.initialize_ray")
    def test_get_ray_nodes_info_collects_alive_worker_nodes(self, mock_initialize_ray):
        class FakeRemoteFunction:
            def __init__(self, fn):
                self.fn = fn

            def options(self, scheduling_strategy):
                self.scheduling_strategy = scheduling_strategy
                return self

            def remote(self):
                return self.scheduling_strategy.node_id

        class FakeRay:
            def remote(self, fn):
                return FakeRemoteFunction(fn)

            def nodes(self):
                return [
                    {"Alive": True, "NodeManagerHostname": "worker-1", "NodeID": "node-1"},
                    {"Alive": False, "NodeManagerHostname": "worker-2", "NodeID": "node-2"},
                    {"Alive": True, "NodeManagerHostname": "head-1", "NodeID": "head-1"},
                ]

            def get(self, futures):
                self.futures = futures
                return [{"free_memory": 16, "free_gpus_memory": [1], "gpus_memory": [2]}]

        class FakeStrategy:
            def __init__(self, node_id, soft):
                self.node_id = node_id
                self.soft = soft

        ray_module = types.ModuleType("ray")
        ray_util_module = types.ModuleType("ray.util")
        ray_util_module.scheduling_strategies = SimpleNamespace(NodeAffinitySchedulingStrategy=FakeStrategy)
        fake_ray = FakeRay()

        with patch.object(ray_utils, "ray", fake_ray), patch.dict(
            sys.modules, {"ray": ray_module, "ray.util": ray_util_module}
        ):
            self.assertEqual(
                ray_utils.get_ray_nodes_info(),
                {"node-1": {"free_memory": 16, "free_gpus_memory": [1], "gpus_memory": [2]}},
            )

        self.assertEqual(fake_ray.futures, ["node-1"])
        mock_initialize_ray.assert_called_once_with(None)
        self.assertEqual(
            ray_utils.get_ray_nodes_info(),
            {"node-1": {"free_memory": 16, "free_gpus_memory": [1], "gpus_memory": [2]}},
        )

    def test_ray_cluster_resource_helpers(self):
        ray = MagicMock()
        ray.cluster_resources.return_value = {"CPU": 12, "GPU": 2}

        with patch.object(ray_utils, "ray", ray):
            self.assertEqual(ray_utils.ray_cpu_count(), 12)
            self.assertEqual(ray_utils.ray_gpu_count(), 2)

    @patch(
        "data_juicer.utils.ray_utils.get_ray_nodes_info",
        return_value={
            "node-1": {"free_memory": 10, "free_gpus_memory": [1, 2], "gpus_memory": [3, 4]},
            "node-2": {"free_memory": 20, "free_gpus_memory": [5], "gpus_memory": [6]},
        },
    )
    def test_ray_memory_helpers_flatten_node_info(self, mock_get_ray_nodes_info):
        self.assertEqual(ray_utils.ray_available_memories(), [10, 20])
        self.assertEqual(ray_utils.ray_available_gpu_memories(), [1, 2, 5])
        self.assertEqual(ray_utils.ray_gpu_memories(), [3, 4, 6])


if __name__ == "__main__":
    unittest.main()
