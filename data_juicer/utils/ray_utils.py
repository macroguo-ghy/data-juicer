import os
import shutil
import subprocess
from pathlib import Path

import psutil
from loguru import logger

from data_juicer.utils.constant import (
    METRICS_JOB_ID_ENV_VAR,
    METRICS_RAY_ADDRESS_ENV_VAR,
    RAY_JOB_ENV_VAR,
    SPECIAL_TOKEN_ENV_PREFIX,
)
from data_juicer.utils.lazy_loader import LazyLoader
from data_juicer.utils.metrics_utils import set_metrics_context

ray = LazyLoader("ray")

_RAY_NODES_INFO = None

_HADOOP_HIVE_ENV_KEYS = [
    "CLASSPATH",
    "JAVA_HOME",
    "HADOOP_HOME",
    "HADOOP_CONF_DIR",
    "YARN_CONF_DIR",
    "HIVE_HOME",
    "HIVE_CONF_DIR",
    "ARROW_LIBHDFS_DIR",
    "HADOOP_COMMON_LIB_NATIVE_DIR",
    "LIBHDFS_OPTS",
    "LD_LIBRARY_PATH",
]
_METRICS_ENV_KEYS = [
    METRICS_JOB_ID_ENV_VAR,
    METRICS_RAY_ADDRESS_ENV_VAR,
]
_DEFAULT_LIBHDFS_LOGGER_OPTION = "-Dhadoop.root.logger=WARN,console"


def _get_data_juicer_import_root():
    return str(Path(__file__).resolve().parents[2])


def _merge_path_entries(*path_values):
    entries = []
    seen = set()
    for path_value in path_values:
        if not path_value:
            continue
        for path_entry in str(path_value).split(os.pathsep):
            if not path_entry or path_entry in seen:
                continue
            entries.append(path_entry)
            seen.add(path_entry)
    return os.pathsep.join(entries)


def _get_hadoop_command():
    hadoop_home = os.environ.get("HADOOP_HOME")
    if hadoop_home:
        hadoop_cmd = os.path.join(hadoop_home, "bin", "hadoop")
        if os.path.exists(hadoop_cmd) and os.access(hadoop_cmd, os.X_OK):
            return hadoop_cmd

    return shutil.which("hadoop")


def _get_hadoop_classpath():
    hadoop_cmd = _get_hadoop_command()
    if not hadoop_cmd:
        return None

    try:
        return subprocess.check_output(
            [hadoop_cmd, "classpath", "--glob"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception as err:
        logger.warning(f"Failed to get Hadoop classpath via `{hadoop_cmd} classpath --glob`: {err}")
        return None


def _ensure_libhdfs_warning_logging_env():
    libhdfs_opts = os.environ.get("LIBHDFS_OPTS", "").strip()
    if "hadoop.root.logger=" in libhdfs_opts:
        return

    if libhdfs_opts:
        os.environ["LIBHDFS_OPTS"] = f"{libhdfs_opts} {_DEFAULT_LIBHDFS_LOGGER_OPTION}"
    else:
        os.environ["LIBHDFS_OPTS"] = _DEFAULT_LIBHDFS_LOGGER_OPTION


def ensure_hadoop_classpath_env():
    _ensure_libhdfs_warning_logging_env()

    if os.environ.get("CLASSPATH"):
        return

    classpath_entries = []
    hadoop_classpath = _get_hadoop_classpath()
    if hadoop_classpath:
        classpath_entries.append(hadoop_classpath)

    hive_conf_dir = os.environ.get("HIVE_CONF_DIR")
    if hive_conf_dir:
        classpath_entries.append(hive_conf_dir)

    hive_home = os.environ.get("HIVE_HOME")
    if hive_home:
        classpath_entries.append(os.path.join(hive_home, "lib", "*"))

    if classpath_entries:
        os.environ["CLASSPATH"] = ":".join(classpath_entries)
        logger.info("Set CLASSPATH from Hadoop and Hive runtime paths.")


def is_ray_mode():
    if int(os.environ.get(RAY_JOB_ENV_VAR, "0")):
        return True

    return False


def initialize_ray(cfg=None, force=False):
    ensure_hadoop_classpath_env()

    if ray.is_initialized() and not force:
        return

    if cfg is None:
        ray_address = "auto"
        logger.warning("No ray config provided, using default ray address 'auto'.")
    else:
        ray_address = cfg.ray_address
        set_metrics_context(
            job_id=getattr(cfg, "job_id", None),
            ray_address=ray_address,
        )

    # collect ray envs
    env_vars = {RAY_JOB_ENV_VAR: os.environ.get(RAY_JOB_ENV_VAR, "0")}
    for k in [*_HADOOP_HIVE_ENV_KEYS, *_METRICS_ENV_KEYS]:
        if os.environ.get(k):
            env_vars[k] = os.environ[k]
    for k, v in dict(os.environ).items():
        if k.startswith(SPECIAL_TOKEN_ENV_PREFIX):
            env_vars.update({k: v})
    pythonpath = _merge_path_entries(_get_data_juicer_import_root(), os.environ.get("PYTHONPATH"))
    if pythonpath:
        env_vars["PYTHONPATH"] = pythonpath
    custom_operator_paths = None
    if cfg is not None and cfg.get("custom_operator_paths", None):
        custom_operator_paths = cfg.custom_operator_paths

    ray.init(
        ray_address,
        ignore_reinit_error=True,
        runtime_env=dict(
            py_modules=custom_operator_paths,
            env_vars=env_vars,
        ),
    )


def check_and_initialize_ray(cfg=None):
    if is_ray_mode():
        try:
            initialize_ray(cfg)
            return True
        except:  # noqa: E722
            return False

    return False


def _collect_node_info():
    mem_info = psutil.virtual_memory()
    free_mem = int(mem_info.available / (1024**2))  # MB
    cpu_count = psutil.cpu_count()

    try:
        gpus_memory, free_gpus_memory = [], []
        nvidia_smi_output = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=memory.free,memory.total", "--format=csv,noheader,nounits"]
        ).decode("utf-8")

        for line in nvidia_smi_output.strip().split("\n"):
            free_mem_str, total_mem_str = line.split(", ")
            free_gpus_memory.append(int(free_mem_str))
            gpus_memory.append(int(total_mem_str))
    except Exception:
        # no gpu
        gpus_memory, free_gpus_memory = [], []

    return {
        "free_memory": free_mem,  # MB
        "cpu_count": cpu_count,
        "gpu_count": len(free_gpus_memory),
        "gpus_memory": gpus_memory,
        "free_gpus_memory": free_gpus_memory,  # MB
    }


def get_ray_nodes_info(cfg=None):
    global _RAY_NODES_INFO

    if _RAY_NODES_INFO is not None:
        return _RAY_NODES_INFO

    collect_node_info = ray.remote(_collect_node_info)

    initialize_ray(cfg)

    nodes = ray.nodes()
    logger.info(f"Ray nodes:\n{nodes}")

    alive_nodes = [node for node in nodes if node["Alive"]]
    # skip head node
    worker_nodes = [node for node in alive_nodes if "head" not in node["NodeManagerHostname"]]

    futures = []
    for node in worker_nodes:
        node_id = node["NodeID"]
        from ray.util import scheduling_strategies

        strategy = scheduling_strategies.NodeAffinitySchedulingStrategy(node_id=node_id, soft=True)
        future = collect_node_info.options(scheduling_strategy=strategy).remote()
        futures.append(future)

    results = ray.get(futures)

    _RAY_NODES_INFO = {}
    for i, (node, info) in enumerate(zip(alive_nodes, results)):
        node_id = node["NodeID"]
        _RAY_NODES_INFO[node_id] = info

    logger.info(f"Ray cluster info:\n{_RAY_NODES_INFO}")

    return _RAY_NODES_INFO


def ray_cpu_count():
    cluster_resources = ray.cluster_resources()
    available_cpu = cluster_resources.get("CPU", 0)
    return available_cpu


def ray_gpu_count():
    cluster_resources = ray.cluster_resources()
    available_gpu = cluster_resources.get("GPU", 0)
    return available_gpu


def ray_available_memories():
    """Available memory for each alive node in MB."""
    ray_nodes_info = get_ray_nodes_info()

    available_mems = []
    for nodeid, info in ray_nodes_info.items():
        available_mems.append(info["free_memory"])

    return available_mems


def ray_available_gpu_memories():
    """Available gpu memory of each gpu card for each alive node in MB."""
    ray_nodes_info = get_ray_nodes_info()

    available_gpu_mems = []
    for nodeid, info in ray_nodes_info.items():
        available_gpu_mems.extend(info["free_gpus_memory"])

    return available_gpu_mems


def ray_gpu_memories():
    """Total gpu memory of each gpu card for each alive node in MB."""
    ray_nodes_info = get_ray_nodes_info()

    gpu_mems = []
    for nodeid, info in ray_nodes_info.items():
        gpu_mems.extend(info["gpus_memory"])

    return gpu_mems
