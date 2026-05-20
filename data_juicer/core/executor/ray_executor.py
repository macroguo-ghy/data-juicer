import os
import shutil
import time
from copy import copy, deepcopy
from typing import Optional

from jsonargparse import Namespace
from loguru import logger
from pydantic import PositiveInt

from data_juicer.core.data.dataset_builder import DatasetBuilder
from data_juicer.core.data.ray_dataset import RayDataset
from data_juicer.core.executor import ExecutorBase
from data_juicer.core.executor.dag_execution_mixin import DAGExecutionMixin
from data_juicer.core.executor.event_logging_mixin import EventLoggingMixin
from data_juicer.core.export_hooks import run_after_export_hook
from data_juicer.core.export_manager import ExportManager
from data_juicer.core.data.ray_dataset import get_configured_ray_columns
from data_juicer.core.io_utils import build_arrow_schema_from_config
from data_juicer.core.tracer.ray_tracer import RayTracer
from data_juicer.ops import load_ops
from data_juicer.ops.op_fusion import fuse_operators
from data_juicer.utils.lazy_loader import LazyLoader

ray = LazyLoader("ray")
_MISSING_CONTEXT_VALUE = object()


def format_ray_data_plan(dataset) -> str:
    """Return Ray Data logical/physical plan text without executing the dataset."""
    plan = getattr(dataset, "_plan", None)
    if plan is not None and callable(getattr(plan, "explain", None)):
        return plan.explain()

    logical_plan = getattr(dataset, "_logical_plan", None)
    if logical_plan is None and plan is not None:
        logical_plan = getattr(plan, "_logical_plan", None)
    if logical_plan is None:
        raise RuntimeError("Ray Dataset does not expose a logical plan for dry-run inspection")

    sections = [
        ("Logical Plan", logical_plan.dag.dag_str),
    ]

    try:
        from ray.data._internal.logical.optimizers import LogicalOptimizer, PhysicalOptimizer
        from ray.data._internal.planner.planner import Planner

        try:
            logical_plan_for_optimization = deepcopy(logical_plan)
        except Exception:
            logical_plan_for_optimization = copy(logical_plan)
        optimized_logical = LogicalOptimizer().optimize(logical_plan_for_optimization)
        sections.append(("Logical Plan (Optimized)", optimized_logical.dag.dag_str))
        physical_plan = Planner().plan(optimized_logical)
        sections.append(("Physical Plan", physical_plan.dag.dag_str))
        optimized_physical = PhysicalOptimizer().optimize(physical_plan)
        sections.append(("Physical Plan (Optimized)", optimized_physical.dag.dag_str))
    except Exception as exc:
        sections.append(("Physical Plan", f"Unavailable without execution: {exc}"))

    return "".join(f"\n-------- {title} --------\n{body}\n" for title, body in sections)


def build_dry_run_ray_dataset(cfg) -> RayDataset:
    """Build a schema-only RayDataset for plan inspection without loading sources."""
    import pyarrow as pa

    export_cfg = getattr(cfg, "export", None)
    if isinstance(export_cfg, dict):
        schema_config = export_cfg.get("schema")
    else:
        schema_config = getattr(export_cfg, "schema", None) if export_cfg is not None else None

    schema = build_arrow_schema_from_config(schema_config) if schema_config else None
    if schema is None:
        columns = get_configured_ray_columns(cfg) or []
        schema = pa.schema([pa.field(column, pa.null()) for column in columns])

    table = pa.Table.from_arrays([pa.array([], type=field.type) for field in schema], schema=schema)
    return RayDataset(ray.data.from_arrow(table), cfg=cfg)


class TempDirManager:
    def __init__(self, tmp_dir):
        self.tmp_dir = tmp_dir

    def __enter__(self):
        os.makedirs(self.tmp_dir, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if os.path.exists(self.tmp_dir):
            logger.info(f"Removing tmp dir {self.tmp_dir} ...")
            shutil.rmtree(self.tmp_dir)


class RayDataCheckpointManager:
    def __init__(self, cfg):
        self.cfg = cfg
        self.checkpoint_cfg = getattr(cfg, "ray_data_checkpoint", None)
        self.enabled = bool(getattr(self.checkpoint_cfg, "enabled", False))
        self.context = None
        self.original_values = {}

    def __enter__(self):
        if not self.enabled:
            return self

        checkpoint_dir = getattr(self.checkpoint_cfg, "dir", None)
        if not checkpoint_dir:
            raise ValueError("`ray_data_checkpoint.dir` must be set when Ray Data checkpoint is enabled")

        data_context_cls = ray.data.DataContext
        self.context = data_context_cls.get_current()
        self.original_values = {
            "data_checkpoint_dir": getattr(self.context, "data_checkpoint_dir", _MISSING_CONTEXT_VALUE),
            "data_delete_no_checkpoint_files": getattr(
                self.context,
                "data_delete_no_checkpoint_files",
                _MISSING_CONTEXT_VALUE,
            ),
            "data_checkpoint_write_interval": getattr(
                self.context,
                "data_checkpoint_write_interval",
                _MISSING_CONTEXT_VALUE,
            ),
        }

        self.context.data_checkpoint_dir = checkpoint_dir
        self.context.data_delete_no_checkpoint_files = bool(
            getattr(self.checkpoint_cfg, "delete_no_checkpoint_files", False)
        )
        write_interval = getattr(self.checkpoint_cfg, "write_interval", None)
        if write_interval is not None:
            self.context.data_checkpoint_write_interval = write_interval
        if hasattr(data_context_cls, "_set_current"):
            data_context_cls._set_current(self.context)

        logger.info(
            "Enabled Ray Data checkpointing: "
            f"dir={checkpoint_dir}, "
            f"delete_no_checkpoint_files={self.context.data_delete_no_checkpoint_files}"
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.enabled or self.context is None:
            return None

        for key, value in self.original_values.items():
            if value is _MISSING_CONTEXT_VALUE:
                try:
                    delattr(self.context, key)
                except AttributeError:
                    pass
            else:
                setattr(self.context, key, value)
        data_context_cls = ray.data.DataContext
        if hasattr(data_context_cls, "_set_current"):
            data_context_cls._set_current(self.context)
        return None


class RayExecutor(ExecutorBase, DAGExecutionMixin, EventLoggingMixin):
    """
    Executor based on Ray.

    Run Data-Juicer data processing in a distributed cluster.

        1. Support Filter, Mapper and Exact Deduplicator operators for now.
        2. Only support loading `.json` files.
        3. Ray Data checkpointing is supported when `ray_data_checkpoint.enabled` is true.

    """

    def __init__(self, cfg: Optional[Namespace] = None):
        """
        Initialization method.

        :param cfg: optional config dict.
        """
        super().__init__(cfg)

        self.executor_type = "ray"
        self.work_dir = self.cfg.work_dir

        # Initialize EventLoggingMixin for job management and event logging
        EventLoggingMixin.__init__(self, cfg)

        # Initialize DAGExecutionMixin for AST/DAG functionality
        DAGExecutionMixin.__init__(self)

        # init ray
        logger.info("Initializing Ray ...")

        from data_juicer.utils.ray_utils import initialize_ray

        initialize_ray(cfg=cfg, force=True)

        self.tmp_dir = os.path.join(self.work_dir, ".tmp", ray.get_runtime_context().get_job_id())

        # absolute path resolution logic

        # init dataset builder
        self.datasetbuilder = DatasetBuilder(self.cfg, executor_type="ray")

        logger.info("Preparing exporter...")
        self.exporter = ExportManager(self.cfg, executor_type=self.executor_type)

        # setup tracer
        self.tracer = None
        self.open_tracer = self.cfg.open_tracer
        if self.open_tracer:
            logger.info("Preparing tracer...")
            self.tracer = RayTracer.remote(
                self.work_dir,
                self.cfg.op_list_to_trace,
                show_num=self.cfg.trace_num,
                trace_keys=self.cfg.trace_keys,
            )

        # setup OPEnvManager
        self.op_env_manager = None
        if self.cfg.min_common_dep_num_to_combine >= 0:
            from data_juicer.ops import OPEnvManager

            logger.info("Preparing OPEnvManager...")
            self.op_env_manager = OPEnvManager(
                min_common_dep_num_to_combine=self.cfg.min_common_dep_num_to_combine,
                conflict_resolve_strategy=self.cfg.conflict_resolve_strategy,
            )

    def run(self, load_data_np: Optional[PositiveInt] = None, skip_export: bool = False, skip_return: bool = False):
        """
        Running the dataset process pipeline

        :param load_data_np: number of workers when loading the dataset.
        :param skip_export: whether export the results into disk
        :param skip_return: skip return for API called.
        :return: processed dataset.
        """
        checkpoint_cfg = getattr(self.cfg, "ray_data_checkpoint", None)
        ray_data_checkpoint_enabled = bool(getattr(checkpoint_cfg, "enabled", False))
        if ray_data_checkpoint_enabled:
            if skip_export:
                raise ValueError("Ray Data checkpointing requires an export sink; `skip_export=True` is not supported.")
            self.datasetbuilder.validate_ray_data_checkpoint_support()
            validate_checkpoint_sink = getattr(self.exporter, "validate_ray_data_checkpoint_sink", None)
            if callable(validate_checkpoint_sink):
                validate_checkpoint_sink()

        with RayDataCheckpointManager(self.cfg) as ray_data_checkpoint:
            dry_run_plan = bool(getattr(self.cfg, "ray_dry_run_plan", False))

            # 1. load data
            logger.info("Loading dataset with Ray...")
            if dry_run_plan:
                logger.info("Ray dry-run plan requested; using schema-only input dataset and skipping source loading.")
                dataset = build_dry_run_ray_dataset(self.cfg)
            else:
                dataset = self.datasetbuilder.load_dataset(num_proc=load_data_np)
            if dry_run_plan:
                columns = get_configured_ray_columns(self.cfg)
            elif ray_data_checkpoint.enabled:
                columns = get_configured_ray_columns(self.cfg)
            else:
                columns = dataset.data.columns()

            # 2. extract processes
            logger.info("Preparing process operators...")
            ops = load_ops(self.cfg.process, self.op_env_manager)

            # Initialize DAG execution planning (pass ops to avoid redundant loading)
            self._initialize_dag_execution(self.cfg, ops=ops)

            # Log job start with DAG context
            # Handle both dataset_path (string) and dataset (dict) configurations
            dataset_info = {}
            if hasattr(self.cfg, "dataset_path") and self.cfg.dataset_path:
                dataset_info["dataset_path"] = self.cfg.dataset_path
            if hasattr(self.cfg, "dataset") and self.cfg.dataset:
                dataset_info["dataset"] = self.cfg.dataset

            job_config = {
                **dataset_info,
                "work_dir": self.work_dir,
                "executor_type": self.executor_type,
                "dag_node_count": len(self.pipeline_dag.nodes) if self.pipeline_dag else 0,
                "dag_edge_count": len(self.pipeline_dag.edges) if self.pipeline_dag else 0,
                "parallel_groups_count": len(self.pipeline_dag.parallel_groups) if self.pipeline_dag else 0,
            }
            self.log_job_start(job_config, len(ops))

            if self.cfg.op_fusion:
                logger.info(f"Start OP fusion and reordering with strategy " f"[{self.cfg.fusion_strategy}]...")
                ops = fuse_operators(ops)

            with TempDirManager(self.tmp_dir):
                # 3. data process with DAG monitoring
                logger.info("Processing data with DAG monitoring...")
                tstart = time.time()

                # TODO: Make input row counting configurable. Calling count() here
                # triggers a full upstream Ray Data execution before processing,
                # which is too expensive for large remote sources such as Hive.
                input_rows = None
                start_time = time.time()

                # Pre-execute DAG monitoring (log operation start events)
                if self.pipeline_dag and not dry_run_plan:
                    self._pre_execute_operations_with_dag_monitoring(ops)

                # Execute operations (Ray executor uses simple dataset.process)
                if dry_run_plan:
                    dataset = dataset.process(ops, tracer=None, plan_only=True)
                    logger.info("Ray dry-run plan requested; printing Ray Data plan and skipping execution/export.")
                    print(format_ray_data_plan(dataset.data), flush=True)
                    duration = time.time() - start_time
                    output_rows = None
                else:
                    process_kwargs = {"tracer": self.tracer}
                    if getattr(self.cfg, "ray_materialize_after_each_op", False):
                        process_kwargs["materialize_after_each_op"] = True
                    dataset = dataset.process(ops, **process_kwargs)

                    collect_real_metrics = getattr(self.cfg, "ray_collect_real_metrics", False)
                    if ray_data_checkpoint.enabled:
                        logger.info(
                            "Ray Data checkpointing is enabled; keeping dataset lazy until export "
                            "and skipping eager materialize/count metrics."
                        )
                        duration = time.time() - start_time
                        output_rows = None
                    elif collect_real_metrics:
                        # Force materialization to get real execution and row metrics.
                        logger.info("Materializing dataset to collect real metrics...")
                        dataset.data = dataset.data.materialize()

                        duration = time.time() - start_time
                        output_rows = dataset.data.count()
                    else:
                        logger.info(
                            "Skipping eager Ray Data materialize/count metrics; "
                            "dataset execution remains lazy until export."
                        )
                        duration = time.time() - start_time
                        output_rows = None

                # Post-execute DAG monitoring (log operation completion events with real metrics)
                if self.pipeline_dag and not dry_run_plan:
                    metrics = {"duration": duration, "input_rows": input_rows, "output_rows": output_rows}
                    self._post_execute_operations_with_dag_monitoring(ops, metrics=metrics)

                # 4. data export
                # skip_export is a Python API/testing escape hatch. When real metrics
                # collection is disabled, this path may return a lazy Ray Dataset
                # without triggering execution; callers that need execution without
                # writing output should explicitly run an action on the returned dataset.
                if not skip_export and not dry_run_plan:
                    logger.info("Exporting dataset to disk...")
                    self.exporter.export(dataset.data, columns=columns)
                    run_after_export_hook(getattr(self.cfg, "export", None))
                tend = time.time()
                logger.info(f"All Ops are done in {tend - tstart:.3f}s.")

        # Log job completion with DAG context
        job_duration = time.time() - tstart
        self.log_job_complete(job_duration, self.cfg.export_path)

        # 5. finalize the tracer results
        # Finalize sample-level traces after all operators have finished
        if self.tracer:
            ray.get(self.tracer.finalize_traces.remote())

        if not skip_return:
            return dataset
