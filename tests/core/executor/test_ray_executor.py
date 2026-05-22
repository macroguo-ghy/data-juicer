import os
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from data_juicer.core.executor.ray_executor import (
    RayDataCheckpointManager,
    RayExecutor,
    build_dry_run_ray_dataset,
    format_ray_data_plan,
)
from data_juicer.config import init_configs
from data_juicer.utils.unittest_utils import DataJuicerTestCaseBase, TEST_TAG

class RayExecutorTest(DataJuicerTestCaseBase):
    root_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..', '..')

    def setUp(self) -> None:
        super().setUp()
        # tmp dir
        self.tmp_dir = os.path.join(self.root_path, 'tmp/test_ray_executor/')
        os.makedirs(self.tmp_dir, exist_ok=True)

    def tearDown(self) -> None:
        super().tearDown()
        if os.path.exists(self.tmp_dir):
            os.system(f'rm -rf {self.tmp_dir}')

    @TEST_TAG('ray')
    def test_end2end_execution(self):
        cfg = init_configs(['--config', os.path.join(self.root_path, 'demos/process_on_ray/configs/demo-new-config.yaml')])
        cfg.export_path = os.path.join(self.tmp_dir, 'test_end2end_execution', 'res.jsonl')
        cfg.work_dir = os.path.join(self.tmp_dir, 'test_end2end_execution')
        executor = RayExecutor(cfg)
        executor.run()

        # check result files
        self.assertTrue(os.path.exists(cfg.export_path))

    @TEST_TAG('ray')
    def test_end2end_execution_skip_export(self):
        cfg = init_configs(
            ['--config', os.path.join(self.root_path, 'demos/process_on_ray/configs/demo-new-config.yaml')])
        cfg.export_path = os.path.join(self.tmp_dir, 'test_end2end_execution_skip_export', 'res.jsonl')
        cfg.work_dir = os.path.join(self.tmp_dir, 'test_end2end_execution_skip_export')
        executor = RayExecutor(cfg)
        executor.run(skip_export=True)

        # check result files
        self.assertFalse(os.path.exists(cfg.export_path))

    @TEST_TAG('ray')
    def test_end2end_execution_op_fusion(self):
        cfg = init_configs(['--config', os.path.join(self.root_path, 'demos/process_on_ray/configs/demo-new-config.yaml')])
        cfg.export_path = os.path.join(self.tmp_dir, 'test_end2end_execution_op_fusion', 'res.jsonl')
        cfg.work_dir = os.path.join(self.tmp_dir, 'test_end2end_execution_op_fusion')
        cfg.op_fusion = True
        executor = RayExecutor(cfg)
        executor.run()

        # check result files
        self.assertTrue(os.path.exists(cfg.export_path))

    def test_run_skips_real_metrics_by_default(self):
        class FakeData:
            def __init__(self):
                self.materialize_calls = 0
                self.count_calls = 0

            def columns(self):
                return ['text']

            def materialize(self):
                self.materialize_calls += 1
                return self

            def count(self):
                self.count_calls += 1
                return 3

        class FakeDataset:
            def __init__(self):
                self.data = FakeData()

            def process(self, ops, tracer=None):
                return self

        dataset = FakeDataset()
        executor = RayExecutor.__new__(RayExecutor)
        executor.cfg = SimpleNamespace(
            process=[{'noop': {}}],
            op_fusion=False,
            export_path=os.path.join(self.tmp_dir, 'unused.jsonl'),
            dataset=None,
            dataset_path=None,
            ray_collect_real_metrics=False,
        )
        executor.datasetbuilder = SimpleNamespace(load_dataset=MagicMock(return_value=dataset))
        executor.work_dir = self.tmp_dir
        executor.executor_type = 'ray'
        executor.tmp_dir = os.path.join(self.tmp_dir, '.tmp')
        executor.op_env_manager = None
        executor.tracer = None
        executor.pipeline_dag = SimpleNamespace(nodes=[object()], edges=[], parallel_groups=[])
        executor.log_job_start = MagicMock()
        executor._initialize_dag_execution = MagicMock()
        executor._pre_execute_operations_with_dag_monitoring = MagicMock()
        executor._post_execute_operations_with_dag_monitoring = MagicMock()
        executor.log_job_complete = MagicMock()

        with patch('data_juicer.core.executor.ray_executor.load_ops', return_value=[]):
            executor.run(skip_export=True)

        self.assertEqual(dataset.data.materialize_calls, 0)
        self.assertEqual(dataset.data.count_calls, 0)
        metrics = executor._post_execute_operations_with_dag_monitoring.call_args.kwargs['metrics']
        self.assertIsNone(metrics['input_rows'])
        self.assertIsNone(metrics['output_rows'])

    def test_run_collects_real_metrics_when_enabled(self):
        class FakeData:
            def __init__(self):
                self.materialize_calls = 0
                self.count_calls = 0

            def columns(self):
                return ['text']

            def materialize(self):
                self.materialize_calls += 1
                return self

            def count(self):
                self.count_calls += 1
                return 3

        class FakeDataset:
            def __init__(self):
                self.data = FakeData()

            def process(self, ops, tracer=None):
                return self

        dataset = FakeDataset()
        executor = RayExecutor.__new__(RayExecutor)
        executor.cfg = SimpleNamespace(
            process=[{'noop': {}}],
            op_fusion=False,
            export_path=os.path.join(self.tmp_dir, 'unused.jsonl'),
            dataset=None,
            dataset_path=None,
            ray_collect_real_metrics=True,
        )
        executor.datasetbuilder = SimpleNamespace(load_dataset=MagicMock(return_value=dataset))
        executor.work_dir = self.tmp_dir
        executor.executor_type = 'ray'
        executor.tmp_dir = os.path.join(self.tmp_dir, '.tmp')
        executor.op_env_manager = None
        executor.tracer = None
        executor.pipeline_dag = SimpleNamespace(nodes=[object()], edges=[], parallel_groups=[])
        executor.log_job_start = MagicMock()
        executor._initialize_dag_execution = MagicMock()
        executor._pre_execute_operations_with_dag_monitoring = MagicMock()
        executor._post_execute_operations_with_dag_monitoring = MagicMock()
        executor.log_job_complete = MagicMock()

        with patch('data_juicer.core.executor.ray_executor.load_ops', return_value=[]):
            executor.run(skip_export=True)

        self.assertEqual(dataset.data.materialize_calls, 1)
        self.assertEqual(dataset.data.count_calls, 1)
        metrics = executor._post_execute_operations_with_dag_monitoring.call_args.kwargs['metrics']
        self.assertIsNone(metrics['input_rows'])
        self.assertEqual(metrics['output_rows'], 3)

    def test_run_dry_run_plan_prints_plan_and_skips_actions(self):
        class FakeData:
            def columns(self):
                raise AssertionError("dry-run plan must not fetch columns")

            def materialize(self):
                raise AssertionError("dry-run plan must not materialize")

            def count(self):
                raise AssertionError("dry-run plan must not count")

        class FakeDataset:
            def __init__(self):
                self.data = FakeData()
                self.plan_only = None

            def process(self, ops, tracer=None, plan_only=False):
                self.plan_only = plan_only
                return self

        dataset = FakeDataset()
        executor = RayExecutor.__new__(RayExecutor)
        executor.cfg = SimpleNamespace(
            process=[{'noop': {}}],
            op_fusion=False,
            export_path=os.path.join(self.tmp_dir, 'unused.jsonl'),
            dataset={"configs": []},
            dataset_path=None,
            ray_collect_real_metrics=True,
            ray_dry_run_plan=True,
        )
        executor.datasetbuilder = SimpleNamespace(load_dataset=MagicMock(return_value=dataset))
        executor.work_dir = self.tmp_dir
        executor.executor_type = 'ray'
        executor.tmp_dir = os.path.join(self.tmp_dir, '.tmp')
        executor.op_env_manager = None
        executor.tracer = None
        executor.pipeline_dag = SimpleNamespace(nodes=[object()], edges=[], parallel_groups=[])
        executor.log_job_start = MagicMock()
        executor._initialize_dag_execution = MagicMock()
        executor._pre_execute_operations_with_dag_monitoring = MagicMock()
        executor._post_execute_operations_with_dag_monitoring = MagicMock()
        executor.log_job_complete = MagicMock()
        executor.exporter = SimpleNamespace(export=MagicMock())

        with (
            patch('data_juicer.core.executor.ray_executor.build_dry_run_ray_dataset', return_value=dataset)
            as mock_build_dry_run_dataset,
            patch('data_juicer.core.executor.ray_executor.load_ops', return_value=[]),
            patch('data_juicer.core.executor.ray_executor.format_ray_data_plan', return_value='PLAN'),
            patch('data_juicer.core.executor.ray_executor.run_after_export_hook') as mock_hook,
            patch('builtins.print') as mock_print,
        ):
            result = executor.run()

        self.assertIs(result, dataset)
        mock_build_dry_run_dataset.assert_called_once_with(executor.cfg)
        executor.datasetbuilder.load_dataset.assert_not_called()
        self.assertTrue(dataset.plan_only)
        mock_print.assert_called_once_with('PLAN', flush=True)
        executor.exporter.export.assert_not_called()
        mock_hook.assert_not_called()
        executor._pre_execute_operations_with_dag_monitoring.assert_not_called()
        executor._post_execute_operations_with_dag_monitoring.assert_not_called()

    def test_run_passes_materialize_after_each_op_when_enabled(self):
        class FakeData:
            def columns(self):
                return ['text']

        class FakeDataset:
            def __init__(self):
                self.data = FakeData()
                self.process_kwargs = None

            def process(self, ops, **kwargs):
                self.process_kwargs = kwargs
                return self

        dataset = FakeDataset()
        executor = RayExecutor.__new__(RayExecutor)
        executor.cfg = SimpleNamespace(
            process=[{'noop': {}}],
            op_fusion=False,
            export_path=os.path.join(self.tmp_dir, 'unused.jsonl'),
            dataset=None,
            dataset_path=None,
            ray_collect_real_metrics=False,
            ray_materialize_after_each_op=True,
        )
        executor.datasetbuilder = SimpleNamespace(load_dataset=MagicMock(return_value=dataset))
        executor.work_dir = self.tmp_dir
        executor.executor_type = 'ray'
        executor.tmp_dir = os.path.join(self.tmp_dir, '.tmp')
        executor.op_env_manager = None
        executor.tracer = None
        executor.pipeline_dag = SimpleNamespace(nodes=[object()], edges=[], parallel_groups=[])
        executor.log_job_start = MagicMock()
        executor._initialize_dag_execution = MagicMock()
        executor._pre_execute_operations_with_dag_monitoring = MagicMock()
        executor._post_execute_operations_with_dag_monitoring = MagicMock()
        executor.log_job_complete = MagicMock()

        with patch('data_juicer.core.executor.ray_executor.load_ops', return_value=[]):
            executor.run(skip_export=True)

        self.assertTrue(dataset.process_kwargs['materialize_after_each_op'])

    def test_run_calls_after_export_hook_after_successful_export(self):
        class FakeData:
            def columns(self):
                return ['text']

        class FakeDataset:
            def __init__(self):
                self.data = FakeData()

            def process(self, ops, **kwargs):
                return self

        dataset = FakeDataset()
        executor = RayExecutor.__new__(RayExecutor)
        executor.cfg = SimpleNamespace(
            process=[{'noop': {}}],
            op_fusion=False,
            export_path=os.path.join(self.tmp_dir, 'unused.jsonl'),
            export={
                "target": "magnus",
                "table_name": "zsy_test.default.output_table",
                "after_export_hook": {
                    "enabled": True,
                },
            },
            dataset=None,
            dataset_path=None,
            ray_collect_real_metrics=False,
            ray_materialize_after_each_op=False,
        )
        executor.datasetbuilder = SimpleNamespace(load_dataset=MagicMock(return_value=dataset))
        executor.exporter = SimpleNamespace(export=MagicMock())
        executor.work_dir = self.tmp_dir
        executor.executor_type = 'ray'
        executor.tmp_dir = os.path.join(self.tmp_dir, '.tmp')
        executor.op_env_manager = None
        executor.tracer = None
        executor.pipeline_dag = SimpleNamespace(nodes=[object()], edges=[], parallel_groups=[])
        executor.log_job_start = MagicMock()
        executor._initialize_dag_execution = MagicMock()
        executor._pre_execute_operations_with_dag_monitoring = MagicMock()
        executor._post_execute_operations_with_dag_monitoring = MagicMock()
        executor.log_job_complete = MagicMock()

        with (
            patch('data_juicer.core.executor.ray_executor.load_ops', return_value=[]),
            patch('data_juicer.core.executor.ray_executor.run_after_export_hook') as mock_hook,
        ):
            executor.run()

        executor.exporter.export.assert_called_once_with(dataset.data, columns=['text'])
        mock_hook.assert_called_once_with(executor.cfg.export)

    def test_run_does_not_call_after_export_hook_when_export_is_skipped(self):
        class FakeData:
            def columns(self):
                return ['text']

        class FakeDataset:
            def __init__(self):
                self.data = FakeData()

            def process(self, ops, **kwargs):
                return self

        dataset = FakeDataset()
        executor = RayExecutor.__new__(RayExecutor)
        executor.cfg = SimpleNamespace(
            process=[{'noop': {}}],
            op_fusion=False,
            export_path=os.path.join(self.tmp_dir, 'unused.jsonl'),
            export={"after_export_hook": {"enabled": True}},
            dataset=None,
            dataset_path=None,
            ray_collect_real_metrics=False,
            ray_materialize_after_each_op=False,
        )
        executor.datasetbuilder = SimpleNamespace(load_dataset=MagicMock(return_value=dataset))
        executor.exporter = SimpleNamespace(export=MagicMock())
        executor.work_dir = self.tmp_dir
        executor.executor_type = 'ray'
        executor.tmp_dir = os.path.join(self.tmp_dir, '.tmp')
        executor.op_env_manager = None
        executor.tracer = None
        executor.pipeline_dag = SimpleNamespace(nodes=[object()], edges=[], parallel_groups=[])
        executor.log_job_start = MagicMock()
        executor._initialize_dag_execution = MagicMock()
        executor._pre_execute_operations_with_dag_monitoring = MagicMock()
        executor._post_execute_operations_with_dag_monitoring = MagicMock()
        executor.log_job_complete = MagicMock()

        with (
            patch('data_juicer.core.executor.ray_executor.load_ops', return_value=[]),
            patch('data_juicer.core.executor.ray_executor.run_after_export_hook') as mock_hook,
        ):
            executor.run(skip_export=True)

        executor.exporter.export.assert_not_called()
        mock_hook.assert_not_called()

    def test_config_parses_ray_materialize_after_each_op(self):
        config_path = os.path.join(self.tmp_dir, 'ray_materialize_after_each_op.yaml')
        with open(config_path, 'w') as writer:
            writer.write(
                'project_name: test_ray_materialize_after_each_op\n'
                'ray_materialize_after_each_op: true\n'
                'process: []\n'
            )
        cfg = init_configs(
            [
                '--config',
                config_path,
            ],
            load_configs_only=True,
        )

        self.assertTrue(cfg.ray_materialize_after_each_op)

    def test_build_dry_run_ray_dataset_uses_export_schema_without_loading_sources(self):
        import pyarrow as pa

        captured = {}

        def from_arrow(table):
            captured['table'] = table
            return 'ray-dataset'

        def wrap_ray_dataset(dataset, cfg):
            return SimpleNamespace(data=dataset, cfg=cfg)

        cfg = SimpleNamespace(
            dataset={'configs': [{'type': 'remote', 'source': 'magnus', 'table_name': 'db.table'}]},
            export={
                'schema': {
                    'fields': [
                        {'name': 'id', 'type': 'string'},
                        {'name': 'images', 'type': 'list<binary>'},
                    ]
                }
            },
        )

        with (
            patch('data_juicer.core.executor.ray_executor.ray.data.from_arrow', side_effect=from_arrow)
            as mock_from_arrow,
            patch('data_juicer.core.executor.ray_executor.RayDataset', side_effect=wrap_ray_dataset)
            as mock_ray_dataset,
        ):
            result = build_dry_run_ray_dataset(cfg)

        mock_from_arrow.assert_called_once()
        mock_ray_dataset.assert_called_once_with('ray-dataset', cfg=cfg)
        self.assertIs(result.cfg, cfg)
        self.assertEqual(captured['table'].num_rows, 0)
        self.assertEqual(captured['table'].schema.field('id').type, pa.string())
        self.assertEqual(captured['table'].schema.field('images').type, pa.list_(pa.binary()))

    def test_format_ray_data_plan_prefers_ray_explain_api(self):
        class FakePlan:
            def explain(self):
                return "EXPLAINED"

        class FakeDataset:
            _plan = FakePlan()

        self.assertEqual(format_ray_data_plan(FakeDataset()), "EXPLAINED")

    def test_ray_data_checkpoint_enabled_sets_and_restores_ray_context(self):
        checkpoint_cfg = SimpleNamespace(
            enabled=True,
            dir='hdfs://checkpoint/job-1',
            delete_no_checkpoint_files=True,
            write_interval=7,
        )
        cfg = SimpleNamespace(ray_data_checkpoint=checkpoint_cfg)

        fake_context = SimpleNamespace(
            data_checkpoint_dir='hdfs://checkpoint/old-job',
            data_delete_no_checkpoint_files=False,
            data_checkpoint_write_interval=3,
        )
        fake_data_context = SimpleNamespace(
            get_current=MagicMock(return_value=fake_context),
            _set_current=MagicMock(),
        )
        fake_ray = SimpleNamespace(data=SimpleNamespace(DataContext=fake_data_context))

        with patch('data_juicer.core.executor.ray_executor.ray', fake_ray):
            with RayDataCheckpointManager(cfg) as checkpoint:
                self.assertTrue(checkpoint.enabled)
                self.assertEqual(fake_context.data_checkpoint_dir, 'hdfs://checkpoint/job-1')
                self.assertTrue(fake_context.data_delete_no_checkpoint_files)
                self.assertEqual(fake_context.data_checkpoint_write_interval, 7)

        self.assertTrue(checkpoint_cfg.enabled)
        self.assertEqual(fake_context.data_checkpoint_dir, 'hdfs://checkpoint/old-job')
        self.assertFalse(fake_context.data_delete_no_checkpoint_files)
        self.assertEqual(fake_context.data_checkpoint_write_interval, 3)
        fake_data_context.get_current.assert_called_once()
        self.assertEqual(fake_data_context._set_current.call_count, 2)

    def test_ray_data_checkpoint_enabled_requires_checkpoint_dir(self):
        checkpoint_cfg = SimpleNamespace(
            enabled=True,
            dir=None,
            delete_no_checkpoint_files=False,
            write_interval=None,
        )
        cfg = SimpleNamespace(ray_data_checkpoint=checkpoint_cfg)

        with self.assertRaisesRegex(ValueError, "`ray_data_checkpoint.dir` must be set"):
            with RayDataCheckpointManager(cfg):
                pass

    def test_run_with_ray_data_checkpoint_enabled_keeps_lazy_export_path(self):
        class FakeData:
            def __init__(self):
                self.materialize_calls = 0
                self.count_calls = 0

            def columns(self):
                return ['text']

            def materialize(self):
                self.materialize_calls += 1
                return self

            def count(self):
                self.count_calls += 1
                return 3

        class FakeDataset:
            def __init__(self):
                self.data = FakeData()

            def process(self, ops, tracer=None):
                return self

        dataset = FakeDataset()
        executor = RayExecutor.__new__(RayExecutor)
        executor.cfg = SimpleNamespace(
            process=[{'noop': {}}],
            op_fusion=False,
            export_path=os.path.join(self.tmp_dir, 'unused.jsonl'),
            dataset={"configs": [{"columns": ["text"]}]},
            dataset_path=None,
            ray_collect_real_metrics=True,
            ray_data_checkpoint=SimpleNamespace(
                enabled=True,
                dir='hdfs://checkpoint/job-1',
                delete_no_checkpoint_files=True,
                write_interval=7,
            ),
        )
        executor.datasetbuilder = SimpleNamespace(
            load_dataset=MagicMock(return_value=dataset),
            validate_ray_data_checkpoint_support=MagicMock(),
        )
        executor.work_dir = self.tmp_dir
        executor.executor_type = 'ray'
        executor.tmp_dir = os.path.join(self.tmp_dir, '.tmp')
        executor.op_env_manager = None
        executor.tracer = None
        executor.pipeline_dag = SimpleNamespace(nodes=[object()], edges=[], parallel_groups=[])
        executor.log_job_start = MagicMock()
        executor._initialize_dag_execution = MagicMock()
        executor._pre_execute_operations_with_dag_monitoring = MagicMock()
        executor._post_execute_operations_with_dag_monitoring = MagicMock()
        executor.log_job_complete = MagicMock()

        fake_data_context = SimpleNamespace(
            get_current=MagicMock(),
            _set_current=MagicMock(),
        )
        fake_ray = SimpleNamespace(data=SimpleNamespace(DataContext=fake_data_context))

        def assert_normal_export(exported_data, columns):
            self.assertIs(exported_data, dataset.data)
            self.assertEqual(columns, ['text'])

        executor.exporter = SimpleNamespace(export=MagicMock(side_effect=assert_normal_export))

        with (
            patch('data_juicer.core.executor.ray_executor.ray', fake_ray),
            patch('data_juicer.core.executor.ray_executor.load_ops', return_value=[]),
        ):
            executor.run()

        executor.exporter.export.assert_called_once()
        executor.datasetbuilder.validate_ray_data_checkpoint_support.assert_called_once()
        self.assertTrue(executor.cfg.ray_data_checkpoint.enabled)
        self.assertEqual(dataset.data.materialize_calls, 0)
        self.assertEqual(dataset.data.count_calls, 0)
        fake_data_context.get_current.assert_called_once()
        self.assertEqual(fake_data_context._set_current.call_count, 2)
        metrics = executor._post_execute_operations_with_dag_monitoring.call_args.kwargs['metrics']
        self.assertIsNone(metrics['input_rows'])
        self.assertIsNone(metrics['output_rows'])

    def test_run_with_ray_data_checkpoint_enabled_rejects_skip_export(self):
        class FakeData:
            def columns(self):
                return ['text']

            def materialize(self):
                return self

            def count(self):
                return 1

        class FakeDataset:
            def __init__(self):
                self.data = FakeData()

            def process(self, ops, tracer=None):
                return self

        executor = RayExecutor.__new__(RayExecutor)
        executor.cfg = SimpleNamespace(
            process=[{'noop': {}}],
            op_fusion=False,
            export_path=os.path.join(self.tmp_dir, 'unused.jsonl'),
            dataset=None,
            dataset_path=None,
            ray_data_checkpoint=SimpleNamespace(
                enabled=True,
                dir='hdfs://checkpoint/job-1',
                delete_no_checkpoint_files=True,
                write_interval=None,
            ),
        )

        fake_context = SimpleNamespace(
            data_checkpoint_dir='',
            data_delete_no_checkpoint_files=False,
            data_checkpoint_write_interval=30,
        )
        fake_data_context = SimpleNamespace(
            get_current=MagicMock(return_value=fake_context),
            _set_current=MagicMock(),
        )
        fake_ray = SimpleNamespace(data=SimpleNamespace(DataContext=fake_data_context))

        executor.datasetbuilder = SimpleNamespace(
            load_dataset=MagicMock(return_value=FakeDataset()),
            validate_ray_data_checkpoint_support=MagicMock(),
        )
        executor.work_dir = self.tmp_dir
        executor.executor_type = 'ray'
        executor.tmp_dir = os.path.join(self.tmp_dir, '.tmp')
        executor.op_env_manager = None
        executor.tracer = None
        executor.pipeline_dag = SimpleNamespace(nodes=[object()], edges=[], parallel_groups=[])
        executor.log_job_start = MagicMock()
        executor._initialize_dag_execution = MagicMock()
        executor._pre_execute_operations_with_dag_monitoring = MagicMock()
        executor._post_execute_operations_with_dag_monitoring = MagicMock()
        executor.log_job_complete = MagicMock()
        executor.exporter = SimpleNamespace(export=MagicMock())

        with (
            patch('data_juicer.core.executor.ray_executor.ray', fake_ray),
            patch('data_juicer.core.executor.ray_executor.load_ops', return_value=[]),
        ):
            with self.assertRaisesRegex(ValueError, "requires an export sink"):
                executor.run(skip_export=True)

        executor.exporter.export.assert_not_called()
        executor.datasetbuilder.validate_ray_data_checkpoint_support.assert_not_called()
        executor.datasetbuilder.load_dataset.assert_not_called()
        fake_data_context.get_current.assert_not_called()
        fake_data_context._set_current.assert_not_called()

    def test_run_with_ray_data_checkpoint_enabled_rejects_unsupported_loader_before_context(self):
        executor = RayExecutor.__new__(RayExecutor)
        executor.cfg = SimpleNamespace(
            process=[{'noop': {}}],
            op_fusion=False,
            export_path=os.path.join(self.tmp_dir, 'unused.jsonl'),
            dataset=None,
            dataset_path=None,
            ray_data_checkpoint=SimpleNamespace(
                enabled=True,
                dir='hdfs://checkpoint/job-1',
                delete_no_checkpoint_files=True,
                write_interval=None,
            ),
        )

        fake_data_context = SimpleNamespace(
            get_current=MagicMock(),
            _set_current=MagicMock(),
        )
        fake_ray = SimpleNamespace(data=SimpleNamespace(DataContext=fake_data_context))

        executor.datasetbuilder = SimpleNamespace(
            load_dataset=MagicMock(),
            validate_ray_data_checkpoint_support=MagicMock(
                side_effect=ValueError("unsupported loader")
            ),
        )
        executor.exporter = SimpleNamespace(export=MagicMock())

        with patch('data_juicer.core.executor.ray_executor.ray', fake_ray):
            with self.assertRaisesRegex(ValueError, "unsupported loader"):
                executor.run()

        executor.datasetbuilder.validate_ray_data_checkpoint_support.assert_called_once()
        executor.datasetbuilder.load_dataset.assert_not_called()
        executor.exporter.export.assert_not_called()
        fake_data_context.get_current.assert_not_called()
        fake_data_context._set_current.assert_not_called()

    def test_run_with_ray_data_checkpoint_enabled_rejects_join_before_context(self):
        executor = RayExecutor.__new__(RayExecutor)
        executor.cfg = SimpleNamespace(
            process=[
                {
                    'ray_dataset_join_pipeline': {
                        'right': {'type': 'local', 'path': 'right.jsonl'},
                        'on': 'id',
                    }
                }
            ],
            op_fusion=False,
            export_path=os.path.join(self.tmp_dir, 'unused.jsonl'),
            dataset=None,
            dataset_path=None,
            ray_data_checkpoint=SimpleNamespace(
                enabled=True,
                dir='hdfs://checkpoint/job-1',
                delete_no_checkpoint_files=True,
                write_interval=None,
            ),
        )

        fake_data_context = SimpleNamespace(
            get_current=MagicMock(),
            _set_current=MagicMock(),
        )
        fake_ray = SimpleNamespace(data=SimpleNamespace(DataContext=fake_data_context))

        executor.datasetbuilder = SimpleNamespace(
            load_dataset=MagicMock(),
            validate_ray_data_checkpoint_support=MagicMock(),
        )
        executor.exporter = SimpleNamespace(export=MagicMock())

        with patch('data_juicer.core.executor.ray_executor.ray', fake_ray):
            with self.assertRaisesRegex(ValueError, "ray_data_checkpoint is not supported"):
                executor.run()

        executor.datasetbuilder.validate_ray_data_checkpoint_support.assert_not_called()
        executor.datasetbuilder.load_dataset.assert_not_called()
        executor.exporter.export.assert_not_called()
        fake_data_context.get_current.assert_not_called()
        fake_data_context._set_current.assert_not_called()

    def test_run_with_ray_data_checkpoint_enabled_rejects_unsupported_hdfs_export_before_context(self):
        executor = RayExecutor.__new__(RayExecutor)
        executor.cfg = SimpleNamespace(
            process=[{'noop': {}}],
            op_fusion=False,
            export_path='hdfs://cluster/path/result.csv',
            export={'target': 'hdfs', 'path': 'hdfs://cluster/path/result.csv', 'type': 'csv'},
            dataset=None,
            dataset_path=None,
            ray_data_checkpoint=SimpleNamespace(
                enabled=True,
                dir='hdfs://checkpoint/job-1',
                delete_no_checkpoint_files=True,
                write_interval=None,
            ),
        )

        fake_data_context = SimpleNamespace(
            get_current=MagicMock(),
            _set_current=MagicMock(),
        )
        fake_ray = SimpleNamespace(data=SimpleNamespace(DataContext=fake_data_context))

        executor.datasetbuilder = SimpleNamespace(
            load_dataset=MagicMock(),
            validate_ray_data_checkpoint_support=MagicMock(),
        )
        executor.exporter = SimpleNamespace(
            export=MagicMock(),
            validate_ray_data_checkpoint_sink=MagicMock(side_effect=ValueError("parquet/jsonl")),
        )

        with patch('data_juicer.core.executor.ray_executor.ray', fake_ray):
            with self.assertRaisesRegex(ValueError, "parquet/jsonl"):
                executor.run()

        executor.datasetbuilder.validate_ray_data_checkpoint_support.assert_called_once()
        executor.exporter.validate_ray_data_checkpoint_sink.assert_called_once()
        executor.datasetbuilder.load_dataset.assert_not_called()
        executor.exporter.export.assert_not_called()
        fake_data_context.get_current.assert_not_called()
        fake_data_context._set_current.assert_not_called()


if __name__ == '__main__':
    unittest.main()
