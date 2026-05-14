import csv
import unittest
from unittest.mock import patch, MagicMock

import pyarrow as pa

from data_juicer.core.data.load_strategy import (
    DataLoadStrategyRegistry, DataLoadStrategy, StrategyKey,
    DefaultLocalDataLoadStrategy,
    DefaultHuggingfaceDataLoadStrategy,
    RayLocalJsonDataLoadStrategy,
    DefaultS3DataLoadStrategy,
    RayS3DataLoadStrategy,
    RayHDFSDataLoadStrategy,
    DefaultHiveDataLoadStrategy,
    RayHiveDataLoadStrategy,
    DefaultTQSDataLoadStrategy,
    _build_hive_cast_block_udf,
    _count_parquet_rows_from_filesystem,
    _cast_hive_batch_columns,
)
from data_juicer.core.data.config_validator import ConfigValidationError
from data_juicer.core.io_utils import (
    _ensure_csv_field_size_limit,
    build_tqs_client_result_limited_query,
    run_tqs_query_to_records,
)
from jsonargparse import Namespace
from data_juicer.utils.unittest_utils import DataJuicerTestCaseBase, TEST_TAG
from data_juicer.config import get_default_cfg
import os
import os.path as osp
import json
import shutil
import uuid

WORK_DIR = os.path.dirname(os.path.abspath(__file__))

class MockStrategy(DataLoadStrategy):
    def load_data(self):
        pass

class DataLoadStrategyRegistryTest(DataJuicerTestCaseBase):
    @classmethod
    def setUpClass(cls):
        """Class-level setup run once before all tests"""
        super().setUpClass()
        # Save original strategies
        cls._original_strategies = DataLoadStrategyRegistry._strategies.copy()

    @classmethod
    def tearDownClass(cls):
        """Class-level cleanup run once after all tests"""
        # Restore original strategies
        DataLoadStrategyRegistry._strategies = cls._original_strategies
        super().tearDownClass()

    def setUp(self):
        """Instance-level setup run before each test"""
        super().setUp()
        # Clear strategies before each test
        DataLoadStrategyRegistry._strategies = {}

    def tearDown(self):
        """Instance-level cleanup"""
        # Reset strategies after each test
        DataLoadStrategyRegistry._strategies = {}
        super().tearDown()

    def test_exact_match(self):
        # Register a specific strategy
        DataLoadStrategyRegistry._strategies = {}
        @DataLoadStrategyRegistry.register("default", 'local', 'json')
        class TestStrategy(MockStrategy):
            pass

        # Test exact match
        strategy = DataLoadStrategyRegistry.get_strategy_class(
            "default", 'local', 'json')
        self.assertEqual(strategy, TestStrategy)

        # Test no match
        strategy = DataLoadStrategyRegistry.get_strategy_class(
            "default", 'local', 'csv')
        self.assertIsNone(strategy)

    def test_wildcard_matching(self):
        # Register strategies with different wildcard patterns
        DataLoadStrategyRegistry._strategies = {}
        @DataLoadStrategyRegistry.register("default", 'local', '*')
        class AllFilesStrategy(MockStrategy):
            pass

        @DataLoadStrategyRegistry.register("default", '*', '*')
        class AllLocalStrategy(MockStrategy):
            pass

        @DataLoadStrategyRegistry.register("*", '*', '*')
        class FallbackStrategy(MockStrategy):
            pass

        # Test specific matches
        strategy = DataLoadStrategyRegistry.get_strategy_class(
            "default", 'local', 'json')
        self.assertEqual(strategy, AllFilesStrategy)  # Should match most specific wildcard

        strategy = DataLoadStrategyRegistry.get_strategy_class(
            "default", 'remote', 'json')
        self.assertEqual(strategy, AllLocalStrategy)  # Should match second level wildcard

        strategy = DataLoadStrategyRegistry.get_strategy_class(
            "ray", 'remote', 'json')
        self.assertEqual(strategy, FallbackStrategy)  # Should match most general wildcard

    def test_specificity_priority(self):
        DataLoadStrategyRegistry._strategies = {}

        @DataLoadStrategyRegistry.register("*", '*', '*')
        class GeneralStrategy(MockStrategy):
            pass

        @DataLoadStrategyRegistry.register("default", '*', '*')
        class LocalStrategy(MockStrategy):
            pass

        @DataLoadStrategyRegistry.register("default", 'local', '*')
        class LocalOndiskStrategy(MockStrategy):
            pass

        @DataLoadStrategyRegistry.register("default", 'local', 'json')
        class ExactStrategy(MockStrategy):
            pass

        # Test matching priority
        strategy = DataLoadStrategyRegistry.get_strategy_class(
            "default", 'local', 'json')
        self.assertEqual(strategy, ExactStrategy)  # Should match exact first

        strategy = DataLoadStrategyRegistry.get_strategy_class(
            "default", 'local', 'csv')
        self.assertEqual(strategy, LocalOndiskStrategy)  # Should match one wildcard

        strategy = DataLoadStrategyRegistry.get_strategy_class(
            "default", 'remote', 'json')
        self.assertEqual(strategy, LocalStrategy)  # Should match two wildcards

        strategy = DataLoadStrategyRegistry.get_strategy_class(
            "ray", 'remote', 'json')
        self.assertEqual(strategy, GeneralStrategy)  # Should match general wildcard

    def test_pattern_matching(self):
        @DataLoadStrategyRegistry.register(
            "default", 'local', '*.json')
        class JsonStrategy(MockStrategy):
            pass

        @DataLoadStrategyRegistry.register(
            "default", 'local', 'data_[0-9]*')
        class NumberedDataStrategy(MockStrategy):
            pass

        # Test pattern matching
        strategy = DataLoadStrategyRegistry.get_strategy_class(
            "default", 'local', 'test.json')
        self.assertEqual(strategy, JsonStrategy)

        strategy = DataLoadStrategyRegistry.get_strategy_class(
            "default", 'local', 'data_123')
        self.assertEqual(strategy, NumberedDataStrategy)

        strategy = DataLoadStrategyRegistry.get_strategy_class(
            "default", 'local', 'test.csv')
        self.assertIsNone(strategy)

    def test_strategy_key_matches(self):
        DataLoadStrategyRegistry._strategies = {}

        # Test StrategyKey matching directly
        wildcard_key = StrategyKey("*", 'local', '*.json')
        specific_key = StrategyKey("default", 'local', 'test.json')
        
        # Exact keys don't match wildcards
        self.assertTrue(wildcard_key.matches(specific_key))
        self.assertFalse(specific_key.matches(wildcard_key))  

        # Test pattern matching
        pattern_key = StrategyKey("default", '*', 'data_[0-9]*')
        match_key = StrategyKey("default", 'local', 'data_123')
        no_match_key = StrategyKey("default", 'local', 'data_abc')
        
        self.assertTrue(pattern_key.matches(match_key))
        self.assertFalse(pattern_key.matches(no_match_key))

    def test_load_strategy_default_config(self):
        """Test load strategy with minimal config"""
        DataLoadStrategyRegistry._strategies = {}

        # Create minimal config
        minimal_cfg = Namespace(
            path='test/path'
        )
        
        ds_config = {
            'path': 'test/path'
        }
        
        strategy = DefaultLocalDataLoadStrategy(ds_config, minimal_cfg)
        
        # Verify defaults are used
        assert getattr(strategy.cfg, 'text_keys', ['text']) == ['text']
        assert getattr(strategy.cfg, 'suffixes', None) is None
        assert getattr(strategy.cfg, 'add_suffix', False) is False

    def test_default_hive_loader_requires_ray_executor(self):
        ds_config = {
            "type": "remote",
            "source": "hive",
            "table_name": "db.table",
        }

        strategy = DefaultHiveDataLoadStrategy(ds_config, Namespace(work_dir=WORK_DIR))

        with self.assertRaisesRegex(RuntimeError, "executor_type: ray"):
            strategy.load_data()

    @patch("data_juicer.core.data.load_strategy._load_ray_hive_catalog_cls")
    @patch("ray.data.read_hive_table", create=True)
    def test_ray_hive_loader_uses_read_hive_table(self, mock_read_hive_table, mock_load_hive_catalog_cls):
        catalog = MagicMock()
        mock_hive_catalog_cls = MagicMock(return_value=catalog)
        mock_load_hive_catalog_cls.return_value = mock_hive_catalog_cls
        ray_dataset = MagicMock()
        mock_read_hive_table.return_value = ray_dataset
        ds_config = {
            "type": "remote",
            "source": "hive",
            "table_name": "db.table",
            "columns": {"col_a": "STRING", "col_b": "BIGINT"},
            "filter": "date='20260426'",
            "concurrency": 100,
            "override_num_blocks": 200,
            "ray_remote_args": {"num_cpus": 2},
            "arrow_parquet_args": {"coerce_int96_timestamp_unit": "ms"},
        }

        dataset = RayHiveDataLoadStrategy(
            ds_config,
            Namespace(work_dir=WORK_DIR, auto_op_parallelism=None),
        ).load_data()

        self.assertIs(dataset.data, ray_dataset)
        mock_load_hive_catalog_cls.assert_called_once_with()
        mock_hive_catalog_cls.assert_called_once_with()
        catalog.start.assert_called_once_with()
        mock_read_hive_table.assert_called_once()
        read_kwargs = mock_read_hive_table.call_args.kwargs
        self.assertEqual(
            {
                key: value
                for key, value in read_kwargs.items()
                if key != "_block_udf"
            },
            {
                "table_name": "db.table",
                "columns": ["col_a", "col_b"],
                "filter": "date='20260426'",
                "concurrency": 100,
                "override_num_blocks": 200,
                "ray_remote_args": {"num_cpus": 2},
                "coerce_int96_timestamp_unit": "ms",
                "catalog": catalog,
            },
        )
        block_udf = read_kwargs["_block_udf"]
        casted_table = block_udf(
            pa.table(
                {
                    "col_a": pa.array([1], type=pa.int64()),
                    "col_b": pa.array(["2"], type=pa.string()),
                }
            )
        )
        self.assertEqual(casted_table.schema.field("col_a").type, pa.string())
        self.assertEqual(casted_table.schema.field("col_b").type, pa.int64())
        ray_dataset.map_batches.assert_not_called()

    def test_cast_hive_batch_columns_casts_null_and_mixed_columns(self):
        table = pa.table(
            {
                "string_col": pa.array([None, "1"], type=pa.string()),
                "int_col": pa.array([None, 2], type=pa.int64()),
                "null_col": pa.array([None, None], type=pa.null()),
            }
        )

        result = _cast_hive_batch_columns(
            table,
            {
                "string_col": "STRING",
                "int_col": "BIGINT",
                "null_col": "BIGINT",
                "missing_col": "STRING",
            },
        )

        self.assertEqual(result.schema.field("string_col").type, pa.string())
        self.assertEqual(result.schema.field("int_col").type, pa.int64())
        self.assertEqual(result.schema.field("null_col").type, pa.int64())
        self.assertEqual(result.column("null_col").to_pylist(), [None, None])

    def test_hive_cast_block_udf_preserves_existing_block_udf_order(self):
        def existing_block_udf(table):
            return table.append_column("added", pa.array(["3"], type=pa.string()))

        block_udf = _build_hive_cast_block_udf(
            {"value": "BIGINT", "added": "BIGINT"},
            existing_block_udf,
        )

        result = block_udf(pa.table({"value": pa.array(["2"], type=pa.string())}))

        self.assertEqual(result.schema.field("value").type, pa.int64())
        self.assertEqual(result.schema.field("added").type, pa.int64())
        self.assertEqual(result.to_pydict(), {"value": [2], "added": [3]})

    @patch("data_juicer.core.data.load_strategy._load_ray_hive_catalog_cls")
    @patch("ray.data.read_hive_table", create=True)
    def test_ray_hive_loader_does_not_forward_executor_load_kwargs(
        self, mock_read_hive_table, mock_load_hive_catalog_cls
    ):
        catalog = MagicMock()
        mock_hive_catalog_cls = MagicMock(return_value=catalog)
        mock_load_hive_catalog_cls.return_value = mock_hive_catalog_cls
        ray_dataset = MagicMock()
        mock_read_hive_table.return_value = ray_dataset
        ds_config = {
            "type": "remote",
            "source": "hive",
            "table_name": "db.table",
            "columns": ["col_a", "col_b"],
            "load_kwargs": {"custom_read_option": "kept"},
        }

        dataset = RayHiveDataLoadStrategy(
            ds_config,
            Namespace(work_dir=WORK_DIR, auto_op_parallelism=None),
        ).load_data(num_proc=16, features={"unexpected": "ignored"})

        self.assertIs(dataset.data, ray_dataset)
        mock_read_hive_table.assert_called_once_with(
            table_name="db.table",
            columns=["col_a", "col_b"],
            custom_read_option="kept",
            catalog=catalog,
        )

    def test_ray_hive_loader_rejects_catalog_config(self):
        with self.assertRaisesRegex(ConfigValidationError, "`catalog` is not supported"):
            RayHiveDataLoadStrategy(
                {
                    "type": "remote",
                    "source": "hive",
                    "table_name": "db.table",
                    "catalog": {"type": "hive", "metastore_uri": "thrift://metastore:9083"},
                },
                Namespace(work_dir=WORK_DIR),
            )

    def test_ray_hive_loader_requires_table_name(self):
        with self.assertRaisesRegex(ConfigValidationError, "Missing required fields: table_name"):
            RayHiveDataLoadStrategy(
                {
                    "type": "remote",
                    "source": "hive",
                    "columns": ["col_a"],
                },
                Namespace(work_dir=WORK_DIR),
            )

    def test_ray_hive_loader_rejects_legacy_fields(self):
        for field, value in {
            "sql": "select * from db.table",
            "table": "db.table",
            "cast_columns": {"col_a": "STRING"},
        }.items():
            with self.subTest(field=field):
                with self.assertRaisesRegex(ConfigValidationError, f"`{field}` is not supported"):
                    RayHiveDataLoadStrategy(
                        {
                            "type": "remote",
                            "source": "hive",
                            "table_name": "db.table",
                            field: value,
                        },
                        Namespace(work_dir=WORK_DIR),
                    )

    def test_ray_hive_loader_validates_columns_config(self):
        invalid_columns_configs = [
            [],
            {},
            ["col_a", 1],
            {"col_a": 1},
        ]
        for columns in invalid_columns_configs:
            with self.subTest(columns=columns):
                with self.assertRaisesRegex(ConfigValidationError, "columns"):
                    RayHiveDataLoadStrategy(
                        {
                            "type": "remote",
                            "source": "hive",
                            "table_name": "db.table",
                            "columns": columns,
                        },
                        Namespace(work_dir=WORK_DIR),
                    )

    @patch("ray.data.read_hive_table", new=None, create=True)
    def test_ray_hive_loader_requires_bytedray_hive_api(self):
        strategy = RayHiveDataLoadStrategy(
            {
                "type": "remote",
                "source": "hive",
                "table_name": "db.table",
            },
            Namespace(work_dir=WORK_DIR),
        )

        with self.assertRaisesRegex(ImportError, "bytedray"):
            strategy.load_data()

    @patch("data_juicer.core.data.load_strategy.run_tqs_query_to_records")
    def test_tqs_client_result_uses_query_field(self, mock_run_tqs_query_to_records):
        mock_run_tqs_query_to_records.return_value = [{"id": 1}]
        ds_config = {
            "type": "remote",
            "source": "tqs",
            "read_mode": "client_result",
            "query": "select 1 as id",
            "tqs_app_id": "app-id",
            "tqs_app_key": "app-key",
            "user_name": "user",
        }

        dataset = DefaultTQSDataLoadStrategy(ds_config, Namespace(work_dir=WORK_DIR)).load_data()

        self.assertEqual(dataset.to_list(), [{"id": 1}])
        mock_run_tqs_query_to_records.assert_called_once_with(
            query="select 1 as id",
            tqs_app_id="app-id",
            tqs_app_key="app-key",
            user_name="user",
            tqs_cluster="cn",
            tqs_enable_domain=None,
            tqs_timeout=120,
            max_result_rows=10000,
        )

    def test_tqs_client_result_forwards_client_options(self):
        captured_queries = []

        class AnalysisResult:
            error_message = ""

            def is_failed(self):
                return False

        class Job:
            result_schema = [["id", "BIGINT"]]

            def is_success(self):
                return True

            def get_typed_result(self, return_header=False):
                return [[1]]

        class Client:
            init_kwargs = None

            def __init__(self, *args, **kwargs):
                Client.init_kwargs = kwargs

            def analyze_query(self, user_name, query):
                captured_queries.append(("analyze", query))
                return AnalysisResult()

            def execute_query(self, user_name, query):
                captured_queries.append(("execute", query))
                return Job()

        bytedtqs = MagicMock()
        bytedtqs.TQSClient = Client

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=bytedtqs):
            self.assertEqual(
                run_tqs_query_to_records(
                    "select id from db.table",
                    tqs_app_id="app-id",
                    tqs_app_key="app-key",
                    user_name="user",
                    tqs_cluster="cn",
                    tqs_enable_domain=True,
                    tqs_timeout=30,
                    max_result_rows=1,
                ),
                [{"id": 1}],
            )

        expected_query = (
            "SELECT *\n"
            "FROM (\n"
            "select id from db.table\n"
            ") __dj_tqs_client_result_limit\n"
            "LIMIT 1"
        )
        self.assertEqual(captured_queries, [("analyze", expected_query), ("execute", expected_query)])
        self.assertEqual(
            Client.init_kwargs,
            {
                "app_id": "app-id",
                "app_key": "app-key",
                "cluster": "cn",
                "timeout": 30,
                "enable_domain": True,
            },
        )

    def test_tqs_client_result_limited_query_strips_trailing_semicolon(self):
        self.assertEqual(
            build_tqs_client_result_limited_query("select id from db.table;  ", 10),
            (
                "SELECT *\n"
                "FROM (\n"
                "select id from db.table\n"
                ") __dj_tqs_client_result_limit\n"
                "LIMIT 10"
            ),
        )

    def test_tqs_client_result_rejects_too_many_rows(self):
        class AnalysisResult:
            error_message = ""

            def is_failed(self):
                return False

        class Job:
            results = [{"id": 1}, {"id": 2}]

            def is_success(self):
                return True

        class Client:
            def __init__(self, *args, **kwargs):
                pass

            def analyze_query(self, user_name, query):
                return AnalysisResult()

            def execute_query(self, user_name, query):
                return Job()

        bytedtqs = MagicMock()
        bytedtqs.TQSClient = Client
        bytedtqs.Cluster.CN = "cn"

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=bytedtqs):
            with self.assertRaisesRegex(RuntimeError, "exceeding max_result_rows=1"):
                run_tqs_query_to_records(
                    "select id from db.table",
                    tqs_app_id="app-id",
                    tqs_app_key="app-key",
                    user_name="user",
                    max_result_rows=1,
                )

    def test_tqs_client_result_supports_bytedtqs_query_result_entity_shape(self):
        class AnalysisResult:
            error_message = ""

            def is_failed(self):
                return False

        class QueryResult:
            with_header = True

            def fetch_all_data(self):
                return [["id", "name"], ["1", "alice"]]

        class Job:
            result_schema = [["id", "BIGINT"], ["name", "STRING"]]

            def is_success(self):
                return True

            def get_typed_result(self, return_header=False):
                raise NotImplementedError("get_typed_result callback is not set")

            def get_result(self):
                return QueryResult()

        class Client:
            def __init__(self, *args, **kwargs):
                pass

            def analyze_query(self, user_name, query):
                return AnalysisResult()

            def execute_query(self, user_name, query):
                return Job()

        bytedtqs = MagicMock()
        bytedtqs.TQSClient = Client
        bytedtqs.Cluster.CN = "cn"

        with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=bytedtqs):
            self.assertEqual(
                run_tqs_query_to_records(
                    "select id, name from db.table",
                    tqs_app_id="app-id",
                    tqs_app_key="app-key",
                    user_name="user",
                    max_result_rows=1,
                ),
                [{"id": "1", "name": "alice"}],
            )

    def test_tqs_client_result_raises_csv_field_limit_before_typed_result(self):
        original_limit = csv.field_size_limit()
        csv.field_size_limit(8)

        class AnalysisResult:
            error_message = ""

            def is_failed(self):
                return False

        class Job:
            result_schema = [["payload", "STRING"]]

            def is_success(self):
                return True

            def get_typed_result(self, return_header=False):
                row = next(csv.reader(["x" * 32]))
                return [row]

        class Client:
            def __init__(self, *args, **kwargs):
                pass

            def analyze_query(self, user_name, query):
                return AnalysisResult()

            def execute_query(self, user_name, query):
                return Job()

        bytedtqs = MagicMock()
        bytedtqs.TQSClient = Client
        bytedtqs.Cluster.CN = "cn"

        try:
            with patch("data_juicer.core.io_utils.import_optional_dependency", return_value=bytedtqs):
                self.assertEqual(
                    run_tqs_query_to_records(
                        "select payload from db.table",
                        tqs_app_id="app-id",
                        tqs_app_key="app-key",
                        user_name="user",
                        max_result_rows=1,
                    ),
                    [{"payload": "x" * 32}],
                )
        finally:
            csv.field_size_limit(original_limit)

    def test_tqs_client_result_csv_field_limit_noops_when_current_limit_is_enough(self):
        with patch("data_juicer.core.io_utils.csv.field_size_limit", return_value=2**63):
            _ensure_csv_field_size_limit()

    def test_tqs_client_result_csv_field_limit_retries_after_overflow(self):
        calls = []

        def fake_field_size_limit(value=None):
            if value is None:
                return 8
            calls.append(value)
            if len(calls) == 1:
                raise OverflowError
            return value

        with patch("data_juicer.core.io_utils.sys.maxsize", 1000):
            with patch("data_juicer.core.io_utils.csv.field_size_limit", side_effect=fake_field_size_limit):
                _ensure_csv_field_size_limit()

        self.assertEqual(calls, [1000, 100])

    def test_load_strategy_full_config(self):
        """Test load strategy with full config"""
        DataLoadStrategyRegistry._strategies = {}

        # Create config with all options
        full_cfg = Namespace(
            path='test/path',
            text_keys=['content', 'title'],
            suffixes=['.txt', '.md'],
            add_suffix=True
        )
        
        ds_config = {
            'path': 'test/path'
        }
        
        strategy = DefaultLocalDataLoadStrategy(ds_config, full_cfg)
        
        # Verify all config values are used
        assert strategy.cfg.text_keys == ['content', 'title']
        assert strategy.cfg.suffixes == ['.txt', '.md']
        assert strategy.cfg.add_suffix is True

    def test_load_strategy_partial_config(self):
        """Test load strategy with partial config"""
        DataLoadStrategyRegistry._strategies = {}

        # Create config with some options
        partial_cfg = Namespace(
            path='test/path',
            text_keys=['content'],
            # suffixes and add_suffix omitted
        )
        
        ds_config = {
            'path': 'test/path'
        }
        
        strategy = DefaultLocalDataLoadStrategy(ds_config, partial_cfg)
        
        # Verify mix of specified and default values
        assert strategy.cfg.text_keys == ['content']
        assert getattr(strategy.cfg, 'suffixes', None) is None
        assert getattr(strategy.cfg, 'add_suffix', False) is False

    def test_load_strategy_empty_config(self):
        """Test load strategy with empty config"""
        DataLoadStrategyRegistry._strategies = {}
        
        # Create empty config
        empty_cfg = Namespace()
        
        ds_config = {
            'path': 'test/path'
        }
        
        strategy = DefaultLocalDataLoadStrategy(ds_config, empty_cfg)
        
        # Verify all defaults are used
        assert getattr(strategy.cfg, 'text_keys', ['text']) == ['text']
        assert getattr(strategy.cfg, 'suffixes', None) is None
        assert getattr(strategy.cfg, 'add_suffix', False) is False

    def test_local_strategy_forwards_load_dataset_kwargs(self):
        """Test that extra kwargs passed to load_data reach datasets.load_dataset.

        Passes a ``features`` kwarg that adds an extra column not present in the
        source file.  If kwargs are forwarded correctly, the loaded dataset will
        contain that column; if not, it won't.
        """
        from datasets import Features, Value

        DataLoadStrategyRegistry._strategies = {}

        sample_path = osp.join(WORK_DIR, "test_data", "sample.jsonl")
        cfg = Namespace(text_keys=["text"], suffixes=None, process=[])
        ds_config = {"type": "local", "path": sample_path}

        extra_features = Features({"text": Value("string"), "extra": Value("string")})

        strategy = DefaultLocalDataLoadStrategy(ds_config, cfg)
        ds = strategy.load_data(num_proc=1, features=extra_features)

        self.assertIn("extra", ds.features)

    @patch("data_juicer.core.data.load_strategy.datasets.load_dataset")
    def test_huggingface_strategy_forwards_load_dataset_kwargs(self, mock_load_dataset):
        """Test that extra kwargs passed to load_data reach datasets.load_dataset.

        The HuggingFace strategy calls ``datasets.load_dataset(path, ...)``
        which requires a real hub dataset, so we mock it and assert the
        ``features`` kwarg is present in the call.
        """
        from datasets import Features, Value

        DataLoadStrategyRegistry._strategies = {}

        cfg = Namespace(text_keys=["text"])
        ds_config = {"type": "huggingface", "path": "dummy/dataset"}

        mock_dataset = MagicMock()
        mock_load_dataset.return_value = mock_dataset

        extra_features = Features({"text": Value("string"), "extra": Value("string")})

        strategy = DefaultHuggingfaceDataLoadStrategy(ds_config, cfg)

        with patch("data_juicer.core.data.load_strategy.unify_format") as mock_unify:
            mock_unify.return_value = mock_dataset
            strategy.load_data(num_proc=1, features=extra_features)

        self.assertEqual(mock_load_dataset.call_args.kwargs.get("features"), extra_features)


class TestRayLocalJsonDataLoadStrategy(DataJuicerTestCaseBase):
    def setUp(self):
        """Instance-level setup run before each test"""
        super().setUp()

        cur_dir = osp.dirname(osp.abspath(__file__))
        self.tmp_dir = osp.join(cur_dir, f'tmp_{uuid.uuid4().hex}')
        os.makedirs(self.tmp_dir, exist_ok=True)

        self.cfg = get_default_cfg()
        self.cfg.ray_address = 'local'
        self.cfg.executor_type = 'ray'
        self.cfg.work_dir = self.tmp_dir

        self.test_data = [
            {'text': 'hello world'},
            {'text': 'hello world again'}
        ]

    def tearDown(self):
        if osp.exists(self.tmp_dir):
            shutil.rmtree(self.tmp_dir)

        super().tearDown()


    @TEST_TAG('ray')
    def test_absolute_path_resolution(self):
        """Test loading from absolute path"""
        abs_path = os.path.join(WORK_DIR, 'test_data', 'sample.jsonl')
    
        # Now test the strategy
        strategy = RayLocalJsonDataLoadStrategy({
            'path': abs_path
        }, self.cfg)
        
        dataset = strategy.load_data()
        result = list(dataset.get(2))
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['text'], "Today is Sunday and it's a happy day!")
        self.assertEqual(result[1]['text'], "Today is Monday and it's a happy day!")

    @TEST_TAG('ray')
    def test_relative_path_resolution(self):
        """Test loading from relative path"""
        rel_path = './tests/core/data/test_data/sample.jsonl'
    
        # Now test the strategy
        strategy = RayLocalJsonDataLoadStrategy({
            'path': rel_path
        }, self.cfg)
        
        dataset = strategy.load_data()
        result = list(dataset.get(2))
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['text'], "Today is Sunday and it's a happy day!")
        self.assertEqual(result[1]['text'], "Today is Monday and it's a happy day!")

    @TEST_TAG('ray')
    def test_workdir_resolution(self):
        """Test path resolution for work_dir"""
        test_filename = 'test_resolution.jsonl'
        
        # Create test file in work_dir
        work_path = osp.join(self.cfg.work_dir, test_filename)
        with open(work_path, 'w', encoding='utf-8', newline='\n') as f:
            for item in self.test_data:
                f.write(json.dumps(item, ensure_ascii=False).rstrip() + '\n')
    
        strategy = RayLocalJsonDataLoadStrategy({
            'path': test_filename  # relative to work_dir
        }, self.cfg)
        
        dataset = strategy.load_data()
        result = list(dataset.get(2))
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['text'], 'hello world')

    @TEST_TAG('ray')
    def test_read_parquet(self):
        """Test read parquet"""
        rel_path = './tests/core/data/test_data/parquet/sample.parquet'
        strategy = RayLocalJsonDataLoadStrategy({
            'path': rel_path
        }, self.cfg)

        dataset = strategy.load_data()
        result = list(dataset.get(2))
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['text'], "Today is Sunday and it's a happy day!")
        self.assertEqual(result[1]['text'], "Today is Monday and it's a happy day!")

        rel_path = './tests/core/data/test_data/parquet'
        strategy = RayLocalJsonDataLoadStrategy({
            'path': rel_path
        }, self.cfg)

        dataset = strategy.load_data()
        result = list(dataset.get(2))
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['text'], "Today is Sunday and it's a happy day!")
        self.assertEqual(result[1]['text'], "Today is Monday and it's a happy day!")


class TestDefaultS3DataLoadStrategy(DataJuicerTestCaseBase):
    """Test cases for DefaultS3DataLoadStrategy"""

    def setUp(self):
        """Instance-level setup run before each test"""
        super().setUp()
        self.cfg = Namespace()
        self.cfg.text_keys = ["text"]

    def test_strategy_registration(self):
        """Test that DefaultS3DataLoadStrategy is registered correctly"""
        strategy_class = DataLoadStrategyRegistry.get_strategy_class(
            executor_type="default", data_type="remote", data_source="s3"
        )
        self.assertIsNotNone(strategy_class)
        self.assertEqual(strategy_class, DefaultS3DataLoadStrategy)

    def test_config_validation_valid_path(self):
        """Test config validation with valid S3 path"""
        ds_config = {
            "type": "remote",
            "source": "s3",
            "path": "s3://bucket-name/path/to/file.jsonl"
        }
        
        # Should not raise an error
        strategy = DefaultS3DataLoadStrategy(ds_config, self.cfg)
        self.assertEqual(strategy.ds_config["path"], "s3://bucket-name/path/to/file.jsonl")

    def test_config_validation_invalid_path(self):
        """Test config validation with invalid S3 path"""
        from data_juicer.utils.s3_utils import validate_s3_path
        
        ds_config = {
            "type": "remote",
            "source": "s3",
            "path": "https://bucket-name/path/to/file.jsonl"  # Not s3://
        }
        
        # The custom validator returns False but doesn't raise, so validation passes during init
        # But validate_s3_path will raise ValueError during load_data
        strategy = DefaultS3DataLoadStrategy(ds_config, self.cfg)
        
        # Verify that validate_s3_path raises ValueError for invalid path
        # This is what gets called in load_data()
        with self.assertRaises(ValueError) as ctx:
            validate_s3_path(ds_config["path"])
        self.assertIn("s3://", str(ctx.exception).lower())

    def test_config_validation_optional_fields(self):
        """Test config validation with optional fields"""
        ds_config = {
            "type": "remote",
            "source": "s3",
            "path": "s3://bucket-name/path/to/file.jsonl",
            "aws_access_key_id": "test_key",
            "aws_secret_access_key": "test_secret",
            "aws_session_token": "test_token",
            "aws_region": "us-east-1",
            "endpoint_url": "https://s3.amazonaws.com"
        }
        
        # Should not raise an error
        strategy = DefaultS3DataLoadStrategy(ds_config, self.cfg)
        self.assertEqual(strategy.ds_config["aws_access_key_id"], "test_key")
        self.assertEqual(strategy.ds_config["aws_secret_access_key"], "test_secret")
        self.assertEqual(strategy.ds_config["aws_session_token"], "test_token")
        self.assertEqual(strategy.ds_config["aws_region"], "us-east-1")
        self.assertEqual(strategy.ds_config["endpoint_url"], "https://s3.amazonaws.com")

    def test_path_validation(self):
        """Test S3 path validation"""
        from data_juicer.utils.s3_utils import validate_s3_path
        
        # Valid paths
        valid_paths = [
            "s3://bucket/file.jsonl",
            "s3://bucket/path/to/file.jsonl",
            "s3://my-bucket-name/data/file.json"
        ]
        for path in valid_paths:
            try:
                validate_s3_path(path)
            except ValueError:
                self.fail(f"validate_s3_path raised ValueError for valid path: {path}")
        
        # Invalid paths
        invalid_paths = [
            "https://bucket/file.jsonl",
            "file://bucket/file.jsonl",
            "/local/path/file.jsonl",
            "bucket/file.jsonl"
        ]
        for path in invalid_paths:
            with self.assertRaises(ValueError):
                validate_s3_path(path)

    @patch('data_juicer.core.data.load_strategy.datasets.load_dataset')
    @patch('data_juicer.utils.s3_utils.get_aws_credentials')
    def test_load_data_with_credentials(self, mock_get_credentials, mock_load_dataset):
        """Test load_data with credentials"""
        from datasets import Dataset
        
        # Mock credentials
        mock_get_credentials.return_value = ("test_key", "test_secret", "test_token", "us-east-1")
        
        # Create a proper Dataset object for the mock to return
        test_dataset = Dataset.from_dict({"text": ["Hello", "World"]})
        mock_load_dataset.return_value = test_dataset
        
        ds_config = {
            "type": "remote",
            "source": "s3",
            "path": "s3://bucket-name/path/to/file.jsonl",
            "aws_access_key_id": "test_key",
            "aws_secret_access_key": "test_secret"
        }
        
        strategy = DefaultS3DataLoadStrategy(ds_config, self.cfg)
        
        # Mock unify_format to return the dataset as-is
        with patch('data_juicer.core.data.load_strategy.unify_format') as mock_unify:
            mock_unify.return_value = test_dataset
            result = strategy.load_data()
            
            # Verify load_dataset was called with correct arguments
            mock_load_dataset.assert_called_once()
            call_args = mock_load_dataset.call_args
            # Check that data_files is passed (either as positional or keyword)
            # datasets.load_dataset(data_format, data_files=path, storage_options=...)
            self.assertIn('data_files', call_args[1] or call_args[0])
            if 'data_files' in call_args[1]:
                self.assertEqual(call_args[1]['data_files'], "s3://bucket-name/path/to/file.jsonl")
            self.assertIn('storage_options', call_args[1])
            storage_options = call_args[1]['storage_options']
            self.assertEqual(storage_options['key'], "test_key")
            self.assertEqual(storage_options['secret'], "test_secret")

    @patch('data_juicer.core.data.load_strategy.datasets.load_dataset')
    @patch('data_juicer.utils.s3_utils.get_aws_credentials')
    def test_load_data_without_credentials(self, mock_get_credentials, mock_load_dataset):
        """Test load_data without credentials (uses default credential chain)"""
        from datasets import Dataset
        
        # Mock no credentials
        mock_get_credentials.return_value = (None, None, None, None)
        
        # Create a proper Dataset object for the mock to return
        test_dataset = Dataset.from_dict({"text": ["Hello", "World"]})
        mock_load_dataset.return_value = test_dataset
        
        ds_config = {
            "type": "remote",
            "source": "s3",
            "path": "s3://bucket-name/path/to/file.jsonl"
        }
        
        strategy = DefaultS3DataLoadStrategy(ds_config, self.cfg)
        
        # Mock unify_format to return the dataset as-is
        with patch('data_juicer.core.data.load_strategy.unify_format') as mock_unify:
            mock_unify.return_value = test_dataset
            _ = strategy.load_data()
            
            # Verify load_dataset was called
            mock_load_dataset.assert_called_once()
            call_args = mock_load_dataset.call_args
            storage_options = call_args[1]['storage_options']
            # With no credentials, storage_options should be empty (or minimal)
            # This allows s3fs to use default credential chain (IAM role, ~/.aws/credentials)
            # Anonymous access is NOT automatically enabled
            self.assertNotIn('key', storage_options)
            self.assertNotIn('secret', storage_options)
            self.assertNotIn('token', storage_options)
            self.assertNotIn('anon', storage_options)


class TestRayS3DataLoadStrategy(DataJuicerTestCaseBase):
    """Test cases for RayS3DataLoadStrategy"""

    def setUp(self):
        """Instance-level setup run before each test"""
        super().setUp()
        self.cfg = get_default_cfg()
        self.cfg.text_keys = ["text"]

    def test_strategy_registration(self):
        """Test that RayS3DataLoadStrategy is registered correctly"""
        strategy_class = DataLoadStrategyRegistry.get_strategy_class(
            executor_type="ray", data_type="remote", data_source="s3"
        )
        self.assertIsNotNone(strategy_class)
        self.assertEqual(strategy_class, RayS3DataLoadStrategy)

    def test_config_validation_valid_path(self):
        """Test config validation with valid S3 path"""
        ds_config = {
            "type": "remote",
            "source": "s3",
            "path": "s3://bucket-name/path/to/file.jsonl"
        }
        
        # Should not raise an error
        strategy = RayS3DataLoadStrategy(ds_config, self.cfg)
        self.assertEqual(strategy.ds_config["path"], "s3://bucket-name/path/to/file.jsonl")

    def test_config_validation_invalid_path(self):
        """Test config validation with invalid S3 path"""
        from data_juicer.utils.s3_utils import validate_s3_path
        
        ds_config = {
            "type": "remote",
            "source": "s3",
            "path": "https://bucket-name/path/to/file.jsonl"  # Not s3://
        }
        
        # Verify that validate_s3_path raises ValueError for invalid path
        # This is what gets called in load_data()
        with self.assertRaises(ValueError) as ctx:
            validate_s3_path(ds_config["path"])
        self.assertIn("s3://", str(ctx.exception).lower())

    def test_config_validation_optional_fields(self):
        """Test config validation with optional fields"""
        ds_config = {
            "type": "remote",
            "source": "s3",
            "path": "s3://bucket-name/path/to/file.jsonl",
            "aws_access_key_id": "test_key",
            "aws_secret_access_key": "test_secret",
            "aws_session_token": "test_token",
            "aws_region": "us-east-1",
            "endpoint_url": "https://s3.amazonaws.com"
        }
        
        # Should not raise an error
        strategy = RayS3DataLoadStrategy(ds_config, self.cfg)
        self.assertEqual(strategy.ds_config["aws_access_key_id"], "test_key")
        self.assertEqual(strategy.ds_config["aws_secret_access_key"], "test_secret")
        self.assertEqual(strategy.ds_config["aws_session_token"], "test_token")
        self.assertEqual(strategy.ds_config["aws_region"], "us-east-1")
        self.assertEqual(strategy.ds_config["endpoint_url"], "https://s3.amazonaws.com")


class TestRayHDFSDataLoadStrategy(DataJuicerTestCaseBase):
    """Test cases for RayHDFSDataLoadStrategy"""

    def setUp(self):
        super().setUp()
        self.cfg = get_default_cfg()
        self.cfg.text_keys = ["text"]

    def test_strategy_registration(self):
        strategy_class = DataLoadStrategyRegistry.get_strategy_class(
            executor_type="ray", data_type="remote", data_source="hdfs"
        )
        self.assertIsNotNone(strategy_class)
        self.assertEqual(strategy_class, RayHDFSDataLoadStrategy)

    def test_count_parquet_rows_from_filesystem_uses_metadata(self):
        import pyarrow.fs as pa_fs
        import pyarrow.parquet as pq

        tmp_dir = osp.join(WORK_DIR, f"tmp_hdfs_count_{uuid.uuid4().hex}")
        os.makedirs(tmp_dir, exist_ok=True)
        try:
            pq.write_table(pa.table({"id": [1, 2]}), osp.join(tmp_dir, "part-00000.parquet"))
            pq.write_table(pa.table({"id": [3, 4, 5]}), osp.join(tmp_dir, "part-00001.parquet"))
            open(osp.join(tmp_dir, "_SUCCESS"), "w").close()

            self.assertEqual(_count_parquet_rows_from_filesystem(pa_fs.LocalFileSystem(), tmp_dir), 5)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    @patch("data_juicer.core.data.ray_dataset.RayDataset")
    @patch("ray.data.read_parquet")
    @patch("data_juicer.core.data.load_strategy._count_parquet_rows_from_filesystem")
    @patch("data_juicer.core.data.load_strategy.get_pyarrow_filesystem")
    def test_load_parquet_defers_metadata_row_count_until_count(
        self,
        mock_get_pyarrow_filesystem,
        mock_count_parquet_rows,
        mock_read_parquet,
        mock_ray_dataset,
    ):
        fake_filesystem = MagicMock(name="hdfs_filesystem")
        fake_dataset = MagicMock(name="ray_dataset")
        wrapped_dataset = MagicMock(name="dj_ray_dataset")
        mock_get_pyarrow_filesystem.return_value = (fake_filesystem, "/user/demo/parts")
        mock_count_parquet_rows.return_value = 123
        mock_read_parquet.return_value = fake_dataset
        mock_ray_dataset.return_value = wrapped_dataset

        ds_config = {
            "type": "remote",
            "source": "hdfs",
            "path": "hdfs://haruna/user/demo/parts",
            "format": "parquet",
        }

        result = RayHDFSDataLoadStrategy(ds_config, self.cfg).load_data()

        self.assertEqual(result, wrapped_dataset)
        mock_ray_dataset.assert_called_once()
        self.assertEqual(mock_ray_dataset.call_args.args, (fake_dataset,))
        ray_dataset_kwargs = mock_ray_dataset.call_args.kwargs
        self.assertEqual(ray_dataset_kwargs["dataset_path"], "hdfs://haruna/user/demo/parts")
        self.assertIs(ray_dataset_kwargs["cfg"], self.cfg)
        mock_count_parquet_rows.assert_not_called()

        row_count_getter = ray_dataset_kwargs["row_count_getter"]
        self.assertEqual(row_count_getter(), 123)
        mock_count_parquet_rows.assert_called_once_with(fake_filesystem, "/user/demo/parts")

    @patch("data_juicer.core.data.ray_dataset.RayDataset")
    @patch("ray.data.read_parquet")
    @patch("data_juicer.core.data.load_strategy.get_pyarrow_filesystem")
    @patch("pyarrow.fs.FSSpecHandler")
    @patch("pyarrow.fs.PyFileSystem")
    @patch("fsspec.filesystem")
    def test_load_parquet_can_use_webhdfs_filesystem(
        self,
        mock_fsspec_filesystem,
        mock_pyarrow_filesystem,
        mock_fsspec_handler,
        mock_get_pyarrow_filesystem,
        mock_read_parquet,
        mock_ray_dataset,
    ):
        fake_webhdfs_fs = MagicMock(name="webhdfs_fs")
        fake_handler = MagicMock(name="webhdfs_handler")
        fake_pyarrow_fs = MagicMock(name="pyarrow_fs")
        fake_dataset = MagicMock(name="ray_dataset")
        wrapped_dataset = MagicMock(name="dj_ray_dataset")
        mock_fsspec_filesystem.return_value = fake_webhdfs_fs
        mock_fsspec_handler.return_value = fake_handler
        mock_pyarrow_filesystem.return_value = fake_pyarrow_fs
        mock_read_parquet.return_value = fake_dataset
        mock_ray_dataset.return_value = wrapped_dataset

        ds_config = {
            "type": "remote",
            "source": "hdfs",
            "path": "hdfs://namenode:9000/datasets/demo",
            "format": "parquet",
            "filesystem": "webhdfs",
            "webhdfs": {"host": "localhost", "port": 9870, "user": "bytedance"},
        }

        result = RayHDFSDataLoadStrategy(ds_config, self.cfg).load_data()

        self.assertEqual(result, wrapped_dataset)
        mock_get_pyarrow_filesystem.assert_not_called()
        mock_fsspec_filesystem.assert_called_once_with(
            "webhdfs",
            host="localhost",
            port=9870,
            user="bytedance",
        )
        mock_fsspec_handler.assert_called_once_with(fake_webhdfs_fs)
        mock_pyarrow_filesystem.assert_called_once_with(fake_handler)
        mock_read_parquet.assert_called_once_with(
            "/datasets/demo",
            filesystem=fake_pyarrow_fs,
        )
        mock_ray_dataset.assert_called_once()
        self.assertEqual(mock_ray_dataset.call_args.args, (fake_dataset,))
        ray_dataset_kwargs = mock_ray_dataset.call_args.kwargs
        self.assertEqual(ray_dataset_kwargs["dataset_path"], "hdfs://namenode:9000/datasets/demo")
        self.assertIs(ray_dataset_kwargs["cfg"], self.cfg)
        self.assertTrue(callable(ray_dataset_kwargs["row_count_getter"]))

    @patch("data_juicer.core.data.ray_dataset.RayDataset")
    @patch("ray.data.read_parquet")
    @patch("data_juicer.core.data.load_strategy.copy_uri_to_local")
    @patch("data_juicer.core.data.load_strategy.get_pyarrow_filesystem")
    def test_load_parquet_reads_hdfs_directly(
        self,
        mock_get_pyarrow_filesystem,
        mock_copy_uri_to_local,
        mock_read_parquet,
        mock_ray_dataset,
    ):
        fake_filesystem = MagicMock(name="hdfs_filesystem")
        fake_dataset = MagicMock(name="ray_dataset")
        wrapped_dataset = MagicMock(name="dj_ray_dataset")
        mock_get_pyarrow_filesystem.return_value = (fake_filesystem, "/user/demo/parts")
        mock_read_parquet.return_value = fake_dataset
        mock_ray_dataset.return_value = wrapped_dataset

        ds_config = {
            "type": "remote",
            "source": "hdfs",
            "path": "hdfs://haruna/user/demo/parts",
            "format": "parquet",
            "columns": ["id", "images"],
            "override_num_blocks": 16,
            "ray_remote_args": {"num_cpus": 1},
            "load_kwargs": {"concurrency": 4},
        }

        result = RayHDFSDataLoadStrategy(ds_config, self.cfg).load_data(num_proc=8)

        self.assertEqual(result, wrapped_dataset)
        mock_get_pyarrow_filesystem.assert_called_once_with("hdfs://haruna/user/demo/parts")
        mock_copy_uri_to_local.assert_not_called()
        mock_read_parquet.assert_called_once_with(
            "/user/demo/parts",
            filesystem=fake_filesystem,
            columns=["id", "images"],
            override_num_blocks=16,
            ray_remote_args={"num_cpus": 1},
            concurrency=4,
        )
        mock_ray_dataset.assert_called_once()
        self.assertEqual(mock_ray_dataset.call_args.args, (fake_dataset,))
        ray_dataset_kwargs = mock_ray_dataset.call_args.kwargs
        self.assertEqual(ray_dataset_kwargs["dataset_path"], "hdfs://haruna/user/demo/parts")
        self.assertIs(ray_dataset_kwargs["cfg"], self.cfg)
        self.assertTrue(callable(ray_dataset_kwargs["row_count_getter"]))

    def test_load_rejects_non_parquet_format(self):
        ds_config = {
            "type": "remote",
            "source": "hdfs",
            "path": "hdfs://haruna/user/demo/data.jsonl",
            "format": "jsonl",
        }

        with self.assertRaisesRegex(ValueError, "supports parquet only"):
            RayHDFSDataLoadStrategy(ds_config, self.cfg).load_data()

    def test_load_rejects_unknown_filesystem(self):
        ds_config = {
            "type": "remote",
            "source": "hdfs",
            "path": "hdfs://haruna/user/demo/parts",
            "filesystem": "file",
        }

        with self.assertRaises(ConfigValidationError):
            RayHDFSDataLoadStrategy(ds_config, self.cfg)


if __name__ == '__main__':
    unittest.main()
