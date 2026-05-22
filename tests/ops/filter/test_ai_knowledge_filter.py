import inspect
import os
import sys
import types
import unittest
from unittest.mock import patch

import pyarrow as pa

from data_juicer.ops.base_op import DEFAULT_BATCH_SIZE
from data_juicer.ops.filter.ai_knowledge_filter import (
    DEFAULT_SOURCE_CLUSTER,
    DEFAULT_SOURCE_PSM,
    DEFAULT_TARGET_CLUSTER,
    DEFAULT_TARGET_PSM,
    DEFAULT_TIMEOUT,
    DEFAULT_USER_ID,
    AiKnowledgeFilter,
    _build_target,
    _ensure_requester_env,
    _load_akc_admin_thrift,
)


class _Struct:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _FakeBase(_Struct):
    def __init__(self, Caller="", TrafficEnv=None, Extra=None, **kwargs):
        super().__init__(Caller=Caller, TrafficEnv=TrafficEnv, Extra=Extra, **kwargs)


class _FakeTrafficEnv(_Struct):
    def __init__(self, Open=False, Env=""):
        super().__init__(Open=Open, Env=Env)


class _FakeBizReq(_Struct):
    def __init__(self, UserId=0, Extra=None):
        super().__init__(UserId=UserId, Extra=Extra)


class _FakeBaseThrift:
    Base = _FakeBase
    BizReq = _FakeBizReq
    TrafficEnv = _FakeTrafficEnv


class _FakeAkcAdminThrift:
    base_thrift = _FakeBaseThrift
    AkcAdminService = object()
    AckSearchFilterRequest = _Struct
    Identifier = _Struct
    AckSearchCondition = _Struct
    AckSearchPredicate = _Struct
    AckSearchValue = _Struct


class _FakeFilterClient:
    def __init__(self, returned_identifiers=None, biz_code=0, biz_msg="", status_code=0, status_message=""):
        self.returned_identifiers = returned_identifiers or []
        self.biz_code = biz_code
        self.biz_msg = biz_msg
        self.status_code = status_code
        self.status_message = status_message
        self.calls = []

    def filter(self, req):
        self.calls.append(req)
        return _Struct(
            identifiers=self.returned_identifiers,
            BizResp=_Struct(Code=self.biz_code, Msg=self.biz_msg),
            BaseResp=_Struct(StatusCode=self.status_code, StatusMessage=self.status_message),
        )


def _identifier(identifier, source):
    return _Struct(identifier=identifier, source=source)


class AiKnowledgeFilterTest(unittest.TestCase):
    def _op(self, client=None, **kwargs):
        defaults = {"auto_op_parallelism": False, "num_proc": 1}
        defaults.update(kwargs)
        op = AiKnowledgeFilter(**defaults)
        op._client = client or _FakeFilterClient()
        op._api_thrift = _FakeAkcAdminThrift
        return op

    def test_constructor_only_exposes_condition_keyword_and_env(self):
        signature = inspect.signature(AiKnowledgeFilter.__init__)
        explicit_params = [
            name
            for name, param in signature.parameters.items()
            if name != "self"
            and param.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        ]

        self.assertEqual(explicit_params, ["condition", "keyword", "env"])

    def test_operator_uses_batched_mode_without_overriding_batch_size(self):
        op = self._op()
        custom_batch_op = self._op(batch_size=2048)

        self.assertTrue(op.is_batched_op())
        self.assertEqual(op.batch_size, DEFAULT_BATCH_SIZE)
        self.assertEqual(custom_batch_op.batch_size, 2048)

    def test_process_batched_calls_thrift_filter_and_keeps_matching_identifier(self):
        client = _FakeFilterClient(returned_identifiers=[_identifier("id-1", "wiki")])
        op = self._op(
            client,
            condition={
                "op": "AND",
                "children": [
                    {
                        "predicate": {
                            "field": "permission_level",
                            "operator": "IN",
                            "value": ["public", "internal"],
                        }
                    },
                    {
                        "predicate": {
                            "field": "pv",
                            "operator": "GE",
                            "value": {"longValue": 10},
                        }
                    },
                ],
            },
            keyword="foo, bar",
            env="ppe_sirius2",
        )

        with patch("data_juicer.ops.filter.ai_knowledge_filter.emit_rpc_qps") as emit_metric:
            result = op.process({"identifier": [b"id-1"], "source": ["wiki"], "code": [0]})

        self.assertEqual(result, [True])
        self.assertEqual(emit_metric.call_args.kwargs["status"], "success")
        self.assertEqual(len(client.calls), 1)
        req = client.calls[0]
        self.assertEqual(req.keywords, ["foo", "bar"])
        self.assertEqual([(item.identifier, item.source) for item in req.identifiers], [("id-1", "wiki")])
        self.assertEqual(req.BizReq.UserId, DEFAULT_USER_ID)
        self.assertEqual(req.Base.Caller, DEFAULT_SOURCE_PSM)
        self.assertEqual(req.Base.Extra, {"cluster": DEFAULT_SOURCE_CLUSTER, "env": "ppe_sirius2"})
        self.assertTrue(req.Base.TrafficEnv.Open)
        self.assertEqual(req.Base.TrafficEnv.Env, "ppe_sirius2")
        self.assertEqual(req.condition.op, "AND")
        self.assertEqual(req.condition.children[0].predicate.value.stringListValue, ["public", "internal"])
        self.assertEqual(req.condition.children[1].predicate.value.longValue, 10)

    def test_process_batched_filters_nonzero_code_without_rpc(self):
        client = _FakeFilterClient(returned_identifiers=[_identifier("id-1", "wiki")])
        op = self._op(client)

        self.assertEqual(op.process({"identifier": ["id-1"], "source": ["wiki"], "code": [1]}), [False])

        self.assertEqual(client.calls, [])

    def test_process_batched_only_sends_zero_code_identifiers_to_rpc(self):
        client = _FakeFilterClient(returned_identifiers=[_identifier("id-1", "wiki"), _identifier("id-3", "doc")])
        op = self._op(client)

        with patch("data_juicer.ops.filter.ai_knowledge_filter.emit_rpc_qps"):
            self.assertEqual(
                op.process(
                    {
                        "identifier": ["id-1", "id-2", "id-3"],
                        "source": ["wiki", "wiki", "doc"],
                        "code": [0.0, 1, 0],
                    }
                ),
                [True, False, True],
            )

        self.assertEqual(len(client.calls), 1)
        req = client.calls[0]
        self.assertEqual([(item.identifier, item.source) for item in req.identifiers], [("id-1", "wiki"), ("id-3", "doc")])

    def test_process_batched_splits_rpc_requests_above_five_hundred_identifiers(self):
        class MatchingFilterClient(_FakeFilterClient):
            def filter(self, req):
                self.calls.append(req)
                return _Struct(
                    identifiers=[
                        item
                        for item in req.identifiers
                        if item.identifier in {"id-0", "id-499", "id-500"}
                    ],
                    BizResp=_Struct(Code=0, Msg=""),
                    BaseResp=_Struct(StatusCode=0, StatusMessage=""),
                )

        client = MatchingFilterClient()
        op = self._op(client, batch_size=2048)
        samples = {
            "identifier": [f"id-{index}" for index in range(501)],
            "source": ["wiki"] * 501,
            "code": [0] * 501,
        }

        with patch("data_juicer.ops.filter.ai_knowledge_filter.emit_rpc_qps"):
            result = op.process_batched(samples)

        self.assertEqual(len(client.calls), 2)
        self.assertEqual([len(call.identifiers) for call in client.calls], [500, 1])
        self.assertEqual(client.calls[0].identifiers[0].identifier, "id-0")
        self.assertEqual(client.calls[0].identifiers[-1].identifier, "id-499")
        self.assertEqual(client.calls[1].identifiers[0].identifier, "id-500")
        self.assertEqual(
            [index for index, keep in enumerate(result) if keep],
            [0, 499, 500],
        )

    def test_reversed_range_inverts_rpc_keep_result(self):
        op = self._op(
            _FakeFilterClient(returned_identifiers=[_identifier("id-1", "wiki")]),
            reversed_range=True,
        )

        with patch("data_juicer.ops.filter.ai_knowledge_filter.emit_rpc_qps"):
            self.assertEqual(
                op.process({"identifier": ["id-1", "id-2"], "source": ["wiki", "doc"], "code": [0, 0]}),
                [False, True],
            )

    def test_rpc_success_logs_request_and_elapsed_time(self):
        op = self._op(
            _FakeFilterClient(returned_identifiers=[_identifier("id-1", "wiki")]),
            env="ppe_sirius2",
        )

        with patch("data_juicer.ops.filter.ai_knowledge_filter.emit_rpc_qps"):
            with patch("data_juicer.ops.filter.ai_knowledge_filter.time.monotonic", side_effect=[10.0, 10.125]):
                with patch("data_juicer.ops.filter.ai_knowledge_filter.logger") as logger:
                    self.assertEqual(op.process({"identifier": ["id-1"], "source": ["wiki"], "code": [0]}), [True])

        self.assertEqual(logger.info.call_count, 2)
        start_log = logger.info.call_args_list[0].args[0]
        finish_log = logger.info.call_args_list[1].args[0]
        self.assertIn("rpc start", start_log)
        self.assertIn("target=sd://ad.stats.ai_knowledge_center_admin?cluster=default", start_log)
        self.assertIn("env=ppe_sirius2", start_log)
        self.assertIn("identifiers=1", start_log)
        self.assertIn("status=success", finish_log)
        self.assertIn("matched=1", finish_log)
        self.assertIn("elapsed_ms=125.00", finish_log)

    def test_rpc_failure_logs_elapsed_time_before_reraising(self):
        op = self._op(_FakeFilterClient(status_code=500, status_message="rpc failed"))

        with patch("data_juicer.ops.filter.ai_knowledge_filter.emit_rpc_qps"):
            with patch("data_juicer.ops.filter.ai_knowledge_filter.time.monotonic", side_effect=[20.0, 20.25]):
                with patch("data_juicer.ops.filter.ai_knowledge_filter.logger") as logger:
                    with self.assertRaisesRegex(RuntimeError, "status_code=500"):
                        op.process({"identifier": ["id-1"], "source": ["wiki"], "code": [0]})

        self.assertEqual(logger.info.call_count, 1)
        self.assertEqual(logger.warning.call_count, 1)
        failure_log = logger.warning.call_args.args[0]
        self.assertIn("status=error", failure_log)
        self.assertIn("identifiers=1", failure_log)
        self.assertIn("elapsed_ms=250.00", failure_log)
        self.assertIn("status_code=500", failure_log)

    def test_process_arrow_batch_uses_rpc_filter_path(self):
        op = self._op(_FakeFilterClient(returned_identifiers=[_identifier("id-2", "doc")]))
        table = pa.Table.from_pylist(
            [
                {"identifier": "id-2", "source": "doc", "code": 0},
            ],
            schema=pa.schema(
                [
                    pa.field("identifier", pa.string()),
                    pa.field("source", pa.string()),
                    pa.field("code", pa.int64()),
                ]
            ),
        )

        with patch("data_juicer.ops.filter.ai_knowledge_filter.emit_rpc_qps"):
            self.assertEqual(op.process(table), [True])

    def test_rpc_failure_raises_from_biz_resp_and_base_resp(self):
        with self.subTest("biz resp"):
            op = self._op(_FakeFilterClient(biz_code=400, biz_msg="bad condition"))
            with patch("data_juicer.ops.filter.ai_knowledge_filter.emit_rpc_qps"):
                with self.assertRaisesRegex(RuntimeError, "biz_code=400"):
                    op.process({"identifier": ["id-1"], "source": ["wiki"], "code": [0]})

        with self.subTest("base resp"):
            op = self._op(_FakeFilterClient(status_code=500, status_message="rpc failed"))
            with patch("data_juicer.ops.filter.ai_knowledge_filter.emit_rpc_qps"):
                with self.assertRaisesRegex(RuntimeError, "status_code=500"):
                    op.process({"identifier": ["id-1"], "source": ["wiki"], "code": [0]})

    def test_create_client_uses_euler_client_and_env_middleware(self):
        class FakeEulerClient:
            instances = []

            def __init__(self, service, target, **kwargs):
                self.service = service
                self.target = target
                self.kwargs = kwargs
                self.middlewares = []
                self.__class__.instances.append(self)

            def use(self, middleware):
                self.middlewares.append(middleware)

        fake_base_compat_middleware = types.ModuleType("euler.base_compat_middleware")
        fake_base_compat_middleware.client_middleware = object()
        fake_euler = types.ModuleType("euler")
        fake_euler.Client = FakeEulerClient
        fake_euler.base_compat_middleware = fake_base_compat_middleware
        op = AiKnowledgeFilter(
            env="ppe_sirius2",
            auto_op_parallelism=False,
            num_proc=1,
        )

        with patch.dict(sys.modules, {"euler": fake_euler, "euler.base_compat_middleware": fake_base_compat_middleware}):
            with patch("data_juicer.ops.filter.ai_knowledge_filter._load_akc_admin_thrift", return_value=_FakeAkcAdminThrift):
                client, api_thrift = op._create_client_and_thrift()

        self.assertIs(api_thrift, _FakeAkcAdminThrift)
        self.assertIs(client.service, _FakeAkcAdminThrift.AkcAdminService)
        self.assertEqual(client.target, _build_target(DEFAULT_TARGET_PSM, DEFAULT_TARGET_CLUSTER))
        self.assertEqual(client.kwargs, {"timeout": DEFAULT_TIMEOUT, "transport": "ttheader", "protocol": "binary"})
        self.assertIs(client.middlewares[1], fake_base_compat_middleware.client_middleware)
        self.assertEqual(os.environ["LOAD_SERVICE_PSM"], DEFAULT_SOURCE_PSM)
        self.assertEqual(os.environ["SERVICE_CLUSTER"], DEFAULT_SOURCE_CLUSTER)

        class FakeContext:
            def __init__(self):
                self.persistent = {}

            def next(self, *args, **kwargs):
                return "next-called"

        ctx = FakeContext()
        self.assertEqual(client.middlewares[0](ctx), "next-called")
        self.assertEqual(ctx.persistent, {"cluster": DEFAULT_TARGET_CLUSTER, "env": "ppe_sirius2"})

    def test_getstate_drops_rpc_objects(self):
        op = self._op(_FakeFilterClient())
        state = op.__getstate__()

        self.assertIsNone(state["_client"])
        self.assertIsNone(state["_api_thrift"])

    def test_ensure_requester_env_sets_euler_source_identity(self):
        _ensure_requester_env("source.psm", "source-cluster")

        self.assertEqual(os.environ["PSM"], "source.psm")
        self.assertEqual(os.environ["TCE_CLUSTER"], "source-cluster")

    def test_load_thrift_reads_local_idl_like_existing_euler_mappers(self):
        _load_akc_admin_thrift.cache_clear()
        fake_thriftpy2 = types.ModuleType("thriftpy2")
        calls = []
        fake_module = object()

        def fake_load(*args, **kwargs):
            calls.append((args, kwargs))
            return fake_module

        fake_thriftpy2.load = fake_load

        with patch.dict(sys.modules, {"thriftpy2": fake_thriftpy2}):
            self.assertIs(_load_akc_admin_thrift(), fake_module)
            self.assertIs(_load_akc_admin_thrift(), fake_module)

        self.assertEqual(len(calls), 1)
        args, kwargs = calls[0]
        self.assertTrue(args[0].endswith("data_juicer/ops/filter/idl/akc/admin/akc_admin.thrift"))
        self.assertEqual(kwargs["module_name"], "data_juicer_akc_admin_thrift")
        self.assertEqual(len(kwargs["include_dirs"]), 1)
        self.assertTrue(kwargs["include_dirs"][0].endswith("data_juicer/ops/filter/idl/akc"))


if __name__ == "__main__":
    unittest.main()
