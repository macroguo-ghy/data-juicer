import pickle
import types
import unittest
from unittest.mock import Mock, patch

from data_juicer.core.data import NestedDataset as Dataset

from data_juicer.ops.deduplicator.ray_basic_deduplicator import ActorBackend
from data_juicer.ops.deduplicator.ray_document_deduplicator import \
    RayDocumentDeduplicator
from data_juicer.utils.constant import Fields
from data_juicer.utils.lazy_loader import LazyLoader
from data_juicer.utils.unittest_utils import DataJuicerTestCaseBase, TEST_TAG


class _FakeDedupActor:
    def __init__(self, actor_id):
        self.actor_id = actor_id


class _FakeRemoteDedupSet:
    created = []

    @classmethod
    def remote(cls):
        actor = _FakeDedupActor(len(cls.created))
        cls.created.append(actor)
        return actor


class _FakeRemoteMethod:
    def __init__(self, actor, method_name):
        self.actor = actor
        self.method_name = method_name

    def remote(self, keys, row_ids):
        ref = {
            "actor_id": self.actor.actor_id,
            "method": self.method_name,
            "keys": list(keys),
            "row_ids": list(row_ids),
        }
        self.actor.calls.append(ref)
        return ref


class _FakeBatchDedupActor:
    def __init__(self, actor_id):
        self.actor_id = actor_id
        self.calls = []
        self.is_unique_many = _FakeRemoteMethod(self, "is_unique_many")


class _FakeBatchRemoteDedupSet:
    created = []

    @classmethod
    def remote(cls):
        actor = _FakeBatchDedupActor(len(cls.created))
        cls.created.append(actor)
        return actor


class ActorUnavailableError(Exception):
    pass


class RayDocumentDeduplicatorTest(DataJuicerTestCaseBase):
    def _skip_unless_ray_tag(self):
        if not getattr(self, 'current_tag', 'standalone').startswith('ray'):
            self.skipTest('requires ray test tag')

    def _run_doc_dedup(self, dataset: Dataset, target_list, op):
        res_list = self.run_single_op(dataset, op, [op.text_key])
        res_list.sort(key=lambda x: x['text'])
        target_list.sort(key=lambda x: x['text'])
        self.assertEqual(res_list, target_list)

    @TEST_TAG("ray")
    def test_english_deduplication(self):
        self._skip_unless_ray_tag()
        ds_list = [
            {
                'text': 'Today is Sunday and it\'s a happy day!'
            },
            {
                'text': 'Do you need a cup of coffee?'
            },
            {
                'text': 'Today is sunday and it\'s a happy day!'
            },
            {
                'text':
                'This paper proposed a novel method on LLM pretraining.'
            },
            {
                'text':
                'This paper proposed a novel method on LLM pretraining.'
            },
        ]
        tgt_list = [{
            'text': 'Today is Sunday and it\'s a happy day!'
        }, {
            'text': 'Do you need a cup of coffee?'
        }, {
            'text': 'Today is sunday and it\'s a happy day!'
        }, {
            'text':
            'This paper proposed a novel method on LLM pretraining.'
        }]
        dataset = self.generate_dataset(ds_list)
        op = RayDocumentDeduplicator(lowercase=False, ignore_non_character=False)
        self._run_doc_dedup(dataset, tgt_list, op)

    def test_md5_text_key_preserves_duplicate_relation(self):
        op = RayDocumentDeduplicator(text_key='md5', lowercase=False, ignore_non_character=False)

        self.assertEqual(
            op.calculate_hash({'md5': 'same-image-md5'}),
            op.calculate_hash({'md5': 'same-image-md5'}),
        )
        self.assertNotEqual(
            op.calculate_hash({'md5': 'same-image-md5'}),
            op.calculate_hash({'md5': 'different-image-md5'}),
        )

    def test_actor_backend_prepare_reuses_actors_after_task_serialization(self):
        _FakeRemoteDedupSet.created = []
        backend = ActorBackend(dedup_set_num=3, RemoteDedupSet=_FakeRemoteDedupSet)

        backend.prepare_for_ray_tasks()

        self.assertEqual(len(_FakeRemoteDedupSet.created), 3)
        serialized = pickle.dumps(backend)
        task_backends = [pickle.loads(serialized) for _ in range(4)]

        for task_backend in task_backends:
            task_backend.prepare_for_ray_tasks()

        self.assertEqual(len(_FakeRemoteDedupSet.created), 3)
        for task_backend in task_backends:
            self.assertEqual(
                [actor.actor_id for actor in task_backend._dedup_sets],
                [0, 1, 2],
            )

    def test_actor_backend_retries_same_future_after_get_timeout(self):
        _FakeBatchRemoteDedupSet.created = []
        backend = ActorBackend(
            dedup_set_num=1,
            RemoteDedupSet=_FakeBatchRemoteDedupSet,
            actor_get_timeout=600,
            actor_get_retry_times=2,
        )

        ray_get = Mock(side_effect=[TimeoutError("first wait timed out"), [True, False]])
        with patch(
            "data_juicer.ops.deduplicator.ray_basic_deduplicator.ray",
            types.SimpleNamespace(get=ray_get),
        ):
            decisions = backend.is_unique_many(["a", "b"], ["row-a", "row-b"])

        self.assertEqual(decisions, [True, False])
        self.assertEqual(ray_get.call_count, 2)
        self.assertEqual(ray_get.call_args_list[0].kwargs["timeout"], 600)
        self.assertEqual(ray_get.call_args_list[1].kwargs["timeout"], 600)
        self.assertIs(ray_get.call_args_list[0].args[0], ray_get.call_args_list[1].args[0])
        self.assertEqual(len(_FakeBatchRemoteDedupSet.created[0].calls), 1)

    def test_actor_backend_retries_same_future_after_actor_unavailable(self):
        _FakeBatchRemoteDedupSet.created = []
        backend = ActorBackend(
            dedup_set_num=1,
            RemoteDedupSet=_FakeBatchRemoteDedupSet,
            actor_get_timeout=600,
            actor_get_retry_times=2,
        )

        ray_get = Mock(side_effect=[ActorUnavailableError("temporarily unavailable"), [True, False]])
        with patch(
            "data_juicer.ops.deduplicator.ray_basic_deduplicator.ray",
            types.SimpleNamespace(get=ray_get),
        ):
            decisions = backend.is_unique_many(["a", "b"], ["row-a", "row-b"])

        self.assertEqual(decisions, [True, False])
        self.assertEqual(ray_get.call_count, 2)
        self.assertEqual(ray_get.call_args_list[0].kwargs["timeout"], 600)
        self.assertEqual(ray_get.call_args_list[1].kwargs["timeout"], 600)
        self.assertIs(ray_get.call_args_list[0].args[0], ray_get.call_args_list[1].args[0])
        self.assertEqual(len(_FakeBatchRemoteDedupSet.created[0].calls), 1)

    def test_actor_backend_raises_clear_error_after_get_timeouts(self):
        _FakeBatchRemoteDedupSet.created = []
        backend = ActorBackend(
            dedup_set_num=1,
            RemoteDedupSet=_FakeBatchRemoteDedupSet,
            actor_get_timeout=600,
            actor_get_retry_times=2,
        )

        ray_get = Mock(side_effect=TimeoutError("still waiting"))
        with patch(
            "data_juicer.ops.deduplicator.ray_basic_deduplicator.ray",
            types.SimpleNamespace(get=ray_get),
        ):
            with self.assertRaisesRegex(
                TimeoutError,
                "Ray dedup actor call timed out.*shard_id=0.*rows=2.*attempts=2",
            ):
                backend.is_unique_many(["a", "b"], ["row-a", "row-b"])

    def test_actor_backend_wraps_non_timeout_actor_errors(self):
        _FakeBatchRemoteDedupSet.created = []
        backend = ActorBackend(
            dedup_set_num=1,
            RemoteDedupSet=_FakeBatchRemoteDedupSet,
            actor_get_timeout=600,
            actor_get_retry_times=2,
        )

        ray_get = Mock(side_effect=RuntimeError("actor died"))
        with patch(
            "data_juicer.ops.deduplicator.ray_basic_deduplicator.ray",
            types.SimpleNamespace(get=ray_get),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "Ray dedup actor call failed.*shard_id=0.*actor died",
            ):
                backend.is_unique_many(["a"], ["row-a"])

    @TEST_TAG("ray")
    def test_global_deduplication_across_ray_blocks(self):
        self._skip_unless_ray_tag()

        ray = LazyLoader("ray")
        from data_juicer.core.data.ray_dataset import RayDataset

        ds_list = [
            {'text': 'duplicate', 'id': 0, Fields.stats: {}},
            {'text': 'unique-a', 'id': 1, Fields.stats: {}},
            {'text': 'duplicate', 'id': 2, Fields.stats: {}},
            {'text': 'unique-b', 'id': 3, Fields.stats: {}},
            {'text': 'duplicate', 'id': 4, Fields.stats: {}},
            {'text': 'unique-c', 'id': 5, Fields.stats: {}},
            {'text': 'duplicate', 'id': 6, Fields.stats: {}},
            {'text': 'unique-d', 'id': 7, Fields.stats: {}},
        ]
        dataset = RayDataset(ray.data.from_items(ds_list, override_num_blocks=4))
        op = RayDocumentDeduplicator(
            lowercase=False,
            ignore_non_character=False,
            dedup_set_num=2,
            batch_size=1,
            num_proc=4,
        )

        res_list = dataset.process(op).data.take_all()
        text_counts = {}
        for sample in res_list:
            text_counts[sample['text']] = text_counts.get(sample['text'], 0) + 1

        self.assertEqual(text_counts['duplicate'], 1)
        self.assertEqual(len(res_list), 5)

    @TEST_TAG("ray")
    def test_chinese_deduplication(self):
        self._skip_unless_ray_tag()
        ds_list = [
            {
                'text': '你好，请问你是谁'
            },
            {
                'text': '欢迎来到阿里巴巴！'
            },
            {
                'text':
                '第九届会议\n2003年7月28日至8月8日\n牙买加金斯敦\n为来自发展中国家的法'
                '律和技术委员会以及财务委员会成员\n参加委员会会议支付费用的方式\n1.'
            },
            {
                'text':
                '第九届会议\n2003年7月28日至8月8日\n牙买加金斯敦\n为来自发展中国家的法'
                '律和技术委员会以及财务委员会成员\n参加委员会会议支付费用的方式\n1.'
            },
            {
                'text':
                '第九届会议\n时间：2003年7月28日至8月8日\n牙买加金斯敦\n为来自发展中国家的法'
                '律和技术委员会以及财务委员会成员\n参加委员会会议支付费用的方式\n1.'
            },
        ]
        tgt_list = [
            {
                'text': '你好，请问你是谁'
            },
            {
                'text': '欢迎来到阿里巴巴！'
            },
            {
                'text':
                '第九届会议\n2003年7月28日至8月8日\n牙买加金斯敦\n为来自发展中国家的法'
                '律和技术委员会以及财务委员会成员\n参加委员会会议支付费用的方式\n1.'
            },
            {
                'text':
                '第九届会议\n时间：2003年7月28日至8月8日\n牙买加金斯敦\n为来自发展中国家的法'
                '律和技术委员会以及财务委员会成员\n参加委员会会议支付费用的方式\n1.'
            },
        ]
        dataset = self.generate_dataset(ds_list)
        op = RayDocumentDeduplicator(lowercase=False, ignore_non_character=False)
        self._run_doc_dedup(dataset, tgt_list, op)


if __name__ == '__main__':
    unittest.main()
