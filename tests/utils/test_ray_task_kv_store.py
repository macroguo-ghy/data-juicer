import unittest
from unittest.mock import patch

from data_juicer.utils.ray_task_kv_store import (
    TaskKVStoreActor,
    clear_task_kv,
    get_task_kv,
    incr_task_kv,
    put_many_task_kv,
    put_task_kv,
    snapshot_task_kv,
    task_kv_store_actor_name,
)


def _ray_put_and_incr(namespace, index):
    from data_juicer.utils.ray_task_kv_store import incr_task_kv, put_task_kv

    put_task_kv(f"worker_{index}", index, namespace=namespace, wait=True)
    return incr_task_kv("worker_count", namespace=namespace, wait=True)


class TaskKVStoreActorTest(unittest.TestCase):
    def test_actor_stores_values_by_namespace(self):
        actor = TaskKVStoreActor()

        actor.put("ocr", "success", 1)
        actor.incr("ocr", "success", 2)
        actor.put_many("vlm", {"429": 3, "model": "seed"})

        self.assertEqual(actor.get("ocr", "success"), 3)
        self.assertEqual(actor.get("missing", "success", "fallback"), "fallback")
        self.assertEqual(actor.snapshot("ocr"), {"success": 3})
        self.assertEqual(actor.snapshot("vlm"), {"429": 3, "model": "seed"})
        self.assertEqual(
            actor.snapshot(),
            {
                "ocr": {"success": 3},
                "vlm": {"429": 3, "model": "seed"},
            },
        )

        actor.clear("ocr")
        self.assertEqual(actor.snapshot("ocr"), {})
        actor.clear()
        self.assertEqual(actor.snapshot(), {})

    def test_actor_name_normalizes_job_id(self):
        self.assertEqual(task_kv_store_actor_name("job-1.2"), "dj_task_kv_store_job_1_2")

    def test_helpers_are_noop_when_ray_is_unavailable(self):
        class RayUnavailable:
            @staticmethod
            def is_initialized():
                return False

        with patch("data_juicer.utils.ray_task_kv_store._try_import_ray", return_value=RayUnavailable):
            self.assertIsNone(put_task_kv("key", "value"))
            self.assertIsNone(incr_task_kv("count"))
            self.assertEqual(get_task_kv("key", default="fallback"), "fallback")
            self.assertEqual(snapshot_task_kv(), {})
            self.assertIsNone(clear_task_kv())


class RayTaskKVStoreIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            import ray
        except ImportError:
            raise unittest.SkipTest("ray is not installed")

        cls.ray = ray
        if not ray.is_initialized():
            ray.init(num_cpus=2, ignore_reinit_error=True)
            cls._started_ray = True
        else:
            cls._started_ray = False

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "_started_ray", False):
            cls.ray.shutdown()

    def setUp(self):
        clear_task_kv()

    def tearDown(self):
        clear_task_kv()

    def test_driver_helpers_share_values_through_named_actor(self):
        put_task_kv("last_qps", 12.5, namespace="ocr", wait=True)
        self.assertEqual(incr_task_kv("success_count", 2, namespace="ocr", wait=True), 2)
        put_many_task_kv({"rpm": 2500, "tpm": 5000000}, namespace="vlm", wait=True)

        self.assertEqual(get_task_kv("last_qps", namespace="ocr"), 12.5)
        self.assertEqual(snapshot_task_kv("ocr"), {"last_qps": 12.5, "success_count": 2})
        self.assertEqual(snapshot_task_kv("vlm"), {"rpm": 2500, "tpm": 5000000})

    def test_async_helpers_return_refs(self):
        put_ref = put_task_kv("qps", 200, namespace="async")
        put_many_ref = put_many_task_kv({"rpm": 2500}, namespace="async")
        incr_ref = incr_task_kv("success", 3, namespace="async")

        self.ray.get([put_ref, put_many_ref])
        self.assertEqual(self.ray.get(incr_ref), 3)
        self.assertEqual(snapshot_task_kv("async"), {"qps": 200, "rpm": 2500, "success": 3})

        clear_ref = clear_task_kv("async", wait=False)
        self.ray.get(clear_ref)
        self.assertEqual(snapshot_task_kv("async"), {})

    def test_workers_share_one_job_scoped_actor(self):
        remote_put = self.ray.remote(_ray_put_and_incr)

        counts = self.ray.get([remote_put.remote("workers", index) for index in range(8)])

        self.assertEqual(sorted(counts), list(range(1, 9)))
        snapshot = snapshot_task_kv("workers")
        self.assertEqual(snapshot["worker_count"], 8)
        for index in range(8):
            self.assertEqual(snapshot[f"worker_{index}"], index)


if __name__ == "__main__":
    unittest.main()
