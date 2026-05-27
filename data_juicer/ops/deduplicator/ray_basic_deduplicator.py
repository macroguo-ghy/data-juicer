from abc import ABC, abstractmethod
from typing import Union

from data_juicer.utils.constant import HashKeys
from data_juicer.utils.lazy_loader import LazyLoader

from ..base_op import Filter

ray = LazyLoader("ray")
redis = LazyLoader("redis")

MERSENNE_PRIME = (1 << 61) - 1


class DedupSet:
    def __init__(self):
        self.hash_record = set()
        self.row_decisions = {}

    def is_unique(self, key, row_id=None):
        return self.is_unique_many([key], [row_id] if row_id is not None else None)[0]

    def is_unique_many(self, keys, row_ids=None):
        row_ids = row_ids or [None] * len(keys)
        decisions = []
        for key, row_id in zip(keys, row_ids):
            if row_id is not None:
                row_decision_key = (key, str(row_id))
                if row_decision_key in self.row_decisions:
                    decisions.append(self.row_decisions[row_decision_key])
                    continue

            is_unique = key not in self.hash_record
            if is_unique:
                self.hash_record.add(key)
            if row_id is not None:
                self.row_decisions[(key, str(row_id))] = is_unique
            decisions.append(is_unique)
        return decisions


def get_remote_dedup_set():
    """Get the remote version of DedupSet with Ray decorator applied at runtime."""
    return ray.remote(scheduling_strategy="SPREAD")(DedupSet)


class Backend(ABC):
    """
    Backend for deduplicator.
    """

    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def is_unique(self, md5_value: str):
        pass


class ActorBackend(Backend):
    """
    Ray actor backend for deduplicator.
    Uses lazy initialization as a fallback, while Ray Data paths should
    prepare actors on the driver before task serialization so all tasks share
    the same deduplication state.
    """

    def __init__(self, dedup_set_num: Union[int, str], RemoteDedupSet=None):
        # Store config but don't create actors yet.
        # dedup_set_num can be int or "auto".
        self._dedup_set_num_config = dedup_set_num
        self._RemoteDedupSet = RemoteDedupSet
        self._dedup_sets = None  # Lazy fallback for direct backend use.
        self._actual_dedup_set_num = None

    @property
    def dedup_set_num(self):
        """Get actual dedup_set_num, calculating from cluster resources if 'auto'."""
        if self._actual_dedup_set_num is None:
            if self._dedup_set_num_config == "auto":
                self._actual_dedup_set_num = max(1, int(ray.cluster_resources().get("CPU", 1) / 2))
            else:
                self._actual_dedup_set_num = int(self._dedup_set_num_config)
        return self._actual_dedup_set_num

    def _ensure_actors(self):
        """Create actors once and keep the shared actor handles on this backend."""
        if self._dedup_sets is None:
            RemoteDedupSet = self._RemoteDedupSet or get_remote_dedup_set()
            self._dedup_sets = [RemoteDedupSet.remote() for _ in range(self.dedup_set_num)]

    def _dedup_set_id(self, md5_value: str) -> int:
        return int.from_bytes(md5_value.encode(), byteorder="little") % MERSENNE_PRIME % self.dedup_set_num

    def prepare_for_ray_tasks(self):
        """Create actor handles before this backend is serialized to Ray tasks."""
        self._ensure_actors()

    def is_unique(self, md5_value: str, row_id=None):
        return self.is_unique_many([md5_value], [row_id] if row_id is not None else None)[0]

    def is_unique_many(self, md5_values: list[str], row_ids=None):
        if not md5_values:
            return []
        self._ensure_actors()
        row_ids = row_ids or [None] * len(md5_values)
        shard_items = {}
        for index, (md5_value, row_id) in enumerate(zip(md5_values, row_ids)):
            shard_items.setdefault(self._dedup_set_id(md5_value), []).append((index, md5_value, row_id))

        grouped_calls = []
        for dedup_set_id, items in shard_items.items():
            actor = self._dedup_sets[dedup_set_id]
            if hasattr(actor, "is_unique_many"):
                grouped_calls.append(
                    (
                        items,
                        actor.is_unique_many.remote(
                            [item[1] for item in items],
                            [item[2] for item in items],
                        ),
                    )
                )
            else:
                grouped_calls.append(
                    (
                        items,
                        [
                            actor.is_unique.remote(md5_value)
                            for _, md5_value, _ in items
                        ],
                    )
                )

        decisions = [False] * len(md5_values)
        for items, future_or_futures in grouped_calls:
            shard_decisions = ray.get(future_or_futures)
            for (index, _, _), decision in zip(items, shard_decisions):
                decisions[index] = decision
        return decisions


class RedisBackend(Backend):
    """
    Redis backend for deduplicator.
    """

    def __init__(self, redis_address: str):
        self.redis_address = redis_address
        self.redis_client = redis.from_url(url=self.redis_address)
        self.redis_client.flushdb(0)

    def is_unique(self, md5_value: str):
        return self.redis_client.setnx(md5_value, 1)


class RayBasicDeduplicator(Filter):
    """
    A basic exact matching deduplicator for RAY.
    Although its functionality is deduplication,
    it is implemented as Filter sub-class.
    """

    # TODO: Set a more reasonable value
    EMPTY_HASH_VALUE = "EMPTY"

    def __init__(
        self,
        backend: str = "ray_actor",
        redis_address: str = "redis://localhost:6379",
        dedup_set_num: Union[int, str] = "auto",
        *args,
        **kwargs,
    ):
        """
        Initialization.
        :param backend: the backend for dedup, either 'ray_actor' or 'redis'
        :param redis_address: the address of redis server
        :param dedup_set_num: number of dedup set actors, or 'auto' to use CPU/2
        :param args: extra args
        :param kwargs: extra args
        """
        super().__init__(*args, **kwargs)
        self.redis_address = redis_address
        self.backend = backend
        if backend == "ray_actor":
            # Pass dedup_set_num directly - ActorBackend handles "auto" lazily
            self.backend = ActorBackend(dedup_set_num)
        elif backend == "redis":
            # TODO: add a barrier to ensure that flushdb is performed before
            # the operator is called
            self.backend = RedisBackend(redis_address)
        else:
            raise ValueError(f"Unknown backend: {backend}")

    def prepare_backend_for_ray_tasks(self):
        """Prepare shared state before Ray Data serializes this operator to tasks."""
        if isinstance(self.backend, ActorBackend):
            self.backend.prepare_for_ray_tasks()

    def calculate_hash(self, sample, context=False):
        """Calculate hash value for the sample."""
        raise NotImplementedError

    def compute_stats_single(self, sample, context=False):
        # compute hash
        md5_value = self.calculate_hash(sample, context)
        # check existing
        sample[HashKeys.is_unique] = self.backend.is_unique(md5_value)
        return sample

    def process_single(self, sample):
        return sample[HashKeys.is_unique]
