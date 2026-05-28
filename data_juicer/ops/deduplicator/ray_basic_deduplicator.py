from abc import ABC, abstractmethod
from typing import Union

from loguru import logger

from data_juicer.utils.constant import HashKeys
from data_juicer.utils.lazy_loader import LazyLoader

from ..base_op import Filter

ray = LazyLoader("ray")
redis = LazyLoader("redis")

MERSENNE_PRIME = (1 << 61) - 1
DEFAULT_ACTOR_GET_TIMEOUT = 600.0
DEFAULT_ACTOR_GET_RETRY_TIMES = 2
_RETRYABLE_RAY_GET_ERROR_NAMES = {"GetTimeoutError", "ActorUnavailableError"}


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

    def __init__(
        self,
        dedup_set_num: Union[int, str],
        RemoteDedupSet=None,
        actor_get_timeout: float | None = DEFAULT_ACTOR_GET_TIMEOUT,
        actor_get_retry_times: int = DEFAULT_ACTOR_GET_RETRY_TIMES,
    ):
        # Store config but don't create actors yet.
        # dedup_set_num can be int or "auto".
        self._dedup_set_num_config = dedup_set_num
        self._RemoteDedupSet = RemoteDedupSet
        self._dedup_sets = None  # Lazy fallback for direct backend use.
        self._actual_dedup_set_num = None
        self.actor_get_timeout = None if actor_get_timeout is None else float(actor_get_timeout)
        if self.actor_get_timeout is not None and self.actor_get_timeout <= 0:
            raise ValueError("actor_get_timeout must be positive or None")
        self.actor_get_retry_times = int(actor_get_retry_times)
        if self.actor_get_retry_times < 1:
            raise ValueError("actor_get_retry_times must be at least 1")

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
                        dedup_set_id,
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
                        dedup_set_id,
                        items,
                        [
                            actor.is_unique.remote(md5_value)
                            for _, md5_value, _ in items
                        ],
                    )
                )

        decisions = [False] * len(md5_values)
        for dedup_set_id, items, future_or_futures in grouped_calls:
            shard_decisions = self._get_actor_result(
                future_or_futures,
                dedup_set_id=dedup_set_id,
                row_count=len(items),
            )
            for (index, _, _), decision in zip(items, shard_decisions):
                decisions[index] = decision
        return decisions

    def _get_actor_result(self, future_or_futures, dedup_set_id: int, row_count: int):
        last_retryable_error = None
        for attempt in range(1, self.actor_get_retry_times + 1):
            try:
                if self.actor_get_timeout is None:
                    return ray.get(future_or_futures)
                return ray.get(future_or_futures, timeout=self.actor_get_timeout)
            except Exception as exc:
                if not self._is_retryable_ray_get_error(exc):
                    raise RuntimeError(
                        "Ray dedup actor call failed: "
                        f"shard_id={dedup_set_id}, rows={row_count}, "
                        f"dedup_set_num={self.dedup_set_num}, error={exc}"
                    ) from exc
                last_retryable_error = exc
                if attempt < self.actor_get_retry_times:
                    logger.warning(
                        "Ray dedup actor result unavailable; waiting again on the same ObjectRef: "
                        "shard_id={}, rows={}, dedup_set_num={}, timeout_seconds={}, "
                        "error_type={}, attempt={}/{}",
                        dedup_set_id,
                        row_count,
                        self.dedup_set_num,
                        self.actor_get_timeout,
                        exc.__class__.__name__,
                        attempt,
                        self.actor_get_retry_times,
                    )

        if self._is_ray_get_timeout(last_retryable_error):
            raise TimeoutError(
                "Ray dedup actor call timed out: "
                f"shard_id={dedup_set_id}, rows={row_count}, "
                f"dedup_set_num={self.dedup_set_num}, timeout_seconds={self.actor_get_timeout}, "
                f"attempts={self.actor_get_retry_times}"
            ) from last_retryable_error

        raise RuntimeError(
            "Ray dedup actor call failed after retries: "
            f"shard_id={dedup_set_id}, rows={row_count}, dedup_set_num={self.dedup_set_num}, "
            f"error_type={last_retryable_error.__class__.__name__}, "
            f"attempts={self.actor_get_retry_times}, error={last_retryable_error}"
        ) from last_retryable_error

    @staticmethod
    def _is_ray_get_timeout(exc: BaseException | None) -> bool:
        return isinstance(exc, TimeoutError) or (
            exc is not None and exc.__class__.__name__ == "GetTimeoutError"
        )

    @classmethod
    def _is_retryable_ray_get_error(cls, exc: BaseException) -> bool:
        return cls._is_ray_get_timeout(exc) or exc.__class__.__name__ in _RETRYABLE_RAY_GET_ERROR_NAMES


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
        actor_get_timeout: float | None = DEFAULT_ACTOR_GET_TIMEOUT,
        actor_get_retry_times: int = DEFAULT_ACTOR_GET_RETRY_TIMES,
        *args,
        **kwargs,
    ):
        """
        Initialization.
        :param backend: the backend for dedup, either 'ray_actor' or 'redis'
        :param redis_address: the address of redis server
        :param dedup_set_num: number of dedup set actors, or 'auto' to use CPU/2
        :param actor_get_timeout: max seconds to wait for a Ray actor result, or None to wait forever
        :param actor_get_retry_times: number of times to wait on the same actor result before failing
        :param args: extra args
        :param kwargs: extra args
        """
        super().__init__(*args, **kwargs)
        self.redis_address = redis_address
        self.backend = backend
        if backend == "ray_actor":
            # Pass dedup_set_num directly - ActorBackend handles "auto" lazily
            self.backend = ActorBackend(
                dedup_set_num,
                actor_get_timeout=actor_get_timeout,
                actor_get_retry_times=actor_get_retry_times,
            )
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
