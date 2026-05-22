import re
from typing import Any

DEFAULT_NAMESPACE = "default"
_ACTOR_NAME_PREFIX = "dj_task_kv_store"
_ACTOR_HANDLE_CACHE: dict[str, Any] = {}


class TaskKVStoreActor:
    """Small Ray actor used for job-scoped runtime key-value state."""

    def __init__(self):
        self._store: dict[str, dict[str, Any]] = {}

    def put(self, namespace: str, key: str, value: Any):
        self._namespace(namespace)[key] = value

    def put_many(self, namespace: str, mapping: dict[str, Any]):
        self._namespace(namespace).update(dict(mapping or {}))

    def get(self, namespace: str, key: str, default: Any = None):
        return self._store.get(namespace, {}).get(key, default)

    def incr(self, namespace: str, key: str, delta: int | float = 1):
        namespace_store = self._namespace(namespace)
        namespace_store[key] = namespace_store.get(key, 0) + delta
        return namespace_store[key]

    def snapshot(self, namespace: str | None = None):
        if namespace is None:
            return {name: dict(values) for name, values in self._store.items()}
        return dict(self._store.get(namespace, {}))

    def clear(self, namespace: str | None = None):
        if namespace is None:
            self._store.clear()
        else:
            self._store.pop(namespace, None)

    def _namespace(self, namespace: str):
        return self._store.setdefault(namespace, {})


def _try_import_ray():
    try:
        import ray
    except ImportError:
        return None
    return ray


def _normalize_name_part(value: Any) -> str:
    return re.sub(r"[^0-9A-Za-z_]", "_", str(value or "default"))


def task_kv_store_actor_name(job_id: str | None = None) -> str:
    return f"{_ACTOR_NAME_PREFIX}_{_normalize_name_part(job_id)}"


def _normalize_namespace(namespace: str | None) -> str:
    return str(namespace or DEFAULT_NAMESPACE)


def get_task_kv_store_actor():
    ray = _try_import_ray()
    if ray is None or not ray.is_initialized():
        return None

    job_id = ray.get_runtime_context().get_job_id()
    actor_name = task_kv_store_actor_name(job_id)
    if actor_name in _ACTOR_HANDLE_CACHE:
        return _ACTOR_HANDLE_CACHE[actor_name]

    try:
        actor = ray.get_actor(actor_name)
    except ValueError:
        actor = None
    if actor is not None:
        _ACTOR_HANDLE_CACHE[actor_name] = actor
        return actor

    remote_actor = ray.remote(TaskKVStoreActor)
    try:
        actor = remote_actor.options(name=actor_name, num_cpus=0, lifetime="detached").remote()
    except TypeError:
        actor = remote_actor.options(name=actor_name, num_cpus=0).remote()
    except ValueError:
        actor = ray.get_actor(actor_name)
    _ACTOR_HANDLE_CACHE[actor_name] = actor
    return actor


def put_task_kv(key: str, value: Any, namespace: str = DEFAULT_NAMESPACE, wait: bool = False):
    actor = get_task_kv_store_actor()
    if actor is None:
        return None
    ref = actor.put.remote(_normalize_namespace(namespace), key, value)
    if wait:
        ray = _try_import_ray()
        return ray.get(ref)
    return ref


def put_many_task_kv(mapping: dict[str, Any], namespace: str = DEFAULT_NAMESPACE, wait: bool = False):
    actor = get_task_kv_store_actor()
    if actor is None:
        return None
    ref = actor.put_many.remote(_normalize_namespace(namespace), dict(mapping or {}))
    if wait:
        ray = _try_import_ray()
        return ray.get(ref)
    return ref


def incr_task_kv(
    key: str,
    delta: int | float = 1,
    namespace: str = DEFAULT_NAMESPACE,
    wait: bool = False,
):
    actor = get_task_kv_store_actor()
    if actor is None:
        return None
    ref = actor.incr.remote(_normalize_namespace(namespace), key, delta)
    if wait:
        ray = _try_import_ray()
        return ray.get(ref)
    return ref


def get_task_kv(key: str, default: Any = None, namespace: str = DEFAULT_NAMESPACE):
    actor = get_task_kv_store_actor()
    if actor is None:
        return default
    ray = _try_import_ray()
    return ray.get(actor.get.remote(_normalize_namespace(namespace), key, default))


def snapshot_task_kv(namespace: str | None = None):
    actor = get_task_kv_store_actor()
    if actor is None:
        return {}
    ray = _try_import_ray()
    normalized_namespace = None if namespace is None else _normalize_namespace(namespace)
    return ray.get(actor.snapshot.remote(normalized_namespace))


def clear_task_kv(namespace: str | None = None, wait: bool = True):
    actor = get_task_kv_store_actor()
    if actor is None:
        return None
    normalized_namespace = None if namespace is None else _normalize_namespace(namespace)
    ref = actor.clear.remote(normalized_namespace)
    if wait:
        ray = _try_import_ray()
        return ray.get(ref)
    return ref
