import re
import threading
from typing import Any

from loguru import logger

DEFAULT_NAMESPACE = "default"
_ACTOR_NAME_PREFIX = "dj_task_kv_store"
_ACTOR_HANDLE_CACHE: dict[str, Any] = {}
_WARNING_KEYS: set[str] = set()
_WARNING_KEYS_LOCK = threading.Lock()


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


def _log_kv_store_warning_once(action: str, exc: BaseException) -> None:
    key = f"{action}:{type(exc).__name__}:{str(exc)[:200]}"
    with _WARNING_KEYS_LOCK:
        if key in _WARNING_KEYS:
            return
        _WARNING_KEYS.add(key)
    logger.warning(
        "Task KV store [{}] failed; continuing without job-scoped runtime state: {}",
        action,
        exc,
    )


def _discard_cached_actor(actor) -> None:
    for actor_name, cached_actor in list(_ACTOR_HANDLE_CACHE.items()):
        if cached_actor is actor:
            _ACTOR_HANDLE_CACHE.pop(actor_name, None)


def _actor_or_none(action: str):
    try:
        return get_task_kv_store_actor()
    except Exception as exc:  # noqa: BLE001
        _log_kv_store_warning_once(f"{action}.get_actor", exc)
        return None


def _call_actor_method(action: str, actor, method_name: str, *args, wait: bool = False, default=None):
    try:
        ref = getattr(actor, method_name).remote(*args)
    except Exception as exc:  # noqa: BLE001
        _discard_cached_actor(actor)
        _log_kv_store_warning_once(f"{action}.remote", exc)
        return default

    if not wait:
        return ref

    ray = _try_import_ray()
    if ray is None:
        return default
    try:
        return ray.get(ref)
    except Exception as exc:  # noqa: BLE001
        _discard_cached_actor(actor)
        _log_kv_store_warning_once(f"{action}.get", exc)
        return default


def put_task_kv(key: str, value: Any, namespace: str = DEFAULT_NAMESPACE, wait: bool = False):
    actor = _actor_or_none("put")
    if actor is None:
        return None
    return _call_actor_method("put", actor, "put", _normalize_namespace(namespace), key, value, wait=wait)


def put_many_task_kv(mapping: dict[str, Any], namespace: str = DEFAULT_NAMESPACE, wait: bool = False):
    actor = _actor_or_none("put_many")
    if actor is None:
        return None
    return _call_actor_method(
        "put_many",
        actor,
        "put_many",
        _normalize_namespace(namespace),
        dict(mapping or {}),
        wait=wait,
    )


def incr_task_kv(
    key: str,
    delta: int | float = 1,
    namespace: str = DEFAULT_NAMESPACE,
    wait: bool = False,
):
    actor = _actor_or_none("incr")
    if actor is None:
        return None
    return _call_actor_method("incr", actor, "incr", _normalize_namespace(namespace), key, delta, wait=wait)


def get_task_kv(key: str, default: Any = None, namespace: str = DEFAULT_NAMESPACE):
    actor = _actor_or_none("get")
    if actor is None:
        return default
    return _call_actor_method(
        "get",
        actor,
        "get",
        _normalize_namespace(namespace),
        key,
        default,
        wait=True,
        default=default,
    )


def snapshot_task_kv(namespace: str | None = None):
    actor = _actor_or_none("snapshot")
    if actor is None:
        return {}
    normalized_namespace = None if namespace is None else _normalize_namespace(namespace)
    return _call_actor_method("snapshot", actor, "snapshot", normalized_namespace, wait=True, default={})


def clear_task_kv(namespace: str | None = None, wait: bool = True):
    actor = _actor_or_none("clear")
    if actor is None:
        return None
    normalized_namespace = None if namespace is None else _normalize_namespace(namespace)
    return _call_actor_method("clear", actor, "clear", normalized_namespace, wait=wait)
