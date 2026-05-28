from __future__ import annotations

import asyncio
from collections import defaultdict, deque
import hashlib
import re
import time

RPC_RATE_LIMIT_WINDOW_SECONDS = 1.0


class RayJobRpcQpsRateLimiter:
    def __init__(self):
        self._request_events = defaultdict(deque)
        self._next_available_at = defaultdict(float)
        self._limits = {}

    def register(self, key: str, qps: int | None) -> None:
        self._update_limit(key, qps)

    async def acquire(self, key: str, qps: int | None) -> None:
        self._update_limit(key, qps)
        while True:
            limit = self._limits.get(key)
            if limit is None:
                return
            now = time.monotonic()
            self._prune(key, now)
            wait_seconds = 0.0
            if now < self._next_available_at[key]:
                wait_seconds = max(wait_seconds, self._next_available_at[key] - now)
            if len(self._request_events[key]) >= limit:
                wait_seconds = max(
                    wait_seconds,
                    RPC_RATE_LIMIT_WINDOW_SECONDS - (now - self._request_events[key][0]),
                )
            if wait_seconds <= 0:
                self._request_events[key].append(now)
                self._next_available_at[key] = now + (RPC_RATE_LIMIT_WINDOW_SECONDS / limit)
                return
            await asyncio.sleep(wait_seconds)

    def _update_limit(self, key: str, qps: int | None) -> None:
        if qps is None:
            return
        self._limits[key] = qps if key not in self._limits else min(self._limits[key], qps)

    def _prune(self, key: str, now: float) -> None:
        while self._request_events[key] and now - self._request_events[key][0] >= RPC_RATE_LIMIT_WINDOW_SECONDS:
            self._request_events[key].popleft()


def validate_qps(qps: int | None) -> None:
    if qps is not None and qps <= 0:
        raise ValueError("qps must be positive when set")


def try_import_ray():
    try:
        import ray
    except ImportError:
        return None
    return ray


def rpc_rate_limiter_actor_name(job_id: str, limiter_key: str) -> str:
    normalized_job = re.sub(r"[^0-9A-Za-z_]", "_", job_id or "default")
    key_digest = hashlib.sha1(limiter_key.encode("utf-8")).hexdigest()[:12]
    return f"dj_rpc_qps_rate_limiter_{normalized_job}_{key_digest}"


class RpcQpsRateLimiter:
    def __init__(self, qps: int | None, key: str):
        validate_qps(qps)
        self.qps = qps
        self.key = key
        self._request_events = deque()
        self._next_available_at = 0.0
        self._actor = None
        self.actor_name = None

    def setup_ray_actor(self) -> None:
        self._actor = None
        self.actor_name = None
        if self.qps is None:
            return
        ray = try_import_ray()
        if ray is None or not ray.is_initialized():
            return
        actor_name = rpc_rate_limiter_actor_name(ray.get_runtime_context().get_job_id(), self.key)
        try:
            actor = ray.get_actor(actor_name)
        except ValueError:
            actor = ray.remote(RayJobRpcQpsRateLimiter).options(name=actor_name, num_cpus=0).remote()
        ray.get(actor.register.remote(self.key, self.qps))
        self._actor = actor
        self.actor_name = actor_name

    def acquire(self) -> None:
        if self.qps is None:
            return
        if self._actor is not None:
            ray = try_import_ray()
            if ray is not None:
                ray.get(self._actor.acquire.remote(self.key, self.qps))
                return
        while True:
            now = time.monotonic()
            self._prune(now)
            wait_seconds = 0.0
            if self._next_available_at > now:
                wait_seconds = max(wait_seconds, self._next_available_at - now)
            if len(self._request_events) >= self.qps:
                wait_seconds = max(
                    wait_seconds,
                    RPC_RATE_LIMIT_WINDOW_SECONDS - (now - self._request_events[0]),
                )
            if wait_seconds <= 0:
                self._request_events.append(now)
                self._next_available_at = now + (RPC_RATE_LIMIT_WINDOW_SECONDS / self.qps)
                return
            time.sleep(wait_seconds)

    def _prune(self, now: float) -> None:
        while self._request_events and now - self._request_events[0] >= RPC_RATE_LIMIT_WINDOW_SECONDS:
            self._request_events.popleft()
