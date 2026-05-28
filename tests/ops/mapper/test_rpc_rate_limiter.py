import asyncio
import unittest
from unittest.mock import patch

from data_juicer.ops.mapper.rpc_rate_limiter import RayJobRpcQpsRateLimiter, RpcQpsRateLimiter


class RpcQpsRateLimiterTest(unittest.TestCase):
    def test_local_qps_rate_limiter_smooths_requests(self):
        limiter = RpcQpsRateLimiter(qps=2, key="rpc")
        clock = {"now": 0.0}
        sleeps = []

        def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        with patch("data_juicer.ops.mapper.rpc_rate_limiter.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("data_juicer.ops.mapper.rpc_rate_limiter.time.sleep", side_effect=fake_sleep):
                limiter.acquire()
                limiter.acquire()

        self.assertEqual(sleeps, [0.5])

    def test_ray_job_qps_rate_limiter_smooths_requests(self):
        limiter = RayJobRpcQpsRateLimiter()
        clock = {"now": 0.0}
        sleeps = []

        async def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        with patch("data_juicer.ops.mapper.rpc_rate_limiter.time.monotonic", side_effect=lambda: clock["now"]):
            with patch("data_juicer.ops.mapper.rpc_rate_limiter.asyncio.sleep", side_effect=fake_sleep):
                asyncio.run(limiter.acquire("rpc", 2))
                asyncio.run(limiter.acquire("rpc", 2))

        self.assertEqual(sleeps, [0.5])
