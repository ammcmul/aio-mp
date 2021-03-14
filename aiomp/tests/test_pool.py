import asyncio
import unittest
from functools import wraps

from aiomp.pool import AIOPool


def async_test(coroutine):
    @wraps(coroutine)
    def run_async(self):
        asyncio.run(coroutine(self))
    return run_async


async def add(a, b):
    return a + b


async def double(nbr):
    return nbr * 2


class TestAIOPool(unittest.TestCase):

    @async_test
    async def test_pool_apply_returns_result(self):
        pool = AIOPool()
        result = await pool.apply(add, 3, 5)
        self.assertEquals(8, result)
        pool.terminate()

    @async_test
    async def test_pool_apply_closed_throws_exception(self):
        pool = AIOPool()
        pool.terminate()
        with self.assertRaisesRegex(RuntimeError, "pool is closed"):
            await pool.apply(add, 4, 7)

    @async_test
    async def test_pool_map_happy_case(self):
        pool = AIOPool()
        results = await pool.map(double, [1, 2, 3, 4, 5])
        pool.terminate()
        self.assertEqual([2, 4, 6, 8, 10], results)


if __name__ == "__main__":
    unittest.main()
