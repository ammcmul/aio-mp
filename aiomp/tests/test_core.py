import asyncio
import contextlib
import unittest
from functools import wraps

from aiomp.core import AIOProcess, AIOWorker


def async_test(coroutine):
    @wraps(coroutine)
    def run_async(self):
        asyncio.run(coroutine(self))
    return run_async


def two():
    return 2


async def async_two(delay=0):
    await asyncio.sleep(delay)
    return 2


async def sleepy(duration=1):
    await asyncio.sleep(duration)


async def async_exception(message=None):
    raise RuntimeError(message)


class TestCore(unittest.TestCase):

    def test_aio_process(self):
        proc = AIOProcess(sleepy)
        self.assertFalse(proc.daemon)
        self.assertIsNone(proc.exit_code)
        self.assertFalse(proc.is_alive())
        self.assertIsNone(proc.pid)

    def test_aio_process_not_coroutine(self):
        expected_message = "target must be a coroutine function"
        with self.assertRaisesRegex(ValueError, expected_message):
            AIOProcess(two)

    @async_test
    async def test_aio_process_join_happy(self):
        proc = AIOProcess(sleepy, duration=0.05)
        proc.start()
        await proc.join()
        self.assertEqual(0, proc.exit_code)
        self.assertFalse(proc.is_alive())

    @async_test
    async def test_aio_process_join_timeout(self):
        proc = AIOProcess(sleepy, duration=2)
        proc.start()
        with self.assertRaises(asyncio.TimeoutError):
            await proc.join(timeout=0.05)

    @async_test
    async def test_aio_process_join_not_started(self):
        proc = AIOProcess(sleepy)
        expected_message = "must start process before joining"
        with self.assertRaisesRegex(ValueError, expected_message):
            await proc.join()

    @async_test
    async def test_aio_process_start_then_terminate(self):
        proc = AIOProcess(sleepy, duration=10)
        proc.start()
        self.assertIsNotNone(proc.pid)
        self.assertTrue(proc.is_alive())
        self.assertIsNone(proc.exit_code)
        proc.terminate()
        await asyncio.sleep(0.25)
        self.assertFalse(proc.is_alive())
        self.assertLessEqual(proc.exit_code, 0)

    @async_test
    async def test_worker_results_completed(self):
        worker = AIOWorker(async_two)
        worker.start()
        await worker.join()
        result = worker.result
        self.assertEqual(2, result)
        self.assertIsNone(worker.exception)

    @async_test
    async def test_worker_results_not_completed(self):
        worker = AIOWorker(async_two, delay=2)
        worker.start()
        with contextlib.redirect_stderr(None):
            expected_message = "coroutine not completed yet"
            with self.assertRaisesRegex(ValueError, expected_message):
                worker.result()
        await worker.join()

    @async_test
    async def test_worker_results_throws_exception(self):
        expected_message = "custom worker error message"
        worker = AIOWorker(async_exception, expected_message)
        with contextlib.redirect_stderr(None):
            worker.start()
        await asyncio.sleep(0.25)
        e, tb = worker.exception
        self.assertIsInstance(e, RuntimeError)
        self.assertEqual(expected_message, str(e))
        self.assertIsNotNone(tb)


if __name__ == "__main__":
    unittest.main()
