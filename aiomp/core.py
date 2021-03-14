import asyncio
import logging
import multiprocessing
import os
import traceback
from functools import partial
from multiprocessing import managers
from multiprocessing.context import Process
from typing import Callable

import uvloop

log = logging.getLogger(__name__)

_manager = None


def get_manager() -> managers.SyncManager:
    """ Return a singleton shared memory manager """
    global _manager
    if _manager is None:
        _manager = multiprocessing.get_context().Manager()
    return _manager


class AIOProcess:
    """ Execute a coroutine on a separate process """
    def __init__(self, coroutine: Callable = None, *args, daemon: bool = False,
                 target_override: Callable = None, **kwargs):
        if not asyncio.iscoroutinefunction(coroutine):
            raise ValueError("target must be a coroutine function")

        self.aio_process = Process(
            target=target_override or partial(AIOProcess.run_async, coroutine),
            args=args,
            kwargs=kwargs,
            daemon=daemon
        )

    @staticmethod
    def run_async(coroutine: Callable, *args, **kwargs):
        try:
            loop = uvloop.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(coroutine(*args, **kwargs))

            return result
        except BaseException:
            log.exception(f"aio process {os.getpid()} failed")
            raise

    def start(self):
        self.aio_process.start()

    async def join(self, timeout=None):
        if not self.is_alive() and self.exit_code is None:
            raise ValueError("must start process before joining")

        if timeout is not None:
            return await asyncio.wait_for(self.join(), timeout)

        while self.exit_code is None:
            await asyncio.sleep(0.005)

    @property
    def pid(self):
        return self.aio_process.pid

    @property
    def daemon(self):
        return self.aio_process.daemon

    @property
    def exit_code(self):
        return self.aio_process.exitcode

    def is_alive(self):
        return self.aio_process.is_alive()

    def terminate(self):
        self.aio_process.terminate()


class AIOWorker(AIOProcess):
    """ Execute a coroutine on a separate process and return the results """
    def __init__(self, coroutine: Callable, *args, **kwargs):
        self.namespace = get_manager().Namespace()
        self.namespace.result = None
        self.namespace.exception = None
        super().__init__(
            coroutine,
            *args,
            target_override=partial(AIOWorker.run_async, coroutine, self.namespace),
            **kwargs
        )

    @staticmethod
    def run_async(coroutine, namespace, *args, **kwargs):
        try:
            result = AIOProcess.run_async(coroutine, *args, **kwargs)
            namespace.result = result
            return result
        except BaseException as e:
            namespace.exception = (e, traceback.format_exc())

    async def join(self, timeout=None):
        await super().join(timeout)
        return self.namespace.result

    @property
    def exception(self):
        if self.exit_code is None and self.is_alive():
            raise ValueError("coroutine not completed yet")
        return self.namespace.exception

    @property
    def result(self):
        if self.exit_code is None and self.is_alive():
            raise ValueError("coroutine not completed yet")
        return self.namespace.result
