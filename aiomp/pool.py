import asyncio
import os
import queue
from copy import copy
from itertools import count, cycle
from multiprocessing.queues import Queue
from typing import Dict, Tuple, Sequence, Any
import multiprocessing as mp

from aiomp.core import AIOProcess


QueueID = int


class AIOPoolWorker(AIOProcess):

    def __init__(
        self,
        task_x: Queue,
        result_x: Queue,
        concurrency: int = 16,
        ttl: int = None
    ):
        self.task_x = task_x
        self.result_x = result_x
        self.concurrency = concurrency
        self.ttl = ttl
        super().__init__(coroutine=self.run)

    async def run(self) -> None:
        running = True
        completed = 0
        pending = {}
        while running or pending:
            if self.ttl and completed >= self.ttl:
                running = False
            while running and len(pending) < self.concurrency:
                try:
                    task = self.task_x.get_nowait()
                    (tid, func, args, kwargs) = task
                    future = asyncio.ensure_future(func(*args, **kwargs))
                    pending[future] = tid
                except queue.Empty:
                    break
                if task is None:
                    running = False
                    break
            if not pending:
                await asyncio.sleep(0.005)
                continue

            done, _ = await asyncio.wait(pending.keys(), timeout=0.005, return_when=asyncio.FIRST_COMPLETED)
            for future in done:
                tid = pending.pop(future)
                result = None
                # noinspection PyBroadException
                try:
                    result = future.result()
                except BaseException:
                    pass
                self.result_x.put_nowait((tid, result))
                completed += 1


class AIOPool:
    DEFAULT_CONCURRENCY = 16

    def __init__(
            self,
            processes: int = 0,
            concurrency: int = DEFAULT_CONCURRENCY,
            queue_count: int = None
    ):
        self.process_count = max(1, processes or os.cpu_count() or 2)
        self.concurrency = max(1, concurrency or 1)
        self.queue_count = max(1, queue_count or 1)
        self._results = {}
        self.last_tid = count()
        self.last_qid = cycle(range(self.queue_count))
        self.running = True

        if self.process_count < self.queue_count:
            raise ValueError("queue count must not exceed process count")

        self.processes: Dict[AIOPoolWorker, QueueID] = {}
        self.queues: Dict[QueueID, Tuple[Queue, Queue]] = {}
        self.init()
        self._loop = asyncio.ensure_future(self.loop())

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.terminate()
        await self.join()

    def init(self):
        for index in range(self.queue_count):
            task_x, result_x = mp.Queue(), mp.Queue()
            self.queues[index] = (task_x, result_x)
        for index in range(self.process_count):
            queue_id = index % self.queue_count
            task_x, result_x = self.queues[queue_id]
            worker = AIOPoolWorker(task_x, result_x)
            worker.start()
            self.processes[worker] = queue_id

    async def loop(self):
        while self.running or self.processes:
            for proc in self.processes.keys():
                if not proc.is_alive():
                    qid = self.processes.pop(proc)
                    task_x, result_x = self.queues[qid]
                    new_worker = AIOPoolWorker(task_x, result_x)
                    new_worker.start()
                    self.processes[new_worker] = qid
            for _, result_x in self.queues.values():
                try:
                    task_id, result = result_x.get_nowait()
                    self._results[task_id] = result
                except queue.Empty:
                    pass
            await asyncio.sleep(0.005)

    async def apply(
            self,
            coroutine,
            *args,
            **kwargs):
        if not self.running:
            raise RuntimeError("pool is closed")

        task_id = self.queue_work(coroutine, args, kwargs)
        result = await self.results([task_id])
        return result[0]

    async def results(self, task_ids: Sequence[int]) -> Sequence[Any]:
        ready = {}
        pending = copy(task_ids)
        while pending:
            for task_id in pending:
                if task_id in self._results:
                    ready[task_id] = self._results.pop(task_id)
                    pending.remove(task_id)
            await asyncio.sleep(0.005)
        return [ready[tid] for tid in task_ids]

    def map(self, func, iterable):
        if not self.running:
            raise RuntimeError("pool is closed")
        task_ids = []
        for item in iterable:
            task_id = self.queue_work(func, (item,), {})
            task_ids.append(task_id)
        return self.results(task_ids)

    def close(self):
        self.running = False
        for (task_x, _) in self.queues.values():
            task_x.put_nowait(None)

    def terminate(self):
        if self.running:
            self.close()
        for proc in self.processes.keys():
            proc.terminate()

    async def join(self):
        if self.running:
            raise RuntimeError("pool is still open")
        await self._loop

    def queue_work(self, coroutine, args, kwargs):
        task_id = next(self.last_tid)
        task_x, _ = self.queues[next(self.last_qid)]
        task_x.put_nowait((task_id, coroutine, args, kwargs))
        return task_id
