import asyncio
from typing import NamedTuple, Optional

from uxapi import Event


class ExecutionError(Exception):
    def __init__(self, name):
        self.name = name


class ExecutionResult(NamedTuple):
    name: Optional[str]
    result: object


class Awaitables:
    @staticmethod
    def default():
        return _default

    @staticmethod
    def set_default(aws):
        _default = aws

    def __init__(self):
        self.aws = {}
        self.aws_changed = Event()

    def __iter__(self):
        return iter(self.aws.keys())

    def __len__(self):
        return len(self.aws)

    def __contains__(self, aw):
        return aw in self.aws

    def get_name(self, aw):
        return self.aws[aw]

    def add(self, aw, name=None):
        if aw in self.aws:
            raise ValueError('awaitable object already exist')
        if name and name in self.aws.values():
            raise ValueError('name already exist')
        self.aws[aw] = name
        self.aws_changed.set()

    def create_task(self, coro, name=None):
        task = asyncio.create_task(coro)
        self.add(task, name)
        return task

    def run_in_executor(self, func, *args, executor=None, name=None):
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(executor, func, *args)
        self.add(fut, name)
        return fut

    def _cancel_all(self):
        for task in self.aws:
            task.cancel()

    async def cleanup(self):
        if not self.aws:
            return
        self._cancel_all()
        try:
            await asyncio.wait(
                self.aws.keys(),
                return_when=asyncio.ALL_COMPLETED)
        except Exception:
            pass
        self.aws.clear()

    async def wait(self, timeout=None):
        while True:
            self.aws_changed.clear()
            wait_aws_change = asyncio.create_task(self.aws_changed.wait())
            try:
                done, _ = await asyncio.wait(
                    self.aws.keys() | {wait_aws_change},
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED)
            except asyncio.CancelledError:
                await self.cleanup()
                raise
            finally:
                wait_aws_change.cancel()
            if wait_aws_change not in done:
                break
   
        if not done:
            raise asyncio.TimeoutError()

        task = done.pop()
        name = self.aws[task]
        del self.aws[task]
        try:
            res = task.result()
        except Exception as exc:
            raise ExecutionError(name) from exc
        return ExecutionResult(name, res)


_default = Awaitables()