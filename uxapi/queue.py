import asyncio


class Queue:
    def __init__(self):
        self._queue_obj = None

    @property
    def queue_obj(self):
        if self._queue_obj is None:
            loop = asyncio.get_running_loop()
            self._queue_obj = asyncio.Queue(loop=loop)
        return self._queue_obj

    def __getattr__(self, attr):
        return getattr(self.queue_obj, attr)
