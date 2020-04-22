import asyncio


class Event:
    def __init__(self):
        self._event_obj = None

    @property
    def event_obj(self):
        if self._event_obj is None:
            loop = asyncio.get_running_loop()
            self._event_obj = asyncio.Event(loop=loop)
        return self._event_obj

    def __getattr__(self, attr):
        return getattr(self.event_obj, attr)
