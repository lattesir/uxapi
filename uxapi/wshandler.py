import logging

from aiohttp import WSMsgType

from uxapi import UXTopic
from uxapi import Session
from uxapi import Awaitables
from uxapi import listiter


class WSHandler:
    logger = logging.getLogger(__name__)

    def __init__(self, exchange, wsurl, topic_set):
        self.exchange = exchange
        self.wsurl = wsurl
        self.topic_set = topic_set
        self.session = None
        self.own_session = False
        self.ws = None
        self.pending_topics = None
        self.awaitables = Awaitables()
        self.pre_processors = listiter([])

    def get_credentials(self):
        credentials = self.exchange.requiredCredentials
        requires = [key for key in credentials if credentials[key]]
        result = {}
        for key in requires:
            value = getattr(self.exchange, key)
            if value:
                result[key] = value
            else:
                raise RuntimeError(f'requires `{key}`')
        return result

    async def run(self, collector=None):
        try:
            self.ws = await self.connect()
            self.prepare()
            await self.do_run(collector)
        finally:
            await self.cleanup()

    async def do_run(self, collector):
        while True:
            if 'recv' not in self.awaitables:
                self.awaitables.create_task(self.recv(), 'recv')
            name, result = await self.awaitables.wait()
            if name == 'recv' and result is not None:
                try:
                    msg = self.pre_process(result)
                except StopIteration:
                    continue
                else:
                    if collector:
                        collector(msg)

    async def connect(self):
        if not self.session:
            self.session = Session()
            self.own_session = True
        ws = await self.session.ws_connect(self.wsurl)
        self.on_connected()
        return ws

    def prepare(self):
        self.create_keepalive_task()
        if self.login_required:
            self.create_login_task()
        else:
            self.on_prepared()

    def pre_process(self, data):
        msg = self.decode(data)
        self.pre_processors.rewind()
        for processor in self.pre_processors:
            msg = processor(msg)
        return msg

    def on_connected(self):
        pass

    def create_keepalive_task(self):
        self.pre_processors.prepend(self.on_keepalive_message)
        return self.awaitables.create_task(self.keepalive(), 'keepalive')

    async def keepalive(self):
        raise NotImplementedError

    def on_keepalive_message(self, message):
        raise NotImplementedError

    @property
    def login_required(self):
        return False

    def create_login_task(self):
        self.pre_processors.append(self.on_login_message)
        credentials = self.get_credentials()
        return self.awaitables.create_task(self.login(credentials), 'login')

    async def login(self, credentials):
        command = self.login_command(credentials)
        await self.send(command)

    def login_command(self, credentials):
        raise NotImplementedError

    def on_login_message(self, message):
        raise NotImplementedError

    def on_logged_in(self):
        self.pre_processors.remove()
        self.on_prepared()

    def on_prepared(self):
        self.create_subscribe_task()

    def create_subscribe_task(self):
        self.pre_processors.append(self.on_subscribe_message)
        topic_set = {self.convert_topic(topic) for topic in self.topic_set}
        self.pending_topics = topic_set
        return self.awaitables.create_task(self.subscribe(topic_set), 'subscribe')

    def convert_topic(self, topic: UXTopic):
        return self.exchange.convert_topic(topic)

    async def subscribe(self, topic_set):
        commands = self.subscribe_commands(topic_set)
        for command in commands:
            await self.send(command)

    def subscribe_commands(self, topic_set):
        raise NotImplementedError

    def on_subscribe_message(self, message):
        raise NotImplementedError

    def on_subscribed(self, topic):
        self.pending_topics.remove(topic)
        if not self.pending_topics:
            self.pre_processors.remove()
            self.pending_topics = None

    async def send(self, command):
        if isinstance(command, dict):
            await self.ws.send_json(command)
        else:
            await self.ws.send_str(command)

    async def recv(self):
        wsmsg = await self.ws.receive()
        if wsmsg.type in (WSMsgType.BINARY, WSMsgType.TEXT):
            return wsmsg.data
        else:
            raise RuntimeError(f'unexpected message: {wsmsg}')

    def decode(self, data):
        return data

    async def cleanup(self):
        await self.awaitables.cleanup()

        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass

        if self.session and self.own_session:
            try:
                await self.session.close()
            except Exception:
                pass
