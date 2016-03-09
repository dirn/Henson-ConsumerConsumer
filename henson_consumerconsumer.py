import asyncio

from henson import Abort, Extension


class ConsumerConsumer(Extension):
    def __init__(self, *consumers):
        self.consumers = list(consumers)

        self._queue = asyncio.Queue()
        self._consuming = False

    def init_app(self, app):
        super().init_app(app)

        app.consumer = self

    async def read(self):
        if not self._consuming:
            self._consume()

        return await self._queue.get()

    def register_consumer(self, consumer):
        self.consumers.append(consumer)

    def _consume(self):
        self._consuming = True
        loop = asyncio.get_event_loop()

        for consumer in self.consumers:
            loop.create_task(self._read_from(consumer))

    async def _read_from(self, consumer):
        while True:
            value = await consumer.read()
            await self._queue.put(value)
