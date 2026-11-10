"""Брокер событий между инстансами FastAPI.
Один канал на пользователя: user:{id}. publish возвращает число подписчиков,
по нему видно, что получатель офлайн (нужен push)."""
import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import AsyncIterator, Protocol


class Broker(Protocol):
    async def publish(self, channel: str, payload: str) -> int: ...
    def subscribe(self, channel: str): ...  # async context manager -> AsyncIterator[str]


class RedisBroker:
    def __init__(self, redis):
        self._redis = redis  # redis.asyncio.Redis(decode_responses=True)

    async def publish(self, channel: str, payload: str) -> int:
        return await self._redis.publish(channel, payload)

    @asynccontextmanager
    async def subscribe(self, channel: str):
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(channel)

        async def messages() -> AsyncIterator[str]:
            async for m in pubsub.listen():
                if m["type"] == "message":
                    yield m["data"]

        try:
            yield messages()
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()


class InMemoryBroker:
    """Для тестов и локального запуска без Redis (один процесс)."""

    def __init__(self):
        self._subs: dict[str, set[asyncio.Queue]] = defaultdict(set)

    async def publish(self, channel: str, payload: str) -> int:
        queues = list(self._subs.get(channel, ()))
        for q in queues:
            q.put_nowait(payload)
        return len(queues)

    @asynccontextmanager
    async def subscribe(self, channel: str):
        q: asyncio.Queue[str] = asyncio.Queue()
        self._subs[channel].add(q)

        async def messages() -> AsyncIterator[str]:
            while True:
                yield await q.get()

        try:
            yield messages()
        finally:
            self._subs[channel].discard(q)