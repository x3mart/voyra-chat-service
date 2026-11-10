from contextlib import asynccontextmanager

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from .broker import InMemoryBroker
from .config import Settings
from .db import Base
from .main import create_app

from .helpers import SECRET


@asynccontextmanager
async def test_lifespan(app):
    """SQLite в памяти + брокер в памяти. StaticPool: одно соединение на всех,
    иначе каждое новое соединение получило бы свою пустую базу."""
    engine = create_async_engine(
        "sqlite+aiosqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    app.state.sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
    app.state.broker = InMemoryBroker()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    try:
        yield
    finally:
        await engine.dispose()


@pytest.fixture
def client():
    app = create_app(Settings(jwt_secret=SECRET), lifespan=test_lifespan)
    with TestClient(app) as c:
        yield c