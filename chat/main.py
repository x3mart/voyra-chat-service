from contextlib import asynccontextmanager
 
from fastapi import FastAPI
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
 
from . import ws
from . import rest
from .broker import RedisBroker
from .config import Settings
 
 
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Боевой жизненный цикл: Postgres + Redis. Тесты подставляют свой lifespan."""
    settings: Settings = app.state.settings
    engine = create_async_engine(settings.database_url)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
 
    app.state.sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
    app.state.broker = RedisBroker(redis)
    try:
        yield
    finally:
        await redis.aclose()
        await engine.dispose()
 
 
def create_app(settings: Settings | None = None, lifespan=lifespan) -> FastAPI:
    app = FastAPI(lifespan=lifespan)
    app.state.settings = settings or Settings()
    app.include_router(rest.router)
    app.include_router(ws.router)
    return app
 
 
app = create_app()