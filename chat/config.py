import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql+asyncpg://voyra:voyra@localhost:5432/voyra"
    )
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    # Общий с Django секрет подписи JWT. Предполагаем, что sub = id пользователя.
    jwt_secret: str = os.getenv("JWT_SECRET", "django-insecure-voyra-mvp-2026-secret-key-change-me")
    jwt_alg: str = os.getenv("JWT_ALG", "HS256")