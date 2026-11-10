import jwt
from fastapi import HTTPException, WebSocketException, status
from starlette.requests import HTTPConnection

from .config import Settings


def _token_from_header(conn: HTTPConnection) -> str | None:
    scheme, _, token = conn.headers.get("authorization", "").partition(" ")
    return token.strip() if scheme.lower() == "bearer" and token.strip() else None


def _decode(token: str, settings: Settings) -> int:
    payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_alg])
    return int(payload["sub"])


async def rest_user(conn: HTTPConnection) -> int:
    token = _token_from_header(conn)
    try:
        if not token:
            raise ValueError
        return _decode(token, conn.app.state.settings)
    except (ValueError, KeyError, jwt.PyJWTError):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid token")


async def ws_user(conn: HTTPConnection) -> int:
    """Токен берём из заголовка Authorization. Ошибка до accept -> рукопожатие отклонено."""
    token = _token_from_header(conn)
    try:
        if not token:
            raise ValueError
        return _decode(token, conn.app.state.settings)
    except (ValueError, KeyError, jwt.PyJWTError):
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)