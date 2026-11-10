import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from starlette.requests import HTTPConnection

from . import service
from .auth import rest_user
from .service import ChatError

router = APIRouter(prefix="/chats")


class DirectIn(BaseModel):
    user_id: int


def _http(e: ChatError) -> HTTPException:
    code = 403 if e.code == "not_participant" else 400
    return HTTPException(code, e.code)


@router.post("/direct")
async def create_direct(body: DirectIn, conn: HTTPConnection, user_id: int = Depends(rest_user)):
    # TODO: проверить, что body.user_id существует (таблица пользователей в Django)
    try:
        async with conn.app.state.sessionmaker() as session, session.begin():
            conv_id = await service.create_direct(session, user_id, body.user_id)
    except ChatError as e:
        raise _http(e)
    return {"conversation_id": str(conv_id)}


@router.get("/{conversation_id}/messages")
async def get_history(
    conversation_id: uuid.UUID,
    conn: HTTPConnection,
    before_seq: int | None = None,
    limit: int = Query(50, ge=1, le=100),
    user_id: int = Depends(rest_user),
):
    """Новые сверху: следующая страница запрашивается с before_seq = минимальный seq."""
    try:
        async with conn.app.state.sessionmaker() as session:
            msgs = await service.history(session, user_id, conversation_id, before_seq, limit)
    except ChatError as e:
        raise _http(e)
    return {
        "messages": [service.message_out(m) for m in msgs],
        "has_more": len(msgs) == limit,
    }
