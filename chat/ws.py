"""WebSocket-эндпоинт. Один сокет на пользователя, события в конверте
{"type": "...", "ref": "...", "data": {...}}."""
import asyncio
import json
import uuid
from contextlib import suppress

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, ValidationError, field_validator
from starlette.requests import HTTPConnection

from . import service
from chat.auth import ws_user
from chat.notifications import enqueue_push
from chat.service import ChatError

router = APIRouter()


# ---------- входящие payload'ы ----------
class SendIn(BaseModel):
    conversation_id: uuid.UUID
    client_id: str = Field(min_length=1, max_length=64)
    text: str = Field(max_length=4000)

    @field_validator("text")
    @classmethod
    def _strip(cls, v: str) -> str:
        return v.strip()


class ReceiptIn(BaseModel):
    conversation_id: uuid.UUID
    up_to_seq: int = Field(ge=0)


class TypingIn(BaseModel):
    conversation_id: uuid.UUID


class SyncIn(BaseModel):
    cursors: dict[uuid.UUID, int]


# ---------- соединение ----------
class Conn:
    def __init__(self, ws: WebSocket):
        self.ws = ws
        self._lock = asyncio.Lock()  # ack из обработчика и пересылка из брокера не должны перемешиваться

    async def send_raw(self, raw: str) -> None:
        async with self._lock:
            await self.ws.send_text(raw)

    async def send(self, type_: str, data: dict | None = None, ref: str | None = None) -> None:
        msg: dict = {"type": type_, "data": data or {}}
        if ref:
            msg["ref"] = ref
        await self.send_raw(json.dumps(msg))


async def _publish(broker, user_ids, type_: str, data: dict) -> dict[int, int]:
    raw = json.dumps({"type": type_, "data": data})
    return {uid: await broker.publish(f"user:{uid}", raw) for uid in user_ids}


async def _pump(conn: Conn, stream) -> None:
    async for raw in stream:
        await conn.send_raw(raw)


# ---------- обработчики событий ----------
async def _on_send(conn, user_id, data, ref, app):
    p = SendIn.model_validate(data)
    async with app.state.sessionmaker() as session, session.begin():
        msg, created, members = await service.send_message(
            session, user_id, p.conversation_id, p.client_id, p.text
        )
        out = service.message_out(msg)

    await conn.send("message.ack", {"client_id": p.client_id, "created": created, **out}, ref)
    if not created:
        return
    others = [u for u in members if u != user_id]
    receivers = await _publish(app.state.broker, others, "message.new", out)
    for uid, count in receivers.items():
        if count == 0:
            enqueue_push(uid, {"conversation_id": str(p.conversation_id), "text": out["text"][:100]})


async def _on_receipt(kind, conn, user_id, data, ref, app):
    p = ReceiptIn.model_validate(data)
    async with app.state.sessionmaker() as session, session.begin():
        new_value, members = await service.set_receipt(
            session, user_id, p.conversation_id, kind, p.up_to_seq
        )
    if new_value is None:
        return
    others = [u for u in members if u != user_id]
    await _publish(
        app.state.broker, others, f"message.{kind}",
        {"conversation_id": str(p.conversation_id), "user_id": user_id, "up_to_seq": new_value},
    )


async def _on_typing(conn, user_id, data, ref, app):
    p = TypingIn.model_validate(data)
    async with app.state.sessionmaker() as session:
        members = await service.require_member(session, user_id, p.conversation_id)
    others = [u for u in members if u != user_id]
    await _publish(
        app.state.broker, others, "typing",
        {"conversation_id": str(p.conversation_id), "user_id": user_id},
    )


async def _on_sync(conn, user_id, data, ref, app):
    p = SyncIn.model_validate(data)
    async with app.state.sessionmaker() as session:
        msgs = await service.sync(session, user_id, p.cursors)
    await conn.send("sync.batch", {"messages": [service.message_out(m) for m in msgs]}, ref)


async def _dispatch(conn: Conn, user_id: int, env: dict, app) -> None:
    t, ref, data = env.get("type"), env.get("ref"), env.get("data") or {}
    try:
        match t:
            case "message.send":
                await _on_send(conn, user_id, data, ref, app)
            case "message.delivered" | "message.read":
                await _on_receipt(t.split(".")[1], conn, user_id, data, ref, app)
            case "typing":
                await _on_typing(conn, user_id, data, ref, app)
            case "sync.request":
                await _on_sync(conn, user_id, data, ref, app)
            case _:
                raise ChatError("unknown_type")
    except ValidationError:
        await conn.send("error", {"code": "invalid_payload", "type": t}, ref)
    except ChatError as e:
        await conn.send("error", {"code": e.code, "type": t}, ref)


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket, user_id: int = Depends(ws_user)):
    app = ws.app
    await ws.accept()
    conn = Conn(ws)
    async with app.state.broker.subscribe(f"user:{user_id}") as stream:
        pump = asyncio.create_task(_pump(conn, stream))
        try:
            while True:
                raw = await ws.receive_text()
                try:
                    env = json.loads(raw)
                    if not isinstance(env, dict):
                        raise ValueError
                except ValueError:
                    await conn.send("error", {"code": "invalid_json"})
                    continue
                await _dispatch(conn, user_id, env, app)
        except WebSocketDisconnect:
            pass
        finally:
            pump.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await pump