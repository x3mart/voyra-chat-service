"""Бизнес-логика чата. Работает с AsyncSession, ничего не знает про сокеты."""
import uuid

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .db import Conversation, Message, Participant


class ChatError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def message_out(m: Message) -> dict:
    return {
        "id": str(m.id),
        "conversation_id": str(m.conversation_id),
        "seq": m.seq,
        "sender_id": m.sender_id,
        "client_id": m.client_id,
        "text": m.text,
        "created_at": m.created_at.isoformat(),
    }


async def participant_ids(session: AsyncSession, conv_id: uuid.UUID) -> list[int]:
    rows = await session.execute(
        select(Participant.user_id).where(Participant.conversation_id == conv_id)
    )
    return list(rows.scalars())


async def require_member(session: AsyncSession, user_id: int, conv_id: uuid.UUID) -> list[int]:
    """Проверяет членство и возвращает всех участников диалога."""
    ids = await participant_ids(session, conv_id)
    if user_id not in ids:
        raise ChatError("not_participant")
    return ids


async def create_direct(session: AsyncSession, user_id: int, other_id: int) -> uuid.UUID:
    if user_id == other_id:
        raise ChatError("invalid_user")
    key = f"{min(user_id, other_id)}:{max(user_id, other_id)}"
    existing = (
        await session.execute(select(Conversation.id).where(Conversation.direct_key == key))
    ).scalar_one_or_none()
    if existing:
        return existing
    conv = Conversation(direct_key=key)
    session.add(conv)
    await session.flush()
    session.add_all(
        [Participant(conversation_id=conv.id, user_id=u) for u in (user_id, other_id)]
    )
    await session.flush()
    return conv.id


async def send_message(
    session: AsyncSession, user_id: int, conv_id: uuid.UUID, client_id: str, text: str
) -> tuple[Message, bool, list[int]]:
    """Возвращает (сообщение, создано_ли_новое, участники).
    Повторная отправка с тем же client_id идемпотентна."""
    members = await require_member(session, user_id, conv_id)
    text = text.strip()
    if not text:
        raise ChatError("empty")

    existing = (
        await session.execute(
            select(Message).where(
                Message.conversation_id == conv_id,
                Message.sender_id == user_id,
                Message.client_id == client_id,
            )
        )
    ).scalar_one_or_none()
    if existing:
        return existing, False, members

    # Атомарный инкремент: строка диалога блокируется до коммита,
    # поэтому порядок seq совпадает с порядком коммитов.
    seq = (
        await session.execute(
            update(Conversation)
            .where(Conversation.id == conv_id)
            .values(last_seq=Conversation.last_seq + 1)
            .returning(Conversation.last_seq)
        )
    ).scalar_one()
    msg = Message(
        conversation_id=conv_id, seq=seq, sender_id=user_id, client_id=client_id, text=text
    )
    session.add(msg)
    await session.flush()
    return msg, True, members


async def set_receipt(
    session: AsyncSession, user_id: int, conv_id: uuid.UUID, kind: str, up_to_seq: int
) -> tuple[int | None, list[int]]:
    """kind: 'delivered' | 'read'. Возвращает (новое_значение | None, участники).
    None, если ничего не продвинулось (дубль или откат назад)."""
    members = await require_member(session, user_id, conv_id)
    conv_last = (
        await session.execute(select(Conversation.last_seq).where(Conversation.id == conv_id))
    ).scalar_one()
    up_to = min(up_to_seq, conv_last)

    p = (
        await session.execute(
            select(Participant).where(
                Participant.conversation_id == conv_id, Participant.user_id == user_id
            )
        )
    ).scalar_one()
    current = p.last_read_seq if kind == "read" else p.last_delivered_seq
    if up_to <= current:
        return None, members
    if kind == "read":
        p.last_read_seq = up_to
        p.last_delivered_seq = max(p.last_delivered_seq, up_to)
    else:
        p.last_delivered_seq = up_to
    return up_to, members


async def history(
    session: AsyncSession, user_id: int, conv_id: uuid.UUID, before_seq: int | None, limit: int
) -> list[Message]:
    await require_member(session, user_id, conv_id)
    q = select(Message).where(Message.conversation_id == conv_id)
    if before_seq is not None:
        q = q.where(Message.seq < before_seq)
    rows = await session.execute(q.order_by(Message.seq.desc()).limit(limit))
    return list(rows.scalars())


async def sync(
    session: AsyncSession, user_id: int, cursors: dict[uuid.UUID, int], per_conv: int = 200
) -> list[Message]:
    """Пропущенные сообщения после переподключения: seq > cursor по каждому диалогу."""
    out: list[Message] = []
    for conv_id, last_seq in cursors.items():
        if user_id not in await participant_ids(session, conv_id):
            continue  # чужие диалоги молча пропускаем
        rows = await session.execute(
            select(Message)
            .where(Message.conversation_id == conv_id, Message.seq > last_seq)
            .order_by(Message.seq)
            .limit(per_conv)
        )
        out.extend(rows.scalars())
    return out