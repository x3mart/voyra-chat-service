"""SQLAlchemy-зеркало таблиц, которыми владеет Django (см. django_side/models.py).
Миграции ведёт Django, здесь только чтение и запись."""
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, Uuid,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Conversation(Base):
    __tablename__ = "fastchat_conversation"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    # "минимальный_id:максимальный_id" для личных диалогов, чтобы не плодить дубли
    direct_key: Mapped[str | None] = mapped_column(String(64), unique=True, nullable=True)
    last_seq: Mapped[int] = mapped_column(BigInteger, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Participant(Base):
    __tablename__ = "fastchat_participant"
    __table_args__ = (UniqueConstraint("conversation_id", "user_id", name="uniq_participant"),)

    # Суррогатный ключ, как в Django. Код его не использует, колонку заполняет база.
    # with_variant нужен, чтобы автоинкремент работал и в SQLite (тесты).
    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    conversation_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("fastchat_conversation.id"))
    user_id: Mapped[int] = mapped_column(Integer)
    last_delivered_seq: Mapped[int] = mapped_column(BigInteger, default=0)
    last_read_seq: Mapped[int] = mapped_column(BigInteger, default=0)


class Message(Base):
    __tablename__ = "fastchat_message"
    __table_args__ = (
        UniqueConstraint("conversation_id", "seq"),
        UniqueConstraint("conversation_id", "sender_id", "client_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    conversation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("fastchat_conversation.id"), index=True
    )
    seq: Mapped[int] = mapped_column(BigInteger)
    sender_id: Mapped[int] = mapped_column(Integer)
    client_id: Mapped[str] = mapped_column(String(64))
    text: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
