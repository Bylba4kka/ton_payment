import enum
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    DateTime,
    Enum,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    Uuid,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from ton_payment.database import Base


class TransferStatus(enum.StrEnum):
    """Lifecycle status of a transfer."""

    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


class OutboxStatus(enum.StrEnum):
    """Processing status of an outbox event."""

    NEW = "new"
    PROCESSING = "processing"
    PUBLISHED = "published"
    FAILED = "failed"


def _enum(cls: type, name: str) -> Enum:
    return Enum(
        cls,
        name=name,
        native_enum=False,
        create_constraint=False,
        length=16,
        values_callable=lambda e: [m.value for m in e],
    )


class Transfer(Base):
    """Outgoing TON transfer and its on-chain sending state."""

    __tablename__ = "transfers"
    __table_args__ = (
        Index(
            "ix_transfers_pending_send",
            "created_at",
            postgresql_where=text("msg_hash IS NULL AND status = 'pending'"),
        ),
        Index(
            "ix_transfers_pending_confirm",
            "created_at",
            postgresql_where=text(
                "msg_hash IS NOT NULL AND status = 'pending'",
            ),
        ),
        Index(
            "ix_transfers_expired",
            "expires_at",
            postgresql_where=text("status = 'pending'"),
        ),
    )

    transfer_id: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid.uuid4,
    )
    external_id: Mapped[str] = mapped_column(String(255), unique=True)
    currency: Mapped[str] = mapped_column(String(32))
    amount: Mapped[Decimal] = mapped_column(Numeric(38, 9))
    destination: Mapped[str] = mapped_column(String(255))
    comment: Mapped[str | None] = mapped_column(Text)

    status: Mapped[TransferStatus] = mapped_column(
        _enum(TransferStatus, "transfer_status"),
        default=TransferStatus.PENDING,
    )

    msg_hash: Mapped[str | None] = mapped_column(String(255), unique=True)
    msg_hash_norm: Mapped[str | None] = mapped_column(String(255), unique=True)
    seqno: Mapped[int | None] = mapped_column(Integer)
    boc: Mapped[str | None] = mapped_column(Text)
    from_address: Mapped[str | None] = mapped_column(String(255))
    to_address: Mapped[str | None] = mapped_column(String(255))
    tx_hash: Mapped[str | None] = mapped_column(String(255))
    failure_reason: Mapped[str | None] = mapped_column(Text)

    valid_until: Mapped[int | None] = mapped_column(Integer)
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    def is_final(self) -> bool:
        return self.status in (TransferStatus.COMPLETED, TransferStatus.FAILED)

    def details(self) -> dict[str, Any] | None:
        if (
            self.from_address is None
            and self.to_address is None
            and self.tx_hash is None
        ):
            return None
        return {
            "from_address": self.from_address,
            "to_address": self.to_address,
            "tx_hash": self.tx_hash,
        }


class OutboxEvent(Base):
    """Outbox event that triggers sending a transfer to TON."""

    __tablename__ = "outbox_events"
    __table_args__ = (
        Index(
            "ix_outbox_new",
            "created_at",
            postgresql_where=text("status = 'new'"),
        ),
    )

    event_id: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        primary_key=True,
        default=uuid.uuid4,
    )
    transfer_id: Mapped[uuid.UUID] = mapped_column(Uuid, index=True)
    status: Mapped[OutboxStatus] = mapped_column(
        _enum(OutboxStatus, "outbox_status"),
        default=OutboxStatus.NEW,
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
    )
