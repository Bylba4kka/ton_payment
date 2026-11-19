import logging

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ton_payment.models import OutboxEvent, Transfer, TransferStatus
from ton_payment.schemas import TransferCreate, TransferSearch

logger = logging.getLogger(__name__)


async def create_transfer(
    session: AsyncSession,
    data: TransferCreate,
) -> Transfer:
    stmt = (
        insert(Transfer)
        .values(
            external_id=data.external_id,
            currency=data.currency,
            amount=data.amount,
            destination=data.destination,
            comment=data.comment,
            status=TransferStatus.PENDING,
        )
        .on_conflict_do_nothing(index_elements=[Transfer.external_id])
        .returning(Transfer)
    )
    transfer = await session.scalar(stmt)

    if transfer is None:
        result = await session.execute(
            select(Transfer).where(Transfer.external_id == data.external_id),
        )
        transfer = result.scalar_one()
        if (transfer.currency, transfer.amount, transfer.destination) != (
            data.currency,
            data.amount,
            data.destination,
        ):
            logger.warning(
                "Client resent external_id=%s with different data",
                data.external_id,
            )
        await session.commit()
        return transfer

    session.add(OutboxEvent(transfer_id=transfer.transfer_id))
    await session.commit()
    return transfer


async def search_transfers(
    session: AsyncSession,
    data: TransferSearch,
) -> list[Transfer]:
    stmt = select(Transfer)
    if data.transfer_ids:
        stmt = stmt.where(Transfer.transfer_id.in_(data.transfer_ids))
    if data.external_ids:
        stmt = stmt.where(Transfer.external_id.in_(data.external_ids))
    if data.statuses:
        stmt = stmt.where(
            Transfer.status.in_([TransferStatus(s) for s in data.statuses]),
        )
    return list((await session.scalars(stmt)).all())
