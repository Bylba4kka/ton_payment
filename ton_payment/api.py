from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ton_payment import service
from ton_payment.database import get_async_session
from ton_payment.schemas import (
    TransferCreate,
    TransferResponse,
    TransferSearch,
    TransferSearchItem,
)
from ton_payment.security import require_token

router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_token)])

SessionDep = Annotated[AsyncSession, Depends(get_async_session)]


@router.post("/transfer")
async def create_transfer(
    body: TransferCreate,
    session: SessionDep,
) -> TransferResponse:
    transfer = await service.create_transfer(session, body)
    return TransferResponse.model_validate(transfer)


@router.post("/transfer/search")
async def search_transfers(
    body: TransferSearch,
    session: SessionDep,
) -> list[TransferSearchItem]:
    transfers = await service.search_transfers(session, body)
    return [
        TransferSearchItem(
            transfer_id=t.transfer_id,
            external_id=t.external_id,
            currency=t.currency,
            amount=t.amount,
            destination=t.destination,
            details=t.details(),
            status=t.status.value,
        )
        for t in transfers
    ]
