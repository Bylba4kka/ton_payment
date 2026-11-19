from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
)
from pytoniq import Address, AddressError


def decimal_to_str(value: Decimal) -> str:
    return format(value.normalize(), "f")


class TransferCreate(BaseModel):
    """Request body for creating a transfer."""

    model_config = ConfigDict(str_strip_whitespace=True)

    external_id: str = Field(min_length=1, max_length=255)
    currency: str = Field(min_length=1, max_length=32)
    amount: Decimal = Field(gt=0, max_digits=38, decimal_places=9)
    destination: str = Field(min_length=1, max_length=255)
    comment: str | None = Field(default=None, max_length=1000)

    @field_validator("destination")
    @classmethod
    def _validate_destination(cls, v: str) -> str:
        try:
            addr = Address(v)
        except (AddressError, ValueError, IndexError) as exc:
            msg = "invalid TON address"
            raise ValueError(msg) from exc
        if addr.wc not in (0, -1):
            msg = "unsupported workchain"
            raise ValueError(msg)
        return v


class TransferResponse(BaseModel):
    """Response with the created or existing transfer."""

    model_config = ConfigDict(from_attributes=True)

    transfer_id: UUID
    external_id: str
    currency: str
    amount: Decimal
    destination: str
    status: Literal["pending", "completed", "failed"]

    @field_serializer("amount")
    def _serialize_amount(self, value: Decimal) -> str:
        return decimal_to_str(value)


class TransferSearch(BaseModel):
    """Search filters: transfer IDs, external IDs and statuses."""

    transfer_ids: Annotated[
        list[UUID] | None,
        Field(default=None, max_length=100),
    ] = None
    external_ids: Annotated[
        list[str] | None,
        Field(default=None, max_length=100),
    ] = None
    statuses: list[Literal["pending", "completed", "failed"]] | None = None


class TransferSearchItem(BaseModel):
    """Transfer in search results, with on-chain details once completed."""

    model_config = ConfigDict(from_attributes=True)

    transfer_id: UUID
    external_id: str
    currency: str
    amount: Decimal
    destination: str
    details: dict[str, Any] | None
    status: Literal["pending", "completed", "failed"]

    @field_serializer("amount")
    def _serialize_amount(self, value: Decimal) -> str:
        return decimal_to_str(value)
