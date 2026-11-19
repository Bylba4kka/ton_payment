import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol, Self

from pytoniq_core import Address
from toncenter.rest.client import ToncenterRestClient
from toncenter.rest.v3.models import Transaction
from toncenter.streaming.models import (
    TransactionsNotification,
)
from toncenter.streaming.sse import ToncenterSSE
from toncenter.types import Network

from ton_payment.config import settings
from ton_payment.ton_signer import TonWalletSigner

logger = logging.getLogger(__name__)
NANOTON_PER_TON = Decimal(10**9)


class TonSigningError(Exception):
    """Failed to build or sign a BOC with the internal wallet mnemonic."""


@dataclass(slots=True, frozen=True)
class OutgoingTransfer:
    """Transfer to be signed and sent from the service wallet."""

    transfer_id: Any
    destination: str
    amount: Decimal
    comment: str | None
    valid_until: int


@dataclass(slots=True, frozen=True)
class PrepareTransfer:
    """Signed transfer ready to broadcast: BOC, msg_hash and seqno."""

    transfer_id: Any
    destination: str
    amount: Decimal
    comment: str | None
    valid_until: int
    msg_hash: str
    boc: str
    seqno: int


@dataclass(slots=True, frozen=True)
class SendResult:
    """Result of broadcasting a signed transfer to the network."""

    msg_hash_norm: str | None = None


@dataclass(slots=True, frozen=True)
class TonTransaction:
    """Wallet transaction with its inbound message and execution result."""

    msg_hash: str | None
    msg_hash_norm: str | None
    tx_hash: str | None
    from_address: str | None
    to_address: str | None
    success: bool
    failure_reason: str | None = None


class TonGateway(Protocol):
    """Interface for signing, sending and tracking TON transfers."""

    async def prepare_transfer(
        self,
        transfer: OutgoingTransfer,
    ) -> PrepareTransfer: ...
    async def send_transfer(self, transfer: PrepareTransfer) -> SendResult: ...
    async def wait_seqno_advanced(
        self,
        seqno: int,
        *,
        valid_until: int,
    ) -> bool: ...
    async def fetch_transactions(
        self,
        *,
        start_ut: datetime,
        end_ut: datetime,
    ) -> list[TonTransaction]: ...

    async def run_finalized(
        self,
        handler: Callable[[TonTransaction], Awaitable[None]],
        *,
        min_finality: str = "finalized",
    ) -> None: ...


def transaction_failure_reason(transaction: Transaction) -> str | None:  # noqa: PLR0911
    """
    Return why a transaction failed, or None if it succeeded.

    See section 4.2.5 "Phases of an ordinary transaction":
    https://docs.ton.org/foundations/whitepapers/tblkch
    """
    descr = transaction.description
    if descr is None:
        return "no transaction description"
    if descr.aborted:
        return "transaction aborted"

    compute = descr.compute_ph
    if compute is None or compute.skipped or not compute.success:
        return (
            "compute phase failed: "
            f"exit_code={compute and compute.exit_code}"
            "reason={compute and compute.reason}"
        )

    action = descr.action
    if action is None:
        return "no action phase"
    if not action.success:
        return f"action phase failed: result_code={action.result_code}"
    if action.no_funds:
        return f"insufficient funds: result_code={action.result_code}"
    if action.skipped_actions:
        return (
            f"action skipped: result_code={action.result_code}, "
            f"skipped_actions={action.skipped_actions}"
        )
    if not action.msgs_created:
        return "no outgoing message created"
    return None


def _map_transaction(transaction: Transaction) -> TonTransaction:
    failure_reason = transaction_failure_reason(transaction)
    if transaction.in_msg is None:
        msg = f"Transaction {transaction.hash} without in_msg"
        raise ValueError(msg)
    return TonTransaction(
        msg_hash=transaction.in_msg.hash,
        msg_hash_norm=transaction.in_msg.hash_norm,
        tx_hash=transaction.hash,
        from_address=transaction.in_msg.source,
        to_address=transaction.in_msg.destination,
        success=failure_reason is None,
        failure_reason=failure_reason,
    )


class TonGatewayImpl:
    """TON gateway built on Toncenter REST, SSE and wallet signing."""

    def __init__(
        self,
        *,
        wallet_address: str,
        signer: TonWalletSigner,
        toncenter: ToncenterRestClient,
        sse: ToncenterSSE,
    ) -> None:
        self._toncenter = toncenter
        self._sse = sse
        self._sse_started = False

        signer.ensure_address(wallet_address)
        self._signer = signer
        self._wallet_address = wallet_address
        self._wallet = Address(wallet_address)

    async def _rest(self) -> ToncenterRestClient:
        await self._toncenter.create_session()
        return self._toncenter

    async def aclose(self) -> None:
        await self._sse.stop()
        await self._toncenter.close_session()

    async def __aenter__(self) -> Self:
        await self._rest()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def prepare_transfer(
        self,
        transfer: OutgoingTransfer,
    ) -> PrepareTransfer:
        seqno = await self._get_seqno()

        boc, msg_hash = self._build_and_sign(
            destination=transfer.destination,
            amount_nano=int(transfer.amount * NANOTON_PER_TON),
            comment=transfer.comment,
            valid_until=transfer.valid_until,
            seqno=seqno,
        )
        return PrepareTransfer(
            transfer_id=transfer.transfer_id,
            destination=transfer.destination,
            amount=transfer.amount,
            comment=transfer.comment,
            valid_until=transfer.valid_until,
            msg_hash=msg_hash,
            boc=boc,
            seqno=seqno,
        )

    def _build_and_sign(
        self,
        *,
        destination: str,
        amount_nano: int,
        comment: str | None,
        valid_until: int,
        seqno: int,
    ) -> tuple[str, str]:
        """
        Build and sign a BOC with the wallet mnemonic.

        Return (boc_base64, msg_hash). Signing is done with pytoniq:
        https://github.com/yungwine/pytoniq
        """
        signed = self._signer.sign_transfer(
            destination=destination,
            amount_nano=amount_nano,
            comment=comment,
            valid_until=valid_until,
            seqno=seqno,
        )
        return signed.boc, signed.msg_hash

    async def send_transfer(self, transfer: PrepareTransfer) -> SendResult:
        client = await self._rest()
        ext_msg = await client.v2.send.send_boc_return_hash(transfer.boc)

        logger.info(
            "Transfer %s sent: hash=%s, hash_norm=%s",
            transfer.transfer_id,
            ext_msg.hash,
            ext_msg.hash_norm,
        )
        if ext_msg.hash != transfer.msg_hash:
            logger.error(
                "Transfer %s: msg_hash=%s does not match toncenter hash=%s",
                transfer.transfer_id,
                transfer.msg_hash,
                ext_msg.hash,
            )
        return SendResult(msg_hash_norm=ext_msg.hash_norm)

    async def _get_seqno(self) -> int:
        client = await self._rest()
        result = await client.v2.runmethod.run_get_method(
            self._wallet_address,
            "seqno",
        )
        return int(result.stack[0][1], 16)

    async def wait_seqno_advanced(
        self,
        seqno: int,
        *,
        valid_until: int,
    ) -> bool:
        """Wait until the wallet seqno becomes greater than the given one."""
        while True:
            if await self._get_seqno() > seqno:
                return True

            if datetime.now(UTC).timestamp() > valid_until:
                return False
            await asyncio.sleep(settings.seqno_poll_interval)

    async def run_finalized(
        self,
        handler: Callable[[TonTransaction], Awaitable[None]],
        *,
        min_finality: str = "finalized",
    ) -> None:
        if self._sse_started:
            msg = "run_finalized has already been launched"
            raise RuntimeError(msg)
        self._sse_started = True
        sse = self._sse

        @sse.on_transactions(min_finality=min_finality)
        async def handle_transactions(event: TransactionsNotification) -> None:
            for tx in event.transactions:
                if Address(tx.get("account", "0:" + "0" * 64)) != self._wallet:
                    continue
                if tx.get("in_msg"):
                    await handler(
                        _map_transaction(Transaction.model_validate(tx)),
                    )

        try:
            await sse.start([self._wallet_address])
        finally:
            await sse.stop()

    async def fetch_transactions(
        self,
        *,
        start_ut: datetime,
        end_ut: datetime,
        limit: int = 100,
    ) -> list[TonTransaction]:
        client = await self._rest()
        transactions = await client.v3.blockchain.get_transactions(
            account=[self._wallet_address],
            limit=limit,
            sort="asc",
            start_utime=int(start_ut.timestamp()),
            end_utime=int(end_ut.timestamp()),
        )

        return [
            _map_transaction(transaction)
            for transaction in transactions.transactions or []
            if transaction.in_msg is not None
        ]


def create_ton_gateway() -> TonGatewayImpl:
    signer = TonWalletSigner(
        mnemonic=settings.ton_wallet_mnemonic.get_secret_value(),
        version=settings.ton_wallet_version,
        network_global_id=settings.ton_network_global_id,
    )
    api_key = settings.toncenter_api_key.get_secret_value()
    network = (
        Network.MAINNET
        if settings.toncenter_network == "MAINNET"
        else Network.TESTNET
    )
    return TonGatewayImpl(
        wallet_address=settings.ton_wallet_address,
        signer=signer,
        toncenter=ToncenterRestClient(api_key=api_key, network=network),
        sse=ToncenterSSE(api_key=api_key, network=network),
    )
