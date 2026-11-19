import asyncio
import json
import logging
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import aiohttp
from faststream.rabbit import Channel
from sqlalchemy import or_, select
from toncenter.exceptions import ToncenterError

from ton_payment.broker import (
    TRANSFER_COMPLETED_DLQ,
    TRANSFER_COMPLETED_QUEUE,
    TRANSFER_FINALIZED_QUEUE,
    broker,
    publish,
)
from ton_payment.config import settings
from ton_payment.database import async_session_maker
from ton_payment.models import (
    OutboxEvent,
    OutboxStatus,
    Transfer,
    TransferStatus,
)
from ton_payment.security import SIGNATURE_HEADER, generate_signature
from ton_payment.ton_gateway import (
    OutgoingTransfer,
    TonGateway,
    TonSigningError,
    TonTransaction,
)

logger = logging.getLogger(__name__)


async def process_next_outbox_event(ton_gateway: TonGateway) -> bool:
    async with async_session_maker() as session, session.begin():
        event: OutboxEvent | None = await session.scalar(
            select(OutboxEvent)
            .where(OutboxEvent.status == OutboxStatus.NEW)
            .order_by(OutboxEvent.created_at)
            .limit(1)
            .with_for_update(skip_locked=True),
        )
        if event is None:
            return False

        transfer = await session.get_one(Transfer, event.transfer_id)

        event.status = OutboxStatus.PROCESSING

    now = datetime.now(UTC)
    valid_until = int(
        (now + timedelta(seconds=settings.valid_for_seconds)).timestamp(),
    )

    outgoing = OutgoingTransfer(
        transfer_id=transfer.transfer_id,
        destination=transfer.destination,
        amount=transfer.amount,
        comment=transfer.comment,
        valid_until=valid_until,
    )

    try:
        prepare = await ton_gateway.prepare_transfer(outgoing)
        async with async_session_maker() as session, session.begin():
            transfer = await session.get_one(Transfer, event.transfer_id)
            transfer.msg_hash = prepare.msg_hash
            transfer.seqno = prepare.seqno
            transfer.boc = prepare.boc
            transfer.valid_until = valid_until
            transfer.expires_at = now + timedelta(
                seconds=settings.valid_for_seconds
                + settings.grace_period_seconds,
            )
    except TonSigningError as ex:
        async with async_session_maker() as session, session.begin():
            event = await session.get_one(OutboxEvent, event.event_id)
            logger.exception(
                "Failed to sign transfer %s",
                transfer.transfer_id,
            )
            event.status = OutboxStatus.FAILED
            event.attempts += 1
            event.last_error = f"prepare_transfer raised. {ex}"

            return True

    try:
        result = await ton_gateway.send_transfer(prepare)
    except ToncenterError as ex:
        async with async_session_maker() as session, session.begin():
            event = await session.get_one(OutboxEvent, event.event_id)
            logger.exception(
                "Failed to send transfer %s to TON",
                transfer.transfer_id,
            )
            event.status = OutboxStatus.FAILED
            event.attempts += 1
            event.last_error = f"send_transfer raised. {ex}"
            return True

    async with async_session_maker() as session, session.begin():
        event = await session.get_one(OutboxEvent, event.event_id)
        event.status = OutboxStatus.PUBLISHED
        event.published_at = now
        if result.msg_hash_norm is not None:
            saved = await session.get_one(Transfer, event.transfer_id)
            saved.msg_hash_norm = result.msg_hash_norm

            logger.info(
                "Transfer %s broadcast: %s TON -> %s, msg_hash=%s",
                transfer.transfer_id,
                transfer.amount.normalize(),
                transfer.destination,
                prepare.msg_hash,
            )

    advanced = await ton_gateway.wait_seqno_advanced(
        transfer.seqno,
        valid_until=valid_until,
    )
    if not advanced:
        logger.warning(
            "seqno %s did not advance before valid_until, msg_hash=%s: "
            "message was not included in a block",
            transfer.seqno,
            transfer.msg_hash,
        )
    return True


async def run_outbox_worker(
    ton_gateway: TonGateway,
    stop: asyncio.Event,
) -> None:
    logger.info(
        "Outbox worker started, polling every %s s",
        settings.outbox_poll_interval,
    )
    while not stop.is_set():
        try:
            processed = await process_next_outbox_event(ton_gateway)
        except Exception:
            logger.exception("Error in process_next_outbox_event")
            processed = False
        if not processed:
            await _sleep(stop, settings.outbox_poll_interval)


async def run_sse_tracker_once(ton_gateway: TonGateway) -> None:
    async def on_tx(tx: TonTransaction) -> None:
        logger.info(
            "SSE: transaction tx_hash=%s msg_hash=%s msg_hash_norm=%s "
            "success=%s reason=%s",
            tx.tx_hash,
            tx.msg_hash,
            tx.msg_hash_norm,
            tx.success,
            tx.failure_reason,
        )
        await _publish_finalized_by_msg_hash(
            tx.msg_hash,
            tx.msg_hash_norm,
            status="completed" if tx.success else "failed",
            tx_hash=tx.tx_hash,
            from_address=tx.from_address,
            failure_reason=tx.failure_reason,
        )

    logger.info("Connecting to wallet transactions SSE stream")
    await ton_gateway.run_finalized(on_tx)


async def run_sse_tracker(
    ton_gateway: TonGateway,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        try:
            await run_sse_tracker_once(ton_gateway)
        except Exception:
            logger.exception("Error in SSE tracker")
        await _sleep(stop, settings.tracker_sse_interval)


async def run_polling_tracker_once(ton_gateway: TonGateway) -> bool:
    """Fallback tracker: poll TON for pending transfer confirmations."""
    async with async_session_maker() as session:
        pending = list(
            (
                await session.scalars(
                    select(Transfer)
                    .where(
                        Transfer.status == TransferStatus.PENDING,
                        Transfer.msg_hash.is_not(None),
                    )
                    .order_by(Transfer.created_at)
                    .limit(50),
                )
            ).all(),
        )
    if not pending:
        return False

    start_ut = min(t.created_at for t in pending if t.created_at is not None)
    end_ut = datetime.now(UTC)
    transactions = await ton_gateway.fetch_transactions(
        start_ut=start_ut,
        end_ut=end_ut,
    )

    known_hashes = {t.msg_hash for t in pending if t.msg_hash}
    known_hashes_norm = {t.msg_hash_norm for t in pending if t.msg_hash_norm}
    logger.info(
        "Awaiting confirmation for %s transfers, window %s .. %s, "
        "transactions fetched: %s",
        len(pending),
        start_ut.isoformat(),
        end_ut.isoformat(),
        len(transactions),
    )
    for t in pending:
        logger.info(
            "Awaiting transfer %s msg_hash=%s msg_hash_norm=%s expires_at=%s",
            t.transfer_id,
            t.msg_hash,
            t.msg_hash_norm,
            t.expires_at,
        )
    for tx in transactions:
        matched = (
            tx.msg_hash in known_hashes
            or tx.msg_hash_norm in known_hashes_norm
        )
        logger.info(
            "tx_hash=%s msg_hash=%s msg_hash_norm=%s success=%s -> %s",
            tx.tx_hash,
            tx.msg_hash,
            tx.msg_hash_norm,
            tx.success,
            "matched" if matched else "not matched",
        )
        if not matched:
            continue
        await _publish_finalized_by_msg_hash(
            tx.msg_hash,
            tx.msg_hash_norm,
            status="completed" if tx.success else "failed",
            tx_hash=tx.tx_hash,
            from_address=tx.from_address,
            failure_reason=tx.failure_reason,
        )
    return True


async def run_polling_tracker(
    ton_gateway: TonGateway,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        try:
            await run_polling_tracker_once(ton_gateway)
        except Exception:
            logger.exception("Error in polling tracker")
        await _sleep(stop, settings.tracker_poll_interval)


async def run_expire_tracker_once() -> bool:
    """Fail pending transfers whose expires_at passed unconfirmed."""
    now = datetime.now(UTC)
    outbox_done = (
        select(OutboxEvent.event_id)
        .where(
            OutboxEvent.transfer_id == Transfer.transfer_id,
            OutboxEvent.status == OutboxStatus.FAILED,
        )
        .exists()
    )
    async with async_session_maker() as session, session.begin():
        expired = list(
            (
                await session.scalars(
                    select(Transfer)
                    .where(
                        Transfer.status == TransferStatus.PENDING,
                        Transfer.expires_at <= now,
                        outbox_done,
                    )
                    .order_by(Transfer.expires_at)
                    .limit(50)
                    .with_for_update(skip_locked=True),
                )
            ).all(),
        )
        if not expired:
            return False

        for transfer in expired:
            logger.info(
                "Transfer %s expired (expires_at=%s, msg_hash=%s), "
                "publishing failed",
                transfer.transfer_id,
                transfer.expires_at,
                transfer.msg_hash,
            )
            await publish(
                TRANSFER_FINALIZED_QUEUE,
                {
                    "transfer_id": str(transfer.transfer_id),
                    "status": "failed",
                    "failure_reason": "expired: no confirmation before "
                    "expires_at (valid_until + grace period)",
                },
            )
    return True


async def run_expire_tracker(stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await run_expire_tracker_once()
        except Exception:
            logger.exception("Error in expiry tracker")
        await _sleep(stop, settings.tracker_expire_interval)


async def _publish_finalized_by_msg_hash(
    msg_hash: str | None,
    msg_hash_norm: str | None,
    *,
    status: str | None,
    tx_hash: str | None,
    from_address: str | None,
    failure_reason: str | None = None,
) -> None:
    conditions = []
    if msg_hash:
        conditions.append(Transfer.msg_hash == msg_hash)
    if msg_hash_norm:
        conditions.append(Transfer.msg_hash_norm == msg_hash_norm)
    if not conditions:
        return
    async with async_session_maker() as session:
        transfer = await session.scalar(
            select(Transfer).where(or_(*conditions)),
        )
    if transfer is None:
        logger.warning(
            "Transfer not found: msg_hash=%s, msg_hash_norm=%s",
            msg_hash,
            msg_hash_norm,
        )
        return
    logger.info(
        "Found transfer %s (status=%s) by msg_hash=%s / msg_hash_norm=%s, "
        "publishing %s (reason: %s)",
        transfer.transfer_id,
        transfer.status,
        msg_hash,
        msg_hash_norm,
        status,
        failure_reason,
    )
    payload = {
        "transfer_id": str(transfer.transfer_id),
        "status": status,
        "tx_hash": tx_hash,
        "from_address": from_address,
    }
    if failure_reason is not None:
        payload["failure_reason"] = failure_reason
    await publish(TRANSFER_FINALIZED_QUEUE, payload)


async def finalize_transfer(payload: dict[str, Any]) -> None:
    """Consume the transfer.finalized queue."""
    transfer_id = UUID(str(payload["transfer_id"]))
    async with async_session_maker() as session, session.begin():
        transfer = await session.get_one(
            Transfer,
            transfer_id,
            with_for_update=True,
        )
        if transfer.is_final():
            logger.info("Transfer %s is already final", transfer_id)
            return

        if payload["status"] == "completed":
            transfer.status = TransferStatus.COMPLETED
            transfer.tx_hash = payload.get("tx_hash")
            transfer.from_address = payload.get("from_address")
            transfer.to_address = transfer.destination
        else:
            transfer.status = TransferStatus.FAILED
            transfer.failure_reason = payload.get("failure_reason", "unknown")

        completed_payload = {
            "transfer_id": str(transfer.transfer_id),
            "external_id": transfer.external_id,
            "amount": str(transfer.amount.normalize()),
            "status": transfer.status.value,
            "details": {
                "from_address": transfer.from_address,
                "to_address": transfer.to_address,
                "tx_hash": transfer.tx_hash,
            },
        }
    await publish(TRANSFER_COMPLETED_QUEUE, completed_payload)


@broker.subscriber(
    TRANSFER_FINALIZED_QUEUE,
    channel=Channel(prefetch_count=20),
)
async def handle_transfer_finalized(payload: dict[str, Any]) -> None:
    await finalize_transfer(payload)


class _WebhookHttp:
    session: aiohttp.ClientSession | None = None


def get_webhook_http() -> aiohttp.ClientSession:
    if _WebhookHttp.session is None or _WebhookHttp.session.closed:
        _WebhookHttp.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10.0),
        )
    return _WebhookHttp.session


async def close_webhook_http() -> None:
    if _WebhookHttp.session is not None:
        await _WebhookHttp.session.close()
        _WebhookHttp.session = None


async def send_webhook(payload: dict[str, Any]) -> None:
    """Consume the transfer.completed queue and send the webhook."""
    body = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    headers = {
        "Content-Type": "application/json",
        SIGNATURE_HEADER: generate_signature(
            settings.api_token.get_secret_value(),
            body,
        ),
    }

    last_error: Exception | None = None
    http = get_webhook_http()
    for attempt in range(1, settings.webhook_max_attempts + 1):
        try:
            async with http.post(
                settings.webhook_url,
                data=body,
                headers=headers,
            ) as response:
                response.raise_for_status()
        except (aiohttp.ClientError, TimeoutError) as exc:
            last_error = exc
            logger.warning(
                "Webhook for transfer %s failed (attempt %s/%s): %s",
                payload.get("transfer_id"),
                attempt,
                settings.webhook_max_attempts,
                exc,
            )
            if attempt < settings.webhook_max_attempts:
                await asyncio.sleep(settings.webhook_retry_delay_seconds)
        else:
            logger.info(
                "Webhook for transfer %s delivered (attempt %s)",
                payload.get("transfer_id"),
                attempt,
            )
            return
    await publish(TRANSFER_COMPLETED_DLQ, payload)
    logger.error(
        "Webhook for transfer %s not delivered after %s attempts, "
        "sent to %s: %s",
        payload.get("transfer_id"),
        settings.webhook_max_attempts,
        TRANSFER_COMPLETED_DLQ,
        last_error,
    )


@broker.subscriber(
    TRANSFER_COMPLETED_QUEUE,
    channel=Channel(prefetch_count=20),
)
async def handle_transfer_completed(payload: dict[str, Any]) -> None:
    await send_webhook(payload)


async def _sleep(stop: asyncio.Event, seconds: float) -> None:
    with suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)
