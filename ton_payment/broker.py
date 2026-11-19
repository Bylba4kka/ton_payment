import json
from typing import Any

from faststream.rabbit import RabbitBroker

from ton_payment.config import settings

broker = RabbitBroker(settings.rabbitmq_url)

TRANSFER_FINALIZED_QUEUE = "transfer.finalized"
TRANSFER_COMPLETED_QUEUE = "transfer.completed"
TRANSFER_COMPLETED_DLQ = "transfer.completed.dlq"


async def publish(queue: str, payload: dict[str, Any]) -> None:
    await broker.publish(
        json.dumps(payload).encode(),
        queue=queue,
        persist=True,
    )
