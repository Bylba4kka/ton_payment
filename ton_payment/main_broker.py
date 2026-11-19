from faststream import FastStream
from faststream.rabbit import RabbitQueue

from ton_payment import workers
from ton_payment.broker import TRANSFER_COMPLETED_DLQ, broker

faststream_app = FastStream(broker)


@faststream_app.after_startup
async def declare_dlq() -> None:
    await broker.declare_queue(
        RabbitQueue(TRANSFER_COMPLETED_DLQ, durable=True),
    )


@faststream_app.on_shutdown
async def close_http() -> None:
    await workers.close_webhook_http()
