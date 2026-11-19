import asyncio
import logging
import signal

from ton_payment.broker import broker
from ton_payment.ton_gateway import create_ton_gateway
from ton_payment.workers import run_outbox_worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


async def main() -> None:
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    async with create_ton_gateway() as ton_gateway, broker:
        await run_outbox_worker(ton_gateway, stop)


if __name__ == "__main__":
    asyncio.run(main())
