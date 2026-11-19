import asyncio
import logging

from ton_payment.broker import broker
from ton_payment.ton_gateway import create_ton_gateway
from ton_payment.workers import (
    run_expire_tracker,
    run_polling_tracker,
    run_sse_tracker,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


async def main() -> None:
    stop = asyncio.Event()
    async with create_ton_gateway() as ton_gateway, broker:
        await asyncio.gather(
            run_sse_tracker(ton_gateway, stop),
            run_polling_tracker(ton_gateway, stop),
            run_expire_tracker(stop),
        )


if __name__ == "__main__":
    asyncio.run(main())
