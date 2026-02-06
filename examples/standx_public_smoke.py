import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.adapters.exchanges.adapters.standx_rest import StandXRest
from core.adapters.exchanges.interface import ExchangeConfig
from core.adapters.exchanges.models import ExchangeType


async def main() -> None:
    config = ExchangeConfig(
        exchange_id="standx",
        name="StandX",
        exchange_type=ExchangeType.PERPETUAL,
        api_key="",
        api_secret="",
        base_url="https://perps.standx.com",
        extra_params={"ssl_verify": False},
    )

    rest = StandXRest(config)
    try:
        ticker = await rest.query_symbol_price("BTC-USD")
        orderbook = await rest.query_depth_book("BTC-USD")

        print("StandX BTC-USD ticker:")
        print(ticker)
        print("StandX BTC-USD orderbook levels:")
        print(
            {
                "bids": orderbook.get("bids", [])[:3],
                "asks": orderbook.get("asks", [])[:3],
            }
        )
    finally:
        await rest.close()


if __name__ == "__main__":
    asyncio.run(main())
