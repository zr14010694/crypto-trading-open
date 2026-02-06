import asyncio
from decimal import Decimal

from core.adapters.exchanges.adapters.standx import StandXAdapter
from core.adapters.exchanges.interface import ExchangeConfig
from core.adapters.exchanges.models import ExchangeType, OrderSide, OrderType


class _StubRest:
    def __init__(self):
        self.calls = []

    async def query_symbol_info(self):
        return [{"symbol": "BTC-USD", "price_tick_decimals": 2, "qty_tick_decimals": 3}]

    async def query_symbol_price(self, symbol):
        return {
            "symbol": symbol,
            "last_price": "100",
            "spread_bid": "99",
            "spread_ask": "101",
        }

    async def query_depth_book(self, symbol):
        return {"symbol": symbol, "asks": [["101", "1"]], "bids": [["99", "1"]]}

    async def new_order(self, payload):
        return {
            "id": 1,
            "cl_ord_id": payload.get("cl_ord_id"),
            "symbol": payload["symbol"],
            "side": payload["side"],
            "order_type": payload["order_type"],
            "qty": payload["qty"],
            "fill_qty": "0",
            "price": payload.get("price"),
            "status": "open",
            "created_at": "2025-08-11T03:35:25.559151Z",
            "updated_at": "2025-08-11T03:35:25.559151Z",
        }

    async def cancel_order(self, payload):
        return {
            "id": payload.get("order_id", 1),
            "symbol": "BTC-USD",
            "side": "buy",
            "order_type": "limit",
            "qty": "0.01",
            "fill_qty": "0",
            "price": "100",
            "status": "canceled",
            "created_at": "2025-08-11T03:35:25.559151Z",
        }

    async def query_order(self, order_id):
        return {
            "id": order_id,
            "symbol": "BTC-USD",
            "side": "buy",
            "order_type": "limit",
            "qty": "0.01",
            "fill_qty": "0",
            "price": "100",
            "status": "open",
            "created_at": "2025-08-11T03:35:25.559151Z",
        }

    async def query_open_orders(self, symbol=None):
        return {"result": [await self.query_order(1)]}

    async def query_positions(self, symbol=None):
        return []

    async def query_balance(self):
        return {"token": "DUSD", "free": "1", "locked": "0", "total": "1"}

    async def query_trades(self, symbol=None, limit=None):
        return {"result": []}

    def _parse_symbol_info(self, data):
        from core.adapters.exchanges.adapters.standx_rest import StandXRest
        rest = StandXRest(None, None)
        return rest._parse_symbol_info(data)

    def _parse_ticker(self, data):
        from core.adapters.exchanges.adapters.standx_rest import StandXRest
        rest = StandXRest(None, None)
        return rest._parse_ticker(data)

    def _parse_orderbook(self, data):
        from core.adapters.exchanges.adapters.standx_rest import StandXRest
        rest = StandXRest(None, None)
        return rest._parse_orderbook(data)

    def _build_order_payload(self, **kwargs):
        return {
            "symbol": kwargs["symbol"],
            "side": kwargs["side"].value,
            "order_type": kwargs["order_type"].value,
            "qty": str(kwargs["qty"]),
            "price": str(kwargs["price"]) if kwargs.get("price") else None,
            "cl_ord_id": kwargs.get("cl_ord_id"),
        }

    def _build_cancel_payload(self, order_id=None, cl_ord_id=None):
        return {"order_id": order_id, "cl_ord_id": cl_ord_id}

    def _parse_order(self, data):
        from core.adapters.exchanges.adapters.standx_rest import StandXRest
        rest = StandXRest(None, None)
        return rest._parse_order(data)

    def _parse_positions(self, data):
        from core.adapters.exchanges.adapters.standx_rest import StandXRest
        rest = StandXRest(None, None)
        return rest._parse_positions(data)

    def _parse_balances(self, data):
        from core.adapters.exchanges.adapters.standx_rest import StandXRest
        rest = StandXRest(None, None)
        return rest._parse_balances(data)

    async def close(self):
        return None


class _StubWebSocket:
    def __init__(self):
        self._order_callbacks = []
        self._position_callbacks = []
        self.connected = False

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.connected = False

    async def authenticate(self):
        return None


async def _make_adapter():
    config = ExchangeConfig(
        exchange_id="standx",
        name="StandX",
        exchange_type=ExchangeType.PERPETUAL,
        api_key="",
        api_secret="",
    )
    adapter = StandXAdapter(config)
    adapter.rest = _StubRest()
    adapter.websocket = _StubWebSocket()
    return adapter


def test_adapter_get_ticker_and_orderbook():
    adapter = asyncio.get_event_loop().run_until_complete(_make_adapter())
    ticker = asyncio.get_event_loop().run_until_complete(adapter.get_ticker("BTC-USD"))
    orderbook = asyncio.get_event_loop().run_until_complete(adapter.get_orderbook("BTC-USD"))

    assert ticker.symbol == "BTC-USD"
    assert orderbook.symbol == "BTC-USD"


def test_adapter_create_and_cancel_order():
    adapter = asyncio.get_event_loop().run_until_complete(_make_adapter())
    order = asyncio.get_event_loop().run_until_complete(
        adapter.create_order(
            symbol="BTC-USD",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            amount=Decimal("0.01"),
            price=Decimal("100"),
        )
    )
    assert order.symbol == "BTC-USD"

    canceled = asyncio.get_event_loop().run_until_complete(adapter.cancel_order("1"))
    assert canceled.status.value == "canceled"
