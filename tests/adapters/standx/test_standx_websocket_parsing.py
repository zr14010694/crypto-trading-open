from decimal import Decimal

from core.adapters.exchanges.adapters.standx_websocket import StandXWebSocket
from core.adapters.exchanges.interface import ExchangeConfig
from core.adapters.exchanges.models import ExchangeType


def _make_config():
    return ExchangeConfig(
        exchange_id="standx",
        name="StandX",
        exchange_type=ExchangeType.PERPETUAL,
        api_key="",
        api_secret="",
    )


def test_parse_ws_ticker():
    ws = StandXWebSocket(_make_config())
    raw = {
        "symbol": "BTC-USD",
        "index_price": "121890.651250",
        "last_price": "121897.95",
        "mark_price": "121897.56",
        "spread": ["121897.95", "121898.05"],
        "time": "2025-08-11T07:23:50.923602Z",
    }

    ticker = ws._parse_ticker(raw)
    assert ticker.symbol == "BTC-USD"
    assert ticker.bid == Decimal("121897.95")
    assert ticker.ask == Decimal("121898.05")


def test_parse_ws_orderbook_sorting():
    ws = StandXWebSocket(_make_config())
    raw = {
        "symbol": "BTC-USD",
        "asks": [["121896.32", "1.0"], ["121896.02", "0.5"]],
        "bids": [["121884.52", "0.2"], ["121884.22", "0.1"]],
    }

    orderbook = ws._parse_orderbook(raw)
    assert orderbook.asks[0].price == Decimal("121896.02")
    assert orderbook.bids[0].price == Decimal("121884.52")


def test_parse_ws_order():
    ws = StandXWebSocket(_make_config())
    raw = {
        "id": 2547027,
        "cl_ord_id": "01K2C9H93Y42RW8KD6RSVWVDVV",
        "symbol": "BTC-USD",
        "side": "buy",
        "order_type": "market",
        "qty": "1.000",
        "fill_qty": "1.000",
        "price": "121245.20",
        "fill_avg_price": "121245.21",
        "status": "filled",
        "created_at": "2025-08-11T10:06:37.182464Z",
        "updated_at": "2025-08-11T10:06:37.182465Z",
    }

    order = ws._parse_order(raw)
    assert order.symbol == "BTC-USD"
    assert order.filled == Decimal("1.000")


def test_parse_ws_position_balance():
    ws = StandXWebSocket(_make_config())
    position = ws._parse_position(
        {
            "symbol": "BTC-USD",
            "qty": "23.669",
            "entry_price": "121677.65",
            "mark_price": "121715.05",
            "leverage": "15",
            "margin_mode": "isolated",
            "updated_at": "2025-08-10T09:05:50.265265Z",
        }
    )

    assert position.size == Decimal("23.669")

    balance = ws._parse_balance(
        {
            "token": "DUSD",
            "free": "906946.976225666",
            "locked": "0.000000000",
            "total": "923207.752500717",
        }
    )

    assert balance.currency == "DUSD"
    assert balance.free == Decimal("906946.976225666")
