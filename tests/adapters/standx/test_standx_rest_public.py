from decimal import Decimal

from core.adapters.exchanges.adapters.standx_rest import StandXRest
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


def test_parse_symbol_info_populates_precision_cache():
    rest = StandXRest(_make_config())
    raw = [
        {"symbol": "BTC-USD", "price_tick_decimals": 2, "qty_tick_decimals": 3},
        {"symbol": "ETH-USD", "price_tick_decimals": 2, "qty_tick_decimals": 4},
    ]
    info = rest._parse_symbol_info(raw)

    assert rest._precision_cache["BTC-USD"] == (2, 3)
    assert info.markets["BTC-USD"]["precision"]["price"] == 2
    assert info.markets["ETH-USD"]["precision"]["amount"] == 4


def test_parse_ticker_from_symbol_price():
    rest = StandXRest(_make_config())
    raw = {
        "symbol": "BTC-USD",
        "index_price": "121601.158461",
        "last_price": "121599.94",
        "mark_price": "121602.43",
        "spread_bid": "121599.94",
        "spread_ask": "121600.04",
        "time": "2025-08-11T03:44:40.922233Z",
    }
    ticker = rest._parse_ticker(raw)

    assert ticker.symbol == "BTC-USD"
    assert ticker.bid == Decimal("121599.94")
    assert ticker.ask == Decimal("121600.04")
    assert ticker.last == Decimal("121599.94")
    assert ticker.mark_price == Decimal("121602.43")


def test_parse_orderbook_sorts_levels():
    rest = StandXRest(_make_config())
    raw = {
        "symbol": "BTC-USD",
        "asks": [["121896.11", "1.0"], ["121895.81", "0.5"]],
        "bids": [["121884.31", "0.2"], ["121884.01", "0.1"]],
    }
    orderbook = rest._parse_orderbook(raw)

    assert orderbook.asks[0].price == Decimal("121895.81")
    assert orderbook.bids[0].price == Decimal("121884.31")
