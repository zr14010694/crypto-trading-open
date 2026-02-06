"""Aster REST 公共端点解析单元测试"""

from decimal import Decimal
from datetime import datetime

from core.adapters.exchanges.adapters.aster_rest import AsterRest


def make_rest() -> AsterRest:
    return AsterRest()


# --- _parse_exchange_info ---

def test_parse_exchange_info_populates_precision():
    rest = make_rest()
    data = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "pair": "BTCUSDT",
                "contractType": "PERPETUAL",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "pricePrecision": 2,
                "quantityPrecision": 3,
                "status": "TRADING",
            },
            {
                "symbol": "ETHUSDT",
                "pair": "ETHUSDT",
                "contractType": "PERPETUAL",
                "baseAsset": "ETH",
                "quoteAsset": "USDT",
                "pricePrecision": 2,
                "quantityPrecision": 3,
                "status": "TRADING",
            },
        ]
    }
    info = rest._parse_exchange_info(data)
    assert "BTCUSDT" in info.markets
    assert "ETHUSDT" in info.markets
    assert info.precision["price"]["BTCUSDT"] == 2
    assert info.precision["quantity"]["BTCUSDT"] == 3
    assert rest._precision_cache["BTCUSDT"] == (2, 3)


def test_parse_exchange_info_skips_non_trading():
    rest = make_rest()
    data = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "pricePrecision": 2,
                "quantityPrecision": 3,
                "status": "TRADING",
            },
            {
                "symbol": "OLDUSDT",
                "baseAsset": "OLD",
                "quoteAsset": "USDT",
                "pricePrecision": 2,
                "quantityPrecision": 3,
                "status": "BREAK",
            },
        ]
    }
    info = rest._parse_exchange_info(data)
    assert "BTCUSDT" in info.markets
    assert "OLDUSDT" not in info.markets


# --- _parse_ticker ---

def test_parse_ticker_from_book_ticker():
    rest = make_rest()
    data = {
        "symbol": "BTCUSDT",
        "bidPrice": "97000.50",
        "bidQty": "1.5",
        "askPrice": "97001.00",
        "askQty": "2.0",
        "time": 1700000000000,
    }
    ticker = rest._parse_ticker(data)
    assert ticker.symbol == "BTCUSDT"
    assert ticker.bid == Decimal("97000.50")
    assert ticker.ask == Decimal("97001.00")
    assert ticker.bid_size == Decimal("1.5")
    assert ticker.ask_size == Decimal("2.0")


def test_parse_ticker_missing_fields():
    rest = make_rest()
    data = {"symbol": "ETHUSDT"}
    ticker = rest._parse_ticker(data)
    assert ticker.symbol == "ETHUSDT"
    assert ticker.bid is None
    assert ticker.ask is None


# --- _parse_orderbook ---

def test_parse_orderbook_sorts_levels():
    rest = make_rest()
    data = {
        "symbol": "BTCUSDT",
        "bids": [
            ["96999.00", "1.0"],
            ["97001.00", "2.0"],
            ["97000.00", "1.5"],
        ],
        "asks": [
            ["97005.00", "1.0"],
            ["97003.00", "0.5"],
            ["97004.00", "2.0"],
        ],
        "T": 1700000000000,
    }
    ob = rest._parse_orderbook(data)
    assert ob.symbol == "BTCUSDT"
    # bids 降序
    assert ob.bids[0].price == Decimal("97001.00")
    assert ob.bids[1].price == Decimal("97000.00")
    assert ob.bids[2].price == Decimal("96999.00")
    # asks 升序
    assert ob.asks[0].price == Decimal("97003.00")
    assert ob.asks[1].price == Decimal("97004.00")
    assert ob.asks[2].price == Decimal("97005.00")


def test_parse_orderbook_empty():
    rest = make_rest()
    data = {"symbol": "BTCUSDT", "bids": [], "asks": []}
    ob = rest._parse_orderbook(data)
    assert len(ob.bids) == 0
    assert len(ob.asks) == 0


# --- _parse_mark_price ---

def test_parse_mark_price():
    rest = make_rest()
    data = {
        "symbol": "BTCUSDT",
        "markPrice": "97100.50",
        "indexPrice": "97095.00",
        "lastFundingRate": "0.00012",
        "nextFundingTime": 1700003600000,
        "time": 1700000000000,
    }
    ticker = rest._parse_mark_price(data)
    assert ticker.symbol == "BTCUSDT"
    assert ticker.mark_price == Decimal("97100.50")
    assert ticker.index_price == Decimal("97095.00")
    assert ticker.funding_rate == Decimal("0.00012")
