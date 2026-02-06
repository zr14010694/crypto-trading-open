"""Aster WebSocket 消息解析单元测试"""

import asyncio
from decimal import Decimal

import pytest

from core.adapters.exchanges.adapters.aster_websocket import AsterWebSocket
from core.adapters.exchanges.models import (
    OrderSide,
    OrderStatus,
    PositionSide,
)


def make_ws() -> AsterWebSocket:
    return AsterWebSocket()


# --- _parse_ws_ticker ---

def test_parse_ws_ticker():
    ws = make_ws()
    data = {
        "e": "bookTicker",
        "s": "BTCUSDT",
        "b": "97000.50",
        "B": "1.2",
        "a": "97001.00",
        "A": "0.8",
        "T": 1700000000000,
    }
    ticker = ws._parse_ws_ticker(data)
    assert ticker.symbol == "BTCUSDT"
    assert ticker.bid == Decimal("97000.50")
    assert ticker.ask == Decimal("97001.00")
    assert ticker.bid_size == Decimal("1.2")
    assert ticker.ask_size == Decimal("0.8")


# --- markPriceUpdate ---

@pytest.mark.asyncio
async def test_handle_mark_price_update():
    """markPriceUpdate 事件应解析并推送含 funding_rate 的 TickerData"""
    ws = make_ws()
    received = []

    async def on_ticker(t):
        received.append(t)

    ws._ticker_callbacks.append(on_ticker)

    msg = {
        "e": "markPriceUpdate",
        "E": 1700000000000,
        "s": "BTCUSDT",
        "p": "97500.00",
        "i": "97480.00",
        "r": "0.00015",
        "T": 1700003600000,
    }
    await ws._handle_message(msg)

    assert len(received) == 1
    assert received[0].symbol == "BTCUSDT"
    assert received[0].mark_price == Decimal("97500.00")
    assert received[0].index_price == Decimal("97480.00")
    assert received[0].funding_rate == Decimal("0.00015")
    # 确认缓存也被更新
    assert ws._funding_rates["BTCUSDT"] == "0.00015"


def test_funding_rate_injected_into_book_ticker():
    """bookTicker 应从缓存注入 funding_rate"""
    ws = make_ws()
    # 预填缓存
    ws._funding_rates["BTCUSDT"] = "0.0002"

    data = {
        "e": "bookTicker",
        "s": "BTCUSDT",
        "b": "97000.50",
        "B": "1.2",
        "a": "97001.00",
        "A": "0.8",
        "T": 1700000000000,
    }
    ticker = ws._parse_ws_ticker(data)
    assert ticker.funding_rate == Decimal("0.0002")


def test_funding_rate_none_when_no_cache():
    """没有缓存时 bookTicker 的 funding_rate 应为 None"""
    ws = make_ws()
    data = {
        "e": "bookTicker",
        "s": "ETHUSDT",
        "b": "3800.00",
        "B": "10",
        "a": "3801.00",
        "A": "5",
        "T": 1700000000000,
    }
    ticker = ws._parse_ws_ticker(data)
    assert ticker.funding_rate is None


# --- _parse_ws_orderbook ---

def test_parse_ws_orderbook():
    ws = make_ws()
    data = {
        "e": "depthUpdate",
        "s": "BTCUSDT",
        "b": [
            ["97001.00", "2.0"],
            ["96999.00", "1.0"],
            ["97000.00", "1.5"],
        ],
        "a": [
            ["97005.00", "1.0"],
            ["97003.00", "0.5"],
            ["97004.00", "2.0"],
        ],
        "T": 1700000000000,
    }
    ob = ws._parse_ws_orderbook(data)
    assert ob.symbol == "BTCUSDT"
    # bids 降序
    assert ob.bids[0].price == Decimal("97001.00")
    assert ob.bids[1].price == Decimal("97000.00")
    assert ob.bids[2].price == Decimal("96999.00")
    # asks 升序
    assert ob.asks[0].price == Decimal("97003.00")
    assert ob.asks[1].price == Decimal("97004.00")
    assert ob.asks[2].price == Decimal("97005.00")


# --- _parse_ws_order_update ---

def test_parse_ws_order_update():
    ws = make_ws()
    data = {
        "e": "ORDER_TRADE_UPDATE",
        "T": 1700000000000,
        "o": {
            "s": "BTCUSDT",
            "S": "BUY",
            "o": "LIMIT",
            "q": "0.01",
            "p": "97000.00",
            "ap": "0",
            "z": "0",
            "X": "NEW",
            "i": 12345,
            "c": "my_order_001",
            "T": 1700000000000,
        },
    }
    order = ws._parse_ws_order_update(data)
    assert order.id == "12345"
    assert order.client_id == "my_order_001"
    assert order.symbol == "BTCUSDT"
    assert order.side == OrderSide.BUY
    assert order.status == OrderStatus.OPEN
    assert order.amount == Decimal("0.01")
    assert order.price == Decimal("97000.00")


def test_parse_ws_order_update_filled():
    ws = make_ws()
    data = {
        "e": "ORDER_TRADE_UPDATE",
        "T": 1700000000000,
        "o": {
            "s": "ETHUSDT",
            "S": "SELL",
            "o": "MARKET",
            "q": "1.0",
            "p": "0",
            "ap": "3800.50",
            "z": "1.0",
            "X": "FILLED",
            "i": 12346,
            "c": "",
            "T": 1700000000000,
        },
    }
    order = ws._parse_ws_order_update(data)
    assert order.status == OrderStatus.FILLED
    assert order.filled == Decimal("1.0")
    assert order.average == Decimal("3800.50")


# --- _parse_ws_account_update ---

def test_parse_ws_account_update_positions():
    ws = make_ws()
    data = {
        "e": "ACCOUNT_UPDATE",
        "T": 1700000000000,
        "a": {
            "B": [
                {"a": "USDT", "wb": "10000.50", "cw": "10000.50"},
            ],
            "P": [
                {
                    "s": "BTCUSDT",
                    "pa": "0.05",
                    "ep": "96500.00",
                    "up": "25.00",
                    "mt": "cross",
                },
            ],
        },
    }
    positions, balances = ws._parse_ws_account_update(data)
    assert len(positions) == 1
    assert positions[0].symbol == "BTCUSDT"
    assert positions[0].side == PositionSide.LONG
    assert positions[0].size == Decimal("0.05")
    assert positions[0].entry_price == Decimal("96500.00")

    assert len(balances) == 1
    assert balances[0].currency == "USDT"
    assert balances[0].total == Decimal("10000.50")


def test_parse_ws_account_update_short():
    ws = make_ws()
    data = {
        "e": "ACCOUNT_UPDATE",
        "T": 1700000000000,
        "a": {
            "B": [],
            "P": [
                {
                    "s": "ETHUSDT",
                    "pa": "-2.0",
                    "ep": "3800.00",
                    "up": "100.00",
                    "mt": "isolated",
                },
            ],
        },
    }
    positions, balances = ws._parse_ws_account_update(data)
    assert len(positions) == 1
    assert positions[0].side == PositionSide.SHORT
    assert positions[0].size == Decimal("2.0")


# --- subscription message format ---

def test_build_subscribe_message():
    ws = make_ws()
    msg = ws._build_subscribe_message(["btcusdt@bookTicker", "ethusdt@bookTicker"])
    assert msg["method"] == "SUBSCRIBE"
    assert "btcusdt@bookTicker" in msg["params"]
    assert "ethusdt@bookTicker" in msg["params"]
    assert "id" in msg


def test_build_unsubscribe_message():
    ws = make_ws()
    msg = ws._build_unsubscribe_message(["btcusdt@bookTicker"])
    assert msg["method"] == "UNSUBSCRIBE"
    assert "btcusdt@bookTicker" in msg["params"]


# --- subscribe_orders / subscribe_order_fills ---

@pytest.mark.asyncio
async def test_subscribe_orders_registers_callback():
    ws = make_ws()
    cb = lambda order: None
    await ws.subscribe_orders(cb)
    assert cb in ws._order_callbacks
    # 重复注册不会添加第二次
    await ws.subscribe_orders(cb)
    assert ws._order_callbacks.count(cb) == 1


@pytest.mark.asyncio
async def test_subscribe_order_fills_registers_callback():
    ws = make_ws()
    cb = lambda order: None
    await ws.subscribe_order_fills(cb)
    assert cb in ws._order_fill_callbacks
    await ws.subscribe_order_fills(cb)
    assert ws._order_fill_callbacks.count(cb) == 1


@pytest.mark.asyncio
async def test_order_fill_callback_only_fires_on_filled():
    ws = make_ws()
    all_orders = []
    fill_orders = []

    async def on_order(o):
        all_orders.append(o)

    async def on_fill(o):
        fill_orders.append(o)

    await ws.subscribe_orders(on_order)
    await ws.subscribe_order_fills(on_fill)

    # FILLED 订单
    filled_msg = {
        "e": "ORDER_TRADE_UPDATE", "T": 1700000000000,
        "o": {"s": "BTCUSDT", "S": "BUY", "o": "MARKET", "q": "0.01",
              "p": "0", "ap": "97000.00", "z": "0.01", "X": "FILLED",
              "i": 1, "c": "", "T": 1700000000000},
    }
    await ws._handle_user_message(filled_msg)

    # NEW 订单
    new_msg = {
        "e": "ORDER_TRADE_UPDATE", "T": 1700000000000,
        "o": {"s": "BTCUSDT", "S": "BUY", "o": "LIMIT", "q": "0.01",
              "p": "97000.00", "ap": "0", "z": "0", "X": "NEW",
              "i": 2, "c": "", "T": 1700000000000},
    }
    await ws._handle_user_message(new_msg)

    assert len(all_orders) == 2
    assert len(fill_orders) == 1
    assert fill_orders[0].status == OrderStatus.FILLED


@pytest.mark.asyncio
async def test_subscribe_orders_receives_all_statuses():
    ws = make_ws()
    received = []

    async def on_order(o):
        received.append(o)

    await ws.subscribe_orders(on_order)

    for status_str in ("NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED"):
        msg = {
            "e": "ORDER_TRADE_UPDATE", "T": 1700000000000,
            "o": {"s": "BTCUSDT", "S": "BUY", "o": "LIMIT", "q": "1",
                  "p": "97000", "ap": "0", "z": "0", "X": status_str,
                  "i": 100, "c": "", "T": 1700000000000},
        }
        await ws._handle_user_message(msg)

    assert len(received) == 4
