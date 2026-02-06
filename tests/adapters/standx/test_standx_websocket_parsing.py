from decimal import Decimal

import pytest

from core.adapters.exchanges.adapters.standx_websocket import StandXWebSocket
from core.adapters.exchanges.interface import ExchangeConfig
from core.adapters.exchanges.models import ExchangeType, OrderStatus


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


def test_parse_ws_ticker_with_funding_rate():
    ws = StandXWebSocket(_make_config())
    raw = {
        "symbol": "BTC-USD",
        "last_price": "100000.00",
        "mark_price": "100001.00",
        "spread": ["100000.00", "100002.00"],
        "time": "2025-08-11T07:23:50.923602Z",
        "funding_rate": "0.0001",
    }
    ticker = ws._parse_ticker(raw)
    assert ticker.funding_rate == Decimal("0.0001")


def test_parse_ws_ticker_funding_rate_from_cache():
    """WS price channel 不含 funding_rate 时，从缓存注入"""
    ws = StandXWebSocket(_make_config())
    ws._funding_rates["BTC-USD"] = "0.00025"
    raw = {
        "symbol": "BTC-USD",
        "last_price": "100000.00",
        "mark_price": "100001.00",
        "spread": ["100000.00", "100002.00"],
        "time": "2025-08-11T07:23:50.923602Z",
    }
    ticker = ws._parse_ticker(raw)
    assert ticker.funding_rate == Decimal("0.00025")


def test_parse_ws_ticker_funding_rate_none_without_cache():
    """无缓存且 WS 数据不含 funding_rate 时，应为 None"""
    ws = StandXWebSocket(_make_config())
    raw = {
        "symbol": "ETH-USD",
        "last_price": "3000.00",
        "mark_price": "3001.00",
        "spread": ["3000.00", "3002.00"],
        "time": "2025-08-11T07:23:50.923602Z",
    }
    ticker = ws._parse_ticker(raw)
    assert ticker.funding_rate is None


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


# --- ticker / orderbook 回调分离 ---

@pytest.mark.asyncio
async def test_ticker_and_orderbook_callbacks_are_separated():
    """ticker 和 orderbook 使用独立回调列表，互不干扰"""
    ws = StandXWebSocket(_make_config())
    ticker_results = []
    orderbook_results = []

    async def on_ticker(t):
        ticker_results.append(t)

    async def on_orderbook(o):
        orderbook_results.append(o)

    ws._ticker_callbacks.append(on_ticker)
    ws._orderbook_callbacks.append(on_orderbook)

    # 发送 price 消息
    price_msg = {
        "channel": "price",
        "data": {
            "symbol": "BTC-USD",
            "last_price": "100000.00",
            "mark_price": "100001.00",
            "spread": ["100000.00", "100002.00"],
            "time": "2025-08-11T07:23:50.923602Z",
        },
    }
    await ws._handle_message(price_msg)

    # 发送 depth_book 消息
    depth_msg = {
        "channel": "depth_book",
        "data": {
            "symbol": "BTC-USD",
            "asks": [["100002.00", "1.0"]],
            "bids": [["100000.00", "1.0"]],
        },
    }
    await ws._handle_message(depth_msg)

    # ticker 回调只收到 TickerData，orderbook 回调只收到 OrderBookData
    assert len(ticker_results) == 1
    assert len(orderbook_results) == 1
    assert ticker_results[0].symbol == "BTC-USD"
    assert orderbook_results[0].symbol == "BTC-USD"


# --- subscribe_orders / subscribe_order_fills ---

@pytest.mark.asyncio
async def test_subscribe_orders_registers_callback():
    ws = StandXWebSocket(_make_config())
    cb = lambda order: None
    await ws.subscribe_orders(cb)
    assert cb in ws._order_callbacks
    await ws.subscribe_orders(cb)
    assert ws._order_callbacks.count(cb) == 1


@pytest.mark.asyncio
async def test_subscribe_order_fills_registers_callback():
    ws = StandXWebSocket(_make_config())
    cb = lambda order: None
    await ws.subscribe_order_fills(cb)
    assert cb in ws._order_fill_callbacks
    await ws.subscribe_order_fills(cb)
    assert ws._order_fill_callbacks.count(cb) == 1


@pytest.mark.asyncio
async def test_order_fill_callback_only_fires_on_filled():
    ws = StandXWebSocket(_make_config())
    all_orders = []
    fill_orders = []

    async def on_order(o):
        all_orders.append(o)

    async def on_fill(o):
        fill_orders.append(o)

    await ws.subscribe_orders(on_order)
    await ws.subscribe_order_fills(on_fill)

    # filled 订单 via _handle_message (order channel)
    filled_msg = {
        "channel": "order",
        "data": {
            "id": 1, "cl_ord_id": "test1", "symbol": "BTC-USD",
            "side": "buy", "order_type": "market", "qty": "1.000",
            "fill_qty": "1.000", "price": "97000.00",
            "fill_avg_price": "97000.00", "status": "filled",
            "created_at": "2025-08-11T10:06:37.182464Z",
        },
    }
    await ws._handle_message(filled_msg)

    # open 订单
    open_msg = {
        "channel": "order",
        "data": {
            "id": 2, "cl_ord_id": "test2", "symbol": "BTC-USD",
            "side": "buy", "order_type": "limit", "qty": "1.000",
            "fill_qty": "0", "price": "96000.00",
            "fill_avg_price": "0", "status": "open",
            "created_at": "2025-08-11T10:06:37.182464Z",
        },
    }
    await ws._handle_message(open_msg)

    assert len(all_orders) == 2
    assert len(fill_orders) == 1
    assert fill_orders[0].status == OrderStatus.FILLED


@pytest.mark.asyncio
async def test_order_fill_callback_via_order_ws():
    """测试 _handle_order_message 也能触发 fill 回调"""
    ws = StandXWebSocket(_make_config())
    fill_orders = []

    async def on_fill(o):
        fill_orders.append(o)

    await ws.subscribe_order_fills(on_fill)

    msg = {
        "data": {
            "id": 3, "cl_ord_id": "test3", "symbol": "BTC-USD",
            "side": "sell", "order_type": "market", "qty": "0.5",
            "fill_qty": "0.5", "price": "97500.00",
            "fill_avg_price": "97500.00", "status": "filled",
            "created_at": "2025-08-11T10:07:00.000000Z",
        },
    }
    await ws._handle_order_message(msg)
    assert len(fill_orders) == 1


@pytest.mark.asyncio
async def test_subscribe_orders_receives_all_statuses():
    ws = StandXWebSocket(_make_config())
    received = []

    async def on_order(o):
        received.append(o)

    await ws.subscribe_orders(on_order)

    for status_str in ("new", "open", "filled", "canceled"):
        msg = {
            "channel": "order",
            "data": {
                "id": 100, "cl_ord_id": "t", "symbol": "BTC-USD",
                "side": "buy", "order_type": "limit", "qty": "1",
                "fill_qty": "0", "price": "97000", "fill_avg_price": "0",
                "status": status_str,
                "created_at": "2025-08-11T10:06:37.182464Z",
            },
        }
        await ws._handle_message(msg)

    assert len(received) == 4
