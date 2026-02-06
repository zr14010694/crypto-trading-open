"""Aster REST 私有端点解析单元测试"""

from decimal import Decimal

from core.adapters.exchanges.adapters.aster_rest import AsterRest
from core.adapters.exchanges.models import (
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
    MarginMode,
)


def make_rest() -> AsterRest:
    return AsterRest()


# --- _parse_order ---

def test_parse_order_new():
    rest = make_rest()
    data = {
        "orderId": 12345,
        "clientOrderId": "my_order_001",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "LIMIT",
        "origQty": "0.01",
        "executedQty": "0",
        "price": "97000.00",
        "avgPrice": "0",
        "status": "NEW",
        "time": 1700000000000,
        "updateTime": 1700000000000,
    }
    order = rest._parse_order(data)
    assert order.id == "12345"
    assert order.client_id == "my_order_001"
    assert order.symbol == "BTCUSDT"
    assert order.side == OrderSide.BUY
    assert order.type == OrderType.LIMIT
    assert order.amount == Decimal("0.01")
    assert order.filled == Decimal("0")
    assert order.status == OrderStatus.OPEN
    assert order.price == Decimal("97000.00")


def test_parse_order_filled():
    rest = make_rest()
    data = {
        "orderId": 12346,
        "clientOrderId": "",
        "symbol": "ETHUSDT",
        "side": "SELL",
        "type": "MARKET",
        "origQty": "1.0",
        "executedQty": "1.0",
        "price": "0",
        "avgPrice": "3800.50",
        "status": "FILLED",
        "time": 1700000000000,
        "updateTime": 1700000001000,
    }
    order = rest._parse_order(data)
    assert order.id == "12346"
    assert order.side == OrderSide.SELL
    assert order.type == OrderType.MARKET
    assert order.filled == Decimal("1.0")
    assert order.status == OrderStatus.FILLED
    assert order.average == Decimal("3800.50")


def test_parse_order_canceled():
    rest = make_rest()
    data = {
        "orderId": 12347,
        "clientOrderId": "",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "LIMIT",
        "origQty": "0.5",
        "executedQty": "0.1",
        "price": "96000.00",
        "avgPrice": "96000.00",
        "status": "CANCELED",
        "time": 1700000000000,
        "updateTime": 1700000002000,
    }
    order = rest._parse_order(data)
    assert order.status == OrderStatus.CANCELED
    assert order.filled == Decimal("0.1")
    assert order.remaining == Decimal("0.4")


# --- _parse_positions ---

def test_parse_positions_long():
    rest = make_rest()
    data = [
        {
            "symbol": "BTCUSDT",
            "positionAmt": "0.05",
            "entryPrice": "96500.00",
            "markPrice": "97000.00",
            "unRealizedProfit": "25.00",
            "leverage": "10",
            "marginType": "CROSSED",
            "isolatedMargin": "0",
            "updateTime": 1700000000000,
        }
    ]
    positions = rest._parse_positions(data)
    assert len(positions) == 1
    pos = positions[0]
    assert pos.symbol == "BTCUSDT"
    assert pos.side == PositionSide.LONG
    assert pos.size == Decimal("0.05")
    assert pos.entry_price == Decimal("96500.00")
    assert pos.mark_price == Decimal("97000.00")
    assert pos.unrealized_pnl == Decimal("25.00")
    assert pos.leverage == 10
    assert pos.margin_mode == MarginMode.CROSS


def test_parse_positions_short():
    rest = make_rest()
    data = [
        {
            "symbol": "ETHUSDT",
            "positionAmt": "-2.0",
            "entryPrice": "3800.00",
            "markPrice": "3750.00",
            "unRealizedProfit": "100.00",
            "leverage": "5",
            "marginType": "ISOLATED",
            "isolatedMargin": "1520.00",
            "updateTime": 1700000000000,
        }
    ]
    positions = rest._parse_positions(data)
    assert len(positions) == 1
    pos = positions[0]
    assert pos.side == PositionSide.SHORT
    assert pos.size == Decimal("2.0")  # abs
    assert pos.leverage == 5
    assert pos.margin_mode == MarginMode.ISOLATED
    assert pos.margin == Decimal("1520.00")


def test_parse_positions_filters_zero():
    rest = make_rest()
    data = [
        {
            "symbol": "BTCUSDT",
            "positionAmt": "0",
            "entryPrice": "0",
            "markPrice": "97000.00",
            "unRealizedProfit": "0",
            "leverage": "10",
            "marginType": "CROSSED",
            "isolatedMargin": "0",
            "updateTime": 1700000000000,
        }
    ]
    positions = rest._parse_positions(data)
    # 零仓位也应返回（由调用方决定是否过滤）
    assert len(positions) == 1


# --- _parse_balances ---

def test_parse_balances_usdt():
    rest = make_rest()
    data = [
        {
            "asset": "USDT",
            "balance": "10000.50",
            "availableBalance": "8500.25",
            "crossWalletBalance": "10000.50",
            "crossUnPnl": "50.00",
        },
        {
            "asset": "BTC",
            "balance": "0.01",
            "availableBalance": "0.01",
            "crossWalletBalance": "0.01",
            "crossUnPnl": "0",
        },
    ]
    balances = rest._parse_balances(data)
    assert len(balances) == 2
    usdt = balances[0]
    assert usdt.currency == "USDT"
    assert usdt.total == Decimal("10000.50")
    assert usdt.free == Decimal("8500.25")


def test_parse_balances_empty():
    rest = make_rest()
    balances = rest._parse_balances([])
    assert len(balances) == 0
