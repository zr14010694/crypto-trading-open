"""Aster 主适配器单元测试（Mock REST）"""

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.adapters.exchanges.adapters.aster import AsterAdapter
from core.adapters.exchanges.interface import ExchangeConfig
from core.adapters.exchanges.models import ExchangeType, OrderSide, OrderType


def make_config() -> ExchangeConfig:
    return ExchangeConfig(
        exchange_id="aster",
        name="Aster Finance",
        exchange_type=ExchangeType.FUTURES,
        api_key="0xSigner",
        api_secret="0x4c0883a69102937d6231471b5dbb6204fe512961708279f45ff2c3a1dd3a5e3a",
        extra_params={
            "user": "0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e",
            "signer": "0xSigner",
            "private_key": "0x4c0883a69102937d6231471b5dbb6204fe512961708279f45ff2c3a1dd3a5e3a",
        },
        enable_websocket=False,
        enable_heartbeat=False,
    )


def test_adapter_init():
    config = make_config()
    adapter = AsterAdapter(config)
    assert adapter.rest is not None
    assert adapter.websocket is not None
    assert adapter.rest.user == "0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e"


@pytest.mark.asyncio
async def test_get_ticker():
    config = make_config()
    adapter = AsterAdapter(config)
    mock_data = {
        "symbol": "BTCUSDT",
        "bidPrice": "97000.50",
        "bidQty": "1.5",
        "askPrice": "97001.00",
        "askQty": "2.0",
        "time": 1700000000000,
    }
    adapter.rest.get_book_ticker = AsyncMock(return_value=mock_data)
    ticker = await adapter.get_ticker("BTCUSDT")
    assert ticker.symbol == "BTCUSDT"
    assert ticker.bid == Decimal("97000.50")
    assert ticker.ask == Decimal("97001.00")


@pytest.mark.asyncio
async def test_get_orderbook():
    config = make_config()
    adapter = AsterAdapter(config)
    mock_data = {
        "symbol": "BTCUSDT",
        "bids": [["97000.00", "1.0"], ["96999.00", "2.0"]],
        "asks": [["97001.00", "0.5"], ["97002.00", "1.0"]],
        "T": 1700000000000,
    }
    adapter.rest.get_depth = AsyncMock(return_value=mock_data)
    ob = await adapter.get_orderbook("BTCUSDT", limit=5)
    assert ob.symbol == "BTCUSDT"
    assert ob.bids[0].price == Decimal("97000.00")
    assert ob.asks[0].price == Decimal("97001.00")


@pytest.mark.asyncio
async def test_create_order():
    config = make_config()
    adapter = AsterAdapter(config)
    mock_response = {
        "orderId": 99999,
        "clientOrderId": "test_cl",
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
    adapter.rest.new_order = AsyncMock(return_value=mock_response)
    order = await adapter.create_order(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        amount=Decimal("0.01"),
        price=Decimal("97000.00"),
    )
    assert order.id == "99999"
    assert order.symbol == "BTCUSDT"
    adapter.rest.new_order.assert_called_once()
    call_params = adapter.rest.new_order.call_args[0][0]
    assert call_params["symbol"] == "BTCUSDT"
    assert call_params["side"] == "BUY"
    assert call_params["type"] == "LIMIT"


@pytest.mark.asyncio
async def test_cancel_order():
    config = make_config()
    adapter = AsterAdapter(config)
    mock_response = {
        "orderId": 99999,
        "clientOrderId": "",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "LIMIT",
        "origQty": "0.01",
        "executedQty": "0",
        "price": "97000.00",
        "avgPrice": "0",
        "status": "CANCELED",
        "time": 1700000000000,
        "updateTime": 1700000001000,
    }
    adapter.rest.cancel_order = AsyncMock(return_value=mock_response)
    order = await adapter.cancel_order("99999", symbol="BTCUSDT")
    assert order.id == "99999"
    adapter.rest.cancel_order.assert_called_once_with("BTCUSDT", 99999)


@pytest.mark.asyncio
async def test_get_positions():
    config = make_config()
    adapter = AsterAdapter(config)
    mock_data = [
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
    adapter.rest.get_positions = AsyncMock(return_value=mock_data)
    positions = await adapter.get_positions()
    assert len(positions) == 1
    assert positions[0].symbol == "BTCUSDT"
    assert positions[0].size == Decimal("0.05")


@pytest.mark.asyncio
async def test_get_balances():
    config = make_config()
    adapter = AsterAdapter(config)
    mock_data = [
        {
            "asset": "USDT",
            "balance": "10000.50",
            "availableBalance": "8500.25",
            "crossWalletBalance": "10000.50",
            "crossUnPnl": "50.00",
        }
    ]
    adapter.rest.get_balance = AsyncMock(return_value=mock_data)
    balances = await adapter.get_balances()
    assert len(balances) == 1
    assert balances[0].currency == "USDT"
    assert balances[0].total == Decimal("10000.50")


@pytest.mark.asyncio
async def test_set_leverage():
    config = make_config()
    adapter = AsterAdapter(config)
    adapter.rest.set_leverage = AsyncMock(return_value={"leverage": 10, "symbol": "BTCUSDT"})
    result = await adapter.set_leverage("BTCUSDT", 10)
    assert result["leverage"] == 10
    adapter.rest.set_leverage.assert_called_once_with("BTCUSDT", 10)
