"""Aster Base 模块单元测试"""

from decimal import Decimal
from datetime import datetime, timezone

from core.adapters.exchanges.adapters.aster_base import AsterBase
from core.adapters.exchanges.models import (
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
    MarginMode,
)


def test_order_side_map_buy():
    base = AsterBase()
    assert base._parse_order_side("BUY") == OrderSide.BUY


def test_order_side_map_sell():
    base = AsterBase()
    assert base._parse_order_side("SELL") == OrderSide.SELL


def test_order_side_map_lowercase():
    base = AsterBase()
    assert base._parse_order_side("buy") == OrderSide.BUY


def test_order_status_map_new():
    base = AsterBase()
    assert base._parse_order_status("NEW") == OrderStatus.OPEN


def test_order_status_map_partially_filled():
    base = AsterBase()
    assert base._parse_order_status("PARTIALLY_FILLED") == OrderStatus.OPEN


def test_order_status_map_filled():
    base = AsterBase()
    assert base._parse_order_status("FILLED") == OrderStatus.FILLED


def test_order_status_map_canceled():
    base = AsterBase()
    assert base._parse_order_status("CANCELED") == OrderStatus.CANCELED


def test_order_status_map_rejected():
    base = AsterBase()
    assert base._parse_order_status("REJECTED") == OrderStatus.REJECTED


def test_order_status_map_expired():
    base = AsterBase()
    assert base._parse_order_status("EXPIRED") == OrderStatus.CANCELED


def test_order_status_map_unknown():
    base = AsterBase()
    assert base._parse_order_status("INVALID") == OrderStatus.UNKNOWN


def test_order_type_map():
    base = AsterBase()
    assert base._parse_order_type("LIMIT") == OrderType.LIMIT
    assert base._parse_order_type("MARKET") == OrderType.MARKET


def test_safe_decimal_none():
    base = AsterBase()
    assert base._safe_decimal(None) == Decimal("0")


def test_safe_decimal_empty_string():
    base = AsterBase()
    assert base._safe_decimal("") == Decimal("0")


def test_safe_decimal_valid():
    base = AsterBase()
    assert base._safe_decimal("123.45") == Decimal("123.45")
    assert base._safe_decimal(42) == Decimal("42")


def test_parse_timestamp_ms_epoch():
    base = AsterBase()
    ts = 1700000000000  # 2023-11-14T22:13:20Z
    result = base._parse_timestamp(ts)
    assert isinstance(result, datetime)
    assert result.tzinfo is not None
    assert result.year == 2023


def test_parse_timestamp_none():
    base = AsterBase()
    result = base._parse_timestamp(None)
    assert isinstance(result, datetime)


def test_parse_position_side_positive():
    base = AsterBase()
    assert base._parse_position_side(Decimal("1.5")) == PositionSide.LONG


def test_parse_position_side_negative():
    base = AsterBase()
    assert base._parse_position_side(Decimal("-0.5")) == PositionSide.SHORT


def test_parse_position_side_zero():
    base = AsterBase()
    assert base._parse_position_side(Decimal("0")) == PositionSide.LONG


def test_parse_margin_mode_cross():
    base = AsterBase()
    assert base._parse_margin_mode("CROSSED") == MarginMode.CROSS
    assert base._parse_margin_mode("cross") == MarginMode.CROSS


def test_parse_margin_mode_isolated():
    base = AsterBase()
    assert base._parse_margin_mode("ISOLATED") == MarginMode.ISOLATED
    assert base._parse_margin_mode("isolated") == MarginMode.ISOLATED


def test_default_urls():
    assert AsterBase.DEFAULT_BASE_URL == "https://fapi.asterdex.com"
    assert AsterBase.DEFAULT_WS_URL == "wss://fstream.asterdex.com"
