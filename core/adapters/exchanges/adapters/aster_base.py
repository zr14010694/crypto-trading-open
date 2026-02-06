"""
Aster Finance Futures 交易所基础模块
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

from ..models import (
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
    MarginMode,
)


class AsterBase:
    DEFAULT_BASE_URL = "https://fapi.asterdex.com"
    DEFAULT_WS_URL = "wss://fstream.asterdex.com"

    ORDER_SIDE_MAP = {
        "BUY": OrderSide.BUY,
        "SELL": OrderSide.SELL,
    }

    ORDER_STATUS_MAP = {
        "NEW": OrderStatus.OPEN,
        "PARTIALLY_FILLED": OrderStatus.OPEN,
        "FILLED": OrderStatus.FILLED,
        "CANCELED": OrderStatus.CANCELED,
        "REJECTED": OrderStatus.REJECTED,
        "EXPIRED": OrderStatus.CANCELED,
    }

    ORDER_TYPE_MAP = {
        "LIMIT": OrderType.LIMIT,
        "MARKET": OrderType.MARKET,
        "STOP": OrderType.STOP,
        "STOP_MARKET": OrderType.STOP_MARKET,
        "TAKE_PROFIT": OrderType.TAKE_PROFIT,
        "TAKE_PROFIT_MARKET": OrderType.TAKE_PROFIT_MARKET,
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = None

    def _safe_decimal(self, value: Any) -> Decimal:
        try:
            if value is None or value == "":
                return Decimal("0")
            return Decimal(str(value))
        except (ValueError, TypeError, ArithmeticError):
            return Decimal("0")

    def _parse_timestamp(self, value: Any) -> datetime:
        if value is None:
            return datetime.now(timezone.utc)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return datetime.now(timezone.utc)
        return datetime.now(timezone.utc)

    def _parse_order_side(self, side: Any) -> OrderSide:
        return self.ORDER_SIDE_MAP.get(str(side).upper(), OrderSide.BUY)

    def _parse_order_status(self, status: Any) -> OrderStatus:
        return self.ORDER_STATUS_MAP.get(str(status).upper(), OrderStatus.UNKNOWN)

    def _parse_order_type(self, order_type: Any) -> OrderType:
        return self.ORDER_TYPE_MAP.get(str(order_type).upper(), OrderType.LIMIT)

    def _parse_position_side(self, qty: Decimal) -> PositionSide:
        if qty > 0:
            return PositionSide.LONG
        elif qty < 0:
            return PositionSide.SHORT
        return PositionSide.LONG

    def _parse_margin_mode(self, mode: Optional[str]) -> MarginMode:
        if str(mode).upper() == "ISOLATED":
            return MarginMode.ISOLATED
        return MarginMode.CROSS
