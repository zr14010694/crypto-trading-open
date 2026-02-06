"""
StandX 交易所基础模块
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


class StandXBase:
    DEFAULT_BASE_URL = "https://perps.standx.com"
    DEFAULT_WS_URL = "wss://perps.standx.com/ws-stream/v1"
    DEFAULT_ORDER_WS_URL = "wss://perps.standx.com/ws-api/v1"

    ORDER_SIDE_MAP = {
        "buy": OrderSide.BUY,
        "sell": OrderSide.SELL,
    }

    ORDER_STATUS_MAP = {
        "open": OrderStatus.OPEN,
        "canceled": OrderStatus.CANCELED,
        "filled": OrderStatus.FILLED,
        "rejected": OrderStatus.REJECTED,
        "untriggered": OrderStatus.OPEN,
        "new": OrderStatus.OPEN,
    }

    ORDER_TYPE_MAP = {
        "limit": OrderType.LIMIT,
        "market": OrderType.MARKET,
    }

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = None

    def _safe_decimal(self, value: Any) -> Decimal:
        try:
            if value is None:
                return Decimal("0")
            return Decimal(str(value))
        except (ValueError, TypeError):
            return Decimal("0")

    def _parse_timestamp(self, value: Any) -> datetime:
        if value is None:
            return datetime.now(timezone.utc)
        if isinstance(value, (int, float)):
            # 可能是毫秒时间戳
            return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return datetime.now(timezone.utc)
        return datetime.now(timezone.utc)

    def _parse_order_side(self, side: str) -> OrderSide:
        return self.ORDER_SIDE_MAP.get(str(side).lower(), OrderSide.BUY)

    def _parse_order_status(self, status: str) -> OrderStatus:
        return self.ORDER_STATUS_MAP.get(str(status).lower(), OrderStatus.UNKNOWN)

    def _parse_order_type(self, order_type: str) -> OrderType:
        return self.ORDER_TYPE_MAP.get(str(order_type).lower(), OrderType.LIMIT)

    def _parse_position_side(self, qty: Decimal) -> PositionSide:
        return PositionSide.LONG if qty >= 0 else PositionSide.SHORT

    def _parse_margin_mode(self, mode: Optional[str]) -> MarginMode:
        if str(mode).lower() == "isolated":
            return MarginMode.ISOLATED
        return MarginMode.CROSS
