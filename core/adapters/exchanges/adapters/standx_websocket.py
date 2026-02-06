"""
StandX WebSocket 模块
"""

from __future__ import annotations

import asyncio
import json
import ssl
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import aiohttp

from .standx_base import StandXBase
from ..models import (
    BalanceData,
    OrderBookData,
    OrderBookLevel,
    OrderData,
    OrderSide,
    OrderStatus,
    PositionData,
    PositionSide,
    TickerData,
)


class StandXWebSocket(StandXBase):
    def __init__(self, config=None, logger=None):
        super().__init__({} if config is None else getattr(config, "__dict__", config))
        self.logger = logger

        self.base_url = self.DEFAULT_BASE_URL
        self.ws_url = self.DEFAULT_WS_URL
        self.order_ws_url = self.DEFAULT_ORDER_WS_URL
        self.jwt_token = ""
        self.session_id = str(uuid.uuid4())
        self.ssl_verify: bool = True
        self.ssl_ca_path: Optional[str] = None

        if config is not None:
            self.ws_url = getattr(config, "ws_url", self.DEFAULT_WS_URL) or self.DEFAULT_WS_URL
            extra_params = getattr(config, "extra_params", {}) or {}
            self.jwt_token = extra_params.get("jwt_token") or getattr(config, "jwt_token", "")
            self.order_ws_url = extra_params.get("order_ws_url", self.DEFAULT_ORDER_WS_URL)
            self.ssl_verify = extra_params.get("ssl_verify", True)
            self.ssl_ca_path = extra_params.get("ssl_ca_path")

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._order_ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._funding_rates: Dict[str, Any] = {}  # symbol -> funding_rate
        self._ticker_callbacks: List[Any] = []
        self._orderbook_callbacks: List[Any] = []
        self._order_callbacks: List[Any] = []
        self._order_fill_callbacks: List[Any] = []
        self._position_callbacks: List[Any] = []
        self._balance_callbacks: List[Any] = []
        self._connected = False

    def _build_ssl_param(self):
        if self.ssl_verify is False:
            return False
        if self.ssl_ca_path:
            return ssl.create_default_context(cafile=self.ssl_ca_path)
        return None

    async def connect(self) -> None:
        self._session = aiohttp.ClientSession()
        ssl_param = self._build_ssl_param()
        self._ws = await self._session.ws_connect(self.ws_url, ssl=ssl_param)
        self._connected = True

    async def disconnect(self) -> None:
        if self._ws:
            await self._ws.close()
        if self._order_ws:
            await self._order_ws.close()
        if self._session:
            await self._session.close()
        self._connected = False

    async def authenticate(self) -> None:
        if not self.jwt_token or not self._ws:
            return
        payload = {
            "auth": {
                "token": self.jwt_token,
                "streams": [{"channel": "order"}, {"channel": "position"}, {"channel": "balance"}],
            }
        }
        await self._ws.send_json(payload)

    async def connect_order_stream(self) -> None:
        if not self._session:
            self._session = aiohttp.ClientSession()
        ssl_param = self._build_ssl_param()
        self._order_ws = await self._session.ws_connect(self.order_ws_url, ssl=ssl_param)
        await self._order_ws_auth()

    async def disconnect_order_stream(self) -> None:
        if self._order_ws:
            await self._order_ws.close()
            self._order_ws = None

    async def _order_ws_auth(self) -> None:
        if not self._order_ws or not self.jwt_token:
            return
        payload = {
            "session_id": self.session_id,
            "request_id": str(uuid.uuid4()),
            "method": "auth:login",
            "params": json.dumps({"token": self.jwt_token}),
        }
        await self._order_ws.send_json(payload)

    async def listen_order_stream(self) -> None:
        if not self._order_ws:
            return
        async for msg in self._order_ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    payload = json.loads(msg.data)
                except Exception:
                    continue
                await self._handle_order_message(payload)

    async def _handle_order_message(self, message: Dict[str, Any]) -> None:
        order = self._parse_order_response(message)
        for cb in self._order_callbacks:
            await cb(order)
        if order.status == OrderStatus.FILLED and self._order_fill_callbacks:
            for cb in self._order_fill_callbacks:
                await cb(order)

    async def subscribe_orders(self, callback) -> None:
        if callback not in self._order_callbacks:
            self._order_callbacks.append(callback)

    async def subscribe_order_fills(self, callback) -> None:
        if callback not in self._order_fill_callbacks:
            self._order_fill_callbacks.append(callback)

    async def subscribe(self, channel: str, symbol: Optional[str] = None) -> None:
        if not self._ws:
            return
        message: Dict[str, Any] = {"subscribe": {"channel": channel}}
        if symbol:
            message["subscribe"]["symbol"] = symbol
        await self._ws.send_json(message)

    async def listen(self) -> None:
        if not self._ws:
            return
        async for msg in self._ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    payload = json.loads(msg.data)
                except Exception:
                    continue
                await self._handle_message(payload)

    async def _handle_message(self, message: Dict[str, Any]) -> None:
        channel = message.get("channel")
        data = message.get("data") or {}

        if channel == "price":
            ticker = self._parse_ticker(data)
            for cb in self._ticker_callbacks:
                await cb(ticker)
        elif channel == "depth_book":
            orderbook = self._parse_orderbook(data)
            for cb in self._orderbook_callbacks:
                await cb(orderbook)
        elif channel == "order":
            order = self._parse_order(data)
            for cb in self._order_callbacks:
                await cb(order)
            if order.status == OrderStatus.FILLED and self._order_fill_callbacks:
                for cb in self._order_fill_callbacks:
                    await cb(order)
        elif channel == "position":
            position = self._parse_position(data)
            for cb in self._position_callbacks:
                await cb(position)
        elif channel == "balance":
            balance = self._parse_balance(data)
            for cb in self._balance_callbacks:
                await cb(balance)

    def _parse_order_response(self, message: Dict[str, Any]) -> OrderData:
        data = message.get("data") or {}
        return self._parse_order(data)

    def _parse_ticker(self, data: Dict[str, Any]) -> TickerData:
        spread = data.get("spread")
        bid = data.get("spread_bid")
        ask = data.get("spread_ask")
        if spread and isinstance(spread, list) and len(spread) >= 2:
            bid = spread[0]
            ask = spread[1]
        symbol = data.get("symbol", "")
        # WS price channel 不含 funding_rate，从缓存注入
        funding_rate = data.get("funding_rate") or self._funding_rates.get(symbol)
        return TickerData(
            symbol=symbol,
            timestamp=self._parse_timestamp(data.get("time")),
            bid=bid,
            ask=ask,
            last=data.get("last_price"),
            mark_price=data.get("mark_price"),
            index_price=data.get("index_price"),
            funding_rate=funding_rate,
            raw_data=data,
        )

    def _parse_orderbook(self, data: Dict[str, Any]) -> OrderBookData:
        bids = [OrderBookLevel(price=price, size=size) for price, size in data.get("bids", [])]
        asks = [OrderBookLevel(price=price, size=size) for price, size in data.get("asks", [])]
        bids.sort(key=lambda level: level.price, reverse=True)
        asks.sort(key=lambda level: level.price)
        return OrderBookData(
            symbol=data.get("symbol", ""),
            bids=bids,
            asks=asks,
            timestamp=datetime.now(),
            raw_data=data,
        )

    def _parse_order(self, data: Dict[str, Any]) -> OrderData:
        order_type = self._parse_order_type(data.get("order_type"))
        side = self._parse_order_side(data.get("side"))
        status = self._parse_order_status(data.get("status"))
        amount = self._safe_decimal(data.get("qty"))
        filled = self._safe_decimal(data.get("fill_qty"))
        remaining = amount - filled
        avg_price = data.get("fill_avg_price") or data.get("price")
        cost = filled * self._safe_decimal(avg_price)

        return OrderData(
            id=str(data.get("id")),
            client_id=data.get("cl_ord_id"),
            symbol=data.get("symbol", ""),
            side=side,
            type=order_type,
            amount=amount,
            price=data.get("price"),
            filled=filled,
            remaining=remaining,
            cost=cost,
            average=avg_price,
            status=status,
            timestamp=self._parse_timestamp(data.get("created_at")),
            updated=self._parse_timestamp(data.get("updated_at")) if data.get("updated_at") else None,
            fee=None,
            trades=[],
            params={},
            raw_data=data,
        )

    def _parse_position(self, data: Dict[str, Any]) -> PositionData:
        qty = self._safe_decimal(data.get("qty"))
        side = self._parse_position_side(qty)
        size = qty.copy_abs()
        return PositionData(
            symbol=data.get("symbol", ""),
            side=side,
            size=size,
            entry_price=self._safe_decimal(data.get("entry_price")),
            mark_price=self._safe_decimal(data.get("mark_price")),
            current_price=self._safe_decimal(data.get("mark_price")),
            unrealized_pnl=self._safe_decimal(data.get("upnl")),
            realized_pnl=self._safe_decimal(data.get("realized_pnl")),
            percentage=None,
            leverage=int(float(data.get("leverage", 1))),
            margin_mode=self._parse_margin_mode(data.get("margin_mode")),
            margin=self._safe_decimal(data.get("holding_margin")) if data.get("holding_margin") is not None else self._safe_decimal(data.get("initial_margin")),
            liquidation_price=self._safe_decimal(data.get("liq_price")) if data.get("liq_price") is not None else None,
            timestamp=self._parse_timestamp(data.get("updated_at")),
            raw_data=data,
        )

    def _parse_balance(self, data: Dict[str, Any]) -> BalanceData:
        token = data.get("token") or data.get("asset") or "DUSD"
        free = data.get("free") or data.get("available") or data.get("available_balance")
        used = data.get("locked") or data.get("used") or "0"
        total = data.get("total") or data.get("balance") or data.get("wallet_balance")
        return BalanceData(
            currency=str(token),
            free=self._safe_decimal(free),
            used=self._safe_decimal(used),
            total=self._safe_decimal(total),
            usd_value=None,
            timestamp=datetime.now(),
            raw_data=data,
        )
