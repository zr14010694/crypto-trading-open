"""
Aster Finance Futures WebSocket 客户端

支持公共数据流 (bookTicker, depth, aggTrade, markPrice)
和用户数据流 (listenKey + ACCOUNT_UPDATE/ORDER_TRADE_UPDATE)。
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp

from .aster_base import AsterBase
from ..models import (
    BalanceData,
    OrderBookData,
    OrderBookLevel,
    OrderData,
    OrderStatus,
    PositionData,
    TickerData,
)


class AsterWebSocket(AsterBase):
    WS_BASE = "wss://stream.asterdex.com/stream"

    def __init__(self, config=None, logger=None):
        super().__init__({} if config is None else getattr(config, "__dict__", config))
        self.logger = logger

        self.ws_url = self.DEFAULT_WS_URL
        if config is not None:
            self.ws_url = getattr(config, "ws_url", self.DEFAULT_WS_URL) or self.DEFAULT_WS_URL

        self.ssl_verify: bool = True
        if config is not None:
            extra = getattr(config, "extra_params", {}) or {}
            self.ssl_verify = extra.get("ssl_verify", True)

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._user_ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._connected = False
        self._sub_id = 1

        # 回调
        self._funding_rates: Dict[str, Any] = {}  # symbol -> funding_rate
        self._ticker_callbacks: List[Callable] = []
        self._orderbook_callbacks: List[Callable] = []
        self._order_callbacks: List[Callable] = []
        self._order_fill_callbacks: List[Callable] = []
        self._position_callbacks: List[Callable] = []
        self._balance_callbacks: List[Callable] = []

    # ── 连接管理 ──

    def _make_session(self) -> aiohttp.ClientSession:
        connector = aiohttp.TCPConnector(ssl=False) if not self.ssl_verify else None
        return aiohttp.ClientSession(connector=connector)

    async def connect(self) -> None:
        self._session = self._make_session()
        url = f"{self.ws_url}/stream"
        self._ws = await self._session.ws_connect(url, ssl=False if not self.ssl_verify else None)
        self._connected = True

    async def connect_user_stream(self, listen_key: str) -> None:
        if not self._session:
            self._session = self._make_session()
        url = f"{self.ws_url}/stream?listenKey={listen_key}"
        self._user_ws = await self._session.ws_connect(url, ssl=False if not self.ssl_verify else None)

    async def disconnect(self) -> None:
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._user_ws:
            await self._user_ws.close()
            self._user_ws = None
        if self._session:
            await self._session.close()
            self._session = None
        self._connected = False

    # ── 订阅 ──

    def _build_subscribe_message(self, streams: List[str]) -> Dict[str, Any]:
        msg = {"method": "SUBSCRIBE", "params": streams, "id": self._sub_id}
        self._sub_id += 1
        return msg

    def _build_unsubscribe_message(self, streams: List[str]) -> Dict[str, Any]:
        msg = {"method": "UNSUBSCRIBE", "params": streams, "id": self._sub_id}
        self._sub_id += 1
        return msg

    async def subscribe(self, streams: List[str]) -> None:
        if not self._ws:
            return
        msg = self._build_subscribe_message(streams)
        await self._ws.send_json(msg)

    async def unsubscribe(self, streams: List[str]) -> None:
        if not self._ws:
            return
        msg = self._build_unsubscribe_message(streams)
        await self._ws.send_json(msg)

    async def subscribe_orders(self, callback: Callable) -> None:
        if callback not in self._order_callbacks:
            self._order_callbacks.append(callback)

    async def subscribe_order_fills(self, callback: Callable) -> None:
        if callback not in self._order_fill_callbacks:
            self._order_fill_callbacks.append(callback)

    # ── 监听循环 ──

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

    async def listen_user_stream(self) -> None:
        if not self._user_ws:
            return
        async for msg in self._user_ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    payload = json.loads(msg.data)
                except Exception:
                    continue
                await self._handle_user_message(payload)

    # ── 消息路由 ──

    async def _handle_message(self, message: Dict[str, Any]) -> None:
        # /stream 路径下消息被包装: {"stream": "...", "data": {...}}
        if "data" in message and "stream" in message:
            message = message["data"]

        event = message.get("e")
        if not event:
            return

        if event == "bookTicker":
            ticker = self._parse_ws_ticker(message)
            for cb in self._ticker_callbacks:
                await cb(ticker)
        elif event == "markPriceUpdate":
            symbol = message.get("s", "")
            self._funding_rates[symbol] = message.get("r")
            ticker = TickerData(
                symbol=symbol,
                timestamp=self._parse_timestamp(message.get("E")),
                mark_price=message.get("p"),
                index_price=message.get("i"),
                funding_rate=message.get("r"),
                next_funding_time=self._parse_timestamp(message.get("T")),
                raw_data=message,
            )
            for cb in self._ticker_callbacks:
                await cb(ticker)
        elif event in ("depthUpdate", "depth"):
            ob = self._parse_ws_orderbook(message)
            for cb in self._orderbook_callbacks:
                await cb(ob)

    async def _handle_user_message(self, message: Dict[str, Any]) -> None:
        event = message.get("e")
        if event == "ORDER_TRADE_UPDATE":
            order = self._parse_ws_order_update(message)
            for cb in self._order_callbacks:
                await cb(order)
            if order.status == OrderStatus.FILLED and self._order_fill_callbacks:
                for cb in self._order_fill_callbacks:
                    await cb(order)
        elif event == "ACCOUNT_UPDATE":
            positions, balances = self._parse_ws_account_update(message)
            for pos in positions:
                for cb in self._position_callbacks:
                    await cb(pos)
            for bal in balances:
                for cb in self._balance_callbacks:
                    await cb(bal)

    # ── 解析方法 ──

    def _parse_ws_ticker(self, data: Dict[str, Any]) -> TickerData:
        symbol = data.get("s", "")
        return TickerData(
            symbol=symbol,
            timestamp=self._parse_timestamp(data.get("T")),
            bid=data.get("b"),
            ask=data.get("a"),
            bid_size=data.get("B"),
            ask_size=data.get("A"),
            funding_rate=self._funding_rates.get(symbol),
            raw_data=data,
        )

    def _parse_ws_orderbook(self, data: Dict[str, Any]) -> OrderBookData:
        bids = [OrderBookLevel(price=p, size=s) for p, s in data.get("b", [])]
        asks = [OrderBookLevel(price=p, size=s) for p, s in data.get("a", [])]
        bids.sort(key=lambda lv: lv.price, reverse=True)
        asks.sort(key=lambda lv: lv.price)

        return OrderBookData(
            symbol=data.get("s", ""),
            bids=bids,
            asks=asks,
            timestamp=self._parse_timestamp(data.get("T")),
            raw_data=data,
        )

    def _parse_ws_order_update(self, data: Dict[str, Any]) -> OrderData:
        o = data.get("o", {})
        amount = self._safe_decimal(o.get("q"))
        filled = self._safe_decimal(o.get("z"))
        remaining = amount - filled
        avg_price = o.get("ap")
        price = o.get("p")
        cost = filled * self._safe_decimal(avg_price)

        return OrderData(
            id=str(o.get("i")),
            client_id=o.get("c") or None,
            symbol=o.get("s", ""),
            side=self._parse_order_side(o.get("S")),
            type=self._parse_order_type(o.get("o")),
            amount=amount,
            price=self._safe_decimal(price) if price and price != "0" else None,
            filled=filled,
            remaining=remaining,
            cost=cost,
            average=self._safe_decimal(avg_price) if avg_price and avg_price != "0" else None,
            status=self._parse_order_status(o.get("X")),
            timestamp=self._parse_timestamp(o.get("T")),
            updated=self._parse_timestamp(data.get("T")),
            fee=None,
            trades=[],
            params={},
            raw_data=data,
        )

    def _parse_ws_account_update(
        self, data: Dict[str, Any]
    ) -> Tuple[List[PositionData], List[BalanceData]]:
        account = data.get("a", {})

        positions: List[PositionData] = []
        for p in account.get("P", []):
            qty = self._safe_decimal(p.get("pa"))
            side = self._parse_position_side(qty)
            positions.append(
                PositionData(
                    symbol=p.get("s", ""),
                    side=side,
                    size=qty.copy_abs(),
                    entry_price=self._safe_decimal(p.get("ep")),
                    mark_price=None,
                    current_price=None,
                    unrealized_pnl=self._safe_decimal(p.get("up")),
                    realized_pnl=Decimal("0"),
                    percentage=None,
                    leverage=1,
                    margin_mode=self._parse_margin_mode(p.get("mt")),
                    margin=Decimal("0"),
                    liquidation_price=None,
                    timestamp=self._parse_timestamp(data.get("T")),
                    raw_data=p,
                )
            )

        balances: List[BalanceData] = []
        for b in account.get("B", []):
            total = self._safe_decimal(b.get("wb"))
            cw = self._safe_decimal(b.get("cw"))
            balances.append(
                BalanceData(
                    currency=b.get("a", ""),
                    free=cw,
                    used=total - cw,
                    total=total,
                    usd_value=None,
                    timestamp=datetime.now(),
                    raw_data=b,
                )
            )

        return positions, balances
