"""
StandX REST API 模块
"""

from __future__ import annotations

import asyncio
import json
import time
import ssl
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import aiohttp

from .standx_base import StandXBase
from .standx_signer import build_signature_headers
from ..models import (
    BalanceData,
    ExchangeInfo,
    ExchangeType,
    OrderBookData,
    OrderBookLevel,
    OrderData,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionData,
    PositionSide,
    MarginMode,
    TickerData,
)


class StandXRest(StandXBase):
    def __init__(self, config=None, logger=None):
        super().__init__({} if config is None else getattr(config, "__dict__", config))
        self.logger = logger

        self.api_key = getattr(config, "api_key", "") if config else ""
        self.jwt_token = ""
        self.private_key = ""
        self.session_id: Optional[str] = None
        self.base_url = self.DEFAULT_BASE_URL
        self.ws_url = self.DEFAULT_WS_URL
        self.order_ws_url = self.DEFAULT_ORDER_WS_URL
        self.ssl_verify: bool = True
        self.ssl_ca_path: Optional[str] = None

        if config is not None:
            self.base_url = getattr(config, "base_url", self.DEFAULT_BASE_URL) or self.DEFAULT_BASE_URL
            self.ws_url = getattr(config, "ws_url", self.DEFAULT_WS_URL) or self.DEFAULT_WS_URL
            extra_params = getattr(config, "extra_params", {}) or {}
            self.jwt_token = extra_params.get("jwt_token") or getattr(config, "jwt_token", "")
            self.private_key = getattr(config, "private_key", "") or extra_params.get("private_key", "")
            self.order_ws_url = extra_params.get("order_ws_url", self.DEFAULT_ORDER_WS_URL)
            self.ssl_verify = extra_params.get("ssl_verify", True)
            self.ssl_ca_path = extra_params.get("ssl_ca_path")

        self._precision_cache: Dict[str, tuple[int, int]] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._timeout = aiohttp.ClientTimeout(total=10)
        self._lock = asyncio.Lock()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session and not self._session.closed:
            return self._session
        ssl_param = None
        if self.ssl_verify is False:
            ssl_param = False
        elif self.ssl_ca_path:
            ssl_param = ssl.create_default_context(cafile=self.ssl_ca_path)

        connector = aiohttp.TCPConnector(ssl=ssl_param)
        self._session = aiohttp.ClientSession(timeout=self._timeout, connector=connector)
        return self._session

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    def _auth_headers(self) -> Dict[str, str]:
        if not self.jwt_token:
            return {}
        return {"Authorization": f"Bearer {self.jwt_token}"}

    def _signed_headers(
        self,
        payload: Dict[str, Any],
        request_id: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> Dict[str, str]:
        if not self.private_key:
            return self._auth_headers()

        payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        rid = request_id or f"req-{int(time.time() * 1000)}"
        timestamp = int(time.time() * 1000)

        headers = build_signature_headers(
            payload=payload_json,
            request_id=rid,
            timestamp=timestamp,
            private_key=self.private_key,
        )
        headers.update(self._auth_headers())
        if session_id:
            headers["x-session-id"] = session_id
        return headers

    def _parse_symbol_info(self, data: List[Dict[str, Any]]) -> ExchangeInfo:
        markets: Dict[str, Any] = {}
        precision: Dict[str, Any] = {"price": {}, "quantity": {}}

        for item in data:
            symbol = item.get("symbol")
            if not symbol:
                continue
            price_decimals = int(item.get("price_tick_decimals", 0))
            qty_decimals = int(item.get("qty_tick_decimals", 0))
            self._precision_cache[symbol] = (price_decimals, qty_decimals)
            precision["price"][symbol] = price_decimals
            precision["quantity"][symbol] = qty_decimals

            base, quote = symbol.split("-", 1)
            markets[symbol] = {
                "id": symbol,
                "symbol": symbol,
                "base": base,
                "quote": quote,
                "active": True,
                "type": "swap",
                "precision": {
                    "price": price_decimals,
                    "amount": qty_decimals,
                    "base": qty_decimals,
                    "quote": price_decimals,
                },
                "info": item,
            }

        return ExchangeInfo(
            name="StandX",
            id="standx",
            type=ExchangeType.PERPETUAL,
            supported_features=["trading", "orderbook", "ticker", "user_data"],
            rate_limits=getattr(self.config, "rate_limits", {}) if hasattr(self.config, "rate_limits") else {},
            precision=precision,
            fees={},
            markets=markets,
            status="active",
            timestamp=datetime.now(),
        )

    def _parse_ticker(self, data: Dict[str, Any]) -> TickerData:
        spread = data.get("spread")
        bid = data.get("spread_bid")
        ask = data.get("spread_ask")
        if spread and isinstance(spread, list) and len(spread) >= 2:
            bid = spread[0]
            ask = spread[1]

        return TickerData(
            symbol=data.get("symbol", ""),
            timestamp=self._parse_timestamp(data.get("time")),
            bid=bid,
            ask=ask,
            last=data.get("last_price"),
            mark_price=data.get("mark_price"),
            index_price=data.get("index_price"),
            funding_rate=data.get("funding_rate"),
            open_interest=data.get("open_interest"),
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

    def _build_order_payload(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        qty: Decimal,
        price: Optional[Decimal] = None,
        time_in_force: str = "gtc",
        reduce_only: bool = False,
        margin_mode: Optional[str] = None,
        leverage: Optional[int] = None,
        cl_ord_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "symbol": symbol,
            "side": side.value,
            "order_type": order_type.value,
            "qty": str(qty),
            "time_in_force": time_in_force,
            "reduce_only": reduce_only,
        }
        if price is not None:
            payload["price"] = str(price)
        if cl_ord_id:
            payload["cl_ord_id"] = cl_ord_id
        if margin_mode:
            payload["margin_mode"] = margin_mode
        if leverage is not None:
            payload["leverage"] = leverage
        return payload

    def _build_cancel_payload(self, order_id: Optional[int] = None, cl_ord_id: Optional[str] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if order_id is not None:
            payload["order_id"] = order_id
        if cl_ord_id:
            payload["cl_ord_id"] = cl_ord_id
        return payload

    def _parse_order(self, data: Dict[str, Any]) -> OrderData:
        order_type = self._parse_order_type(data.get("order_type"))
        side = self._parse_order_side(data.get("side"))
        status = self._parse_order_status(data.get("status"))
        amount = self._safe_decimal(data.get("qty"))
        filled = self._safe_decimal(data.get("fill_qty"))
        price = data.get("price")
        avg_price = data.get("fill_avg_price") or data.get("price")
        remaining = amount - filled
        cost = filled * self._safe_decimal(avg_price)

        return OrderData(
            id=str(data.get("id")),
            client_id=data.get("cl_ord_id"),
            symbol=data.get("symbol", ""),
            side=side,
            type=order_type,
            amount=amount,
            price=price,
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

    def _parse_positions(self, data: List[Dict[str, Any]]) -> List[PositionData]:
        positions: List[PositionData] = []
        for item in data:
            qty = self._safe_decimal(item.get("qty"))
            side = self._parse_position_side(qty)
            size = qty.copy_abs()
            positions.append(
                PositionData(
                    symbol=item.get("symbol", ""),
                    side=side,
                    size=size,
                    entry_price=self._safe_decimal(item.get("entry_price")),
                    mark_price=self._safe_decimal(item.get("mark_price")),
                    current_price=self._safe_decimal(item.get("mark_price")),
                    unrealized_pnl=self._safe_decimal(item.get("upnl")),
                    realized_pnl=self._safe_decimal(item.get("realized_pnl")),
                    percentage=None,
                    leverage=int(float(item.get("leverage", 1))),
                    margin_mode=self._parse_margin_mode(item.get("margin_mode")),
                    margin=self._safe_decimal(item.get("holding_margin")) if item.get("holding_margin") is not None else self._safe_decimal(item.get("initial_margin")),
                    liquidation_price=self._safe_decimal(item.get("liq_price")) if item.get("liq_price") is not None else None,
                    timestamp=self._parse_timestamp(item.get("time")),
                    raw_data=item,
                )
            )
        return positions

    def _parse_balances(self, data: Any) -> List[BalanceData]:
        balances: List[BalanceData] = []
        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = [data]

        for item in items:
            token = item.get("token") or item.get("asset") or "DUSD"
            free = item.get("free") or item.get("available") or item.get("available_balance")
            used = item.get("locked") or item.get("used") or "0"
            total = item.get("total") or item.get("balance") or item.get("wallet_balance")
            balances.append(
                BalanceData(
                    currency=str(token),
                    free=self._safe_decimal(free),
                    used=self._safe_decimal(used),
                    total=self._safe_decimal(total),
                    usd_value=None,
                    timestamp=datetime.now(),
                    raw_data=item,
                )
            )
        return balances

    # === Public REST wrappers ===

    async def query_symbol_info(self) -> List[Dict[str, Any]]:
        session = await self._get_session()
        url = f"{self.base_url}/api/query_symbol_info"
        async with session.get(url) as resp:
            data = await resp.json()
            return data if isinstance(data, list) else []

    async def query_symbol_price(self, symbol: str) -> Dict[str, Any]:
        session = await self._get_session()
        url = f"{self.base_url}/api/query_symbol_price"
        async with session.get(url, params={"symbol": symbol}) as resp:
            return await resp.json()

    async def query_funding_rates(self, symbol: str, start_time: int, end_time: int) -> List[Dict[str, Any]]:
        session = await self._get_session()
        url = f"{self.base_url}/api/query_funding_rates"
        params = {"symbol": symbol, "start_time": str(start_time), "end_time": str(end_time)}
        async with session.get(url, params=params) as resp:
            data = await resp.json()
            return data if isinstance(data, list) else []

    async def query_depth_book(self, symbol: str) -> Dict[str, Any]:
        session = await self._get_session()
        url = f"{self.base_url}/api/query_depth_book"
        async with session.get(url, params={"symbol": symbol}) as resp:
            return await resp.json()

    # === Private REST wrappers ===

    async def new_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        session = await self._get_session()
        url = f"{self.base_url}/api/new_order"
        headers = self._signed_headers(payload, session_id=self.session_id)
        async with session.post(url, json=payload, headers=headers) as resp:
            return await resp.json()

    async def cancel_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        session = await self._get_session()
        url = f"{self.base_url}/api/cancel_order"
        headers = self._signed_headers(payload, session_id=self.session_id)
        async with session.post(url, json=payload, headers=headers) as resp:
            return await resp.json()

    async def query_order(self, order_id: int) -> Dict[str, Any]:
        session = await self._get_session()
        url = f"{self.base_url}/api/query_order"
        headers = self._auth_headers()
        async with session.get(url, params={"order_id": order_id}, headers=headers) as resp:
            return await resp.json()

    async def query_open_orders(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        session = await self._get_session()
        url = f"{self.base_url}/api/query_open_orders"
        headers = self._auth_headers()
        params = {"symbol": symbol} if symbol else None
        async with session.get(url, params=params, headers=headers) as resp:
            return await resp.json()

    async def query_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        session = await self._get_session()
        url = f"{self.base_url}/api/query_positions"
        headers = self._auth_headers()
        params = {"symbol": symbol} if symbol else None
        async with session.get(url, params=params, headers=headers) as resp:
            data = await resp.json()
            return data if isinstance(data, list) else []

    async def query_balance(self) -> Any:
        session = await self._get_session()
        url = f"{self.base_url}/api/query_balance"
        headers = self._auth_headers()
        async with session.get(url, headers=headers) as resp:
            return await resp.json()

    async def query_trades(self, symbol: Optional[str] = None, limit: Optional[int] = None) -> Dict[str, Any]:
        session = await self._get_session()
        url = f"{self.base_url}/api/query_trades"
        headers = self._auth_headers()
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        if limit is not None:
            params["limit"] = limit
        async with session.get(url, params=params or None, headers=headers) as resp:
            return await resp.json()
