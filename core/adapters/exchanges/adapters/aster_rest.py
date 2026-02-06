"""
Aster Finance Futures REST API 客户端

提供公共行情和私有交易端点的异步封装。
POST/DELETE 使用 form-urlencoded，GET 签名参数放 query string。
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import aiohttp

from .aster_base import AsterBase
from .aster_signer import build_signed_params
from ..models import (
    BalanceData,
    ExchangeInfo,
    ExchangeType,
    OrderBookData,
    OrderBookLevel,
    OrderData,
    PositionData,
    TickerData,
)


class AsterRest(AsterBase):
    def __init__(self, config=None, logger=None):
        super().__init__({} if config is None else getattr(config, "__dict__", config))
        self.logger = logger

        self.user: str = ""
        self.signer: str = ""
        self.private_key: str = ""
        self.base_url: str = self.DEFAULT_BASE_URL

        if config is not None:
            self.base_url = getattr(config, "base_url", self.DEFAULT_BASE_URL) or self.DEFAULT_BASE_URL
            extra = getattr(config, "extra_params", {}) or {}
            self.user = extra.get("user", "") or getattr(config, "user", "")
            self.signer = extra.get("signer", "") or getattr(config, "signer", "")
            self.private_key = extra.get("private_key", "") or getattr(config, "private_key", "") or getattr(config, "api_secret", "")

        self.ssl_verify: bool = True
        if config is not None:
            extra = getattr(config, "extra_params", {}) or {}
            self.ssl_verify = extra.get("ssl_verify", True)

        self._precision_cache: Dict[str, tuple[int, int]] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._timeout = aiohttp.ClientTimeout(total=15)
        self._lock = asyncio.Lock()

    # ── session 管理 ──

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session and not self._session.closed:
            return self._session
        connector = aiohttp.TCPConnector(ssl=False) if not self.ssl_verify else None
        self._session = aiohttp.ClientSession(timeout=self._timeout, connector=connector)
        return self._session

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    # ── 签名 ──

    def _sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return build_signed_params(params, self.user, self.signer, self.private_key)

    # ── 公共端点 ──

    async def ping(self) -> Dict[str, Any]:
        session = await self._get_session()
        async with session.get(f"{self.base_url}/fapi/v3/ping") as resp:
            return await resp.json()

    async def get_server_time(self) -> Dict[str, Any]:
        session = await self._get_session()
        async with session.get(f"{self.base_url}/fapi/v3/time") as resp:
            return await resp.json()

    async def get_exchange_info(self) -> Dict[str, Any]:
        session = await self._get_session()
        async with session.get(f"{self.base_url}/fapi/v3/exchangeInfo") as resp:
            return await resp.json()

    async def get_depth(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        session = await self._get_session()
        params = {"symbol": symbol, "limit": limit}
        async with session.get(f"{self.base_url}/fapi/v3/depth", params=params) as resp:
            return await resp.json()

    async def get_ticker_price(self, symbol: str) -> Dict[str, Any]:
        session = await self._get_session()
        async with session.get(f"{self.base_url}/fapi/v3/ticker/price", params={"symbol": symbol}) as resp:
            return await resp.json()

    async def get_book_ticker(self, symbol: str) -> Dict[str, Any]:
        session = await self._get_session()
        async with session.get(f"{self.base_url}/fapi/v3/ticker/bookTicker", params={"symbol": symbol}) as resp:
            return await resp.json()

    async def get_mark_price(self, symbol: str) -> Dict[str, Any]:
        session = await self._get_session()
        async with session.get(f"{self.base_url}/fapi/v3/premiumIndex", params={"symbol": symbol}) as resp:
            return await resp.json()

    async def get_funding_rate(self, symbol: str, limit: int = 100) -> Any:
        session = await self._get_session()
        params = {"symbol": symbol, "limit": limit}
        async with session.get(f"{self.base_url}/fapi/v3/fundingRate", params=params) as resp:
            return await resp.json()

    # ── 私有端点 ──

    async def _signed_get(self, path: str, params: Dict[str, Any]) -> Any:
        signed = self._sign(params)
        session = await self._get_session()
        async with session.get(f"{self.base_url}{path}", params=signed) as resp:
            return await resp.json()

    async def _signed_post(self, path: str, params: Dict[str, Any]) -> Any:
        signed = self._sign(params)
        session = await self._get_session()
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        async with session.post(f"{self.base_url}{path}", data=urlencode(signed), headers=headers) as resp:
            return await resp.json()

    async def _signed_put(self, path: str, params: Dict[str, Any]) -> Any:
        signed = self._sign(params)
        session = await self._get_session()
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        async with session.put(f"{self.base_url}{path}", data=urlencode(signed), headers=headers) as resp:
            return await resp.json()

    async def _signed_delete(self, path: str, params: Dict[str, Any]) -> Any:
        signed = self._sign(params)
        session = await self._get_session()
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        async with session.delete(f"{self.base_url}{path}", data=urlencode(signed), headers=headers) as resp:
            return await resp.json()

    # 交易

    async def new_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return await self._signed_post("/fapi/v3/order", params)

    async def cancel_order(self, symbol: str, order_id: int) -> Dict[str, Any]:
        return await self._signed_delete("/fapi/v3/order", {"symbol": symbol, "orderId": order_id})

    async def cancel_all_orders(self, symbol: str) -> Dict[str, Any]:
        return await self._signed_delete("/fapi/v3/allOpenOrders", {"symbol": symbol})

    async def get_order(self, symbol: str, order_id: int) -> Dict[str, Any]:
        return await self._signed_get("/fapi/v3/order", {"symbol": symbol, "orderId": order_id})

    async def get_open_orders(self, symbol: Optional[str] = None) -> Any:
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        return await self._signed_get("/fapi/v3/openOrders", params)

    async def get_all_orders(self, symbol: str, limit: int = 500) -> Any:
        return await self._signed_get("/fapi/v3/allOrders", {"symbol": symbol, "limit": limit})

    async def get_account(self) -> Dict[str, Any]:
        return await self._signed_get("/fapi/v3/account", {})

    async def get_balance(self) -> Any:
        return await self._signed_get("/fapi/v3/balance", {})

    async def get_positions(self) -> Any:
        return await self._signed_get("/fapi/v3/positionRisk", {})

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        return await self._signed_post("/fapi/v3/leverage", {"symbol": symbol, "leverage": leverage})

    async def set_margin_type(self, symbol: str, margin_type: str) -> Dict[str, Any]:
        return await self._signed_post("/fapi/v3/marginType", {"symbol": symbol, "marginType": margin_type})

    # listenKey

    async def create_listen_key(self) -> Dict[str, Any]:
        return await self._signed_post("/fapi/v3/listenKey", {})

    async def keep_listen_key(self) -> Dict[str, Any]:
        return await self._signed_put("/fapi/v3/listenKey", {})

    async def close_listen_key(self) -> Dict[str, Any]:
        return await self._signed_delete("/fapi/v3/listenKey", {})

    # ── 解析方法 ──

    def _parse_exchange_info(self, data: Dict[str, Any]) -> ExchangeInfo:
        markets: Dict[str, Any] = {}
        precision: Dict[str, Any] = {"price": {}, "quantity": {}}

        for item in data.get("symbols", []):
            symbol = item.get("symbol")
            if not symbol:
                continue
            if item.get("status") != "TRADING":
                continue

            price_prec = int(item.get("pricePrecision", 0))
            qty_prec = int(item.get("quantityPrecision", 0))
            self._precision_cache[symbol] = (price_prec, qty_prec)
            precision["price"][symbol] = price_prec
            precision["quantity"][symbol] = qty_prec

            base = item.get("baseAsset", "")
            quote = item.get("quoteAsset", "")
            markets[symbol] = {
                "id": symbol,
                "symbol": symbol,
                "base": base,
                "quote": quote,
                "active": True,
                "type": "swap",
                "precision": {
                    "price": price_prec,
                    "amount": qty_prec,
                },
                "info": item,
            }

        return ExchangeInfo(
            name="Aster Finance",
            id="aster",
            type=ExchangeType.FUTURES,
            supported_features=["trading", "orderbook", "ticker", "user_data", "websocket"],
            rate_limits={},
            precision=precision,
            fees={},
            markets=markets,
            status="active",
            timestamp=datetime.now(),
        )

    def _parse_ticker(self, data: Dict[str, Any]) -> TickerData:
        return TickerData(
            symbol=data.get("symbol", ""),
            timestamp=self._parse_timestamp(data.get("time")),
            bid=data.get("bidPrice"),
            ask=data.get("askPrice"),
            bid_size=data.get("bidQty"),
            ask_size=data.get("askQty"),
            last=data.get("price") or data.get("lastPrice"),
            raw_data=data,
        )

    def _parse_mark_price(self, data: Dict[str, Any]) -> TickerData:
        return TickerData(
            symbol=data.get("symbol", ""),
            timestamp=self._parse_timestamp(data.get("time")),
            mark_price=data.get("markPrice"),
            index_price=data.get("indexPrice"),
            funding_rate=data.get("lastFundingRate"),
            next_funding_time=data.get("nextFundingTime"),
            raw_data=data,
        )

    def _parse_orderbook(self, data: Dict[str, Any]) -> OrderBookData:
        bids = [OrderBookLevel(price=p, size=s) for p, s in data.get("bids", [])]
        asks = [OrderBookLevel(price=p, size=s) for p, s in data.get("asks", [])]
        bids.sort(key=lambda lv: lv.price, reverse=True)
        asks.sort(key=lambda lv: lv.price)

        return OrderBookData(
            symbol=data.get("symbol", ""),
            bids=bids,
            asks=asks,
            timestamp=self._parse_timestamp(data.get("T")),
            raw_data=data,
        )

    def _parse_order(self, data: Dict[str, Any]) -> OrderData:
        amount = self._safe_decimal(data.get("origQty"))
        filled = self._safe_decimal(data.get("executedQty"))
        remaining = amount - filled
        avg_price = data.get("avgPrice")
        price = data.get("price")
        cost = filled * self._safe_decimal(avg_price)

        return OrderData(
            id=str(data.get("orderId")),
            client_id=data.get("clientOrderId") or None,
            symbol=data.get("symbol", ""),
            side=self._parse_order_side(data.get("side")),
            type=self._parse_order_type(data.get("type")),
            amount=amount,
            price=self._safe_decimal(price) if price and price != "0" else None,
            filled=filled,
            remaining=remaining,
            cost=cost,
            average=self._safe_decimal(avg_price) if avg_price and avg_price != "0" else None,
            status=self._parse_order_status(data.get("status")),
            timestamp=self._parse_timestamp(data.get("time")),
            updated=self._parse_timestamp(data.get("updateTime")) if data.get("updateTime") else None,
            fee=None,
            trades=[],
            params={},
            raw_data=data,
        )

    def _parse_positions(self, data: List[Dict[str, Any]]) -> List[PositionData]:
        positions: List[PositionData] = []
        for item in data:
            qty = self._safe_decimal(item.get("positionAmt"))
            side = self._parse_position_side(qty)
            size = qty.copy_abs()
            margin_type = item.get("marginType", "CROSSED")
            margin_mode = self._parse_margin_mode(margin_type)

            margin = self._safe_decimal(item.get("isolatedMargin"))
            if margin_mode != self._parse_margin_mode("ISOLATED") or margin == 0:
                margin = Decimal("0")

            positions.append(
                PositionData(
                    symbol=item.get("symbol", ""),
                    side=side,
                    size=size,
                    entry_price=self._safe_decimal(item.get("entryPrice")),
                    mark_price=self._safe_decimal(item.get("markPrice")),
                    current_price=self._safe_decimal(item.get("markPrice")),
                    unrealized_pnl=self._safe_decimal(item.get("unRealizedProfit")),
                    realized_pnl=Decimal("0"),
                    percentage=None,
                    leverage=int(float(item.get("leverage", 1))),
                    margin_mode=margin_mode,
                    margin=margin,
                    liquidation_price=self._safe_decimal(item.get("liquidationPrice")) if item.get("liquidationPrice") else None,
                    timestamp=self._parse_timestamp(item.get("updateTime")),
                    raw_data=item,
                )
            )
        return positions

    def _parse_balances(self, data: Any) -> List[BalanceData]:
        balances: List[BalanceData] = []
        items = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])

        for item in items:
            balances.append(
                BalanceData(
                    currency=item.get("asset", ""),
                    free=self._safe_decimal(item.get("availableBalance")),
                    used=self._safe_decimal(item.get("balance")) - self._safe_decimal(item.get("availableBalance")),
                    total=self._safe_decimal(item.get("balance")),
                    usd_value=None,
                    timestamp=datetime.now(),
                    raw_data=item,
                )
            )
        return balances
