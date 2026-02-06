"""
StandX 交易所适配器
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import uuid
import asyncio
from decimal import Decimal
import json

from ..adapter import ExchangeAdapter
from ..interface import ExchangeConfig
from ..models import (
    BalanceData,
    ExchangeInfo,
    OrderBookData,
    OrderData,
    OrderSide,
    OrderType,
    PositionData,
    TickerData,
    TradeData,
)
from .standx_base import StandXBase
from .standx_rest import StandXRest
from .standx_websocket import StandXWebSocket


class StandXAdapter(ExchangeAdapter):
    def __init__(self, config: ExchangeConfig, event_bus=None):
        super().__init__(config, event_bus)
        if self.logger and hasattr(self.logger, "logger"):
            self.logger = self.logger.logger

        self.base = StandXBase(config.__dict__)
        self.rest = StandXRest(config, self.logger)
        self.websocket = StandXWebSocket(config, self.logger)
        self.session_id = str(uuid.uuid4())
        self.rest.session_id = self.session_id
        self.websocket.session_id = self.session_id
        self._listen_task: Optional[asyncio.Task] = None
        self._order_listen_task: Optional[asyncio.Task] = None

        self._position_cache: Dict[str, PositionData] = {}
        self._order_cache: Dict[str, OrderData] = {}

        if hasattr(self.websocket, "_order_callbacks"):
            self.websocket._order_callbacks.append(self._handle_internal_order_update)
        if hasattr(self.websocket, "_position_callbacks"):
            self.websocket._position_callbacks.append(self._handle_internal_position_update)

    async def _do_connect(self) -> bool:
        await self.websocket.connect()
        await self.websocket.connect_order_stream()
        if self.config.enable_websocket:
            await self.websocket.authenticate()
        self._listen_task = asyncio.create_task(self.websocket.listen())
        self._order_listen_task = asyncio.create_task(self.websocket.listen_order_stream())
        return True

    async def _do_disconnect(self) -> None:
        if self._listen_task:
            self._listen_task.cancel()
            self._listen_task = None
        if self._order_listen_task:
            self._order_listen_task.cancel()
            self._order_listen_task = None
        await self.websocket.disconnect_order_stream()
        await self.websocket.disconnect()
        await self.rest.close()

    async def _do_authenticate(self) -> bool:
        await self.websocket.authenticate()
        return True

    async def _do_heartbeat(self) -> None:
        """StandX 暂无专用心跳接口，保持空实现避免报错"""
        return None

    async def health_check(self) -> Dict[str, Any]:
        return {"status": self.status.value}

    async def get_exchange_info(self) -> ExchangeInfo:
        info_data = await self.rest.query_symbol_info()
        return self.rest._parse_symbol_info(info_data)

    async def get_ticker(self, symbol: str) -> TickerData:
        data = await self.rest.query_symbol_price(symbol)
        return self.rest._parse_ticker(data)

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        if not symbols:
            symbols = await self.get_supported_symbols()
        results = []
        for symbol in symbols:
            results.append(await self.get_ticker(symbol))
        return results

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        data = await self.rest.query_depth_book(symbol)
        return self.rest._parse_orderbook(data)

    async def get_supported_symbols(self) -> List[str]:
        info_data = await self.rest.query_symbol_info()
        return [item.get("symbol") for item in info_data if item.get("symbol")]

    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> OrderData:
        payload = self.rest._build_order_payload(
            symbol=symbol,
            side=side,
            order_type=order_type,
            qty=amount,
            price=price,
            time_in_force=(params or {}).get("time_in_force", "gtc"),
            reduce_only=(params or {}).get("reduce_only", False),
            margin_mode=(params or {}).get("margin_mode"),
            leverage=(params or {}).get("leverage"),
            cl_ord_id=(params or {}).get("client_id"),
        )
        data = await self.rest.new_order(payload)
        order = self.rest._parse_order(data)
        self._order_cache[order.id] = order
        return order

    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> OrderData:
        payload = self.rest._build_cancel_payload(order_id=int(order_id))
        data = await self.rest.cancel_order(payload)
        return self.rest._parse_order(data)

    async def get_order(self, order_id: str, symbol: Optional[str] = None) -> OrderData:
        data = await self.rest.query_order(order_id=int(order_id))
        return self.rest._parse_order(data)

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        data = await self.rest.query_open_orders(symbol)
        return [self.rest._parse_order(item) for item in data.get("result", [])]

    async def get_positions(self) -> List[PositionData]:
        data = await self.rest.query_positions()
        positions = self.rest._parse_positions(data)
        self._position_cache = {p.symbol: p for p in positions}
        return positions

    async def get_balance(self) -> List[BalanceData]:
        data = await self.rest.query_balance()
        balances = self.rest._parse_balances(data)
        if not balances and self.logger:
            wallet_address = getattr(self.config, "wallet_address", "") or ""
            masked_wallet = self._mask_wallet_address(wallet_address) or "n/a"
            preview = self._safe_preview(data)
            self.logger.warning(
                f"[BalanceDebug] standx balance_empty wallet={masked_wallet} payload={preview}"
            )
        return balances

    async def get_balances(self) -> List[BalanceData]:
        return await self.get_balance()

    @staticmethod
    def _mask_wallet_address(address: Optional[str]) -> str:
        if not address:
            return ""
        addr = str(address)
        if len(addr) <= 10:
            return addr
        return f"{addr[:6]}...{addr[-4:]}"

    @staticmethod
    def _safe_preview(payload: object, limit: int = 2000) -> str:
        try:
            text = json.dumps(payload, ensure_ascii=True, default=str)
        except Exception:
            text = str(payload)
        if len(text) > limit:
            return f"{text[:limit]}...<truncated>"
        return text

    async def get_trades(self, symbol: Optional[str] = None, limit: Optional[int] = None) -> List[TradeData]:
        data = await self.rest.query_trades(symbol=symbol, limit=limit)
        return [] if not data else []

    async def get_ohlcv(self, symbol: str, timeframe: str = "1m", limit: int = 100) -> List[Any]:
        return []

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        open_orders = await self.get_open_orders(symbol=symbol)
        canceled: List[OrderData] = []
        for order in open_orders:
            try:
                canceled.append(await self.cancel_order(order.id, symbol=order.symbol))
            except Exception:
                continue
        return canceled

    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[Any] = None,
        limit: Optional[int] = None,
    ) -> List[OrderData]:
        # StandX 未提供明确历史订单接口，返回空列表
        return []

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        # StandX 通过订单参数指定 leverage，不支持独立设置
        return {"status": "unsupported", "symbol": symbol, "leverage": leverage}

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        # StandX 通过订单参数指定 margin_mode，不支持独立设置
        return {"status": "unsupported", "symbol": symbol, "margin_mode": margin_mode}

    async def subscribe_ticker(self, symbol: str, callback) -> None:
        async def _wrapper(ticker: TickerData):
            callback(ticker.symbol, ticker)

        self.websocket._callbacks.append(_wrapper)
        await self.websocket.subscribe("price", symbol)

    async def subscribe_orderbook(self, symbol: str, callback) -> None:
        async def _wrapper(orderbook: OrderBookData):
            callback(orderbook.symbol, orderbook)

        self.websocket._callbacks.append(_wrapper)
        await self.websocket.subscribe("depth_book", symbol)

    async def subscribe_trades(self, symbol: str, callback) -> None:
        # StandX public trades supported; not wired here
        raise NotImplementedError("StandXAdapter 暂未实现 trades 订阅")

    async def subscribe_user_data(self, callback) -> None:
        self.websocket._order_callbacks.append(callback)
        self.websocket._position_callbacks.append(callback)
        self.websocket._balance_callbacks.append(callback)
        await self.websocket.authenticate()

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        # StandX WebSocket 未实现取消订阅，留空
        return None

    def _handle_internal_order_update(self, order: OrderData) -> None:
        self._order_cache[order.id] = order

    def _handle_internal_position_update(self, position: PositionData) -> None:
        self._position_cache[position.symbol] = position
