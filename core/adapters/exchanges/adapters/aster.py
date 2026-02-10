"""
Aster Finance Futures 交易所适配器

组合 AsterBase + AsterRest + AsterWebSocket，
实现 ExchangeAdapter 接口。
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional

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
from .aster_base import AsterBase
from .aster_rest import AsterRest
from .aster_websocket import AsterWebSocket


class AsterAdapter(ExchangeAdapter):
    # Aster 各交易对数量精度 (quantityPrecision)
    _SYMBOL_QTY_PRECISION = {
        "BTCUSDT": 4,
        "ETHUSDT": 3,
        "SOLUSDT": 0,
    }

    # 标准符号 → Aster 交易所符号
    _SYMBOL_TO_EXCHANGE = {
        "BTC-USDC-PERP": "BTCUSDT",
        "ETH-USDC-PERP": "ETHUSDT",
        "SOL-USDC-PERP": "SOLUSDT",
    }
    _SYMBOL_FROM_EXCHANGE = {v: k for k, v in _SYMBOL_TO_EXCHANGE.items()}

    def _to_exchange_symbol(self, symbol: str) -> str:
        return self._SYMBOL_TO_EXCHANGE.get(symbol, symbol)

    def _from_exchange_symbol(self, symbol: str) -> str:
        return self._SYMBOL_FROM_EXCHANGE.get(symbol, symbol)

    def __init__(self, config: ExchangeConfig, event_bus=None):
        super().__init__(config, event_bus)
        if self.logger and hasattr(self.logger, "logger"):
            self.logger = self.logger.logger

        self.base = AsterBase(config.__dict__)
        self.rest = AsterRest(config, self.logger)
        self.websocket = AsterWebSocket(config, self.logger)

        self._listen_task: Optional[asyncio.Task] = None
        self._user_listen_task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._listen_key: Optional[str] = None

        self._position_cache: Dict[str, PositionData] = {}
        self._order_cache: Dict[str, OrderData] = {}

        # 注册内部回调
        self.websocket._order_callbacks.append(self._handle_internal_order_update)
        self.websocket._position_callbacks.append(self._handle_internal_position_update)

    # ── 生命周期 ──

    async def _do_connect(self) -> bool:
        if self.config.enable_websocket:
            await self.websocket.connect()
            self._listen_task = asyncio.create_task(self.websocket.listen())
        return True

    async def _do_disconnect(self) -> None:
        for task in [self._listen_task, self._user_listen_task, self._keepalive_task]:
            if task:
                task.cancel()
        self._listen_task = None
        self._user_listen_task = None
        self._keepalive_task = None

        if self._listen_key:
            try:
                await self.rest.close_listen_key()
            except Exception:
                pass
            self._listen_key = None

        await self.websocket.disconnect()
        await self.rest.close()

    async def _do_authenticate(self) -> bool:
        try:
            resp = await self.rest.create_listen_key()
            self._listen_key = resp.get("listenKey")
            if self._listen_key:
                await self.websocket.connect_user_stream(self._listen_key)
                self._user_listen_task = asyncio.create_task(self.websocket.listen_user_stream())
                self._keepalive_task = asyncio.create_task(self._keepalive_loop())
            return True
        except Exception as e:
            if self.logger:
                self.logger.error(f"Aster authenticate failed: {e}")
            return False

    async def _do_health_check(self) -> Dict[str, Any]:
        try:
            await self.rest.ping()
            return {"status": "healthy"}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    async def _do_heartbeat(self) -> None:
        if self._listen_key:
            await self.rest.keep_listen_key()

    async def _keepalive_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(30 * 60)  # 30 分钟
                if self._listen_key:
                    await self.rest.keep_listen_key()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    # ── 市场数据 ──

    async def get_exchange_info(self) -> ExchangeInfo:
        data = await self.rest.get_exchange_info()
        return self.rest._parse_exchange_info(data)

    async def get_ticker(self, symbol: str) -> TickerData:
        data = await self.rest.get_book_ticker(symbol)
        return self.rest._parse_ticker(data)

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        if not symbols:
            symbols = await self.get_supported_symbols()
        results = []
        for symbol in symbols:
            results.append(await self.get_ticker(symbol))
        return results

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        data = await self.rest.get_depth(symbol, limit=limit or 20)
        return self.rest._parse_orderbook(data)

    async def get_supported_symbols(self) -> List[str]:
        data = await self.rest.get_exchange_info()
        return [s.get("symbol") for s in data.get("symbols", []) if s.get("status") == "TRADING"]

    async def health_check(self) -> Dict[str, Any]:
        return await self._do_health_check()

    # ── 交易 ──

    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> OrderData:
        exchange_symbol = self._to_exchange_symbol(symbol)
        precision = self.rest._precision_cache.get(exchange_symbol)
        if precision:
            price_prec, qty_prec = precision
        else:
            price_prec = None
            qty_prec = self._SYMBOL_QTY_PRECISION.get(exchange_symbol, 5)

        qty_step = Decimal("1").scaleb(-qty_prec)
        quantized = amount.quantize(qty_step, rounding=ROUND_DOWN)

        order_params: Dict[str, Any] = {
            "symbol": exchange_symbol,
            "side": "BUY" if side == OrderSide.BUY else "SELL",
            "type": order_type.value.upper(),
            "quantity": str(quantized),
        }
        if price is not None:
            if price_prec is not None:
                price_step = Decimal("1").scaleb(-price_prec)
                price = price.quantize(price_step, rounding=ROUND_DOWN)
            order_params["price"] = str(price)
        if order_type == OrderType.LIMIT:
            order_params["timeInForce"] = (params or {}).get("timeInForce", "GTC")
        if params:
            for k in ("reduceOnly", "newClientOrderId", "positionSide"):
                if k in params:
                    order_params[k] = params[k]

        data = await self.rest.new_order(order_params)
        order = self.rest._parse_order(data)
        order.symbol = symbol  # 保持标准符号
        self._order_cache[order.id] = order
        return order

    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> OrderData:
        std_symbol = symbol
        if not symbol:
            cached = self._order_cache.get(order_id)
            symbol = cached.symbol if cached else "BTCUSDT"
            std_symbol = symbol
        exchange_symbol = self._to_exchange_symbol(symbol)
        data = await self.rest.cancel_order(exchange_symbol, int(order_id))
        order = self.rest._parse_order(data)
        order.symbol = std_symbol  # 保持标准符号
        return order

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        if symbol:
            await self.rest.cancel_all_orders(symbol)
        return []

    async def get_order(self, order_id: str, symbol: Optional[str] = None) -> OrderData:
        std_symbol = symbol
        if not symbol:
            cached = self._order_cache.get(order_id)
            symbol = cached.symbol if cached else "BTCUSDT"
            std_symbol = symbol
        exchange_symbol = self._to_exchange_symbol(symbol)
        data = await self.rest.get_order(exchange_symbol, int(order_id))
        order = self.rest._parse_order(data)
        order.symbol = std_symbol  # 保持标准符号
        return order

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        data = await self.rest.get_open_orders(symbol)
        if isinstance(data, list):
            return [self.rest._parse_order(item) for item in data]
        return []

    async def get_positions(self) -> List[PositionData]:
        data = await self.rest.get_positions()
        if isinstance(data, list):
            positions = self.rest._parse_positions(data)
            self._position_cache = {p.symbol: p for p in positions}
            return positions
        return []

    async def get_balance(self) -> List[BalanceData]:
        data = await self.rest.get_balance()
        if self.logger:
            self.logger.info(f"[余额调试] /fapi/v3/balance 原始响应: {data}")
        balances = self.rest._parse_balances(data)
        # 过滤：只保留 total != 0 的资产（Aster 返回 30+ 资产大部分 wallet balance 为 0）
        balances = [b for b in balances if b.total != 0]
        # 如果过滤后为空，尝试从 account API 获取 totalWalletBalance
        if not balances:
            try:
                account = await self.rest.get_account()
                wallet_balance = account.get("totalWalletBalance")
                if wallet_balance and Decimal(str(wallet_balance)) > 0:
                    available = Decimal(str(account.get("availableBalance", "0")))
                    total = Decimal(str(wallet_balance))
                    balances = [BalanceData(
                        currency="USDF",
                        free=available,
                        used=total - available,
                        total=total,
                        usd_value=None,
                        timestamp=datetime.now(),
                        raw_data=account,
                    )]
            except Exception:
                pass
        return balances

    async def get_balances(self) -> List[BalanceData]:
        return await self.get_balance()

    async def get_trades(self, symbol: Optional[str] = None, limit: Optional[int] = None) -> List[TradeData]:
        return []

    async def get_ohlcv(self, symbol: str, timeframe: str = "1m", limit: int = 100) -> List[Any]:
        return []

    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[Any] = None,
        limit: Optional[int] = None,
    ) -> List[OrderData]:
        return []

    # ── 交易设置 ──

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        return await self.rest.set_leverage(symbol, leverage)

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        margin_type = "ISOLATED" if margin_mode.lower() == "isolated" else "CROSSED"
        return await self.rest.set_margin_type(symbol, margin_type)

    # ── 订阅 ──

    async def subscribe_ticker(self, symbol: str, callback) -> None:
        async def _wrapper(ticker: TickerData):
            callback(ticker.symbol, ticker)

        self.websocket._ticker_callbacks.append(_wrapper)
        await self.websocket.subscribe([f"{symbol.lower()}@bookTicker"])
        await self.websocket.subscribe([f"{symbol.lower()}@markPrice@1s"])

    async def subscribe_orderbook(self, symbol: str, callback) -> None:
        async def _wrapper(ob: OrderBookData):
            callback(ob.symbol, ob)

        self.websocket._orderbook_callbacks.append(_wrapper)
        await self.websocket.subscribe([f"{symbol.lower()}@depth20@100ms"])

    async def subscribe_trades(self, symbol: str, callback) -> None:
        raise NotImplementedError("AsterAdapter 暂未实现 trades 订阅")

    async def subscribe_user_data(self, callback) -> None:
        self.websocket._order_callbacks.append(callback)
        self.websocket._position_callbacks.append(callback)
        self.websocket._balance_callbacks.append(callback)
        if not self._listen_key:
            await self._do_authenticate()

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        pass

    def reset_market_callbacks(self) -> None:
        """
        仅清理行情回调，供断流重连后重订阅使用，避免重复回调导致重复入队。
        不影响订单/持仓内部回调。
        """
        if hasattr(self.websocket, "_ticker_callbacks"):
            self.websocket._ticker_callbacks.clear()
        if hasattr(self.websocket, "_orderbook_callbacks"):
            self.websocket._orderbook_callbacks.clear()

    # ── 内部回调 ──

    async def _handle_internal_order_update(self, order: OrderData) -> None:
        self._order_cache[order.id] = order

    async def _handle_internal_position_update(self, position: PositionData) -> None:
        self._position_cache[position.symbol] = position
