"""
StandX 交易所适配器
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set
import uuid
import asyncio
import logging
import time
from datetime import datetime
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
    # 标准符号 → StandX 交易所符号
    _SYMBOL_TO_EXCHANGE = {
        "BTC-USDC-PERP": "BTC-USD",
        "ETH-USDC-PERP": "ETH-USD",
        "SOL-USDC-PERP": "SOL-USD",
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

        self.base = StandXBase(config.__dict__)
        self.rest = StandXRest(config, self.logger)
        self.websocket = StandXWebSocket(config, self.logger)
        self.session_id = str(uuid.uuid4())
        self.rest.session_id = self.session_id
        self.websocket.session_id = self.session_id
        self._listen_task: Optional[asyncio.Task] = None
        self._order_listen_task: Optional[asyncio.Task] = None
        self._funding_rate_task: Optional[asyncio.Task] = None
        self._funding_rate_symbols: Set[str] = set()

        self._position_cache: Dict[str, PositionData] = {}
        self._order_cache: Dict[str, OrderData] = {}
        self._order_waiters: Dict[str, asyncio.Future] = {}
        self._health_summary_interval_seconds = 30.0
        self._last_health_summary_ts = 0.0

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
        if self._funding_rate_task:
            self._funding_rate_task.cancel()
            self._funding_rate_task = None
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
        ws_diag = {}
        try:
            ws_diag = self.websocket.get_diagnostics()
        except Exception as err:
            if self.logger:
                self.logger.debug("[StandXHealth] get_diagnostics failed: %s", err)

        health = {
            "status": self.status.value,
            "exchange_id": self.config.exchange_id,
            "connected": self.is_connected(),
            "listen_task_done": self._listen_task.done() if self._listen_task else None,
            "order_listen_task_done": self._order_listen_task.done() if self._order_listen_task else None,
            "ws_diagnostics": ws_diag,
        }
        self._log_health_summary(ws_diag)
        return health

    def _log_health_summary(self, ws_diag: Dict[str, Any]) -> None:
        if not self.logger:
            return
        now_ts = time.time()
        if now_ts - self._last_health_summary_ts < self._health_summary_interval_seconds:
            return
        self._last_health_summary_ts = now_ts

        depth_ages = ws_diag.get("depth_age_seconds") or {}
        if depth_ages:
            depth_age_str = ", ".join(
                f"{symbol}={age:.1f}s"
                for symbol, age in sorted(depth_ages.items())
            )
        else:
            depth_age_str = "none"

        self.logger.info(
            "[StandXHealth] status=%s connected=%s listen_done=%s order_listen_done=%s "
            "public_msgs=%s order_msgs=%s last_msg_ts=%s depth_age=%s public_exit=%s order_exit=%s",
            self.status.value,
            self.is_connected(),
            self._listen_task.done() if self._listen_task else None,
            self._order_listen_task.done() if self._order_listen_task else None,
            ws_diag.get("public_msg_count"),
            ws_diag.get("order_msg_count"),
            ws_diag.get("last_msg_ts"),
            depth_age_str,
            ws_diag.get("last_public_exit_reason"),
            ws_diag.get("last_order_exit_reason"),
        )

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
        exchange_symbol = self._to_exchange_symbol(symbol)
        await self._ensure_symbol_precision_loaded(exchange_symbol)
        cl_ord_id = (params or {}).get("client_id") or f"arb-{uuid.uuid4().hex[:12]}"
        payload = self.rest._build_order_payload(
            symbol=exchange_symbol,
            side=side,
            order_type=order_type,
            qty=amount,
            price=price,
            time_in_force=(params or {}).get("time_in_force", "gtc"),
            reduce_only=(params or {}).get("reduce_only", False),
            margin_mode=(params or {}).get("margin_mode"),
            leverage=(params or {}).get("leverage"),
            cl_ord_id=cl_ord_id,
        )

        # 设置 WS 等待器 —— StandX new_order 是异步模式，REST 仅返回 ack，
        # 实际订单数据通过 WebSocket 推送
        waiter: asyncio.Future = asyncio.get_event_loop().create_future()
        self._order_waiters[cl_ord_id] = waiter

        try:
            data = await self.rest.new_order(payload)

            # 情况 1: 直接返回订单详情（含 id 字段）
            if data.get("id") is not None:
                order = self.rest._parse_order(data)
                order.symbol = symbol
                self._order_cache[order.id] = order
                return order

            # 情况 2: 异步确认（code=0），等待 WS 推送
            if data.get("code") == 0:
                order = await asyncio.wait_for(waiter, timeout=5.0)
                order.symbol = symbol
                return order

            # 情况 3: 错误响应
            error_msg = data.get("message") or data.get("error") or "unknown"
            raise Exception(f"StandX order rejected: {error_msg}, raw={data}")
        except asyncio.TimeoutError:
            raise Exception(
                f"StandX order ack received but no WS confirmation within 5s, "
                f"request_id={data.get('request_id')}"
            )
        finally:
            self._order_waiters.pop(cl_ord_id, None)

    async def _ensure_symbol_precision_loaded(self, exchange_symbol: str) -> None:
        precision_cache = getattr(self.rest, "_precision_cache", None)
        if isinstance(precision_cache, dict) and exchange_symbol in precision_cache:
            return
        if not hasattr(self.rest, "query_symbol_info") or not hasattr(self.rest, "_parse_symbol_info"):
            return
        try:
            info_data = await self.rest.query_symbol_info()
            self.rest._parse_symbol_info(info_data)
        except Exception as exc:
            if self.logger:
                self.logger.debug(
                    "[StandX] failed to refresh symbol precision for %s: %s",
                    exchange_symbol,
                    exc,
                )

    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> OrderData:
        payload = self.rest._build_cancel_payload(order_id=int(order_id))
        data = await self.rest.cancel_order(payload)

        # 直接返回订单详情
        if data.get("id") is not None:
            order = self.rest._parse_order(data)
            if symbol:
                order.symbol = symbol
            return order

        # 异步确认 —— 等待 WS 推送已缓存的订单
        if data.get("code") == 0:
            await asyncio.sleep(1.0)
            cached = self._order_cache.get(order_id)
            if cached:
                if symbol:
                    cached.symbol = symbol
                return cached
            # WS 未推送，构造最小 OrderData 返回
            from ..models import OrderStatus
            return OrderData(
                id=order_id, client_id=None, symbol=symbol or "",
                side=OrderSide.BUY, type=OrderType.MARKET,
                amount=Decimal(0), price=None, filled=Decimal(0),
                remaining=Decimal(0), cost=Decimal(0), average=None,
                status=OrderStatus.CANCELLED, timestamp=datetime.now(),
                updated=None, fee=None, trades=[], params={}, raw_data=data,
            )

        error_msg = data.get("message") or data.get("error") or "unknown"
        raise Exception(f"StandX cancel rejected: {error_msg}, raw={data}")

    async def get_order(self, order_id: str, symbol: Optional[str] = None) -> OrderData:
        data = await self.rest.query_order(order_id=int(order_id))
        order = self.rest._parse_order(data)
        if symbol:
            order.symbol = symbol  # 保持标准符号
        return order

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        exchange_symbol = self._to_exchange_symbol(symbol) if symbol else None
        data = await self.rest.query_open_orders(exchange_symbol)
        orders = [self.rest._parse_order(item) for item in data.get("result", [])]
        # 将交易所符号转回标准符号
        for order in orders:
            order.symbol = self._from_exchange_symbol(order.symbol)
        return orders

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
            self._log_health_summary(self.websocket.get_diagnostics())

        self.websocket._ticker_callbacks.append(_wrapper)
        await self.websocket.subscribe("price", symbol)
        # 启动费率轮询（WS price channel 不含 funding_rate）
        self._funding_rate_symbols.add(symbol)
        if not self._funding_rate_task:
            self._funding_rate_task = asyncio.create_task(self._poll_funding_rates())

    async def subscribe_orderbook(self, symbol: str, callback) -> None:
        async def _wrapper(orderbook: OrderBookData):
            callback(orderbook.symbol, orderbook)
            self._log_health_summary(self.websocket.get_diagnostics())

        self.websocket._orderbook_callbacks.append(_wrapper)
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

    def reset_market_callbacks(self) -> None:
        """
        仅清理行情回调，供断流重连后重订阅使用，避免重复回调导致重复入队。
        不影响订单/持仓内部回调。
        """
        if hasattr(self.websocket, "_ticker_callbacks"):
            self.websocket._ticker_callbacks.clear()
        if hasattr(self.websocket, "_orderbook_callbacks"):
            self.websocket._orderbook_callbacks.clear()

    async def _poll_funding_rates(self) -> None:
        """后台轮询 query_funding_rates 获取最新 funding_rate，注入到 WS 缓存"""
        logger = self.logger or logging.getLogger("ExchangeAdapter.standx")
        while True:
            try:
                now_ms = int(time.time() * 1000)
                two_hours_ago_ms = now_ms - 2 * 3600 * 1000
                for symbol in list(self._funding_rate_symbols):
                    try:
                        rates = await self.rest.query_funding_rates(
                            symbol, two_hours_ago_ms, now_ms
                        )
                        if rates:
                            latest = rates[-1]
                            rate = latest.get("funding_rate")
                            if rate is not None:
                                self.websocket._funding_rates[symbol] = rate
                    except Exception:
                        pass
                await asyncio.sleep(60)  # 费率每小时更新，60秒轮询足够
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[StandX] 费率轮询异常: {e}")
                await asyncio.sleep(60)

    async def _handle_internal_order_update(self, order: OrderData) -> None:
        self._order_cache[order.id] = order
        self._log_health_summary(self.websocket.get_diagnostics())
        # 解析异步下单等待器
        if order.client_id and order.client_id in self._order_waiters:
            fut = self._order_waiters.pop(order.client_id)
            if not fut.done():
                fut.set_result(order)

    async def _handle_internal_position_update(self, position: PositionData) -> None:
        self._position_cache[position.symbol] = position
