"""
Aster 实盘连接测试

使用 @pytest.mark.live 标记，仅在指定时运行:
    pytest tests/adapters/aster/test_aster_live.py -v -m live
"""

import asyncio
import os

import pytest
from dotenv import load_dotenv

load_dotenv()

from core.adapters.exchanges.adapters.aster_rest import AsterRest
from core.adapters.exchanges.adapters.aster_websocket import AsterWebSocket

# 从环境变量读取认证信息
USER = os.environ.get("ASTER_USER_ADDRESS", "")
SIGNER = os.environ.get("ASTER_SIGNER_ADDRESS", "")
PRIVATE_KEY = os.environ.get("ASTER_PRIVATE_KEY", "")

live = pytest.mark.live


def make_rest() -> AsterRest:
    rest = AsterRest()
    rest.user = USER
    rest.signer = SIGNER
    rest.private_key = PRIVATE_KEY
    rest.ssl_verify = False
    return rest


# ── Aster REST 公共端点 ──


@live
@pytest.mark.asyncio
async def test_live_ping():
    rest = make_rest()
    try:
        result = await rest.ping()
        assert isinstance(result, dict)
        print(f"ping: {result}")
    finally:
        await rest.close()


@live
@pytest.mark.asyncio
async def test_live_exchange_info():
    rest = make_rest()
    try:
        result = await rest.get_exchange_info()
        info = rest._parse_exchange_info(result)
        symbols = list(info.markets.keys())
        assert "BTCUSDT" in symbols, f"BTCUSDT not in {symbols[:10]}"
        assert "ETHUSDT" in symbols, f"ETHUSDT not in {symbols[:10]}"
        print(f"exchange_info: {len(symbols)} symbols, first 5: {symbols[:5]}")
    finally:
        await rest.close()


@live
@pytest.mark.asyncio
async def test_live_depth():
    rest = make_rest()
    try:
        result = await rest.get_depth("BTCUSDT", 5)
        ob = rest._parse_orderbook(result)
        assert len(ob.bids) > 0, "No bids"
        assert len(ob.asks) > 0, "No asks"
        print(f"depth BTCUSDT: best_bid={ob.bids[0].price} best_ask={ob.asks[0].price}")
    finally:
        await rest.close()


@live
@pytest.mark.asyncio
async def test_live_book_ticker():
    rest = make_rest()
    try:
        result = await rest.get_book_ticker("BTCUSDT")
        ticker = rest._parse_ticker(result)
        assert ticker.bid is not None, "No bid"
        assert ticker.ask is not None, "No ask"
        print(f"bookTicker BTCUSDT: bid={ticker.bid} ask={ticker.ask}")
    finally:
        await rest.close()


@live
@pytest.mark.asyncio
async def test_live_mark_price():
    rest = make_rest()
    try:
        result = await rest.get_mark_price("BTCUSDT")
        ticker = rest._parse_mark_price(result)
        assert ticker.mark_price is not None, "No mark price"
        print(f"markPrice BTCUSDT: mark={ticker.mark_price} funding={ticker.funding_rate}")
    finally:
        await rest.close()


# ── Aster REST 私有端点 (签名认证) ──


@live
@pytest.mark.asyncio
async def test_live_balance():
    rest = make_rest()
    try:
        result = await rest.get_balance()
        assert not isinstance(result, dict) or result.get("code") is None, f"API error: {result}"
        balances = rest._parse_balances(result)
        print(f"balance: {len(balances)} assets")
        for b in balances[:3]:
            print(f"  {b.currency}: total={b.total} free={b.free}")
    finally:
        await rest.close()


@live
@pytest.mark.asyncio
async def test_live_positions():
    rest = make_rest()
    try:
        result = await rest.get_positions()
        assert not isinstance(result, dict) or result.get("code") is None, f"API error: {result}"
        positions = rest._parse_positions(result)
        print(f"positions: {len(positions)} entries")
        for p in positions[:3]:
            print(f"  {p.symbol}: side={p.side.value} size={p.size} entry={p.entry_price}")
    finally:
        await rest.close()


# ── Aster WebSocket ──


@live
@pytest.mark.asyncio
async def test_live_ws_book_ticker():
    ws = AsterWebSocket()
    ws.ssl_verify = False
    messages = []

    async def on_ticker(ticker):
        messages.append(ticker)

    ws._ticker_callbacks.append(on_ticker)

    try:
        await ws.connect()
        await ws.subscribe(["btcusdt@bookTicker"])

        listen_task = asyncio.create_task(ws.listen())
        await asyncio.sleep(10)
        listen_task.cancel()

        assert len(messages) >= 3, f"Only received {len(messages)} messages in 10s"
        print(f"ws bookTicker: received {len(messages)} messages")
        for m in messages[:3]:
            print(f"  {m.symbol}: bid={m.bid} ask={m.ask}")
    finally:
        await ws.disconnect()


# ── Aster + StandX 联合价差测试 ──


@live
@pytest.mark.asyncio
async def test_live_aster_standx_spread():
    """同时获取 Aster 和 StandX 的 BTC/ETH orderbook，计算双向价差"""
    from core.adapters.exchanges.adapters.standx_rest import StandXRest

    aster = make_rest()
    standx = StandXRest()
    standx.jwt_token = os.environ.get("STANDX_API_TOKEN", "")
    standx.ssl_verify = False

    try:
        for aster_sym, standx_sym, name in [
            ("BTCUSDT", "BTC-USD", "BTC"),
            ("ETHUSDT", "ETH-USD", "ETH"),
        ]:
            # 并发获取
            aster_data, standx_data = await asyncio.gather(
                aster.get_depth(aster_sym, 5),
                standx.query_depth_book(standx_sym),
            )
            aster_ob = aster._parse_orderbook(aster_data)
            standx_ob = standx._parse_orderbook(standx_data)

            if not aster_ob.bids or not aster_ob.asks or not standx_ob.bids or not standx_ob.asks:
                print(f"  {name}: Skipped - empty orderbook")
                continue

            aster_bid = float(aster_ob.bids[0].price)
            aster_ask = float(aster_ob.asks[0].price)
            standx_bid = float(standx_ob.bids[0].price)
            standx_ask = float(standx_ob.asks[0].price)

            # Direction 1: Buy@Aster, Sell@StandX
            spread1 = (standx_bid - aster_ask) / aster_ask * 100
            # Direction 2: Buy@StandX, Sell@Aster
            spread2 = (aster_bid - standx_ask) / standx_ask * 100

            print(f"\n  {name} Spread:")
            print(f"    Aster:  bid={aster_bid:.2f}  ask={aster_ask:.2f}")
            print(f"    StandX: bid={standx_bid:.2f}  ask={standx_ask:.2f}")
            print(f"    Buy@Aster -> Sell@StandX: {spread1:+.4f}%")
            print(f"    Buy@StandX -> Sell@Aster: {spread2:+.4f}%")

            # 价差应在合理范围 (-2% ~ +2%)
            assert -2.0 < spread1 < 2.0, f"{name} spread1 out of range: {spread1}"
            assert -2.0 < spread2 < 2.0, f"{name} spread2 out of range: {spread2}"

    finally:
        await aster.close()
        await standx.close()
