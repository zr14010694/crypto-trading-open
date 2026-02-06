# StandX Order WS + SSL Compatibility Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 为 StandX 增加订单回报 WebSocket（ws-api）并与 REST 下单 session_id 对齐，同时引入可配置 SSL 校验策略与资源清理。

**Architecture:** 保持现有 StandXAdapter/StandXRest/StandXWebSocket 分层；StandXWebSocket 维护第二条 ws-api 连接用于订单回报，StandXRest 下单/撤单注入 x-session-id 头；SSL 通过 extra_params 控制仅对 StandX 生效；disconnect 时关闭 REST 与两条 WS。

**Tech Stack:** Python 3.12, aiohttp, pytest。

---

### Task 1: 订单回报与 Session-ID 测试先行

**Files:**
- Create: `tests/adapters/standx/test_standx_order_response_ws.py`
- Modify: `tests/adapters/standx/test_standx_rest_private.py`

**Step 1: 新增订单回报解析测试（失败先行）**

```python
from core.adapters.exchanges.adapters.standx_websocket import StandXWebSocket

ws = StandXWebSocket(_make_config())
msg = {"code":0, "message":"success", "request_id":"abc", "data": {"id": 1, "symbol":"BTC-USD", "side":"buy", "order_type":"limit", "qty":"0.1", "fill_qty":"0", "price":"100", "status":"open", "created_at":"2025-01-01T00:00:00Z"}}
order = ws._parse_order_response(msg)
assert order.id == "1"
```

Run: `python3 -m pytest tests/adapters/standx/test_standx_order_response_ws.py -v`
Expected: FAIL (method missing)

**Step 2: REST 注入 session_id 头测试（失败先行）**

```python
headers = rest._signed_headers(payload, request_id="req", session_id="sess")
assert headers["x-session-id"] == "sess"
```

Run: `python3 -m pytest tests/adapters/standx/test_standx_rest_private.py -v`
Expected: FAIL (no x-session-id)

---

### Task 2: StandXRest 注入 session_id + SSL 配置

**Files:**
- Modify: `core/adapters/exchanges/adapters/standx_rest.py`
- Modify: `config/exchanges/standx_config.yaml`

**Step 1: 更新 _signed_headers 支持 session_id**

```python
if session_id:
    headers["x-session-id"] = session_id
```

**Step 2: 增加 SSL 配置（extra_params.ssl_verify / ssl_ca_path）**

```python
connector = aiohttp.TCPConnector(ssl=ssl_context_or_false)
session = aiohttp.ClientSession(connector=connector, timeout=self._timeout)
```

Run: `python3 -m pytest tests/adapters/standx/test_standx_rest_private.py -v`
Expected: PASS

---

### Task 3: StandXWebSocket 订单回报流

**Files:**
- Modify: `core/adapters/exchanges/adapters/standx_websocket.py`
- Modify: `tests/adapters/standx/test_standx_order_response_ws.py`

**Step 1: 增加 order_ws 连接与 auth:login**

```python
self.order_ws = await session.ws_connect(self.order_ws_url, ssl=ssl_ctx)
await self._order_ws_auth()
```

**Step 2: 实现 _parse_order_response**

```python
def _parse_order_response(self, msg):
    data = msg.get("data") or {}
    return self._parse_order(data)
```

Run: `python3 -m pytest tests/adapters/standx/test_standx_order_response_ws.py -v`
Expected: PASS

---

### Task 4: StandXAdapter 统一 session_id + 断开清理

**Files:**
- Modify: `core/adapters/exchanges/adapters/standx.py`

**Step 1: 生成 session_id 并注入到 rest/websocket**

```python
self.session_id = str(uuid.uuid4())
self.rest.session_id = self.session_id
self.websocket.session_id = self.session_id
```

**Step 2: connect/disconnect 同时管理两条 WS**

```python
await self.websocket.connect()
await self.websocket.connect_order_stream()
await self.websocket.disconnect_order_stream()
```

Run: `python3 -m pytest tests/adapters/standx/test_standx_adapter.py -v`
Expected: PASS

---

### Task 5: 冒烟验证（可选联网）

**Files:**
- Modify: `examples/standx_public_smoke.py`

**Step 1: 支持 SSL 选项的本地开关**

```python
config = ExchangeConfig(..., extra_params={"ssl_verify": False})
```

Run: `python3 examples/standx_public_smoke.py`
Expected: 输出 ticker + orderbook（若 SSL OK）

---

Plan complete. Two execution options:

1. Subagent-Driven (this session) — I will implement each task in order with checkpoints.
2. Parallel Session (separate) — Open new session using executing-plans.

Which approach?
