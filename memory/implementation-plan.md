# Aster 交易所适配器 + Aster-StandX 价差套利 实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 增加 Aster Finance Futures (V3 API) 交易所适配（行情 + 交易 + ECDSA/EIP-712 签名 + WebSocket），并基于现有分段套利系统实现 Aster 与 StandX 的 BTC/ETH 价差套利。

**Architecture:** 新增 Aster 适配器沿用现有 ExchangeAdapter 的 base/signer/rest/websocket 分层（参照 StandX 的 5 文件模式）；符号转换配置扩展 Aster 规则 (BTCUSDT <-> BTC-USDC-PERP)；套利走现有 multi_exchange_arbitrage + monitor 配置，不引入新调度逻辑。

**Tech Stack:** Python 3.12, aiohttp, eth-account, eth-abi, web3, pytest。

## 技术决策
- **网络环境**: 直接主网 (`https://fapi.asterdex.com`)
- **交易对**: BTC-USDC-PERP, ETH-USDC-PERP
- **签名方案**: eth-account + eth-abi + web3 (ECDSA/EIP-712)
- **WebSocket**: `wss://fstream.asterdex.com` + listenKey 用户数据流 (注意: 非 stream.asterdex.com)

## Aster vs StandX 关键差异
| 特性 | Aster | StandX |
|------|-------|--------|
| 认证 | ECDSA + EIP-712 (user/signer/nonce/signature) | JWT + Ed25519 |
| 符号格式 | `BTCUSDT` (无分隔符) | `BTC-USD` |
| API风格 | Binance-like `/fapi/v3/*` | 自定义 `/api/*` |
| WebSocket | listenKey + 公共流 | JWT 认证 + 双流 |
| 下单 | form-urlencoded POST | JSON POST + 签名头 |
| 杠杆 | 独立 `POST /fapi/v3/leverage` | 订单参数内置 |

---

### Task 1: 创建 aster_base.py - 基础常量和解析工具

Files: Create `core/adapters/exchanges/adapters/aster_base.py`. Create `tests/adapters/aster/test_aster_base.py`.

Step 1: 创建测试文件 `test_aster_base.py`，编写测试覆盖: ORDER_SIDE_MAP (BUY/SELL -> OrderSide)、ORDER_STATUS_MAP (NEW/PARTIALLY_FILLED/FILLED/CANCELED/REJECTED/EXPIRED -> OrderStatus)、_safe_decimal (None/空/"123.45"/Decimal)、_parse_timestamp (毫秒 epoch -> datetime)、_parse_position_side (正数=LONG/负数=SHORT)、_parse_margin_mode ("CROSSED"=CROSS/"ISOLATED"=ISOLATED)。验证: 运行 `pytest tests/adapters/aster/test_aster_base.py -v`，期望失败(模块缺失)。

Step 2: 实现 `AsterBase` 类(参照 `standx_base.py` 结构)，含 DEFAULT_BASE_URL="https://fapi.asterdex.com"、DEFAULT_WS_URL="wss://stream.asterdex.com"、所有映射字典和工具方法。验证: 运行 `pytest tests/adapters/aster/test_aster_base.py -v`，期望全部通过。

---

### Task 2: 创建 aster_signer.py - ECDSA/Web3 签名模块

Files: Create `core/adapters/exchanges/adapters/aster_signer.py`. Create `tests/adapters/aster/test_aster_signer.py`.

Step 1: 创建测试文件覆盖: trim_dict 将 int/float/bool/list/嵌套dict 全部转字符串、sign_request 使用已知 test 私钥签名后 Account.recover_message 可验回 signer 地址、build_signed_params 返回 dict 含 nonce/user/signer/signature/timestamp/recvWindow。验证: 运行 `pytest tests/adapters/aster/test_aster_signer.py -v`，期望失败。

Step 2: 实现 `trim_dict(d)` (递归转字符串)、`sign_request(params, user, signer, private_key, nonce)` (JSON 排序 + ABI encode + Keccak256 + ECDSA 签名)、`build_signed_params(params, user, signer, private_key)` (自动注入 timestamp/recvWindow/nonce + 调用 sign_request)。参照 Aster 官方 Python Demo tx.py。验证: 运行 `pytest tests/adapters/aster/test_aster_signer.py -v`，期望通过。

---

### Task 3: 创建 aster_rest.py - REST API 客户端

Files: Create `core/adapters/exchanges/adapters/aster_rest.py`. Create `tests/adapters/aster/test_aster_rest_public.py`.

Step 1: 创建公共端点解析测试覆盖: _parse_exchange_info 填充精度缓存 (pricePrecision/quantityPrecision)、_parse_ticker 从 bookTicker 提取 bid/ask/symbol、_parse_orderbook 将乱序 bids/asks 排好序 (bids降序/asks升序)。验证: 运行 `pytest tests/adapters/aster/test_aster_rest_public.py -v`，期望失败。

Step 2: 实现 `AsterRest` 类继承 AsterBase，含: aiohttp session 管理、公共端点 (ping/get_server_time/get_exchange_info/get_depth/get_book_ticker/get_mark_price/get_funding_rate)、私有端点 (new_order/cancel_order/cancel_all_orders/get_order/get_open_orders/get_balance/get_positions/set_leverage/set_margin_type/create_listen_key/keep_listen_key/close_listen_key)、所有解析方法。注意: POST/DELETE 用 form-urlencoded (非 JSON)，GET 签名参数放 query string。验证: 运行 `pytest tests/adapters/aster/test_aster_rest_public.py -v`，期望通过。

---

### Task 4: REST 私有端点测试

Files: Create `tests/adapters/aster/test_aster_rest_private.py`.

Step 1: 编写测试覆盖: 签名参数注入验证 (固定时间下检查 nonce/user/signer/signature 存在)、_parse_order 解析 NEW/FILLED/CANCELED 状态、_parse_positions 解析 LONG/SHORT (正/负 positionAmt)、_parse_balances 提取 USDT 余额。验证: 运行 `pytest tests/adapters/aster/test_aster_rest_private.py -v`，期望通过。

---

### Task 5: 创建 aster_websocket.py - WebSocket 客户端

Files: Create `core/adapters/exchanges/adapters/aster_websocket.py`. Create `tests/adapters/aster/test_aster_websocket_parsing.py`.

Step 1: 创建 WS 解析测试覆盖: _parse_ws_ticker (bookTicker 消息 -> TickerData)、_parse_ws_orderbook (depth 消息 -> OrderBookData 排序)、_parse_ws_order_update (ORDER_TRADE_UPDATE -> OrderData)、_parse_ws_account_update (ACCOUNT_UPDATE -> PositionData + BalanceData)、订阅消息格式验证 (SUBSCRIBE method + params)。验证: 运行 `pytest tests/adapters/aster/test_aster_websocket_parsing.py -v`，期望失败。

Step 2: 实现 AsterWebSocket 类: 连接 `wss://stream.asterdex.com/stream`、订阅格式 `{"method":"SUBSCRIBE","params":["<stream>"],"id":<n>}`、频道映射 (bookTicker/depth20/aggTrade/markPrice)、用户数据流 (listenKey + ACCOUNT_UPDATE/ORDER_TRADE_UPDATE)、回调系统 (_ticker/_orderbook/_order/_position/_balance callbacks)、listen 循环、重连逻辑。验证: 运行 `pytest tests/adapters/aster/test_aster_websocket_parsing.py -v`，期望通过。

---

### Task 6: 创建 aster.py - 主适配器类

Files: Create `core/adapters/exchanges/adapters/aster.py`. Create `tests/adapters/aster/test_aster_adapter.py`.

Step 1: 创建适配器测试覆盖: Mock REST 测试 get_ticker/get_orderbook 数据流通、create_order 签名参数构建、cancel_order DELETE 请求、符号转换 (BTC-USDC-PERP <-> BTCUSDT)。验证: 运行 `pytest tests/adapters/aster/test_aster_adapter.py -v`，期望失败。

Step 2: 实现 AsterAdapter (继承 ExchangeAdapter)，组合 AsterBase+AsterRest+AsterWebSocket。含: _do_connect (WS 连接 + listenKey)、_do_disconnect (取消 tasks + 关闭连接)、_do_heartbeat (PUT keepalive)、符号转换方法、市场数据方法、交易方法、订阅方法、缓存 (_position_cache/_order_cache)。参照 standx.py 结构。验证: 运行 `pytest tests/adapters/aster/test_aster_adapter.py -v`，期望通过。

---

### Task 7: 注册 Aster 到工厂 + 配置文件

Files: Modify `core/adapters/exchanges/factory.py`. Create `config/exchanges/aster_config.yaml`. Create `tests/adapters/aster/test_aster_factory_registration.py`.

Step 1: 创建工厂注册测试: "aster" 在 get_registered_exchanges() 中、create_adapter("aster", config) 返回 AsterAdapter。验证: 运行 `pytest tests/adapters/aster/test_aster_factory_registration.py -v`，期望失败。

Step 2: 在 factory.py 的 _register_builtin_adapters() 中注册 Aster (exchange_id="aster", ExchangeType.FUTURES, "Aster Finance")。创建 aster_config.yaml (参照 standx_config.yaml 格式)。验证: 运行 `pytest tests/adapters/aster/test_aster_factory_registration.py -v`，期望通过。

---

### Task 8: 符号转换 + 套利配置 + 监控配置

Files: Modify `config/symbol_conversion.yaml`. Modify `config/arbitrage/multi_exchange_arbitrage.yaml`. Modify `config/arbitrage/monitor_v2.yaml`.

Step 1: 在 symbol_conversion.yaml 中 4 处添加 aster: exchange_formats (pattern="{base}{quote}", USDC->USDT 映射)、standard_to_exchange (BTC-USDC-PERP -> BTCUSDT 等)、exchange_to_standard (反向)、validation。验证: `python -c "import yaml; d=yaml.safe_load(open('config/symbol_conversion.yaml')); assert 'aster' in d['exchange_formats']"`。

Step 2: 修改 multi_exchange_arbitrage.yaml: center_exchange="aster", counter_exchanges=["standx"], symbols=["BTC-USDC-PERP","ETH-USDC-PERP"]。验证: YAML 加载无错误。

Step 3: 在 monitor_v2.yaml exchanges 列表添加 `- aster`。验证: YAML 语法正确。

---

### Task 9: 运行完整测试套件

Files: None (test execution only).

Step 1: 运行 `pytest tests/adapters/aster/ -v --tb=short`，确认所有 Aster 测试通过。
Step 2: 运行 `pytest tests/ -v --tb=short -x`，确认全项目无回归。

---

### Task 10: 实盘连接测试 - Aster REST + WebSocket

Files: Create `tests/adapters/aster/test_aster_live.py`.

Step 1: 编写 `@pytest.mark.live` 标记的实盘测试:
- test_live_ping: ping() 返回 {}
- test_live_exchange_info: BTCUSDT 在 symbols 中
- test_live_depth: get_depth("BTCUSDT", 5) bids/asks 非空
- test_live_book_ticker: bid/ask 价格存在
- test_live_balance: 签名认证有效 (不返回 -1022)
- test_live_positions: 持仓查询正常
- test_live_ws_book_ticker: WS 订阅 btcusdt@bookTicker 10 秒内收到 3+ 条消息
验证: `pytest tests/adapters/aster/test_aster_live.py -v -m live` 全部通过。

---

### Task 11: 实盘联合测试 - Aster-StandX 价差计算

Files: Add to `tests/adapters/aster/test_aster_live.py`.

Step 1: 编写联合测试:
- 同时获取 Aster BTCUSDT 和 StandX BTC-USD orderbook
- 计算双向价差: spread1 = (standx_bid - aster_ask) / aster_ask * 100, spread2 = (aster_bid - standx_ask) / standx_ask * 100
- 对 ETH 重复
- 打印价差百分比
验证: 价差在 -0.5% ~ +0.5% 合理范围，符号转换正确。

---

### Task 12: 更新记忆文件

Files: Modify MEMORY.md.

Step 1: 更新 Claude MEMORY.md 记录实盘测试结果、价差观测值、已知问题。验证: 读取确认。

---

## 验证清单 (End-to-End)
1. `pytest tests/adapters/aster/ -v` - 所有单元测试通过
2. `pytest tests/ -v -x` - 全项目无回归
3. **实盘**: Aster 公共 API 返回正确数据
4. **实盘**: Aster 签名认证有效
5. **实盘**: Aster WebSocket 收到实时数据
6. **实盘**: StandX API 正常返回
7. **实盘**: Aster-StandX BTC/ETH 价差计算正确
8. 符号转换双向正确
9. 工厂创建 AsterAdapter 成功
10. 配置文件 YAML 语法正确
11. multi_exchange_arbitrage.yaml 中 aster-standx 对配置正确

## 风险和注意事项
1. **Aster 签名机制复杂**: ECDSA + EIP-712 与其他交易所完全不同, 签名模块需要严格测试
2. **Quote 币种差异**: Aster 用 USDT, StandX 用 DUSD(≈USD), 套利时需考虑汇率差异
3. **API 限频**: Aster 有 IP 级别限频, 需注意不超过速率限制
4. **form-urlencoded**: Aster POST 请求使用 form-urlencoded 而非 JSON, 与 StandX 不同
