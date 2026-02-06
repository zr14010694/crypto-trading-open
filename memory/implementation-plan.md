# StandX Adapter + Hyperliquid-StandX Arbitrage Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 增加 StandX 交易所适配（行情 + 交易 + 认证/签名 + WS），并基于现有分段套利系统实现 Hyperliquid 与 StandX 的价差套利。

**Architecture:** 新增 StandX 适配器沿用现有 ExchangeAdapter 的 base/rest/websocket 分层；符号转换配置扩展 StandX 规则并将 StandX 报价统一映射到系统标准符号以便与 Hyperliquid 形成重叠；套利走现有 multi_exchange_arbitrage + monitor 配置，不引入新调度逻辑。

**Tech Stack:** Python 3.12, aiohttp, websockets, PyNaCl(ed25519), pytest。

---

### Task 1: StandX 符号转换测试先行

Files: Create `tests/symbols/test_symbol_conversion_standx.py`.

Step 1: 新增单元测试，覆盖 `SymbolConversionService` 对 StandX 的双向转换（示例：`BTC-USD` <-> `BTC-USDC-PERP`、`ETH-USD` <-> `ETH-USDC-PERP`、不在直映射时的格式转换行为）。验证：运行 `python3 -m pytest tests/symbols/test_symbol_conversion_standx.py -v`，期望失败，提示 StandX 映射或格式缺失。

### Task 2: StandX 符号转换配置落地

Files: Modify `config/symbol_conversion.yaml`.

Step 1: 在 `exchange_formats` 中新增 `standx` 格式，约束 `pattern`、`separator`、默认 `quote` 与类型规则，并明确 `DUSD` 到系统标准 `USDC` 的映射策略（用于跨所重叠）。验证：运行 `python3 -m pytest tests/symbols/test_symbol_conversion_standx.py -v`，期望通过。

Step 2: 在 `symbol_mappings.standard_to_exchange.standx` 与 `symbol_mappings.exchange_to_standard.standx` 中补全至少 BTC/ETH/SOL 的直映射。验证：同上测试用例全部通过。

### Task 3: StandX 交易所配置文件与加载测试

Files: Create `config/exchanges/standx_config.yaml`. Create `tests/config/test_standx_config.py`.

Step 1: 新增配置加载测试，验证 `ExchangeConfigLoader` 能读取 StandX 配置文件并接收环境变量覆盖（JWT 令牌、签名私钥、可选钱包地址）。验证：运行 `python3 -m pytest tests/config/test_standx_config.py -v`，期望失败，提示配置文件缺失。

Step 2: 创建 `standx_config.yaml`，包含 REST/WS 基础地址、订阅模式、默认订阅币种、认证字段占位与备注说明，确保优先走环境变量（如 `STANDX_API_KEY` 或 `STANDX_JWT_TOKEN`、`STANDX_PRIVATE_KEY`）。验证：运行 `python3 -m pytest tests/config/test_standx_config.py -v`，期望通过。

### Task 4: StandX 签名与请求头生成能力

Files: Create `core/adapters/exchanges/adapters/standx_signer.py`. Create `tests/adapters/standx/test_standx_signer.py`. Modify `requirements.txt` and `requirements-py312.txt`.

Step 1: 新增签名测试，覆盖请求签名串拼接规则（`version,id,timestamp,payload`）、Ed25519 签名生成、base58/hex 私钥解析，以及签名结果可被公钥验证。验证：运行 `python3 -m pytest tests/adapters/standx/test_standx_signer.py -v`，期望失败，提示签名模块缺失或依赖缺失。

Step 2: 在依赖中加入 Ed25519 所需库（建议 PyNaCl + base58），并实现 StandX 签名与请求头生成工具。验证：运行 `python3 -m pytest tests/adapters/standx/test_standx_signer.py -v`，期望通过。

### Task 5: StandX REST 公共接口解析

Files: Create `core/adapters/exchanges/adapters/standx_base.py`. Create `core/adapters/exchanges/adapters/standx_rest.py`. Create `tests/adapters/standx/test_standx_rest_public.py`.

Step 1: 添加 REST 公共接口解析测试，覆盖 `query_symbol_info`、`query_symbol_price`、`query_depth_book` 的响应解析与标准化（TickerData、OrderBookData、ExchangeInfo），并确认价格/数量精度缓存。验证：运行 `python3 -m pytest tests/adapters/standx/test_standx_rest_public.py -v`，期望失败，提示模块缺失。

Step 2: 实现 StandX REST 客户端的公共接口与解析逻辑（含订单簿排序与时间戳处理），并将结果转换为统一数据模型。验证：运行 `python3 -m pytest tests/adapters/standx/test_standx_rest_public.py -v`，期望通过。

### Task 6: StandX REST 私有接口与下单

Files: Modify `core/adapters/exchanges/adapters/standx_rest.py`. Create `tests/adapters/standx/test_standx_rest_private.py`.

Step 1: 添加私有接口测试，覆盖 `new_order`、`cancel_order`、`query_order(s)`、`query_positions`、`query_balance` 的请求头签名、payload 生成与响应标准化（OrderData、PositionData、BalanceData）。验证：运行 `python3 -m pytest tests/adapters/standx/test_standx_rest_private.py -v`，期望失败。

Step 2: 实现私有接口调用与标准化，确保 `Authorization: Bearer <jwt>` 与 `x-request-*` 头正确生成，`x-session-id` 可配置或自动生成。验证：运行 `python3 -m pytest tests/adapters/standx/test_standx_rest_private.py -v`，期望通过。

### Task 7: StandX WebSocket 行情与用户数据

Files: Create `core/adapters/exchanges/adapters/standx_websocket.py`. Create `tests/adapters/standx/test_standx_websocket_parsing.py`.

Step 1: 添加 WS 消息解析测试，覆盖 `price`、`depth_book`、`order`、`position`、`balance` 的消息结构，确保订单簿排序与序号更新逻辑正确。验证：运行 `python3 -m pytest tests/adapters/standx/test_standx_websocket_parsing.py -v`，期望失败。

Step 2: 实现 WS 连接、订阅与消息分发，支持 Market Stream 的行情订阅与 Order Response Stream 的下单回报绑定（与 REST `x-session-id` 一致）。验证：运行 `python3 -m pytest tests/adapters/standx/test_standx_websocket_parsing.py -v`，期望通过。

### Task 8: StandX 适配器主类

Files: Create `core/adapters/exchanges/adapters/standx.py`. Create `tests/adapters/standx/test_standx_adapter.py`.

Step 1: 添加适配器行为测试，覆盖连接流程、公共行情获取、下单与撤单路径的委派关系，以及 `_position_cache`/`_order_cache` 同步。验证：运行 `python3 -m pytest tests/adapters/standx/test_standx_adapter.py -v`，期望失败。

Step 2: 实现 `StandXAdapter`（继承 `ExchangeAdapter`），集成 base/rest/ws，补齐统一接口方法，保持与现有适配器一致的缓存与日志行为。验证：运行 `python3 -m pytest tests/adapters/standx/test_standx_adapter.py -v`，期望通过。

### Task 9: 注册 StandX 到工厂与调度器

Files: Modify `core/adapters/exchanges/factory.py`. Modify `core/adapters/exchanges/adapters/__init__.py`. Modify `core/services/arbitrage_monitor_v2/core/orchestrator_bootstrap.py`. Create `tests/adapters/standx/test_standx_factory_registration.py`.

Step 1: 新增工厂注册测试，验证 `ExchangeFactory` 可创建 `standx` 适配器，`orchestrator_bootstrap` 的类型映射包含 `standx`。验证：运行 `python3 -m pytest tests/adapters/standx/test_standx_factory_registration.py -v`，期望失败。

Step 2: 完成注册与类型映射更新，补齐 `standx` 的 `ExchangeType.PERPETUAL`、特性列表与默认限频。验证：运行 `python3 -m pytest tests/adapters/standx/test_standx_factory_registration.py -v`，期望通过。

### Task 10: Hyperliquid ↔ StandX 套利配置

Files: Create `config/arbitrage/monitor_hyperliquid_standx.yaml`. Modify `config/arbitrage/multi_exchange_arbitrage.yaml`. Modify `config/arbitrage/arbitrage_segmented.yaml`. Create `tests/arbitrage/test_hyperliquid_standx_config.py`.

Step 1: 添加套利配置测试，验证新监控配置加载后包含 `hyperliquid` 与 `standx`，且多交易所配置生成期望的交易对组合。验证：运行 `python3 -m pytest tests/arbitrage/test_hyperliquid_standx_config.py -v`，期望失败。

Step 2: 创建 `monitor_hyperliquid_standx.yaml`，限定交易所与币种集合（建议 BTC/ETH/SOL 起步），并在 `multi_exchange_arbitrage.yaml` 中启用 `one_to_many` 或 `many_to_many`，确保包含 `hyperliquid` 与 `standx`。验证：运行 `python3 -m pytest tests/arbitrage/test_hyperliquid_standx_config.py -v`，期望通过。

Step 3: 在 `arbitrage_segmented.yaml` 中为上述币种补充或覆盖 `symbol_configs` 的阈值与数量配置（与既有风控一致）。验证：运行 `python3 -m pytest tests/arbitrage/test_hyperliquid_standx_config.py -v`，期望通过。

### Task 11: StandX 公共数据冒烟测试

Files: Create `examples/standx_public_smoke.py`.

Step 1: 添加冒烟脚本说明与最小化公共请求流程（只读行情），并确保脚本在无交易权限时也能运行。验证：运行 `python3 examples/standx_public_smoke.py`，期望打印行情与订单簿摘要且无异常退出。

### Task 12: 套利系统联调（监控模式）

Files: None.

Step 1: 以监控模式启动统一调度器，使用新监控配置验证价格订阅与价差计算链路（不下单）。验证：运行 `python3 main_unified.py --monitor-config config/arbitrage/monitor_hyperliquid_standx.yaml` 并观察日志中 `standx`/`hyperliquid` 行情更新与价差输出。

Step 2: 启用多交易所套利组合配置进行一次完整启动，确认多交易所组合被加载且无异常退出。验证：运行 `python3 main_unified.py --config config/arbitrage/arbitrage_segmented.yaml --monitor-config config/arbitrage/monitor_hyperliquid_standx.yaml` 并确认日志提示多交易所套利配置已加载。

---

**Notes for Implementation:**
确保 StandX 的 DUSD 报价与系统标准 USDC 映射策略在符号转换中一致，否则重叠符号无法生成；订单相关 REST 请求必须带 `Authorization` 与 `x-request-*` 头，且 `x-session-id` 与 WS Order Response Stream 的 `session_id` 保持一致。
