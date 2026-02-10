# 项目架构与文件构成速览

最后更新：2026-02-09

本文件用于快速理解项目整体架构、主要模块与目录构成，适合新成员或临时回顾使用。

## 架构分层（core/）

- `core/adapters/`：交易所适配层，封装不同交易所的 API/WS 交互。
- `core/domain/`：领域模型与核心业务概念。
- `core/services/`：业务服务层与策略实现（网格、套利、刷量、价格提醒等）。
- `core/infrastructure/`：基础设施能力（日志、配置、持久化等）。
- `core/di/`：依赖注入容器与模块注册。
- `core/logging/`：结构化日志与日志工具。
- `core/utils/`：通用工具与辅助模块。
- `core/data_aggregator.py`：多交易所数据聚合入口。

## 主要业务系统

- 网格交易系统：`core/services/grid/`，入口 `run_grid_trading.py`。
- 套利监控/执行系统：`core/services/arbitrage_monitor/` 与 `core/services/arbitrage_monitor_v2/`，入口 `run_arbitrage_monitor.py`、`run_arbitrage_monitor_v2.py`、`main_unified.py`。
- 刷量交易系统：`core/services/volume_maker/`，入口 `run_volume_maker.py`、`run_lighter_volume_maker.py`。
- 价格提醒系统：`core/services/price_alert/`，入口 `run_price_alert.py`。
- 网格波动率扫描器：`grid_volatility_scanner/`，入口 `grid_volatility_scanner/run_scanner.py`。

## 运行入口脚本

- `run_grid_trading.py`：网格交易主入口。
- `run_arbitrage_monitor.py`：基础套利监控+执行。
- `run_arbitrage_monitor_v2.py`：V2 套利监控+执行。
- `main_unified.py`：统一套利系统（支持分段/网格模式）。
- `run_volume_maker.py`：刷量系统（Backpack 挂单）。
- `run_lighter_volume_maker.py`：刷量系统（Lighter 市价）。
- `run_price_alert.py`：价格提醒。
- `grid_volatility_scanner/run_scanner.py`：波动率扫描器。

## 配置结构（config/）

- `config/exchanges/`：交易所 API 密钥与账户配置。
- `config/grid/`：网格交易策略与参数配置。
- `config/arbitrage/`：套利监控与执行配置。
- `config/volume_maker/`：刷量策略配置。
- `config/price_alert/`：价格提醒配置。
- `config/symbol_conversion.yaml`：跨交易所交易对映射。
- `config/logging.yaml`：日志配置。

## 顶层目录速览

- `core/`：核心业务与架构分层实现。
- `config/`：各类运行配置。
- `docs/`：文档与设计说明。
- `scripts/`：运维与启动脚本（tmux/部署/检查）。
- `tools/`：工具脚本与计算器。
- `grid_volatility_scanner/`：独立的网格波动率扫描器子模块。
- `tests/`：测试与验证脚本。
- `examples/`：示例代码与演示。
- `data/`：运行时数据（如价差历史）。
- `logs/`：运行日志输出。
- `requirements*.txt`：依赖清单（主环境 / Py312 / Lighter 现货）。
- `env.template`：环境变量模板。
- `.env`：本地环境变量（不建议提交）。

## 环境与依赖

- Python 依赖：`requirements.txt`、`requirements-py312.txt`、`requirements-lighter-spot.txt`。
- 现货与永续依赖存在隔离要求，建议按脚本提示使用不同虚拟环境。

## 快速定位建议


- 想跑策略：从 `run_*.py` 与 `config/` 开始。
- 想看实现：从 `core/services/` 与 `core/adapters/` 入手。

