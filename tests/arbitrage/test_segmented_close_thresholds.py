from pathlib import Path

import pytest
import yaml

from core.services.arbitrage_monitor_v2.config.symbol_config import SegmentedConfigManager
from core.services.arbitrage_monitor_v2.decision.unified_decision_engine import UnifiedDecisionEngine


def _write_config(tmp_path: Path, *, include_t0_ratio: bool) -> Path:
    grid_cfg = {
        "initial_spread_threshold": 0.06,
        "grid_step": 0.14,
        "max_segments": 5,
        "segment_quantity_ratio": 1.0,
        "segment_partial_order_ratio": 1.0,
        "min_partial_order_quantity": 0.0,
        "profit_per_segment": 0.02,
        "use_symmetric_close": True,
        "scalp_profit_threshold": 0.02,
        "scalping_enabled": False,
        "scalping_trigger_segment": 10,
        "scalping_profit_threshold": 0.02,
        "spread_persistence_seconds": 1,
        "strict_persistence_check": True,
        "require_orderbook_liquidity": False,
        "min_orderbook_quantity": 0.001,
        "slippage_tolerance": 0.001,
        "price_stability_window_seconds": 1,
        "price_stability_threshold_pct": 0.01,
        "max_local_orderbook_spread_pct": 0.05,
    }
    if include_t0_ratio:
        grid_cfg["t0_close_ratio"] = 0.1

    config_data = {
        "system_mode": {
            "monitor_only": True,
            "data_freshness_seconds": 3.0,
        },
        "default_config": {
            "grid_config": grid_cfg,
            "quantity_config": {
                "base_quantity": 0.001,
                "quantity_mode": "fixed",
                "target_value_usdc": 100.0,
                "quantity_precision": 5,
            },
            "risk_config": {
                "max_position_value": 500.0,
                "max_loss_percent": 2.0,
            },
        },
        "symbol_configs": {
            "BTC-USDC-PERP": {
                "enabled": True,
                "grid_config": {
                    "initial_spread_threshold": 0.06,
                    "grid_step": 0.14,
                    "max_segments": 5,
                },
                "quantity_config": {
                    "base_quantity": 0.001,
                    "quantity_mode": "fixed",
                    "target_value_usdc": 300.0,
                    "quantity_precision": 5,
                },
                "risk_config": {
                    "max_position_value": 500.0,
                    "max_loss_percent": 2.0,
                },
            }
        },
    }

    config_path = tmp_path / "segmented_test.yaml"
    config_path.write_text(yaml.safe_dump(config_data), encoding="utf-8")
    return config_path


def test_custom_t0_close_ratio_applies_to_first_close_threshold(tmp_path: Path):
    config_path = _write_config(tmp_path, include_t0_ratio=True)
    manager = SegmentedConfigManager(config_path=config_path)
    engine = UnifiedDecisionEngine(config_manager=manager)
    cfg = manager.get_config("BTC-USDC-PERP")

    open_thresholds, close_thresholds = engine._build_grid_thresholds(cfg)

    assert open_thresholds[0] == pytest.approx(0.06)
    assert open_thresholds[1] == pytest.approx(0.20)
    assert close_thresholds[0] == pytest.approx(0.006)
    assert close_thresholds[1] == pytest.approx(0.06)


def test_default_t0_close_ratio_remains_legacy_behavior(tmp_path: Path):
    config_path = _write_config(tmp_path, include_t0_ratio=False)
    manager = SegmentedConfigManager(config_path=config_path)
    engine = UnifiedDecisionEngine(config_manager=manager)
    cfg = manager.get_config("BTC-USDC-PERP")

    _, close_thresholds = engine._build_grid_thresholds(cfg)

    assert close_thresholds[0] == pytest.approx(0.024)
