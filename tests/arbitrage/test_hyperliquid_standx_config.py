from pathlib import Path

from core.services.arbitrage_monitor_v2.config.monitor_config import ConfigManager
from core.services.arbitrage_monitor_v2.config.multi_exchange_config import (
    MultiExchangeArbitrageConfigManager,
)


def test_monitor_config_has_hyperliquid_and_standx():
    config_path = Path("config/arbitrage/monitor_hyperliquid_standx.yaml")
    manager = ConfigManager(config_path)
    config = manager.get_config()
    assert "hyperliquid" in config.exchanges
    assert "standx" in config.exchanges
    assert "BTC-USDC-PERP" in config.symbols


def test_multi_exchange_pairs_include_hyperliquid_standx():
    manager = MultiExchangeArbitrageConfigManager()
    pairs = manager.get_pairs()
    assert pairs
    matches = [
        pair
        for pair in pairs
        if {pair.exchange_a, pair.exchange_b} == {"hyperliquid", "standx"}
    ]
    assert matches
