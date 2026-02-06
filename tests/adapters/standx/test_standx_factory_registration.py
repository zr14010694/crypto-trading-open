from pathlib import Path

from core.adapters.exchanges.factory import ExchangeFactory
from core.adapters.exchanges.adapters.standx import StandXAdapter
from core.services.arbitrage_monitor_v2.core.orchestrator_bootstrap import OrchestratorBootstrap
from core.utils.config_loader import ExchangeConfigLoader
from core.adapters.exchanges.models import ExchangeType


def test_exchange_factory_creates_standx_adapter():
    factory = ExchangeFactory()
    adapter = factory.create_adapter("standx", api_key="key", api_secret="secret")
    assert isinstance(adapter, StandXAdapter)


def test_orchestrator_bootstrap_type_map_for_standx():
    config_path = Path("config/exchanges/standx_config.yaml")
    loader = ExchangeConfigLoader()
    bootstrap = OrchestratorBootstrap(object())
    exchange_config = bootstrap._load_exchange_config("standx", config_path, loader)
    assert exchange_config.exchange_type == ExchangeType.PERPETUAL
