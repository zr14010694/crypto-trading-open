"""Aster 工厂注册测试"""

from core.adapters.exchanges.factory import ExchangeFactory


def test_aster_registered_in_factory():
    factory = ExchangeFactory()
    exchanges = factory.get_registered_exchanges()
    assert "aster" in exchanges


def test_aster_registry_entry():
    factory = ExchangeFactory()
    info = factory.get_exchange_info("aster")
    assert info is not None
    assert info.name == "Aster Finance"
    assert "perpetual_trading" in info.supported_features
    assert "websocket" in info.supported_features
