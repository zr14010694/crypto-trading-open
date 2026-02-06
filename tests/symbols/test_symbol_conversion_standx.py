import pytest

from core.infrastructure.config_manager import ConfigManager
from core.services.symbol_manager.implementations.symbol_conversion_service import (
    SymbolConversionService,
)


@pytest.mark.asyncio
async def test_standx_exchange_to_standard_direct_mappings():
    service = SymbolConversionService(ConfigManager())
    assert (
        await service.convert_from_exchange_format("BTC-USD", "standx")
        == "BTC-USDC-PERP"
    )
    assert (
        await service.convert_from_exchange_format("ETH-USD", "standx")
        == "ETH-USDC-PERP"
    )
    assert (
        await service.convert_from_exchange_format("SOL-USD", "standx")
        == "SOL-USDC-PERP"
    )


@pytest.mark.asyncio
async def test_standx_standard_to_exchange_direct_mappings():
    service = SymbolConversionService(ConfigManager())
    assert (
        await service.convert_to_exchange_format("BTC-USDC-PERP", "standx")
        == "BTC-USD"
    )
    assert (
        await service.convert_to_exchange_format("ETH-USDC-PERP", "standx")
        == "ETH-USD"
    )
    assert (
        await service.convert_to_exchange_format("SOL-USDC-PERP", "standx")
        == "SOL-USD"
    )


@pytest.mark.asyncio
async def test_standx_format_fallback_for_unmapped_symbol():
    service = SymbolConversionService(ConfigManager())
    assert (
        await service.convert_to_exchange_format("XRP-USDC-PERP", "standx")
        == "XRP-USD"
    )
    assert (
        await service.convert_from_exchange_format("XRP-USD", "standx")
        == "XRP-USDC-PERP"
    )
