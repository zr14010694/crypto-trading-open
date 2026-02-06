from core.adapters.exchanges.adapters.standx_websocket import StandXWebSocket
from core.adapters.exchanges.interface import ExchangeConfig
from core.adapters.exchanges.models import ExchangeType


def _make_config():
    return ExchangeConfig(
        exchange_id="standx",
        name="StandX",
        exchange_type=ExchangeType.PERPETUAL,
        api_key="",
        api_secret="",
    )


def test_parse_order_response_payload():
    ws = StandXWebSocket(_make_config())
    msg = {
        "code": 0,
        "message": "success",
        "request_id": "abc",
        "data": {
            "id": 1,
            "symbol": "BTC-USD",
            "side": "buy",
            "order_type": "limit",
            "qty": "0.1",
            "fill_qty": "0",
            "price": "100",
            "status": "open",
            "created_at": "2025-01-01T00:00:00Z",
        },
    }
    order = ws._parse_order_response(msg)
    assert order.id == "1"
