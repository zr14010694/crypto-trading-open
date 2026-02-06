import base64
import json
import time
from decimal import Decimal

from base58 import b58encode
from nacl.signing import SigningKey

from core.adapters.exchanges.adapters.standx_rest import StandXRest
from core.adapters.exchanges.adapters.standx_signer import build_signing_message
from core.adapters.exchanges.interface import ExchangeConfig
from core.adapters.exchanges.models import ExchangeType, OrderSide, OrderStatus, OrderType, PositionSide


def _make_config(private_key: str, jwt_token: str):
    return ExchangeConfig(
        exchange_id="standx",
        name="StandX",
        exchange_type=ExchangeType.PERPETUAL,
        api_key="test-key",
        api_secret="",
        private_key=private_key,
        extra_params={"jwt_token": jwt_token},
    )


def test_signed_headers_and_payload(monkeypatch):
    signing_key = SigningKey.generate()
    private_key_b58 = b58encode(signing_key.encode()).decode("utf-8")
    rest = StandXRest(_make_config(private_key_b58, "jwt-token"))

    payload = rest._build_order_payload(
        symbol="BTC-USD",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        qty=Decimal("0.01"),
        price=Decimal("12000"),
        time_in_force="gtc",
        reduce_only=False,
    )

    fixed_time = 1700000000.123
    monkeypatch.setattr(time, "time", lambda: fixed_time)
    headers = rest._signed_headers(payload, request_id="req-fixed", session_id="sess")

    assert headers["Authorization"] == "Bearer jwt-token"
    assert headers["x-request-id"] == "req-fixed"
    assert headers["x-session-id"] == "sess"

    payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    message = build_signing_message("v1", "req-fixed", int(fixed_time * 1000), payload_json)
    signature = base64.b64decode(headers["x-request-signature"])
    signing_key.verify_key.verify(message.encode("utf-8"), signature)


def test_parse_order_response():
    rest = StandXRest(_make_config("0x" + "00" * 32, "jwt-token"))
    raw = {
        "id": 1820682,
        "cl_ord_id": "01K2BK4ZKQE0C308SRD39P8N9Z",
        "symbol": "BTC-USD",
        "side": "sell",
        "order_type": "limit",
        "qty": "0.060",
        "fill_qty": "0",
        "price": "121900.00",
        "fill_avg_price": "0",
        "status": "open",
        "created_at": "2025-08-11T03:35:25.559151Z",
        "updated_at": "2025-08-11T03:35:25.559151Z",
    }

    order = rest._parse_order(raw)
    assert order.id == "1820682"
    assert order.side == OrderSide.SELL
    assert order.status == OrderStatus.OPEN
    assert order.amount == Decimal("0.060")


def test_parse_positions_and_balances():
    rest = StandXRest(_make_config("0x" + "00" * 32, "jwt-token"))
    positions = rest._parse_positions(
        [
            {
                "symbol": "BTC-USD",
                "qty": "0.94",
                "entry_price": "121737.96",
                "mark_price": "121715.05",
                "upnl": "-21.53540",
                "realized_pnl": "31.61532",
                "leverage": "10",
                "margin_mode": "isolated",
                "holding_margin": "11443.3682400",
                "liq_price": "112373.50",
                "time": "2025-08-11T03:41:40.922818Z",
            }
        ]
    )

    assert positions[0].side == PositionSide.LONG
    assert positions[0].size == Decimal("0.94")

    balances = rest._parse_balances(
        {
            "token": "DUSD",
            "free": "100",
            "locked": "5",
            "total": "105",
        }
    )

    assert balances[0].currency == "DUSD"
    assert balances[0].free == Decimal("100")
    assert balances[0].used == Decimal("5")
