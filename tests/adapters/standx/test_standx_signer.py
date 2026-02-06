import base64

import pytest
from base58 import b58encode
from nacl.signing import SigningKey

from core.adapters.exchanges.adapters.standx_signer import (
    build_signature_headers,
    build_signing_message,
)


@pytest.mark.parametrize("use_prefix", [True, False])
def test_signature_with_hex_private_key(use_prefix):
    signing_key = SigningKey.generate()
    private_key_hex = signing_key.encode().hex()
    if use_prefix:
        private_key_hex = f"0x{private_key_hex}"

    request_id = "req-123"
    timestamp = 1700000000000
    payload = "{\"symbol\":\"BTC-USD\"}"

    headers = build_signature_headers(
        payload=payload,
        request_id=request_id,
        timestamp=timestamp,
        private_key=private_key_hex,
    )

    signature = base64.b64decode(headers["x-request-signature"])
    message = build_signing_message("v1", request_id, timestamp, payload)
    signing_key.verify_key.verify(message.encode("utf-8"), signature)


def test_signature_with_base58_private_key():
    signing_key = SigningKey.generate()
    private_key_b58 = b58encode(signing_key.encode()).decode("utf-8")

    request_id = "req-456"
    timestamp = 1700000001000
    payload = "{\"symbol\":\"ETH-USD\"}"

    headers = build_signature_headers(
        payload=payload,
        request_id=request_id,
        timestamp=timestamp,
        private_key=private_key_b58,
    )

    signature = base64.b64decode(headers["x-request-signature"])
    message = build_signing_message("v1", request_id, timestamp, payload)
    signing_key.verify_key.verify(message.encode("utf-8"), signature)


def test_signature_headers_shape():
    signing_key = SigningKey.generate()
    private_key_hex = signing_key.encode().hex()
    payload = "{\"symbol\":\"SOL-USD\"}"

    headers = build_signature_headers(
        payload=payload,
        request_id="req-789",
        timestamp=1700000002000,
        private_key=private_key_hex,
    )

    assert headers["x-request-sign-version"] == "v1"
    assert headers["x-request-id"] == "req-789"
    assert headers["x-request-timestamp"] == "1700000002000"
    assert headers["x-request-signature"]
