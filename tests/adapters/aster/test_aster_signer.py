"""Aster Signer 模块单元测试"""

from eth_account import Account

from core.adapters.exchanges.adapters.aster_signer import (
    trim_dict,
    sign_request,
    build_signed_params,
)

# 测试用 Ethereum 私钥和地址
TEST_PRIVATE_KEY = "0x4c0883a69102937d6231471b5dbb6204fe512961708279f45ff2c3a1dd3a5e3a"
TEST_SIGNER = Account.from_key(TEST_PRIVATE_KEY).address
TEST_USER = "0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e"


def test_trim_dict_converts_int():
    d = {"a": 123, "b": 45.6}
    result = trim_dict(d)
    assert result["a"] == "123"
    assert result["b"] == "45.6"


def test_trim_dict_converts_bool():
    d = {"flag": True, "other": False}
    result = trim_dict(d)
    assert result["flag"] == "True"
    assert result["other"] == "False"


def test_trim_dict_converts_nested_dict():
    d = {"nested": {"x": 1}}
    result = trim_dict(d)
    assert isinstance(result["nested"], str)
    assert '"x": "1"' in result["nested"]


def test_trim_dict_converts_list():
    d = {"items": [1, 2, 3]}
    result = trim_dict(d)
    assert isinstance(result["items"], str)


def test_sign_request_produces_valid_signature():
    params = {"symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT"}
    nonce = 1700000000000000

    signature = sign_request(params, TEST_USER, TEST_SIGNER, TEST_PRIVATE_KEY, nonce)

    assert signature.startswith("0x")
    assert len(signature) > 10


def test_sign_request_recovers_signer():
    """验证签名后可以通过 ecrecover 恢复出 signer 地址"""
    import json
    from eth_abi import encode
    from eth_account.messages import encode_defunct
    from web3 import Web3

    params = {"symbol": "BTCUSDT", "side": "BUY"}
    nonce = 1700000000000000

    signature = sign_request(params, TEST_USER, TEST_SIGNER, TEST_PRIVATE_KEY, nonce)

    # 重建签名消息以验证
    trimmed = trim_dict(dict(params))
    json_str = json.dumps(trimmed, sort_keys=True).replace(" ", "").replace("'", '\\"')
    encoded = encode(
        ["string", "address", "address", "uint256"],
        [json_str, TEST_USER, TEST_SIGNER, nonce],
    )
    keccak_hex = Web3.keccak(encoded).hex()
    signable_msg = encode_defunct(hexstr=keccak_hex)

    recovered = Account.recover_message(signable_msg, signature=bytes.fromhex(signature[2:]))
    assert recovered.lower() == TEST_SIGNER.lower()


def test_build_signed_params_includes_all_required_fields():
    params = {"symbol": "BTCUSDT", "side": "BUY"}
    result = build_signed_params(params, TEST_USER, TEST_SIGNER, TEST_PRIVATE_KEY)

    assert "nonce" in result
    assert "user" in result
    assert "signer" in result
    assert "signature" in result
    assert "timestamp" in result
    assert "recvWindow" in result

    assert result["user"] == TEST_USER
    assert result["signer"] == TEST_SIGNER
    assert result["signature"].startswith("0x")
    assert isinstance(result["nonce"], int)
    assert isinstance(result["timestamp"], int)


def test_build_signed_params_preserves_original():
    params = {"symbol": "ETHUSDT", "quantity": "10"}
    result = build_signed_params(params, TEST_USER, TEST_SIGNER, TEST_PRIVATE_KEY)

    assert result["symbol"] == "ETHUSDT"
    assert result["quantity"] == "10"
