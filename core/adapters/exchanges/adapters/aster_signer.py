"""
Aster Finance ECDSA/EIP-712 请求签名工具

使用 Web3 + eth-account 进行 ECDSA 签名，
参照 Aster V3 官方 Python Demo (tx.py)。
"""

from __future__ import annotations

import json
import math
import time
from typing import Any, Dict

from eth_abi import encode
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3


def trim_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    """递归将 dict 中所有值转为字符串（与 Aster API 要求一致）"""
    for key in d:
        value = d[key]
        if isinstance(value, list):
            new_value = []
            for item in value:
                if isinstance(item, dict):
                    new_value.append(json.dumps(trim_dict(item)))
                else:
                    new_value.append(str(item))
            d[key] = json.dumps(new_value)
            continue
        if isinstance(value, dict):
            d[key] = json.dumps(trim_dict(value))
            continue
        d[key] = str(value)
    return d


def sign_request(
    params: Dict[str, Any],
    user: str,
    signer: str,
    private_key: str,
    nonce: int,
) -> str:
    """
    对请求参数进行 ECDSA 签名

    流程：
    1. 参数值全部转字符串
    2. 按 key 排序生成紧凑 JSON
    3. ABI encode(json_str, user, signer, nonce)
    4. Keccak256 哈希
    5. ECDSA 签名

    返回: "0x" + 签名 hex 字符串
    """
    trimmed = trim_dict(dict(params))
    json_str = json.dumps(trimmed, sort_keys=True).replace(" ", "").replace("'", '\\"')
    encoded = encode(
        ["string", "address", "address", "uint256"],
        [json_str, user, signer, nonce],
    )
    keccak_hex = Web3.keccak(encoded).hex()
    signable_msg = encode_defunct(hexstr=keccak_hex)
    signed_message = Account.sign_message(
        signable_message=signable_msg, private_key=private_key
    )
    return "0x" + signed_message.signature.hex()


def build_signed_params(
    params: Dict[str, Any],
    user: str,
    signer: str,
    private_key: str,
    recv_window: int = 50000,
) -> Dict[str, Any]:
    """
    构建包含签名的完整请求参数

    自动注入 recvWindow, timestamp, nonce, user, signer, signature
    """
    params = dict(params)
    params["recvWindow"] = recv_window
    params["timestamp"] = int(round(time.time() * 1000))
    nonce = math.trunc(time.time() * 1000000)
    signature = sign_request(params, user, signer, private_key, nonce)
    params["nonce"] = nonce
    params["user"] = user
    params["signer"] = signer
    params["signature"] = signature
    return params
