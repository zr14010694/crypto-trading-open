"""
StandX 请求签名工具

用于生成 x-request-* 头部所需的 Ed25519 签名。
"""

from __future__ import annotations

import base64
import binascii
from typing import Dict

from base58 import b58decode
from nacl.signing import SigningKey


def build_signing_message(version: str, request_id: str, timestamp: int, payload: str) -> str:
    """构建 StandX 签名消息串"""
    return f"{version},{request_id},{timestamp},{payload}"


def _decode_private_key(private_key: str) -> bytes:
    key = private_key.strip()
    if key.startswith("0x"):
        key = key[2:]

    # 优先解析 hex
    is_hex = all(c in "0123456789abcdefABCDEF" for c in key) and len(key) % 2 == 0
    if is_hex:
        try:
            key_bytes = binascii.unhexlify(key)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("Invalid hex private key") from exc
    else:
        try:
            key_bytes = b58decode(key)
        except Exception as exc:
            raise ValueError("Invalid base58 private key") from exc

    # 兼容 64 字节私钥（取前 32 字节作为 seed）
    if len(key_bytes) == 64:
        key_bytes = key_bytes[:32]

    if len(key_bytes) != 32:
        raise ValueError("Invalid private key length, expected 32 bytes")

    return key_bytes


def sign_payload(
    payload: str,
    request_id: str,
    timestamp: int,
    private_key: str,
    version: str = "v1",
) -> str:
    """对 payload 进行签名并返回 base64 签名字符串"""
    key_bytes = _decode_private_key(private_key)
    signer = SigningKey(key_bytes)
    message = build_signing_message(version, request_id, timestamp, payload)
    signature = signer.sign(message.encode("utf-8")).signature
    return base64.b64encode(signature).decode("utf-8")


def build_signature_headers(
    payload: str,
    request_id: str,
    timestamp: int,
    private_key: str,
    version: str = "v1",
) -> Dict[str, str]:
    """构建 StandX 签名头"""
    signature = sign_payload(
        payload=payload,
        request_id=request_id,
        timestamp=timestamp,
        private_key=private_key,
        version=version,
    )
    return {
        "x-request-sign-version": version,
        "x-request-id": request_id,
        "x-request-timestamp": str(timestamp),
        "x-request-signature": signature,
    }
