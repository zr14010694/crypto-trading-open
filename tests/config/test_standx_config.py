from pathlib import Path

import yaml

from core.utils.config_loader import ExchangeConfigLoader


def test_standx_config_file_exists_and_has_api_block():
    config_path = Path("config/exchanges/standx_config.yaml")
    assert config_path.exists()
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert "standx" in data
    assert "api" in data["standx"]


def test_standx_env_overrides_auth(monkeypatch):
    monkeypatch.setenv("STANDX_API_KEY", "test-key")
    monkeypatch.setenv("STANDX_JWT_TOKEN", "test-jwt")
    loader = ExchangeConfigLoader()
    auth = loader.load_auth_config(
        "standx",
        use_env=True,
        config_file="config/exchanges/standx_config.yaml",
    )
    assert auth.api_key == "test-key"
    assert auth.jwt_token == "test-jwt"
