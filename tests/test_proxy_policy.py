from __future__ import annotations

from pathlib import Path
from typing import Any

from mcp_jumpserver_gui_sucks import cli_login
from mcp_jumpserver_gui_sucks.config import USE_ENV_PROXY_ENV_NAME, Settings


def make_settings(tmp_path: Path) -> Settings:
    return Settings(
        base_url="https://jumpserver.example.com",
        org_id="",
        state_dir=tmp_path,
        state_file=tmp_path / "auth-state.json",
        log_level="INFO",
        verify_tls=True,
        request_timeout_seconds=20.0,
        terminal_idle_timeout_seconds=3600.0,
        terminal_reap_interval_seconds=30.0,
        max_terminal_sessions=8,
    )


def test_cli_login_does_not_inherit_environment_proxy_by_default(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    captured: dict[str, Any] = {}

    class RecordingClient:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

        def close(self) -> None:
            return None

    monkeypatch.setattr(cli_login.httpx, "Client", RecordingClient)

    flow = cli_login.JumpServerCLILogin(
        make_settings(tmp_path),
        base_url="https://jumpserver.example.com",
    )
    flow.close()

    assert captured["trust_env"] is False


def test_cli_login_can_explicitly_inherit_environment_proxy(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    captured: dict[str, Any] = {}

    class RecordingClient:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

        def close(self) -> None:
            return None

    settings = make_settings(tmp_path)
    settings.use_environment_proxy = True
    monkeypatch.setattr(cli_login.httpx, "Client", RecordingClient)

    flow = cli_login.JumpServerCLILogin(
        settings,
        base_url="https://jumpserver.example.com",
    )
    flow.close()

    assert captured["trust_env"] is True


def test_environment_proxy_is_opt_in(monkeypatch: Any) -> None:
    monkeypatch.delenv(USE_ENV_PROXY_ENV_NAME, raising=False)
    assert Settings.from_env().use_environment_proxy is False

    monkeypatch.setenv(USE_ENV_PROXY_ENV_NAME, "true")
    assert Settings.from_env().use_environment_proxy is True
