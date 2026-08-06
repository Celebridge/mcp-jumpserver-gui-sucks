from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

from mcp_jumpserver_gui_sucks import sftp_manager
from mcp_jumpserver_gui_sucks.auth_state import AuthState, CookieState
from mcp_jumpserver_gui_sucks.config import Settings
from mcp_jumpserver_gui_sucks.sftp import (
    KoKoSFTPSession,
    SFTPError,
    build_koko_sftp_ws_url,
    normalize_remote_path,
)
from mcp_jumpserver_gui_sucks.sftp_manager import ManagedSFTPSession


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
        sftp_idle_timeout_seconds=3600.0,
        sftp_reap_interval_seconds=30.0,
        max_sftp_sessions=4,
    )


def make_auth_state() -> AuthState:
    return AuthState(
        base_url="https://jumpserver.example.com",
        cookies=[
            CookieState(
                name="jms_sessionid",
                value="session-value",
                domain="jumpserver.example.com",
                path="/",
            )
        ],
    )


def make_session(tmp_path: Path) -> KoKoSFTPSession:
    return KoKoSFTPSession(
        make_settings(tmp_path),
        make_auth_state(),
        asset_id="asset-id",
        account="account-id",
    )


def test_remote_paths_must_be_absolute_and_cannot_traverse() -> None:
    assert normalize_remote_path("/tmp//project/./file.txt") == "/tmp/project/file.txt"

    with pytest.raises(SFTPError, match="absolute"):
        normalize_remote_path("tmp/file.txt")
    with pytest.raises(SFTPError, match="'..'"):
        normalize_remote_path("/tmp/../etc/passwd")


def test_sftp_websocket_url_uses_asset_and_connection_token() -> None:
    parsed = urlparse(
        build_koko_sftp_ws_url(
            base_url="https://jumpserver.example.com",
            asset_id="asset-id",
            token_id="token-id",
        )
    )

    assert parsed.scheme == "wss"
    assert parsed.path == "/koko/ws/elfinder/"
    assert parse_qs(parsed.query) == {
        "type": ["elfinder"],
        "target_id": ["asset-id"],
        "token": ["token-id"],
    }


def test_remote_delete_requires_confirmation_and_recursive_opt_in(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        session = make_session(tmp_path)
        requests: list[tuple[str, dict[str, Any]]] = []

        async def resolve_entry(path: str) -> dict[str, Any]:
            return {"name": "project", "hash": "directory-hash", "mime": "directory"}

        async def request_json(command: str, **params: Any) -> dict[str, Any]:
            requests.append((command, params))
            return {"removed": ["directory-hash"]}

        session._resolve_entry = resolve_entry  # type: ignore[method-assign]
        session._request_json = request_json  # type: ignore[method-assign]

        with pytest.raises(SFTPError, match="confirm=true"):
            await session.delete("/tmp/project")
        with pytest.raises(SFTPError, match="recursive=true"):
            await session.delete("/tmp/project", confirm=True)

        result = await session.delete("/tmp/project", confirm=True, recursive=True)
        assert result["deleted"] is True
        assert requests == [("rm", {"targets[]": ["directory-hash"]})]

    asyncio.run(scenario())


def test_upload_refuses_implicit_remote_overwrite(tmp_path: Path) -> None:
    async def scenario() -> None:
        source = tmp_path / "artifact.txt"
        source.write_text("content", encoding="utf-8")
        session = make_session(tmp_path)

        async def resolve_entry(path: str) -> dict[str, Any]:
            return {"name": "tmp", "hash": "parent-hash", "mime": "directory"}

        async def find_child(parent: dict[str, Any], name: str) -> dict[str, Any]:
            return {"name": name, "hash": "existing-hash", "mime": "file"}

        session._resolve_entry = resolve_entry  # type: ignore[method-assign]
        session._find_child = find_child  # type: ignore[method-assign]

        with pytest.raises(SFTPError, match="overwrite=true"):
            await session.upload(
                local_path=str(source),
                remote_path="/tmp/artifact.txt",
            )

    asyncio.run(scenario())


def test_manager_reuses_one_sftp_session_per_target(
    monkeypatch: Any, tmp_path: Path
) -> None:
    async def scenario() -> None:
        created: list[FakeSFTP] = []

        class FakeSFTP:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self.sid = "sid"
                self.root_entry = {"name": "Home"}
                self.closed = False
                self.close_reason = ""
                self.cleanup_state = "not_attempted"
                self.cleanup_error = ""
                created.append(self)

            async def open(self) -> None:
                return None

            async def close(self, *, close_reason: str) -> None:
                self.closed = True
                self.close_reason = close_reason
                self.cleanup_state = "expired"

        monkeypatch.setattr(sftp_manager, "KoKoSFTPSession", FakeSFTP)
        manager = sftp_manager.SFTPSessionManager()
        settings = make_settings(tmp_path)

        first = await manager.open_session(
            settings,
            make_auth_state(),
            asset_id="asset-id",
            account="account-id",
        )
        second = await manager.open_session(
            settings,
            make_auth_state(),
            asset_id="asset-id",
            account="account-id",
        )

        assert first["opened"] is True
        assert second["reused_existing"] is True
        assert second["session_handle"] == first["session_handle"]
        assert len(created) == 1

        closed = await manager.close_all_sessions(close_reason="process_exit")
        assert closed["closed_count"] == 1
        assert created[0].closed is True

    asyncio.run(scenario())


def test_idle_reaper_skips_an_active_sftp_operation(tmp_path: Path) -> None:
    async def scenario() -> None:
        operation_started = asyncio.Event()
        operation_release = asyncio.Event()

        class BusySFTP:
            def __init__(self) -> None:
                self.sid = "sid"
                self.root_entry = {"name": "Home"}
                self.closed = False
                self.close_reason = ""
                self.cleanup_state = "not_attempted"
                self.cleanup_error = ""

            async def stat(self, path: str) -> dict[str, Any]:
                operation_started.set()
                await operation_release.wait()
                return {"path": path, "entry": {"name": "file.txt"}}

            async def close(self, *, close_reason: str) -> None:
                self.closed = True
                self.close_reason = close_reason
                self.cleanup_state = "expired"

        manager = sftp_manager.SFTPSessionManager()
        managed = ManagedSFTPSession(
            handle="session-1",
            sftp=BusySFTP(),  # type: ignore[arg-type]
            asset_id="asset-id",
            account="account-id",
            idle_timeout_seconds=0.0,
        )
        manager._sessions[managed.handle] = managed

        operation = asyncio.create_task(manager.stat(managed.handle, path="/file.txt"))
        await operation_started.wait()
        await manager._reap_idle_sessions()

        assert managed.handle in manager._sessions
        assert managed.active_operations == 1
        assert managed.sftp.closed is False

        operation_release.set()
        result = await operation
        assert result["path"] == "/file.txt"
        assert managed.active_operations == 0

        await manager._reap_idle_sessions()
        assert managed.handle not in manager._sessions
        assert managed.sftp.closed is True

    asyncio.run(scenario())


def test_close_all_sftp_sessions_continues_after_one_close_error() -> None:
    async def scenario() -> None:
        class ClosingSFTP:
            def __init__(self, *, fail: bool) -> None:
                self.sid = "sid"
                self.root_entry = {"name": "Home"}
                self.closed = False
                self.close_reason = ""
                self.cleanup_state = "not_attempted"
                self.cleanup_error = ""
                self.fail = fail
                self.close_calls = 0

            async def close(self, *, close_reason: str) -> None:
                self.close_calls += 1
                self.close_reason = close_reason
                if self.fail:
                    raise RuntimeError("close failed")
                self.closed = True
                self.cleanup_state = "expired"

        manager = sftp_manager.SFTPSessionManager()
        failing = ClosingSFTP(fail=True)
        healthy = ClosingSFTP(fail=False)
        manager._sessions = {
            "failing": ManagedSFTPSession(
                handle="failing",
                sftp=failing,  # type: ignore[arg-type]
                asset_id="asset-1",
                account="account-1",
                idle_timeout_seconds=3600.0,
            ),
            "healthy": ManagedSFTPSession(
                handle="healthy",
                sftp=healthy,  # type: ignore[arg-type]
                asset_id="asset-2",
                account="account-2",
                idle_timeout_seconds=3600.0,
            ),
        }

        result = await manager.close_all_sessions(close_reason="process_exit")

        assert result["closed_count"] == 2
        assert result["close_error_count"] == 1
        assert failing.close_calls == 1
        assert healthy.close_calls == 1
        assert healthy.closed is True
        assert manager._sessions == {}

    asyncio.run(scenario())


def test_sftp_close_expires_token_after_http_transport_error(tmp_path: Path) -> None:
    async def scenario() -> None:
        class FailingHTTP:
            async def aclose(self) -> None:
                raise RuntimeError("http close failed")

        class FakeWebSocket:
            async def close(self) -> None:
                return None

        expired_tokens: list[str] = []

        async def expire_connection_token(token_id: str) -> None:
            expired_tokens.append(token_id)

        session = make_session(tmp_path)
        session._token_id = "token-id"
        session._http = FailingHTTP()  # type: ignore[assignment]
        session._ws = FakeWebSocket()
        session._api_client.expire_connection_token = expire_connection_token  # type: ignore[method-assign]

        await session.close(close_reason="test")

        assert expired_tokens == ["token-id"]
        assert session.cleanup_state == "expired"
        assert "http_close" in session.cleanup_error

    asyncio.run(scenario())
