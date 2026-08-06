from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import mimetypes
import os
import posixpath
from pathlib import Path, PurePosixPath
from time import monotonic
from typing import Any
from urllib.parse import urlencode, urlparse, urlunparse
from uuid import uuid4

import httpx
import websockets
from websockets.exceptions import ConnectionClosed

from .auth_state import AuthState
from .client import JumpServerClient
from .config import Settings
from .errors import JumpServerMCPError
from .koko import KOKO_SUBPROTOCOL, build_cookie_header

LOGGER = logging.getLogger(__name__)
KOKO_ELFINDER_WS_PATH = "/koko/ws/elfinder/"
KOKO_ELFINDER_CONNECTOR_PREFIX = "/koko/elfinder/connector"


class SFTPError(JumpServerMCPError):
    """Raised when a managed KoKo SFTP operation cannot complete safely."""


def normalize_remote_path(path: str) -> str:
    raw = path.strip()
    if not raw or not raw.startswith("/"):
        raise SFTPError("Remote SFTP paths must be absolute and start with '/'.")
    if "\x00" in raw:
        raise SFTPError("Remote SFTP paths must not contain NUL bytes.")
    if ".." in PurePosixPath(raw).parts:
        raise SFTPError("Remote SFTP paths must not contain '..' components.")
    normalized = posixpath.normpath(raw)
    return normalized if normalized.startswith("/") else f"/{normalized}"


def build_koko_sftp_ws_url(*, base_url: str, asset_id: str, token_id: str) -> str:
    parsed = urlparse(base_url)
    query = urlencode(
        {
            "type": "elfinder",
            "target_id": asset_id,
            "token": token_id,
        }
    )
    return urlunparse(
        (
            "wss" if parsed.scheme == "https" else "ws",
            parsed.netloc,
            KOKO_ELFINDER_WS_PATH,
            "",
            query,
            "",
        )
    )


def normalize_entry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": entry.get("name"),
        "hash": entry.get("hash"),
        "parent_hash": entry.get("phash") or None,
        "mime": entry.get("mime"),
        "size": entry.get("size"),
        "modified_epoch": entry.get("ts"),
        "readable": bool(entry.get("read")),
        "writable": bool(entry.get("write")),
        "is_directory": entry.get("mime") == "directory",
    }


class KoKoSFTPSession:
    def __init__(
        self,
        settings: Settings,
        auth_state: AuthState,
        *,
        asset_id: str,
        account: str,
    ) -> None:
        self._settings = settings
        self._auth_state = auth_state
        self._asset_id = asset_id
        self._account = account
        self._api_client = JumpServerClient(settings, auth_state)
        self._token_id = ""
        self._sid = ""
        self._ws: Any = None
        self._http: httpx.AsyncClient | None = None
        self._root_entry: dict[str, Any] = {}
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._closed = False
        self._close_reason = ""
        self._cleanup_state = "not_attempted"
        self._cleanup_error = ""

    @property
    def token_id(self) -> str:
        return self._token_id

    @property
    def sid(self) -> str:
        return self._sid

    @property
    def root_entry(self) -> dict[str, Any]:
        return normalize_entry(self._root_entry) if self._root_entry else {}

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def close_reason(self) -> str:
        return self._close_reason

    @property
    def cleanup_state(self) -> str:
        return self._cleanup_state

    @property
    def cleanup_error(self) -> str:
        return self._cleanup_error

    async def open(self) -> None:
        if not self._auth_state.has_cookie_auth():
            raise SFTPError(
                "KoKo SFTP access requires a cookie-backed web session. "
                "Run login again first."
            )
        token_payload = await self._api_client.create_connection_token(
            asset_id=self._asset_id,
            account=self._account,
            protocol="sftp",
            connect_method="web_sftp",
            is_reusable=False,
        )
        self._token_id = str(token_payload.get("id", ""))
        if not self._token_id:
            raise SFTPError("JumpServer did not return an SFTP connection-token ID.")

        try:
            self._ws = await websockets.connect(
                build_koko_sftp_ws_url(
                    base_url=self._auth_state.base_url or self._settings.base_url,
                    asset_id=self._asset_id,
                    token_id=self._token_id,
                ),
                origin=self._auth_state.base_url or self._settings.base_url,
                subprotocols=[KOKO_SUBPROTOCOL],
                additional_headers={"Cookie": build_cookie_header(self._auth_state)},
                open_timeout=self._settings.request_timeout_seconds,
                close_timeout=5,
                proxy=True if self._settings.use_environment_proxy else None,
            )
            await self._await_connect()
            self._http = httpx.AsyncClient(
                base_url=self._auth_state.base_url or self._settings.base_url,
                headers={
                    "Accept": "application/json",
                    "Cookie": build_cookie_header(self._auth_state),
                    "JMS-KoKo-Request-ID": self._sid,
                },
                timeout=self._settings.request_timeout_seconds,
                verify=self._settings.verify_tls,
                trust_env=self._settings.use_environment_proxy,
            )
            root_payload = await self._request_json(
                "open",
                init="1",
                tree="1",
            )
            root_entry = root_payload.get("cwd")
            if not isinstance(root_entry, dict) or not root_entry.get("hash"):
                raise SFTPError("KoKo elFinder did not return a valid root directory.")
            self._root_entry = root_entry
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            self._reader_task = asyncio.create_task(self._reader_loop())
        except BaseException:
            await self.close(close_reason="open_failed")
            raise

    async def _await_connect(self) -> None:
        deadline = monotonic() + self._settings.request_timeout_seconds
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise SFTPError(
                    "Timed out waiting for the KoKo elFinder CONNECT message."
                )
            message = await asyncio.wait_for(self._ws.recv(), timeout=remaining)
            if not isinstance(message, str):
                continue
            try:
                payload = json.loads(message)
            except json.JSONDecodeError:
                continue
            message_type = payload.get("type")
            if message_type == "CONNECT":
                self._sid = str(payload.get("id", ""))
                if not self._sid:
                    raise SFTPError(
                        "KoKo elFinder CONNECT did not include a session ID."
                    )
                return
            if message_type in {"ERROR", "TERMINAL_ERROR", "CLOSE"}:
                raise SFTPError(
                    "KoKo elFinder connection failed: "
                    f"{payload.get('err') or message_type}"
                )

    async def _heartbeat_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(25)
            if self._closed or self._ws is None:
                return
            try:
                await self._ws.send(
                    json.dumps(
                        {"id": self._sid, "type": "PING", "data": ""},
                        separators=(",", ":"),
                    )
                )
            except Exception as exc:  # noqa: BLE001 - transport implementations vary
                self._mark_closed(f"heartbeat_failed: {type(exc).__name__}")
                return

    async def _reader_loop(self) -> None:
        try:
            while not self._closed:
                message = await self._ws.recv()
                if not isinstance(message, str):
                    continue
                with contextlib.suppress(json.JSONDecodeError):
                    payload = json.loads(message)
                    if payload.get("type") in {"CLOSE", "ERROR", "TERMINAL_ERROR"}:
                        self._mark_closed(
                            str(
                                payload.get("err")
                                or payload.get("type")
                                or "remote_closed"
                            )
                        )
                        return
        except asyncio.CancelledError:
            raise
        except ConnectionClosed as exc:
            self._mark_closed(f"{type(exc).__name__}: {exc}")
        except Exception as exc:  # noqa: BLE001 - transport implementations vary
            self._mark_closed(f"{type(exc).__name__}: {exc}")

    def _mark_closed(self, reason: str) -> None:
        self._closed = True
        self._close_reason = reason

    def _ensure_open(self) -> httpx.AsyncClient:
        if self._closed or self._http is None or not self._sid:
            detail = f" ({self._close_reason})" if self._close_reason else ""
            raise SFTPError(f"The KoKo SFTP session is closed{detail}.")
        return self._http

    async def _request_json(self, command: str, **params: Any) -> dict[str, Any]:
        client = self._ensure_open()
        response = await client.get(
            f"{KOKO_ELFINDER_CONNECTOR_PREFIX}/{self._asset_id}/",
            params={"sid": self._sid, "cmd": command, **params},
        )
        if response.status_code >= 400:
            raise SFTPError(
                f"KoKo elFinder command {command!r} failed with HTTP "
                f"{response.status_code}: "
                f"{response.text.strip() or 'empty response'}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise SFTPError(
                f"KoKo elFinder command {command!r} returned a non-JSON response."
            ) from exc
        if not isinstance(payload, dict):
            raise SFTPError(
                f"KoKo elFinder command {command!r} returned a non-object payload."
            )
        if payload.get("error"):
            raise SFTPError(
                f"KoKo elFinder command {command!r} failed: {payload['error']}"
            )
        return payload

    async def _open_directory(
        self, entry: dict[str, Any]
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if entry.get("mime") != "directory":
            raise SFTPError(f"Remote path {entry.get('name')!r} is not a directory.")
        payload = await self._request_json("open", target=str(entry["hash"]))
        cwd = payload.get("cwd")
        files = payload.get("files")
        if not isinstance(cwd, dict) or not isinstance(files, list):
            raise SFTPError("KoKo elFinder returned an invalid directory listing.")
        children = [
            item
            for item in files
            if isinstance(item, dict) and item.get("hash") != cwd.get("hash")
        ]
        return cwd, children

    async def _resolve_entry(self, path: str) -> dict[str, Any]:
        normalized = normalize_remote_path(path)
        current = self._root_entry
        if normalized == "/":
            return current
        for component in PurePosixPath(normalized).parts[1:]:
            _, children = await self._open_directory(current)
            match = next(
                (item for item in children if item.get("name") == component), None
            )
            if match is None:
                raise SFTPError(f"Remote path does not exist: {normalized}")
            current = match
        return current

    async def _find_child(
        self,
        parent: dict[str, Any],
        name: str,
    ) -> dict[str, Any] | None:
        _, children = await self._open_directory(parent)
        return next((item for item in children if item.get("name") == name), None)

    async def stat(self, path: str) -> dict[str, Any]:
        normalized = normalize_remote_path(path)
        return {
            "path": normalized,
            "entry": normalize_entry(await self._resolve_entry(normalized)),
        }

    async def list_directory(self, path: str) -> dict[str, Any]:
        normalized = normalize_remote_path(path)
        cwd, children = await self._open_directory(
            await self._resolve_entry(normalized)
        )
        return {
            "path": normalized,
            "cwd": normalize_entry(cwd),
            "count": len(children),
            "entries": [normalize_entry(item) for item in children],
        }

    async def mkdir(self, path: str, *, parents: bool = False) -> dict[str, Any]:
        normalized = normalize_remote_path(path)
        if normalized == "/":
            raise SFTPError("The remote root directory already exists.")
        created: list[dict[str, Any]] = []
        current = self._root_entry
        components = PurePosixPath(normalized).parts[1:]
        for index, component in enumerate(components):
            existing = await self._find_child(current, component)
            if existing is not None:
                if existing.get("mime") != "directory":
                    raise SFTPError(
                        f"A non-directory entry already exists at {normalized!r}."
                    )
                if index == len(components) - 1 and not parents:
                    raise SFTPError(f"Remote directory already exists: {normalized}")
                current = existing
                continue
            if index != len(components) - 1 and not parents:
                raise SFTPError(
                    f"Remote parent directory does not exist for {normalized!r}. "
                    "Use parents=true."
                )
            payload = await self._request_json(
                "mkdir",
                target=str(current["hash"]),
                name=component,
            )
            added = payload.get("added")
            if (
                not isinstance(added, list)
                or not added
                or not isinstance(added[0], dict)
            ):
                raise SFTPError("KoKo elFinder did not return the created directory.")
            current = added[0]
            created.append(normalize_entry(current))
        return {
            "path": normalized,
            "created": bool(created),
            "created_entries": created,
            "entry": normalize_entry(current),
        }

    async def upload(
        self,
        *,
        local_path: str,
        remote_path: str,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        source = Path(local_path).expanduser()
        if not source.is_file():
            raise SFTPError(f"Local upload source is not a regular file: {source}")
        normalized = normalize_remote_path(remote_path)
        if normalized == "/":
            raise SFTPError("A remote file path is required for upload.")
        parent_path = posixpath.dirname(normalized) or "/"
        filename = posixpath.basename(normalized)
        parent = await self._resolve_entry(parent_path)
        existing = await self._find_child(parent, filename)
        if existing is not None and not overwrite:
            raise SFTPError(
                f"Remote path already exists: {normalized}. "
                "Set overwrite=true to replace it."
            )
        if existing is not None and existing.get("mime") == "directory":
            raise SFTPError(
                f"Cannot overwrite a remote directory with a file: {normalized}"
            )

        client = self._ensure_open()
        media_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        size = source.stat().st_size
        LOGGER.info("Uploading %d bytes to remote SFTP path %s.", size, normalized)
        with source.open("rb") as stream:
            response = await client.post(
                f"{KOKO_ELFINDER_CONNECTOR_PREFIX}/{self._asset_id}/",
                data={"sid": self._sid, "cmd": "upload", "target": str(parent["hash"])},
                files={"upload[]": (filename, stream, media_type)},
            )
        if response.status_code >= 400:
            raise SFTPError(
                f"KoKo elFinder upload failed with HTTP {response.status_code}: "
                f"{response.text.strip() or 'empty response'}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise SFTPError(
                "KoKo elFinder upload returned a non-JSON response."
            ) from exc
        if not isinstance(payload, dict) or payload.get("error"):
            raise SFTPError(f"KoKo elFinder upload failed: {payload}")
        added = payload.get("added")
        entry = added[0] if isinstance(added, list) and added else None
        LOGGER.info("Uploaded local file %s to %s.", source, normalized)
        return {
            "uploaded": True,
            "overwritten": existing is not None,
            "local_path": str(source),
            "remote_path": normalized,
            "size_bytes": size,
            "entry": normalize_entry(entry) if isinstance(entry, dict) else None,
        }

    async def download(
        self,
        *,
        remote_path: str,
        local_path: str,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        normalized = normalize_remote_path(remote_path)
        entry = await self._resolve_entry(normalized)
        if entry.get("mime") == "directory":
            raise SFTPError(f"Cannot download a directory as a file: {normalized}")
        destination = Path(local_path).expanduser()
        destination_existed = destination.exists()
        if destination_existed and not overwrite:
            raise SFTPError(
                f"Local path already exists: {destination}. "
                "Set overwrite=true to replace it."
            )
        if not destination.parent.is_dir():
            raise SFTPError(
                f"Local destination directory does not exist: {destination.parent}"
            )
        temp_path = destination.with_name(
            f".{destination.name}.mcp-jms-{uuid4().hex}.tmp"
        )
        client = self._ensure_open()
        size = 0
        LOGGER.info("Downloading remote SFTP path %s to %s.", normalized, destination)
        try:
            async with client.stream(
                "GET",
                f"{KOKO_ELFINDER_CONNECTOR_PREFIX}/{self._asset_id}/",
                params={
                    "sid": self._sid,
                    "cmd": "file",
                    "target": str(entry["hash"]),
                    "download": "1",
                },
            ) as response:
                if response.status_code >= 400:
                    detail = (await response.aread()).decode("utf-8", "replace").strip()
                    raise SFTPError(
                        "KoKo elFinder download failed with HTTP "
                        f"{response.status_code}: "
                        f"{detail or 'empty response'}"
                    )
                with temp_path.open("xb") as stream:
                    async for chunk in response.aiter_bytes():
                        stream.write(chunk)
                        size += len(chunk)
            os.replace(temp_path, destination)
        except BaseException:
            temp_path.unlink(missing_ok=True)
            raise
        return {
            "downloaded": True,
            "overwritten": destination_existed,
            "remote_path": normalized,
            "local_path": str(destination),
            "size_bytes": size,
        }

    async def rename(self, path: str, *, new_name: str) -> dict[str, Any]:
        normalized = normalize_remote_path(path)
        clean_name = new_name.strip()
        if not clean_name or clean_name in {".", ".."} or "/" in clean_name:
            raise SFTPError("The new SFTP name must be one path component.")
        if normalized == "/":
            raise SFTPError("The remote root directory cannot be renamed.")
        entry = await self._resolve_entry(normalized)
        parent_path = posixpath.dirname(normalized) or "/"
        parent = await self._resolve_entry(parent_path)
        if await self._find_child(parent, clean_name) is not None:
            destination = posixpath.join(parent_path, clean_name)
            raise SFTPError(f"Rename destination already exists: {destination}")
        payload = await self._request_json(
            "rename",
            target=str(entry["hash"]),
            name=clean_name,
        )
        added = payload.get("added")
        renamed = added[0] if isinstance(added, list) and added else None
        return {
            "renamed": True,
            "old_path": normalized,
            "new_path": posixpath.join(parent_path, clean_name),
            "entry": normalize_entry(renamed) if isinstance(renamed, dict) else None,
        }

    async def delete(
        self,
        path: str,
        *,
        confirm: bool = False,
        recursive: bool = False,
    ) -> dict[str, Any]:
        normalized = normalize_remote_path(path)
        if normalized == "/":
            raise SFTPError("The remote root directory cannot be deleted.")
        if not confirm:
            raise SFTPError(
                "Remote deletion requires confirm=true after the user approves "
                "the exact path."
            )
        entry = await self._resolve_entry(normalized)
        is_directory = entry.get("mime") == "directory"
        if is_directory and not recursive:
            raise SFTPError(
                "KoKo recursively deletes directories. Set recursive=true only after "
                "the user "
                "explicitly approves recursive deletion of this exact path."
            )
        payload = await self._request_json("rm", **{"targets[]": [str(entry["hash"])]})
        removed = payload.get("removed")
        return {
            "deleted": isinstance(removed, list) and str(entry["hash"]) in removed,
            "remote_path": normalized,
            "recursive": is_directory,
        }

    async def close(self, *, close_reason: str = "explicit") -> None:
        if self._closed and self._cleanup_state != "not_attempted":
            return
        self._mark_closed(self._close_reason or close_reason)
        current_task = asyncio.current_task()
        for task in (self._heartbeat_task, self._reader_task):
            if task is not None and task is not current_task and not task.done():
                task.cancel()
        for task in (self._heartbeat_task, self._reader_task):
            if task is not None and task is not current_task:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        close_errors: list[str] = []
        if self._http is not None:
            try:
                await self._http.aclose()
            except Exception as exc:  # noqa: BLE001 - token cleanup must still run
                close_errors.append(f"http_close: {type(exc).__name__}: {exc}")
                LOGGER.warning("Could not close SFTP HTTP transport: %s", type(exc).__name__)
            finally:
                self._http = None
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception as exc:  # noqa: BLE001 - token cleanup must still run
                close_errors.append(f"websocket_close: {type(exc).__name__}: {exc}")
                LOGGER.warning(
                    "Could not close SFTP websocket transport: %s", type(exc).__name__
                )
            finally:
                self._ws = None
        if not self._token_id:
            self._cleanup_state = "not_required"
            self._cleanup_error = "; ".join(close_errors)
            return
        try:
            await self._api_client.expire_connection_token(self._token_id)
        except Exception as exc:  # noqa: BLE001 - cleanup must not mask the caller result
            self._cleanup_state = "failed"
            close_errors.append(f"token_expiry: {type(exc).__name__}: {exc}")
            LOGGER.warning(
                "Could not expire SFTP connection token: %s", type(exc).__name__
            )
        else:
            self._cleanup_state = "expired"
        self._cleanup_error = "; ".join(close_errors)
