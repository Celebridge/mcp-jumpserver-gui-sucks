from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from time import monotonic
from typing import Any
from uuid import uuid4

from .auth_state import AuthState
from .config import Settings
from .errors import MissingAuthStateError
from .sftp import KoKoSFTPSession, SFTPError

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class ManagedSFTPSession:
    handle: str
    sftp: KoKoSFTPSession
    asset_id: str
    account: str
    idle_timeout_seconds: float
    created_at_monotonic: float = field(default_factory=monotonic, repr=False)
    last_activity_monotonic: float = field(default_factory=monotonic, repr=False)
    closed: bool = False
    close_reason: str = ""
    active_operations: int = 0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    def target_key(self) -> str:
        return f"{self.asset_id}:{self.account}:sftp:web_sftp"

    def touch(self) -> None:
        self.last_activity_monotonic = monotonic()

    def idle_expired(self, now: float) -> bool:
        return now - self.last_activity_monotonic >= self.idle_timeout_seconds

    def snapshot(self) -> dict[str, Any]:
        idle_seconds = max(0.0, monotonic() - self.last_activity_monotonic)
        return {
            "session_handle": self.handle,
            "target_key": self.target_key(),
            "asset_id": self.asset_id,
            "account": self.account,
            "protocol": "sftp",
            "connect_method": "web_sftp",
            "idle_timeout_seconds": self.idle_timeout_seconds,
            "idle_seconds": round(idle_seconds, 3),
            "expires_in_seconds": round(
                0.0
                if self.closed
                else max(0.0, self.idle_timeout_seconds - idle_seconds),
                3,
            ),
            "root": self.sftp.root_entry or None,
            "remote_session_id": self.sftp.sid or None,
            "active_operations": self.active_operations,
            "closed": self.closed or self.sftp.closed,
            "close_reason": self.close_reason or self.sftp.close_reason or None,
            "token_cleanup": self.sftp.cleanup_state,
            "token_cleanup_error": self.sftp.cleanup_error or None,
        }


class SFTPSessionManager:
    def __init__(self) -> None:
        self._sessions: dict[str, ManagedSFTPSession] = {}
        self._opening_targets: dict[str, asyncio.Event] = {}
        self._lock = asyncio.Lock()
        self._idle_timeout_seconds = 3600.0
        self._reap_interval_seconds = 30.0
        self._max_sessions = 4
        self._sweeper_task: asyncio.Task[None] | None = None

    async def prepare(self, settings: Settings) -> None:
        self._idle_timeout_seconds = settings.sftp_idle_timeout_seconds
        self._reap_interval_seconds = settings.sftp_reap_interval_seconds
        self._max_sessions = settings.max_sftp_sessions
        self._ensure_sweeper_running()
        await self._reap_idle_sessions()

    def _ensure_sweeper_running(self) -> None:
        if self._sweeper_task is None or self._sweeper_task.done():
            self._sweeper_task = asyncio.create_task(self._sweeper_loop())

    async def _sweeper_loop(self) -> None:
        while True:
            await asyncio.sleep(self._reap_interval_seconds)
            await self._reap_idle_sessions()

    async def _reap_idle_sessions(self) -> None:
        now = monotonic()
        detached: list[ManagedSFTPSession] = []
        async with self._lock:
            for handle, session in list(self._sessions.items()):
                if session.active_operations > 0:
                    continue
                if session.sftp.closed or session.idle_expired(now):
                    detached.append(self._sessions.pop(handle))
        for session in detached:
            await self._close_detached(session, close_reason="idle_timeout")

    async def list_sessions(self) -> dict[str, Any]:
        async with self._lock:
            results = [session.snapshot() for session in self._sessions.values()]
        return {
            "count": len(results),
            "opening_count": len(self._opening_targets),
            "max_sftp_sessions": self._max_sessions,
            "sftp_idle_timeout_seconds": self._idle_timeout_seconds,
            "sftp_reap_interval_seconds": self._reap_interval_seconds,
            "results": results,
        }

    async def open_session(
        self,
        settings: Settings,
        auth_state: AuthState | None,
        *,
        asset_id: str,
        account: str,
        reuse_existing: bool = True,
    ) -> dict[str, Any]:
        await self.prepare(settings)
        if auth_state is None:
            raise MissingAuthStateError(
                "Opening an SFTP session requires persisted auth state."
            )
        target_key = f"{asset_id}:{account}:sftp:web_sftp"
        opening_key = target_key
        opening_event: asyncio.Event | None = None
        while True:
            async with self._lock:
                if reuse_existing:
                    reusable = next(
                        (
                            item
                            for item in self._sessions.values()
                            if item.target_key() == target_key
                            and not item.closed
                            and not item.sftp.closed
                        ),
                        None,
                    )
                    if reusable is not None:
                        reusable.idle_timeout_seconds = self._idle_timeout_seconds
                        reusable.touch()
                        return {
                            "opened": False,
                            "reused_existing": True,
                            **reusable.snapshot(),
                        }
                    opening_event = self._opening_targets.get(target_key)
                    if opening_event is None:
                        opening_event = asyncio.Event()
                        self._opening_targets[target_key] = opening_event
                        break
                else:
                    opening_key = f"{target_key}:{uuid4().hex}"
                    opening_event = asyncio.Event()
                    self._opening_targets[opening_key] = opening_event
                    break
            await opening_event.wait()

        async with self._lock:
            if len(self._sessions) + len(self._opening_targets) > self._max_sessions:
                if self._opening_targets.pop(opening_key, None) is opening_event:
                    opening_event.set()
                raise SFTPError(
                    f"The managed SFTP session limit ({self._max_sessions}) "
                    "has been reached."
                )

        sftp = KoKoSFTPSession(
            settings,
            auth_state,
            asset_id=asset_id,
            account=account,
        )
        try:
            await sftp.open()
            managed = ManagedSFTPSession(
                handle=uuid4().hex,
                sftp=sftp,
                asset_id=asset_id,
                account=account,
                idle_timeout_seconds=self._idle_timeout_seconds,
            )
            async with self._lock:
                self._sessions[managed.handle] = managed
            LOGGER.info(
                "Opened managed KoKo SFTP session %s for asset %s.",
                managed.handle,
                asset_id,
            )
            return {
                "opened": True,
                "reused_existing": False,
                **managed.snapshot(),
            }
        except BaseException:
            with contextlib.suppress(Exception):
                await sftp.close(close_reason="open_failed")
            raise
        finally:
            async with self._lock:
                waiter = self._opening_targets.pop(opening_key, None)
                if waiter is not None:
                    waiter.set()

    async def _with_session(
        self,
        session_handle: str,
        operation: Callable[[KoKoSFTPSession], Awaitable[dict[str, Any]]],
    ) -> dict[str, Any]:
        async with self._lock:
            session = self._sessions.get(session_handle)
            if session is None:
                raise SFTPError(
                    "The managed SFTP session handle is unknown or expired. "
                    "Acquire a new session."
                )
            session.active_operations += 1
        try:
            async with session._lock:
                if session.closed or session.sftp.closed:
                    raise SFTPError(
                        "The managed SFTP session is closed. Acquire a new session."
                    )
                payload = await operation(session.sftp)
        finally:
            async with self._lock:
                session.active_operations = max(0, session.active_operations - 1)
                session.touch()
        return {**payload, **session.snapshot()}

    async def stat(self, session_handle: str, *, path: str) -> dict[str, Any]:
        return await self._with_session(session_handle, lambda sftp: sftp.stat(path))

    async def list_directory(self, session_handle: str, *, path: str) -> dict[str, Any]:
        return await self._with_session(
            session_handle, lambda sftp: sftp.list_directory(path)
        )

    async def mkdir(
        self,
        session_handle: str,
        *,
        path: str,
        parents: bool,
    ) -> dict[str, Any]:
        return await self._with_session(
            session_handle,
            lambda sftp: sftp.mkdir(path, parents=parents),
        )

    async def upload(
        self,
        session_handle: str,
        *,
        local_path: str,
        remote_path: str,
        overwrite: bool,
    ) -> dict[str, Any]:
        return await self._with_session(
            session_handle,
            lambda sftp: sftp.upload(
                local_path=local_path,
                remote_path=remote_path,
                overwrite=overwrite,
            ),
        )

    async def download(
        self,
        session_handle: str,
        *,
        remote_path: str,
        local_path: str,
        overwrite: bool,
    ) -> dict[str, Any]:
        return await self._with_session(
            session_handle,
            lambda sftp: sftp.download(
                remote_path=remote_path,
                local_path=local_path,
                overwrite=overwrite,
            ),
        )

    async def rename(
        self,
        session_handle: str,
        *,
        path: str,
        new_name: str,
    ) -> dict[str, Any]:
        return await self._with_session(
            session_handle,
            lambda sftp: sftp.rename(path, new_name=new_name),
        )

    async def delete(
        self,
        session_handle: str,
        *,
        path: str,
        confirm: bool,
        recursive: bool,
    ) -> dict[str, Any]:
        return await self._with_session(
            session_handle,
            lambda sftp: sftp.delete(path, confirm=confirm, recursive=recursive),
        )

    async def close_session(self, session_handle: str) -> dict[str, Any]:
        async with self._lock:
            session = self._sessions.pop(session_handle, None)
        if session is None:
            raise SFTPError(
                "The managed SFTP session handle is unknown or already closed."
            )
        return await self._close_detached(session, close_reason="explicit")

    async def _close_detached(
        self,
        session: ManagedSFTPSession,
        *,
        close_reason: str,
    ) -> dict[str, Any]:
        async with session._lock:
            if not session.closed:
                session.closed = True
                session.close_reason = close_reason
                try:
                    await session.sftp.close(close_reason=close_reason)
                except Exception as exc:  # noqa: BLE001 - continue closing other sessions
                    LOGGER.warning(
                        "Could not fully close managed SFTP session %s: %s",
                        session.handle,
                        type(exc).__name__,
                    )
                    return {
                        "closed": True,
                        "close_error": f"{type(exc).__name__}: {exc}",
                        **session.snapshot(),
                    }
            return {"closed": True, "close_error": None, **session.snapshot()}

    async def close_all_sessions(
        self, *, close_reason: str = "process_exit"
    ) -> dict[str, Any]:
        if self._sweeper_task is not None and not self._sweeper_task.done():
            self._sweeper_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sweeper_task
        async with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        results = await asyncio.gather(
            *(
                self._close_detached(session, close_reason=close_reason)
                for session in sessions
            )
        )
        return {
            "closed_count": len(results),
            "close_error_count": sum(bool(item.get("close_error")) for item in results),
            "results": results,
        }


_MANAGER = SFTPSessionManager()


def get_sftp_session_manager() -> SFTPSessionManager:
    return _MANAGER
