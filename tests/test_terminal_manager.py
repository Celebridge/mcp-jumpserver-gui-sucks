import asyncio
import re
import unittest
from pathlib import Path
from unittest.mock import patch

from mcp_jumpserver_gui_sucks.auth_state import AuthState, CookieState
from mcp_jumpserver_gui_sucks.config import Settings
from mcp_jumpserver_gui_sucks.koko import KoKoProbeError, Transcript
from mcp_jumpserver_gui_sucks.terminal_manager import (
    ManagedTerminalSession,
    TerminalSessionManager,
)


class FakeTerminal:
    def __init__(self) -> None:
        self.terminal_id = "terminal-1"
        self.session_id = "remote-session-1"
        self.connect_info = {"asset": {"name": "asset-1"}, "platform": "linux"}
        self.cleanup_state = "expired"
        self.cleanup_error = ""
        self.close_reason = ""
        self.sent_data: list[str] = []
        self.closed = False
        self.close_calls = 0
        self.transcript_override: Transcript | None = None
        self._command_release = asyncio.Event()
        self._drain_call_count = 0

    def current_seq(self) -> int:
        return 0

    def buffered_output_bytes(self) -> int:
        return 128

    async def send_terminal_data(self, text: str) -> None:
        self.sent_data.append(text)

    async def resize(self, *, cols: int, rows: int) -> None:
        return None

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True
        self.cleanup_state = "expired"

    async def drain_until_idle(
        self,
        *,
        idle_timeout_seconds: float,
        total_timeout_seconds: float,
        after_seq: int | None = None,
    ) -> Transcript:
        self._drain_call_count += 1
        if self.transcript_override is not None:
            return self.transcript_override
        if self._drain_call_count == 1:
            if self.sent_data == ["\u0003"]:
                return Transcript(
                    text_chunks=["^C\n"],
                    control_types=[],
                    control_payloads=[],
                    binary_frame_count=1,
                    idle_timeout=True,
                    connection_closed=False,
                    from_seq=after_seq or 0,
                    next_seq=1,
                )
            await self._command_release.wait()
            script = self.sent_data[0]
            start_match = re.search(r"printf '(__MCP_JMS_COMMAND_START_[^']+)\\\\n'", script)
            end_match = re.search(r"printf '\\\\n(__MCP_JMS_EXIT_STATUS_[^:]+):%s\\\\n' \"\\$status\"", script)
            start_marker = start_match.group(1) if start_match else "__START__"
            end_marker = end_match.group(1) if end_match else "__END__"
            return Transcript(
                text_chunks=[f"{start_marker}\nhostname\n{end_marker}:0\n"],
                control_types=[],
                control_payloads=[],
                binary_frame_count=1,
                idle_timeout=False,
                connection_closed=False,
                from_seq=after_seq or 0,
                next_seq=2,
            )
        return Transcript(
            text_chunks=["partial output\n"],
            control_types=[],
            control_payloads=[],
            binary_frame_count=1,
            idle_timeout=True,
            connection_closed=False,
            from_seq=after_seq or 0,
            next_seq=1,
        )


class TerminalManagerTests(unittest.IsolatedAsyncioTestCase):
    def make_session(self, manager: TerminalSessionManager, handle: str = "session-1") -> ManagedTerminalSession:
        terminal = FakeTerminal()
        session = ManagedTerminalSession(
            handle=handle,
            terminal=terminal,
            asset_id="asset-1",
            account="account-1",
            protocol="ssh",
            connect_method="web_cli",
            cols=120,
            rows=32,
            idle_timeout_seconds=3600.0,
        )
        session.touch()
        manager._sessions[handle] = session
        return session

    async def test_read_output_is_not_blocked_by_running_command(self) -> None:
        manager = TerminalSessionManager()
        session = self.make_session(manager)
        terminal = session.terminal

        run_task = asyncio.create_task(
            manager.run_command(
                "session-1",
                command="hostname",
                settle_timeout_seconds=1.0,
                total_timeout_seconds=5.0,
            )
        )

        await asyncio.sleep(0)
        read_payload = await manager.read_output(
            "session-1",
            idle_timeout_seconds=0.1,
            total_timeout_seconds=0.5,
        )

        self.assertEqual(read_payload["stdout_text"], "partial output\n")
        self.assertEqual(session.status, "command_running")

        terminal._command_release.set()
        run_payload = await run_task
        self.assertIn("command_completed", run_payload)
        self.assertTrue(terminal.sent_data)

    async def test_interrupt_clears_running_command_state(self) -> None:
        manager = TerminalSessionManager()
        session = self.make_session(manager)
        session.start_command("command-1")

        payload = await manager.interrupt_session(
            "session-1",
            signal="ctrl_c",
            settle_timeout_seconds=0.1,
            total_timeout_seconds=0.5,
        )

        self.assertTrue(payload["interrupted"])
        self.assertEqual(session.status, "idle")
        self.assertEqual(session.terminal.sent_data, ["\u0003"])

    async def test_interrupt_accepts_sigint_alias(self) -> None:
        manager = TerminalSessionManager()
        session = self.make_session(manager)
        session.start_command("command-1")

        payload = await manager.interrupt_session(
            "session-1",
            signal="SIGINT",
            settle_timeout_seconds=0.1,
            total_timeout_seconds=0.5,
        )

        self.assertTrue(payload["interrupted"])
        self.assertEqual(payload["signal"], "ctrl_c")
        self.assertEqual(session.status, "idle")
        self.assertEqual(session.terminal.sent_data, ["\u0003"])

    async def test_collect_expired_sessions_skips_busy_sessions(self) -> None:
        manager = TerminalSessionManager()
        session = self.make_session(manager)
        session.idle_timeout_seconds = 0.0
        session.status = "command_running"
        expired = await manager._collect_expired_sessions()
        self.assertEqual(expired, [])
        self.assertIn("session-1", manager._sessions)

    async def test_remote_close_closes_terminal_and_expires_token_once(self) -> None:
        manager = TerminalSessionManager()
        session = self.make_session(manager)
        terminal = session.terminal
        terminal.transcript_override = Transcript(
            connection_closed=True,
            close_reason="CLOSE",
            from_seq=0,
            next_seq=1,
        )

        payload = await manager.read_output(
            "session-1",
            idle_timeout_seconds=0.1,
            total_timeout_seconds=0.5,
        )
        await manager._close_detached_session(session, close_reason="remote_closed")

        self.assertTrue(payload["closed"])
        self.assertEqual(payload["token_cleanup"], "expired")
        self.assertEqual(terminal.close_calls, 1)
        self.assertNotIn("session-1", manager._sessions)

    async def test_cancelled_command_closes_session_without_zombie_state(self) -> None:
        manager = TerminalSessionManager()
        session = self.make_session(manager)
        terminal = session.terminal
        task = asyncio.create_task(
            manager.run_command(
                "session-1",
                command="sleep 30",
                settle_timeout_seconds=1.0,
                total_timeout_seconds=60.0,
            )
        )
        while session.status != "command_running":
            await asyncio.sleep(0)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertNotIn("session-1", manager._sessions)
        self.assertTrue(session.closed)
        self.assertEqual(session.close_reason, "command_cancelled")
        self.assertEqual(session.status, "idle")
        self.assertEqual(session.current_command_id, "")
        self.assertEqual(terminal.close_calls, 1)

    async def test_remote_close_during_command_expires_token_once(self) -> None:
        manager = TerminalSessionManager()
        session = self.make_session(manager)
        terminal = session.terminal
        terminal.transcript_override = Transcript(
            connection_closed=True,
            close_reason="CLOSE",
            from_seq=0,
            next_seq=1,
        )

        payload = await manager.run_command(
            "session-1",
            command="hostname",
            settle_timeout_seconds=0.1,
            total_timeout_seconds=0.5,
        )
        await manager._close_detached_session(session, close_reason="remote_closed")

        self.assertTrue(payload["closed"])
        self.assertEqual(payload["token_cleanup"], "expired")
        self.assertEqual(session.status, "idle")
        self.assertEqual(terminal.close_calls, 1)
        self.assertNotIn("session-1", manager._sessions)


class TerminalManagerConcurrentOpenTests(unittest.IsolatedAsyncioTestCase):
    def make_settings(self) -> Settings:
        return Settings(
            base_url="https://jumpserver.example.com",
            org_id="",
            state_dir=Path("/tmp"),
            state_file=Path("/tmp/jumpserver-auth-state.json"),
            log_level="INFO",
            verify_tls=True,
            request_timeout_seconds=1.0,
            terminal_idle_timeout_seconds=3600.0,
            terminal_reap_interval_seconds=30.0,
            max_terminal_sessions=1,
        )

    async def test_concurrent_reuse_opens_one_terminal_with_single_session_limit(
        self,
    ) -> None:
        settings = self.make_settings()
        auth_state = AuthState(
            base_url=settings.base_url,
            cookies=[CookieState(name="jms_sessionid", value="cookie-value")],
        )
        manager = TerminalSessionManager()

        class OpeningTerminal(FakeTerminal):
            open_calls = 0
            open_started = asyncio.Event()
            open_release = asyncio.Event()

            def __init__(self, *args, **kwargs) -> None:
                super().__init__()

            async def open(self) -> None:
                type(self).open_calls += 1
                type(self).open_started.set()
                await type(self).open_release.wait()

            async def drain_until_idle(
                self,
                *,
                idle_timeout_seconds: float,
                total_timeout_seconds: float,
                after_seq: int | None = None,
            ) -> Transcript:
                return Transcript(from_seq=after_seq or 0, next_seq=after_seq or 0)

        async def open_session() -> dict:
            return await manager.open_session(
                settings,
                auth_state,
                asset_id="asset-1",
                account="account-1",
                reuse_existing=True,
            )

        try:
            with patch(
                "mcp_jumpserver_gui_sucks.terminal_manager.KoKoTerminalSession",
                OpeningTerminal,
            ):
                first_task = asyncio.create_task(open_session())
                await OpeningTerminal.open_started.wait()
                second_task = asyncio.create_task(open_session())
                await asyncio.sleep(0)
                with self.assertRaises(KoKoProbeError):
                    await manager.open_session(
                        settings,
                        auth_state,
                        asset_id="asset-2",
                        account="account-1",
                        reuse_existing=True,
                    )
                OpeningTerminal.open_release.set()
                first, second = await asyncio.gather(first_task, second_task)

            self.assertEqual(OpeningTerminal.open_calls, 1)
            self.assertEqual({first["opened"], second["opened"]}, {True, False})
            self.assertEqual(
                {first["reused_existing"], second["reused_existing"]},
                {True, False},
            )
            self.assertEqual(first["session_handle"], second["session_handle"])
            sessions = await manager.list_sessions()
            self.assertEqual(sessions["count"], 1)
            self.assertEqual(sessions["opening_count"], 0)
        finally:
            OpeningTerminal.open_release.set()
            await manager.close_all_sessions(close_reason="test_cleanup")


if __name__ == "__main__":
    unittest.main()
