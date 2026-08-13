import asyncio
import re
import shlex
import unittest
from pathlib import Path
from typing import ClassVar
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
        self.emit_identity = True
        self._command_release = asyncio.Event()
        self._drain_call_count = 0

    def current_seq(self) -> int:
        return 0

    def buffered_output_bytes(self) -> int:
        return 128

    async def clear_buffered_output(
        self, *, after_seq: int, through_seq: int
    ) -> None:
        return None

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
            identity_match = re.search(
                r"printf '(__MCP_JMS_IDENTITY_[A-Za-z0-9]+__)\\t",
                script,
            )
            identity_marker = identity_match.group(1) if identity_match else ""
            identity_record = (
                f"{identity_marker}\t1002\treadonly\t/home/readonly\t/bin/bash\n"
                if identity_marker and self.emit_identity
                else ""
            )
            return Transcript(
                text_chunks=[
                    f"{start_marker}\nhostname\n{identity_record}{end_marker}:0\n"
                ],
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


class UserSwitchTerminal:
    users: ClassVar[dict[str, tuple[int, str, str]]] = {
        "readonly": (1002, "/home/readonly", "/bin/bash"),
        "root": (0, "/root", "/bin/bash"),
        "deploy": (1003, "/home/deploy", "/bin/bash"),
    }

    def __init__(self) -> None:
        self.terminal_id = "terminal-1"
        self.session_id = "remote-session-1"
        self.connect_info = {"asset": {"name": "asset-1"}, "platform": "linux"}
        self.cleanup_state = "expired"
        self.cleanup_error = ""
        self.close_reason = ""
        self.sent_data: list[str] = []
        self.closed = False
        self.current_user = "readonly"
        self.remote_user_stack: list[str] = []
        self.expected_password = "rotating-secret"
        self.pending_password_target = ""
        self.password_prompt = ""
        self._transcripts: list[Transcript] = []
        self._seq = 0
        self.cleared_ranges: list[tuple[int, int]] = []

    def current_seq(self) -> int:
        return self._seq

    def buffered_output_bytes(self) -> int:
        return 128

    async def clear_buffered_output(
        self, *, after_seq: int, through_seq: int
    ) -> None:
        self.cleared_ranges.append((after_seq, through_seq))

    async def resize(self, *, cols: int, rows: int) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    def _switch(self, target_user: str) -> None:
        self.remote_user_stack.append(self.current_user)
        self.current_user = target_user

    def _exit(self) -> None:
        self.current_user = self.remote_user_stack.pop()

    def _prompt(self) -> str:
        suffix = "#" if self.current_user == "root" else "$"
        return f"[{self.current_user}@host ~]{suffix} "

    def _identity_record(self, marker: str) -> str:
        uid, home, shell = self.users[self.current_user]
        return f"{marker}\t{uid}\t{self.current_user}\t{home}\t{shell}\n"

    def _queue(self, text: str) -> None:
        self._seq += 1
        self._transcripts.append(
            Transcript(
                text_chunks=[text],
                binary_frame_count=1,
                idle_timeout=True,
                from_seq=self._seq - 1,
                next_seq=self._seq,
            )
        )

    async def send_terminal_data(self, text: str) -> None:
        self.sent_data.append(text)
        if self.pending_password_target:
            if text == "\u0003":
                self.pending_password_target = ""
                self._queue("^C\n" + self._prompt())
                return
            if text.rstrip("\n") == self.expected_password:
                self._switch(self.pending_password_target)
                self.pending_password_target = ""
                self._queue(self._prompt())
            else:
                self._queue(f"Sorry, try again.\n{self.password_prompt}")
            return

        start_match = re.search(r"(__MCP_JMS_COMMAND_START_[A-Za-z0-9]+__)", text)
        end_match = re.search(r"(__MCP_JMS_EXIT_STATUS_[A-Za-z0-9]+__)", text)
        identity_match = re.search(r"(__MCP_JMS_IDENTITY_[A-Za-z0-9]+__)", text)
        if start_match and end_match and identity_match:
            sudo_match = re.search(r"^sudo -n -iu ([^\n]+)$", text, re.MULTILINE)
            if sudo_match:
                self._switch(shlex.split(sudo_match.group(1))[0])
            elif re.search(r"^sudo su$", text, re.MULTILINE):
                self._switch("root")
            elif re.search(r"^exit$", text, re.MULTILINE):
                self._exit()
            self._queue(
                f"{start_match.group(1)}\n"
                f"{self._identity_record(identity_match.group(1))}"
                f"{end_match.group(1)}:0\n{self._prompt()}"
            )
            return

        sudo_password_match = re.match(r"sudo -S -p (\S+) -iu (\S+)\n", text)
        if sudo_password_match:
            self.password_prompt = shlex.split(sudo_password_match.group(1))[0]
            self.pending_password_target = shlex.split(sudo_password_match.group(2))[0]
            self._queue(self.password_prompt)
            return

        su_match = re.match(r"su - (\S+)\n", text)
        if su_match:
            target_user = shlex.split(su_match.group(1))[0]
            if self.current_user == "root":
                self._switch(target_user)
                self._queue(self._prompt())
            else:
                self.pending_password_target = target_user
                self.password_prompt = "Password: "
                self._queue(self.password_prompt)
            return
        if text == "\u0003":
            self.pending_password_target = ""
            self._queue("^C\n" + self._prompt())

    async def drain_until_idle(
        self,
        *,
        idle_timeout_seconds: float,
        total_timeout_seconds: float,
        after_seq: int | None = None,
    ) -> Transcript:
        return self._transcripts.pop(0)


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

    async def test_run_command_tracks_the_effective_remote_identity(self) -> None:
        manager = TerminalSessionManager()
        session = self.make_session(manager)
        session.terminal._command_release.set()

        payload = await manager.run_command("session-1", command="hostname")

        self.assertEqual(payload["current_identity"]["user"], "readonly")
        self.assertEqual(payload["current_identity"]["uid"], 1002)
        self.assertTrue(payload["identity_verified"])
        self.assertNotIn("__MCP_JMS_IDENTITY_", payload["command_stdout_text"])

    async def test_identity_probe_rejects_a_stale_cached_identity(self) -> None:
        manager = TerminalSessionManager()
        session = self.make_session(manager)
        session.current_identity = {"uid": 1002, "user": "readonly"}
        session.terminal.emit_identity = False
        session.terminal._command_release.set()

        with self.assertRaisesRegex(KoKoProbeError, "verifiable remote identity"):
            await manager.get_identity(session.handle)

    async def test_user_switch_busy_state_blocks_commands_and_raw_input(self) -> None:
        manager = TerminalSessionManager()
        session = self.make_session(manager)
        session.status = "user_switch"

        with self.assertRaisesRegex(KoKoProbeError, "busy.*user_switch"):
            await manager.run_command(session.handle, command="id")
        with self.assertRaisesRegex(KoKoProbeError, "busy.*user_switch"):
            await manager.send_input(session.handle, data="id", append_newline=True)

    def make_user_switch_session(self, manager: TerminalSessionManager) -> ManagedTerminalSession:
        terminal = UserSwitchTerminal()
        session = ManagedTerminalSession(
            handle="switch-session",
            terminal=terminal,  # type: ignore[arg-type]
            asset_id="asset-1",
            account="account-1",
            protocol="ssh",
            connect_method="web_cli",
            cols=120,
            rows=32,
            idle_timeout_seconds=3600.0,
        )
        manager._sessions[session.handle] = session
        return session

    async def test_switch_user_passwordless_and_exit_restores_previous_user(self) -> None:
        manager = TerminalSessionManager()
        session = self.make_user_switch_session(manager)

        switched = await manager.switch_user(
            session.handle,
            target_user="root",
            method="sudo",
        )
        restored = await manager.exit_user(session.handle)

        self.assertTrue(switched["switched"])
        self.assertEqual(switched["current_identity"]["user"], "root")
        self.assertEqual(switched["user_switch_depth"], 1)
        self.assertTrue(restored["exited"])
        self.assertEqual(restored["current_identity"]["user"], "readonly")
        self.assertEqual(restored["user_switch_depth"], 0)

    async def test_switch_user_sends_password_separately_and_redacts_result(self) -> None:
        manager = TerminalSessionManager()
        session = self.make_user_switch_session(manager)
        password = "rotating-secret"

        switched = await manager.switch_user(
            session.handle,
            target_user="deploy",
            method="sudo",
            password=password,
        )

        self.assertEqual(switched["current_identity"]["user"], "deploy")
        self.assertNotIn(password, repr(switched))
        self.assertEqual(session.terminal.sent_data.count(f"{password}\n"), 1)
        transition_command = next(
            data for data in session.terminal.sent_data if data.startswith("sudo -S")
        )
        self.assertNotIn(password, transition_command)
        self.assertEqual(session.terminal.cleared_ranges, [(1, 3)])

    async def test_switch_user_supports_su_with_a_password(self) -> None:
        manager = TerminalSessionManager()
        session = self.make_user_switch_session(manager)

        switched = await manager.switch_user(
            session.handle,
            target_user="deploy",
            method="su",
            password="rotating-secret",
        )

        self.assertEqual(switched["method"], "su")
        self.assertEqual(switched["current_identity"]["user"], "deploy")
        self.assertEqual(switched["user_switch_depth"], 1)

    async def test_rejected_password_is_cancelled_and_never_returned(self) -> None:
        manager = TerminalSessionManager()
        session = self.make_user_switch_session(manager)
        rejected_password = "wrong-rotating-secret"

        with self.assertRaisesRegex(KoKoProbeError, "rejected") as raised:
            await manager.switch_user(
                session.handle,
                target_user="deploy",
                method="sudo",
                password=rejected_password,
            )

        self.assertNotIn(rejected_password, str(raised.exception))
        self.assertIn("\u0003", session.terminal.sent_data)
        self.assertEqual(session.status, "idle")
        self.assertEqual(session.terminal.pending_password_target, "")

    async def test_manual_sudo_su_still_refreshes_the_effective_identity(self) -> None:
        manager = TerminalSessionManager()
        session = self.make_user_switch_session(manager)

        switched = await manager.run_command(session.handle, command="sudo su")
        restored = await manager.run_command(session.handle, command="exit")

        self.assertEqual(switched["current_identity"]["user"], "root")
        self.assertEqual(switched["user_switch_depth"], 0)
        self.assertEqual(restored["current_identity"]["user"], "readonly")


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
