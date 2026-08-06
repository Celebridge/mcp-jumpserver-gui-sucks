import unittest
from pathlib import Path

from mcp_jumpserver_gui_sucks.auth_state import AuthState
from mcp_jumpserver_gui_sucks.config import Settings
from mcp_jumpserver_gui_sucks.koko import (
    KoKoTerminalSession,
    Transcript,
    extract_between_markers,
)


class KoKoEventBufferTests(unittest.IsolatedAsyncioTestCase):
    def make_session(self) -> KoKoTerminalSession:
        settings = Settings(
            base_url="https://jumpserver.example.com",
            org_id="",
            state_dir=Path("/tmp"),
            state_file=Path("/tmp/jumpserver-auth-state.json"),
            log_level="INFO",
            verify_tls=True,
            request_timeout_seconds=1.0,
            terminal_idle_timeout_seconds=3600.0,
            terminal_reap_interval_seconds=30.0,
            max_terminal_sessions=8,
        )
        return KoKoTerminalSession(
            settings,
            AuthState(base_url=settings.base_url),
            asset_id="asset-1",
            account="account-1",
        )

    async def test_event_buffer_prunes_old_events_without_renumbering_sequences(
        self,
    ) -> None:
        session = self.make_session()
        session._max_buffered_events = 3
        session._max_buffered_output_bytes = 1024

        for chunk in (b"one", b"two", b"three", b"four", b"five"):
            await session._record_message(chunk)

        retained = await session._events_after(0)
        transcript = await session.drain_until_idle(
            after_seq=2,
            idle_timeout_seconds=0.001,
            total_timeout_seconds=0.01,
        )

        self.assertEqual([event.seq for event in retained], [3, 4, 5])
        self.assertEqual(transcript.from_seq, 2)
        self.assertEqual(transcript.next_seq, 5)
        self.assertEqual(transcript.raw_text(), "threefourfive")
        self.assertEqual(session.current_seq(), 5)
        self.assertLessEqual(session.buffered_output_bytes(), 1024)

    async def test_event_buffer_prunes_to_byte_limit(self) -> None:
        session = self.make_session()
        session._max_buffered_events = 10
        session._max_buffered_output_bytes = 6

        for chunk in (b"111", b"222", b"333"):
            await session._record_message(chunk)

        retained = await session._events_after(0)

        self.assertEqual([event.seq for event in retained], [2, 3])
        self.assertEqual(session.buffered_output_bytes(), 6)

    async def test_event_buffer_truncates_one_oversized_event(self) -> None:
        session = self.make_session()
        session._max_buffered_events = 10
        session._max_buffered_output_bytes = 4

        await session._record_message(b"oversized")

        retained = await session._events_after(0)

        self.assertEqual([event.seq for event in retained], [1])
        self.assertEqual(retained[0].text, "ized")
        self.assertEqual(session.buffered_output_bytes(), 4)

    async def test_transcript_keeps_markers_while_bounding_continuous_output(
        self,
    ) -> None:
        start_marker = "__MCP_JMS_COMMAND_START_test__"
        end_marker = "__MCP_JMS_EXIT_STATUS_test__"
        transcript = Transcript(max_output_bytes=256)

        transcript.append_text(f"{start_marker}\n")
        for _ in range(100):
            transcript.append_text("0123456789")
        transcript.append_text(f"\n{end_marker}:0\n")

        raw = transcript.raw_text()
        exit_status, output = extract_between_markers(
            raw,
            start_marker=start_marker,
            end_marker=end_marker,
        )

        self.assertTrue(transcript.output_truncated)
        self.assertGreater(transcript.dropped_output_bytes, 0)
        self.assertLess(len(raw.encode("utf-8")), 400)
        self.assertEqual(exit_status, 0)
        self.assertIn("output bytes truncated", output)

    async def test_transcript_reports_pruned_event_gap(self) -> None:
        session = self.make_session()
        session._max_buffered_events = 2

        for chunk in (b"one", b"two", b"three"):
            await session._record_message(chunk)

        transcript = await session.drain_until_idle(
            after_seq=0,
            idle_timeout_seconds=0.001,
            total_timeout_seconds=0.01,
        )

        self.assertTrue(transcript.buffer_gap)
        self.assertEqual(transcript.dropped_event_count, 1)
        self.assertEqual(transcript.raw_text(), "twothree")


if __name__ == "__main__":
    unittest.main()
