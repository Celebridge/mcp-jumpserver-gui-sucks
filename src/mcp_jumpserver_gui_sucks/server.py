from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, Literal

from .mcp_compat import create_mcp_server
from .service import (
    acquire_sftp_session_payload,
    acquire_terminal_session_payload,
    build_paths_payload,
    build_sftp_usage_guide_payload,
    build_status_payload,
    build_terminal_usage_guide_payload,
    close_sftp_session_payload,
    close_terminal_session_payload,
    create_connection_token_payload,
    exit_terminal_user_payload,
    expire_connection_token_payload,
    get_asset_access_payload,
    get_asset_payload,
    get_profile_payload,
    get_terminal_identity_payload,
    interrupt_terminal_session_payload,
    list_assets_payload,
    list_connect_methods_payload,
    list_connection_tokens_payload,
    list_nodes_payload,
    list_sftp_sessions_payload,
    list_terminal_sessions_payload,
    probe_koko_terminal_payload,
    read_terminal_output_payload,
    refresh_terminal_auth_payload,
    resize_terminal_session_payload,
    resolve_terminal_target_payload,
    run_terminal_command_payload,
    send_terminal_input_payload,
    sftp_delete_payload,
    sftp_download_payload,
    sftp_list_payload,
    sftp_mkdir_payload,
    sftp_rename_payload,
    sftp_stat_payload,
    sftp_upload_payload,
    switch_terminal_user_payload,
)
from .sftp_manager import get_sftp_session_manager
from .terminal_manager import get_terminal_session_manager

LOGGER = logging.getLogger(__name__)


async def close_all_managed_sessions() -> None:
    results = await asyncio.gather(
        get_terminal_session_manager().close_all_sessions(close_reason="process_exit"),
        get_sftp_session_manager().close_all_sessions(close_reason="process_exit"),
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, BaseException):
            LOGGER.error(
                "Managed session cleanup failed during process exit: %s",
                type(result).__name__,
            )


@asynccontextmanager
async def managed_session_lifespan(_server: Any):
    try:
        yield {}
    finally:
        await close_all_managed_sessions()


mcp = create_mcp_server(
    name="mcp-jumpserver-gui-sucks",
    instructions=(
        "JumpServer discovery and session-preparation tools backed by persisted "
        "CLI-derived authentication state. Use status and asset-discovery tools first. "
        "For terminal work, call jms_terminal_usage_guide first, then prefer "
        "jms_acquire_terminal_session so one shell is reused per target. "
        "Use jms_run_terminal_command for short command execution, "
        "jms_get_terminal_identity and jms_switch_terminal_user for explicit remote identity management, "
        "jms_exit_terminal_user to restore the previous remote identity, "
        "jms_send_terminal_input plus jms_read_terminal_output for shell-style interaction, "
        "and jms_interrupt_terminal_session when a command needs to be stopped. "
        "Close managed shells with jms_close_terminal_session when the task is done. "
        "For file work, call jms_sftp_usage_guide, acquire one reusable SFTP session, "
        "and preserve the explicit overwrite and deletion safety gates."
    ),
    lifespan=managed_session_lifespan,
)


@mcp.tool()
def jms_paths() -> dict[str, Any]:
    """Show runtime environment keys and resolved state paths."""
    return build_paths_payload()


@mcp.tool()
async def jms_status() -> dict[str, Any]:
    """Probe the local auth state and the current JumpServer session."""
    return await build_status_payload()


@mcp.tool()
async def jms_terminal_usage_guide() -> dict[str, Any]:
    """Return the recommended terminal workflow for coding agents using this MCP."""
    return await build_terminal_usage_guide_payload()


@mcp.tool()
async def jms_sftp_usage_guide() -> dict[str, Any]:
    """Return the recommended SFTP workflow and destructive-operation safety rules."""
    return await build_sftp_usage_guide_payload()


@mcp.tool()
async def jms_profile() -> dict[str, Any]:
    """Return the current JumpServer user profile."""
    return await get_profile_payload()


@mcp.tool()
async def jms_list_assets(
    asset: str = "",
    node: str = "",
    limit: int = 15,
    offset: int = 0,
) -> dict[str, Any]:
    """List assets through the observed perms endpoint used by the current GUI."""
    return await list_assets_payload(asset=asset, node=node, limit=limit, offset=offset)


@mcp.tool()
async def jms_get_asset(asset_id: str) -> dict[str, Any]:
    """Fetch one asset detail through the observed perms endpoint."""
    return await get_asset_payload(asset_id)


@mcp.tool()
async def jms_list_nodes() -> dict[str, Any]:
    """List the observed JumpServer node tree exposed to the current user."""
    return await list_nodes_payload()


@mcp.tool()
async def jms_list_connect_methods(protocol: str = "") -> dict[str, Any]:
    """List observed JumpServer connect methods, optionally narrowed to one protocol."""
    return await list_connect_methods_payload(protocol=protocol)


@mcp.tool()
async def jms_get_asset_access(asset_id: str) -> dict[str, Any]:
    """Summarize accounts, protocols, and connect methods available for one asset."""
    return await get_asset_access_payload(asset_id)


@mcp.tool()
async def jms_resolve_terminal_target(
    asset_ref: str,
    account_ref: str = "",
    protocol: str = "ssh",
) -> dict[str, Any]:
    """Resolve user-facing asset and account references into the concrete IDs needed by terminal entrypoints."""
    return await resolve_terminal_target_payload(
        asset_ref=asset_ref,
        account_ref=account_ref,
        protocol=protocol,
    )


@mcp.tool()
async def jms_list_connection_tokens(
    limit: int = 20, offset: int = 0
) -> dict[str, Any]:
    """List active connection tokens without exposing the token secret value."""
    return await list_connection_tokens_payload(limit=limit, offset=offset)


@mcp.tool()
async def jms_create_connection_token(
    asset_id: str,
    account: str,
    protocol: str = "ssh",
    connect_method: str = "web_cli",
    reusable: bool = False,
) -> dict[str, Any]:
    """Create a connection token using a concrete account ID or a user-facing account reference."""
    return await create_connection_token_payload(
        asset_id=asset_id,
        account=account,
        protocol=protocol,
        connect_method=connect_method,
        reusable=reusable,
    )


@mcp.tool()
async def jms_expire_connection_token(token_id: str) -> dict[str, Any]:
    """Expire one connection token explicitly."""
    return await expire_connection_token_payload(token_id)


@mcp.tool()
async def jms_probe_koko_terminal(
    asset_id: str,
    account: str,
    protocol: str = "ssh",
    connect_method: str = "web_cli",
    cols: int = 80,
    rows: int = 24,
    max_messages: int = 4,
) -> dict[str, Any]:
    """Probe KoKo terminal access using a concrete account ID or a user-facing account reference."""
    return await probe_koko_terminal_payload(
        asset_id=asset_id,
        account=account,
        protocol=protocol,
        connect_method=connect_method,
        cols=cols,
        rows=rows,
        max_messages=max_messages,
    )


@mcp.tool()
async def jms_acquire_terminal_session(
    asset_ref: str,
    account_ref: str = "",
    protocol: str = "ssh",
    connect_method: str = "web_cli",
    cols: int = 120,
    rows: int = 32,
    startup_idle_timeout_seconds: float = 1.5,
) -> dict[str, Any]:
    """Resolve a target and get or reuse one managed KoKo shell for that asset/account pair."""
    return await acquire_terminal_session_payload(
        asset_ref=asset_ref,
        account_ref=account_ref,
        protocol=protocol,
        connect_method=connect_method,
        cols=cols,
        rows=rows,
        startup_idle_timeout_seconds=startup_idle_timeout_seconds,
    )


@mcp.tool()
async def jms_refresh_terminal_auth(force: bool = False) -> dict[str, Any]:
    """Refresh the persisted cookie-backed terminal session if it is still valid, or report that re-login is required."""
    return await refresh_terminal_auth_payload(force=force)


@mcp.tool()
async def jms_list_terminal_sessions() -> dict[str, Any]:
    """List active managed KoKo terminal sessions in the current MCP server process."""
    return await list_terminal_sessions_payload()


@mcp.tool()
async def jms_send_terminal_input(
    session_handle: str,
    data: str,
    append_newline: bool = False,
) -> dict[str, Any]:
    """Send raw input to a managed KoKo terminal session."""
    return await send_terminal_input_payload(
        session_handle=session_handle,
        data=data,
        append_newline=append_newline,
    )


@mcp.tool()
async def jms_read_terminal_output(
    session_handle: str,
    after_seq: int | None = None,
    idle_timeout_seconds: float = 1.0,
    total_timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    """Read buffered output from a managed KoKo terminal session until idle or timeout."""
    return await read_terminal_output_payload(
        session_handle=session_handle,
        after_seq=after_seq,
        idle_timeout_seconds=idle_timeout_seconds,
        total_timeout_seconds=total_timeout_seconds,
    )


@mcp.tool()
async def jms_run_terminal_command(
    session_handle: str,
    command: str,
    settle_timeout_seconds: float = 1.5,
    total_timeout_seconds: float = 20.0,
) -> dict[str, Any]:
    """Run one command through an already acquired managed KoKo shell."""
    return await run_terminal_command_payload(
        session_handle=session_handle,
        command=command,
        settle_timeout_seconds=settle_timeout_seconds,
        total_timeout_seconds=total_timeout_seconds,
    )


@mcp.tool()
async def jms_get_terminal_identity(
    session_handle: str,
    settle_timeout_seconds: float = 1.0,
    total_timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    """Verify and return the effective operating-system user in a managed shell."""
    return await get_terminal_identity_payload(
        session_handle=session_handle,
        settle_timeout_seconds=settle_timeout_seconds,
        total_timeout_seconds=total_timeout_seconds,
    )


@mcp.tool()
async def jms_switch_terminal_user(
    session_handle: str,
    target_user: str,
    method: Literal["sudo", "su"] = "sudo",
    password: str = "",
    settle_timeout_seconds: float = 1.0,
    total_timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    """Switch to a verified remote user within the existing JumpServer shell.

    Use ``sudo`` for sudo-policy authentication and ``su`` for target-account
    authentication. Supply ``password`` only when the remote host requires it.
    The MCP client or model may retain that tool argument in its trace history.
    """
    return await switch_terminal_user_payload(
        session_handle=session_handle,
        target_user=target_user,
        method=method,
        password=password,
        settle_timeout_seconds=settle_timeout_seconds,
        total_timeout_seconds=total_timeout_seconds,
    )


@mcp.tool()
async def jms_exit_terminal_user(
    session_handle: str,
    settle_timeout_seconds: float = 1.0,
    total_timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    """Exit one MCP-managed user shell and verify restoration of the previous user."""
    return await exit_terminal_user_payload(
        session_handle=session_handle,
        settle_timeout_seconds=settle_timeout_seconds,
        total_timeout_seconds=total_timeout_seconds,
    )


@mcp.tool()
async def jms_interrupt_terminal_session(
    session_handle: str,
    signal: str = "ctrl_c",
    settle_timeout_seconds: float = 1.0,
    total_timeout_seconds: float = 5.0,
) -> dict[str, Any]:
    """Interrupt a running command in a managed KoKo terminal session."""
    return await interrupt_terminal_session_payload(
        session_handle=session_handle,
        signal=signal,
        settle_timeout_seconds=settle_timeout_seconds,
        total_timeout_seconds=total_timeout_seconds,
    )


@mcp.tool()
async def jms_resize_terminal_session(
    session_handle: str,
    cols: int,
    rows: int,
) -> dict[str, Any]:
    """Resize a managed KoKo terminal session."""
    return await resize_terminal_session_payload(
        session_handle=session_handle,
        cols=cols,
        rows=rows,
    )


@mcp.tool()
async def jms_close_terminal_session(session_handle: str) -> dict[str, Any]:
    """Close a managed KoKo terminal session and expire its underlying connection token."""
    return await close_terminal_session_payload(session_handle)


@mcp.tool()
async def jms_acquire_sftp_session(
    asset_ref: str,
    account_ref: str = "",
) -> dict[str, Any]:
    """Resolve a target and get or reuse one managed KoKo SFTP session."""
    return await acquire_sftp_session_payload(
        asset_ref=asset_ref,
        account_ref=account_ref,
    )


@mcp.tool()
async def jms_list_sftp_sessions() -> dict[str, Any]:
    """List active managed KoKo SFTP sessions in the current MCP process."""
    return await list_sftp_sessions_payload()


@mcp.tool()
async def jms_sftp_stat(session_handle: str, path: str) -> dict[str, Any]:
    """Return metadata for one absolute remote path."""
    return await sftp_stat_payload(session_handle=session_handle, path=path)


@mcp.tool()
async def jms_sftp_list(session_handle: str, path: str = "/") -> dict[str, Any]:
    """List one absolute remote directory path."""
    return await sftp_list_payload(session_handle=session_handle, path=path)


@mcp.tool()
async def jms_sftp_mkdir(
    session_handle: str,
    path: str,
    parents: bool = False,
) -> dict[str, Any]:
    """Create a remote directory, optionally creating missing parents."""
    return await sftp_mkdir_payload(
        session_handle=session_handle,
        path=path,
        parents=parents,
    )


@mcp.tool()
async def jms_sftp_upload(
    session_handle: str,
    local_path: str,
    remote_path: str,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Upload a local file; replacing a remote file requires overwrite=true."""
    return await sftp_upload_payload(
        session_handle=session_handle,
        local_path=local_path,
        remote_path=remote_path,
        overwrite=overwrite,
    )


@mcp.tool()
async def jms_sftp_download(
    session_handle: str,
    remote_path: str,
    local_path: str,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Download a remote file; replacing a local file requires overwrite=true."""
    return await sftp_download_payload(
        session_handle=session_handle,
        remote_path=remote_path,
        local_path=local_path,
        overwrite=overwrite,
    )


@mcp.tool()
async def jms_sftp_rename(
    session_handle: str,
    path: str,
    new_name: str,
) -> dict[str, Any]:
    """Rename one remote entry without overwriting an existing sibling."""
    return await sftp_rename_payload(
        session_handle=session_handle,
        path=path,
        new_name=new_name,
    )


@mcp.tool()
async def jms_sftp_delete(
    session_handle: str,
    path: str,
    confirm: bool = False,
    recursive: bool = False,
) -> dict[str, Any]:
    """Delete an approved path; directories additionally require recursive=true."""
    return await sftp_delete_payload(
        session_handle=session_handle,
        path=path,
        confirm=confirm,
        recursive=recursive,
    )


@mcp.tool()
async def jms_close_sftp_session(session_handle: str) -> dict[str, Any]:
    """Close one managed KoKo SFTP session and clean up its connection token."""
    return await close_sftp_session_payload(session_handle)


def run_server(*, transport: str = "stdio") -> None:
    mcp.run(transport=transport)
