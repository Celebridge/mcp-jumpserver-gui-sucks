# KoKo SFTP Flow

## Scope

The project implements SFTP-style file operations through JumpServer's existing KoKo web file
manager. It does not connect directly to port 22 or 2222 and does not bypass JumpServer's
authorization, file-operation logging, or session controls.

## Confirmed 443 Flow

The target instance uses this sequence:

1. Resolve an asset and permitted account with protocol `sftp`.
2. Create a connection token with connect method `web_sftp`.
3. Open `/koko/ws/elfinder/` with query parameters `type=elfinder`, `target_id`, and `token`.
4. Authenticate the WebSocket with persisted web-session cookies and subprotocol `JMS-KOKO`.
5. Read the `CONNECT` message and retain its `id` as the elFinder session ID.
6. Call `/koko/elfinder/connector/<target>/` with both `sid` and header
   `JMS-KoKo-Request-ID` set to that ID.
7. Keep the WebSocket alive while issuing standard elFinder commands such as `open`, `mkdir`,
   `upload`, `file`, `rename`, and `rm`.
8. Close the WebSocket and expire the connection token when the managed session ends.

KoKo can delete a non-reusable connection token as part of WebSocket shutdown. A subsequent
token-expire response with HTTP 404 is therefore treated as successful idempotent cleanup.

## Path Model

All MCP paths are absolute within the virtual root exposed by JumpServer. That virtual root may
map to a configured operating-system path rather than `/`.

On the verified test account, the virtual SFTP root `/` maps to the server's `/tmp`. Therefore:

```text
MCP path:    /mcp-jumpserver-gui-sucks-sftp-test/file.txt
Server path: /tmp/mcp-jumpserver-gui-sucks-sftp-test/file.txt
```

The client rejects relative paths, NUL bytes, and `..` path components.

## Safety Contract

- Upload and download refuse destinations observed to exist unless `overwrite=true`.
- KoKo's elFinder upload connector has no atomic create-if-absent primitive. The managed
  session serializes this MCP process's operations, but an external writer can still race the
  preflight existence check. Do not concurrently write the same path when relying on
  `overwrite=false`.
- Rename refuses an existing sibling instead of overwriting it.
- Remote root rename and deletion are forbidden.
- Every remote deletion requires `confirm=true`.
- KoKo implements directory removal recursively, so directory deletion additionally requires
  `recursive=true`.
- Local upload source paths are unrestricted and must resolve to a regular file.

## Session Lifecycle

Managed SFTP sessions are process-local and keyed by asset/account pair. Repeated acquisition
reuses the same session. Sessions are closed by explicit tool call, idle timeout, or MCP process
shutdown. Active operations are not idle-reaped, even when a transfer exceeds the configured
idle timeout. Process-exit cleanup isolates per-session errors so one failed close does not
prevent the remaining sessions from being closed. The defaults are a one-hour idle timeout, a
30-second reap interval, and four concurrent SFTP sessions.
