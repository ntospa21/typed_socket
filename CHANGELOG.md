## 0.1.0

Initial release.

- `TypedSocket`: a WebSocket client with an explicit state machine
  (`idle`, `connecting`, `connected`, `reconnecting`, `closed`) and a
  replaying `states` stream.
- Automatic reconnect with full-jitter exponential backoff,
  `maxReconnectAttempts`, and `reconnect()` for token refreshes.
- Typed channels via `on<T>()` and `stream<T>()`. Type mismatches throw at
  wiring time, and decode errors affect a single message.
- `sendTyped` and `send` with an explicit `OfflinePolicy`
  (`bufferDropOldest`, `bufferDropNew`, `drop`, `reject`), plus
  `pendingSends` and `pendingSendsChanges`.
- Application-level heartbeat (`HeartbeatConfig`) that detects half-open
  connections.
- `onConnected` hook for hello, auth, and resubscribe frames, and
  `uriProvider` for per-attempt URIs.
- `unhandledFrames`, `frameErrors`, and `connectionErrors` for
  observability.
- Pluggable `EnvelopeCodec`, with `JsonEnvelopeCodec` and configurable keys.
- `Transport` seam with `WebSocketTransport` (VM, Flutter, and web) and
  `FakeTransport` in `package:typed_socket/testing.dart`.
