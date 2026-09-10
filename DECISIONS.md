# Decisions

Judgment calls made where `SPEC.md` was ambiguous or silent. The guiding rule
from the spec: prefer the option that makes failures visible rather than
silent.

## API

### D1. `close()` is idempotent; every other method throws after close

The spec says public methods throw `StateError` once closed. `close()` is the
exception: it returns the same future on every call. A socket can close
itself (`attemptsExhausted`), and a Flutter `dispose()` that then calls
`close()` must not crash. `connect`, `reconnect`, `send`, `sendTyped`, `on`,
and `stream` all throw. Getters (`state`, `states`, `done`, ...) never throw.

### D2. `connect()` and `reconnect()` futures are pre-marked as handled

Both can complete with `TypedSocketClosedException`. Fire-and-forget
`unawaited(socket.connect())` in `initState` is idiomatic, and it would
otherwise crash the app through an unhandled async error when the socket
gives up. Callers that `await` still receive the error. The closure is also
visible on `states`, `done`, and `closeReason`.

### D3. `stream<T>()` requires the exact registered type

The type check uses mutual subtyping, so `dynamic` and `Object?` count as the
same type, but a supertype does not match. Leaving off the type argument
(`socket.stream('chat')`) infers `dynamic`, and that is exactly the untyped
mistake this package exists to catch, so it throws `ArgumentError` at wiring
time. `sendTyped` checks that the *value* is an instance of the registered
type, so subclasses are accepted.

### D4. Added `connectionErrors` (additive, beyond the spec)

The spec routes network problems to `states`, `frameErrors`, and channel
errors. None of those carries the *cause* of a failed connect or of a thrown
`onConnected`, and swallowing those errors would make auth bugs invisible.
`Stream<TypedSocketConnectionError> connectionErrors` reports the cause, stack
trace, and attempt number. It is purely observational. Clean remote closes
are not errors and appear only on `states`. **Flagged for maintainer review
before 0.1.0**, since it grows the public API.

### D5. Heartbeat event names are reserved when a heartbeat is configured

With a heartbeat, `on()` rejects `pingEvent` and `pongEvent`, because pongs
are swallowed and a channel registered for them would silently never fire.
Without a heartbeat those names are ordinary events, and pongs reach
`unhandledFrames`.

### D6. `data` must be a JSON object for typed channels

Per the spec, a registered channel's decoder receives a
`Map<String, dynamic>`. A `null`, list, or scalar payload becomes a
`TypedSocketDecodeError` on that channel. Events without an object payload
can be consumed from `unhandledFrames`.

## Engine

### D7. Attempt numbering and `maxReconnectAttempts`

- Attempt 0 is the first attempt after `connect()` or `reconnect()` and never
  waits. Retries are numbered 1, 2, ... and wait `backoff(n)`.
- `maxReconnectAttempts` counts retries per outage: `2` means at most three
  transport connects (the initial attempt plus two retries). `0` means one
  failure closes the socket.
- The counter resets when the socket reaches `connected`.
  **Known limitation:** a server that accepts and then immediately drops
  every connection is retried forever at `backoff(1)` even with
  `maxReconnectAttempts` set. A "stable for N seconds" reset would fix this.
  Revisit if users hit it.
- `ConnectedContext.attempt` is the attempt number that produced the
  connection. It is 0 after `reconnect()`, so it is not a reliable "is this a
  reconnection" signal.

### D8. `reconnect()` during an in-flight connect waits for it

A transport connect cannot be cancelled. To keep "exactly one attempt in
flight", `reconnect()` abandons the current attempt (generation bump), and the
new attempt waits for the old transport connect to settle. The old
connection is closed on arrival and never used.

### D9. The heartbeat starts when the transport connects

It starts before `onConnected` completes, not when the state reaches
`connected`. This way a connection that dies while `onConnected` awaits a
server reply is still detected. `onConnected` has no built-in timeout; if it
awaits a reply that never comes from a live server, the socket stays
`connecting`. Apply a timeout inside the hook if that matters.

### D10. A send that throws is treated as a lost connection

`TransportConnection.send` should throw once the transport knows the socket
is closed. `FakeConnection` and `WebSocketTransport` both do. If a send throws
while the state is still `connected` (the connection died but the socket has
not processed the event yet), the socket treats the connection as lost and
applies the offline policy to that frame instead of losing it. The same
applies during a flush: the failed frame and everything after it stay
buffered in order.

### D11. A transport stream error counts as a connection loss

An error on `TransportConnection.incoming` is reported on `connectionErrors`,
and the connection is torn down and retried. Frame-level problems never
arrive as stream errors; they are decode failures.

### D12. Close codes

`close()`, `reconnect()`, and abandoned connections use 1000 (normal). A
heartbeat timeout uses 4000 with reason `heartbeat timeout`. Both are valid
for browsers, which reject codes outside 1000 and 3000–4999.

## Packaging

### D13. SDK lower bound 3.5, dev tooling needs newer

Runtime dependencies (`meta`, `web_socket_channel`) support Dart 3.5, so
`sdk: ^3.5.0` keeps the package usable on older Flutter versions. The latest
`test` and `lints` require a newer SDK, which only affects contributors. CI
runs on stable and beta.

### D14. `lints` rather than `very_good_analysis`

The spec says to ask before switching. `very_good_analysis` 11 would also
force an SDK lower bound of 3.13. `package:lints/recommended.yaml` plus
`public_member_api_docs` and a few strictness rules are used instead.
