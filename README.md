# typed_socket

[![CI](https://github.com/ntospa21/typed_socket/actions/workflows/ci.yaml/badge.svg)](https://github.com/ntospa21/typed_socket/actions/workflows/ci.yaml)

A typed, resilient WebSocket client for Dart and Flutter that turns plain
JSON-over-WebSocket into typed streams that survive reconnects.

- **Automatic reconnect** with full-jitter exponential backoff, so a server
  restart does not cause a reconnect storm.
- **Heartbeat** ping/pong that tears down half-open connections instead of
  letting them look alive forever.
- **Typed channels**: `socket.stream<ChatMessage>('message')`, with type
  mismatches caught when you wire them up rather than at runtime.
- **Isolated decode errors**: one malformed message never kills the stream or
  the connection.
- **Explicit offline policy**: sends made while disconnected are buffered,
  dropped, or rejected, and you choose which. Loss is never accidental.
- **Testable**: `FakeTransport` scripts connects, messages, and drops in unit
  and widget tests.
- Pure Dart, with no Flutter dependency. Runs on the VM, Flutter (mobile and
  desktop), and the web.

## Before and after

The reliability layer every realtime app ends up hand-writing on top of
`web_socket_channel`:

```dart
final channel = WebSocketChannel.connect(Uri.parse('wss://example.com/ws'));

channel.stream.listen((raw) {
  final json = jsonDecode(raw as String); // bad JSON throws and kills the listener
  if (json['event'] == 'message') {
    onMessage(ChatMessage.fromJson(json['data'])); // so does a bad payload
  }
}, onDone: () {
  // TODO: reconnect. With what delay? Who resubscribes the listeners?
});

// TODO: how do we notice a half-open connection that never closes?
// TODO: where does this go if we are offline right now?
channel.sink.add(jsonEncode({'event': 'message', 'data': msg.toJson()}));
```

With `typed_socket`:

```dart
final socket = TypedSocket(
  uri: Uri.parse('wss://example.com/ws'),
  heartbeat: const HeartbeatConfig(),
)..on<ChatMessage>('message', ChatMessage.fromJson, toJson: (m) => m.toJson());

socket.stream<ChatMessage>('message').listen(onMessage); // survives reconnects
socket.states.listen((state) => print('connection: ${state.name}'));

await socket.connect();
socket.sendTyped('message', msg); // buffered and flushed in order when offline
```

## How it compares

| | typed_socket | web_socket_channel | socket_io_client | phoenix_socket |
| --- | --- | --- | --- | --- |
| Server | Any plain JSON WebSocket | Any WebSocket | Socket.IO servers only | Phoenix Channels only |
| Automatic reconnect | Yes, full-jitter backoff | No, write your own | Yes | Yes |
| Dead-connection detection | App-level heartbeat, all platforms | No | Engine.IO ping | Phoenix heartbeat |
| Typed messages | Yes, checked at wiring time | No, raw frames | No, dynamic payloads | No, map payloads |
| Sends while offline | Explicit policy, 4 choices | Not handled | Buffered | Buffered |
| Test fake shipped | `FakeTransport` | No | No | No |

Pick `socket_io_client` or `phoenix_socket` if your server speaks those
protocols. Pick `typed_socket` when your backend speaks plain JSON over
WebSocket: Node `ws`, Go `gorilla/websocket`, Python `websockets`, Ktor,
Actix, AWS API Gateway WebSockets, and similar.

## Wire format

One JSON object per text frame:

```json
{ "event": "message", "data": { "user": "sam", "text": "hi" } }
```

A registered channel's decoder receives the `data` object. A `data` that is
not a JSON object, or a decoder that throws, produces a
`TypedSocketDecodeError` on that channel's stream. The next message still
arrives. See [custom envelopes](#custom-envelopes) for other shapes.

## Connection lifecycle

```text
idle ──connect()──▶ connecting ──▶ connected
                        ▲              │ drop, heartbeat timeout, reconnect()
                        │              ▼
                        └─ backoff ─ reconnecting ──(attempts exhausted)──▶ closed
```

- `states` replays the current state to every new listener, so a
  `StreamBuilder` is right on its first frame.
- The first connect never waits. Retries wait `backoff(attempt)`.
- `maxReconnectAttempts` caps consecutive retries (`null`, the default,
  retries forever). When they run out, the socket closes with
  `CloseReason.attemptsExhausted`.
- `close()` is terminal and idempotent, so it is safe in `dispose()`. `done`
  completes with the `CloseReason`.
- Network problems never throw from the socket's methods. They show up on
  `states`, `connectionErrors`, `frameErrors`, and channel stream errors.
  Programmer mistakes (unregistered events, wrong types, use after close)
  throw immediately.

## Guides

### Offline policies

`send` and `sendTyped` return synchronously with a `SendOutcome`:

| Policy | While not connected |
| --- | --- |
| `bufferDropOldest` (default) | Buffer. When full, evict the oldest frame. Returns `buffered`. |
| `bufferDropNew` | Buffer. When full, discard the new frame. Returns `dropped`. |
| `drop` | Discard. Returns `dropped`. Good for presence and telemetry. |
| `reject` | Throw `TypedSocketOfflineException`. You manage your own queue. |

```dart
final socket = TypedSocket(
  uri: uri,
  offlinePolicy: OfflinePolicy.bufferDropNew,
  bufferCapacity: 50,
);

// "3 messages waiting to send"
StreamBuilder<int>(
  stream: socket.pendingSendsChanges,
  initialData: socket.pendingSends,
  builder: (context, snapshot) => Text('${snapshot.data} waiting to send'),
);
```

Buffered frames flush in FIFO order after `onConnected` completes. If the
connection drops mid-flush, the unwritten frames stay buffered, in order.

> **Delivery is at-most-once.** `SendOutcome.sent` means the frame was handed
> to a live connection, not that the server received it. A frame written in
> the same instant the connection dies can be lost. If you need guaranteed
> delivery, add acknowledgements at the application level.

### Heartbeat

```dart
final socket = TypedSocket(
  uri: uri,
  heartbeat: const HeartbeatConfig(
    interval: Duration(seconds: 25), // ping after this much silence
    timeout: Duration(seconds: 10), // then give up if nothing arrives
  ),
);
```

Any inbound frame proves the connection is alive, so busy connections are
never pinged. Pongs are swallowed. Your server only needs to answer a ping:

```js
// Node, with the `ws` package
ws.on('message', (raw) => {
  const msg = JSON.parse(raw);
  if (msg.event === '__ping') return ws.send(JSON.stringify({ event: '__pong' }));
  // ... your events
});
```

### Authentication

Browsers cannot send handshake headers, so put credentials in the URI or in a
first message. `uriProvider` runs before every attempt, so tokens stay fresh
across reconnects:

```dart
final socket = TypedSocket(
  uriProvider: () async {
    final token = await auth.freshToken();
    return Uri.parse('wss://api.example.com/ws?token=$token');
  },
  onConnected: (ctx) async {
    // Runs on every connect, before buffered sends flush.
    ctx.send('subscribe', {'rooms': rooms.toList()});
  },
);

// After the user signs in again:
await socket.reconnect();
```

`onConnected` can also wait for the server to accept a hello. While it runs,
the state stays `connecting` and app sends are buffered. If it throws, the
attempt fails and backoff applies:

```dart
late final TypedSocket socket;
socket = TypedSocket(
  uri: uri,
  onConnected: (ctx) async {
    ctx.send('auth', {'token': await auth.freshToken()});
    await socket.unhandledFrames
        .firstWhere((e) => e.event == 'auth_ok')
        .timeout(const Duration(seconds: 5));
  },
);
```

On native platforms you can also send headers:
`WebSocketTransport(headers: {'Authorization': 'Bearer $token'})`. They are
ignored on the web.

### Custom envelopes

For a backend that sends `{"type": "...", "payload": ...}`:

```dart
final socket = TypedSocket(
  uri: uri,
  codec: const JsonEnvelopeCodec(eventKey: 'type', dataKey: 'payload'),
);
```

For anything else, implement `EnvelopeCodec` with `encode(Envelope)` and
`decode(Object frame)`. `decode` should throw `FormatException` for frames it
does not understand; they are reported on `frameErrors`.

### Observing failures

```dart
socket.frameErrors.listen((e) => log('bad frame: ${e.cause}'));
socket.connectionErrors.listen((e) => log('attempt ${e.attempt}: ${e.cause}'));
socket.unhandledFrames.listen((e) => log('no channel for ${e.event}'));
socket.stream<ChatMessage>('message').listen(
  onMessage,
  onError: (Object e) => log('bad message: $e'), // TypedSocketDecodeError
);
```

### Testing with `FakeTransport`

```dart
import 'package:typed_socket/testing.dart';

testWidgets('shows Reconnecting when the connection drops', (tester) async {
  final transport = FakeTransport();
  final socket = TypedSocket(uri: Uri.parse('ws://test'), transport: transport);
  await tester.pumpWidget(MaterialApp(home: ConnectionBadge(socket: socket)));

  socket.connect();
  await tester.pump();
  transport.lastConnection!
    ..receiveEvent('message', {'user': 'sam', 'text': 'hi'})
    ..kill(); // simulate a dropped connection
  await tester.pump();

  expect(find.text('Reconnecting'), findsOneWidget);
  socket.close();
});
```

`FakeTransport` also offers `failNextConnects`, `connectDelay`,
`receiveRaw`, `stall()` (a half-open connection), `killWithError`, and
`sentFrames` / `sentEnvelopes` for assertions. It works with `fake_async`.

## Examples

- [`example/main.dart`](example/main.dart): a CLI demo that kills and
  restarts a real server and shows buffered messages flushing in order.
- [`example/flutter_chat`](example/flutter_chat): a Flutter chat screen with a
  reusable `ConnectionBadge`, a "waiting to send" banner, and a button that
  cuts the connection so you can watch recovery.

## Non-goals for 0.1

- Not a Socket.IO, Phoenix, SignalR, or STOMP client. Protocol adapters are a
  0.2 topic.
- No binary codecs such as protobuf or MessagePack. The transport interface
  is binary-ready, but only the JSON codec ships.
- No guaranteed delivery. Sends are at-most-once. Acknowledgements and replay
  are a 0.3 topic.
- No custom handshake headers on the web, because browsers cannot send them.
  Use query parameters or subprotocols there; headers work on native
  platforms.

## Roadmap

- **0.2**: a Server-Sent Events transport, a `typed_socket_phoenix` adapter
  package, and Socket.IO if users ask for it.
- **0.3**: acknowledgements and replay for at-least-once delivery with
  deduplication, plus small Riverpod and Bloc companion packages.
