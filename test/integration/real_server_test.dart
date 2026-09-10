@Tags(['integration'])
@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:typed_socket/typed_socket.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import '../fixtures/echo_server.dart';
import '../support/harness.dart';

const _fastRetry = Duration(milliseconds: 100);

/// Polls [condition] until it holds, failing after [timeout].
Future<void> waitUntil(
  bool Function() condition, {
  required String reason,
  Duration timeout = const Duration(seconds: 10),
}) async {
  final stopwatch = Stopwatch()..start();
  while (!condition()) {
    if (stopwatch.elapsed > timeout) fail('Timed out waiting for $reason');
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

List<Object?> receivedData(EchoServer server) => [
      for (final frame in server.received)
        (jsonDecode(frame) as Map<String, dynamic>)['data'],
    ];

void main() {
  late EchoServer server;
  late TypedSocket socket;
  late List<TypedSocketState> states;

  TypedSocket track(TypedSocket s) {
    states = [];
    s.states.listen(states.add);
    return socket = s;
  }

  /// Connects and waits until the server has registered the connection. The
  /// client's handshake can complete before the server's upgrade handler
  /// runs, so server-side counters lag slightly behind the client.
  Future<void> connectAndSettle({int expectedOpen = 1}) async {
    await socket.connect();
    await waitUntil(
      () => server.openConnections == expectedOpen,
      reason: 'the server to register the connection',
    );
  }

  setUp(() async {
    server = await EchoServer.start();
    track(TypedSocket(uri: server.uri, backoff: Backoff.fixed(_fastRetry)));
  });

  tearDown(() async {
    await socket.close();
    await server.close();
  });

  test('connects, sends, and receives typed messages', () async {
    socket.on<Chat>('message', Chat.fromJson, toJson: (c) => c.toJson());
    final echoed = socket.stream<Chat>('message').first;
    await socket.connect();
    expect(
      socket.sendTyped('message', const Chat('sam', 'hi')),
      SendOutcome.sent,
    );
    expect(
      await echoed.timeout(const Duration(seconds: 5)),
      const Chat('sam', 'hi'),
    );
  });

  test('reconnects after the server kills the connection', () async {
    await connectAndSettle();

    final killed = await server.killConnections();
    expect(killed, 1, reason: 'the kill must cut a live connection');

    await waitUntil(
      () => states.where((s) => s == TypedSocketState.connected).length == 2,
      reason: 'the second connection',
    );
    expect(
      states,
      containsAllInOrder([
        TypedSocketState.connected,
        TypedSocketState.reconnecting,
        TypedSocketState.connecting,
        TypedSocketState.connected,
      ]),
    );
    await waitUntil(() => server.upgrades == 2, reason: 'the second upgrade');

    socket.send('after', 1);
    await server.waitForReceived(1);
  });

  test('buffers while the server is down and flushes in order', () async {
    await connectAndSettle();
    await server.stop();
    expect(server.killedConnections, 1, reason: 'stop must cut the socket');

    await waitUntil(
      () => socket.state != TypedSocketState.connected,
      reason: 'the client to notice the outage',
    );
    expect(states, contains(TypedSocketState.reconnecting));
    for (var i = 1; i <= 5; i++) {
      expect(socket.send('seq', i), SendOutcome.buffered);
    }
    expect(socket.pendingSends, 5);

    await server.restart();
    await server.waitForReceived(5);
    expect(receivedData(server), [1, 2, 3, 4, 5]);
    expect(socket.pendingSends, 0);
  });

  test('the heartbeat detects a stalled server', () async {
    track(
      TypedSocket(
        uri: server.uri,
        backoff: Backoff.fixed(_fastRetry),
        heartbeat: const HeartbeatConfig(
          interval: Duration(milliseconds: 200),
          timeout: Duration(milliseconds: 300),
        ),
      ),
    );
    final errors = Recorder(socket.connectionErrors);
    await connectAndSettle();
    await waitUntil(() => server.pingsAnswered > 0, reason: 'a pong');
    expect(socket.state, TypedSocketState.connected);

    server.stalled = true;
    await waitUntil(
      () => states.contains(TypedSocketState.reconnecting),
      reason: 'the heartbeat timeout',
    );
    expect(
      server.pingsIgnored,
      greaterThan(0),
      reason: 'the stall must actually swallow a ping',
    );
    expect(
      server.killedConnections,
      0,
      reason: 'the client, not the server, must cut the connection',
    );
    expect(
      errors.values.map((e) => e.cause),
      contains(isA<TimeoutException>()),
    );
    await waitUntil(
      () => server.closeCodes.contains(4000),
      reason: 'the heartbeat close code to reach the server',
    );

    server.stalled = false;
    await waitUntil(
      () => socket.state == TypedSocketState.connected && server.upgrades >= 2,
      reason: 'recovery after the stall',
    );
  });

  test('refused upgrades fail the attempt and are retried', () async {
    server.refuseUpgrades = true;
    final errors = Recorder(socket.connectionErrors);
    final connected = socket.connect();

    await waitUntil(
      () => server.refusedUpgrades >= 2,
      reason: 'repeated refused upgrades',
    );
    expect(socket.state, isNot(TypedSocketState.connected));
    expect(states, contains(TypedSocketState.reconnecting));
    expect(errors.values.first.cause, isA<WebSocketChannelException>());

    server.refuseUpgrades = false;
    await connected.timeout(const Duration(seconds: 5));
    await waitUntil(() => server.upgrades == 1, reason: 'the upgrade');
  });

  test('garbage frames are isolated and the connection stays up', () async {
    socket.on<Chat>('message', Chat.fromJson);
    final chats = Recorder(socket.stream<Chat>('message'));
    final frameErrors = Recorder(socket.frameErrors);
    await connectAndSettle();

    server
      ..broadcastRaw('not json')
      ..broadcastRaw(Uint8List.fromList([1, 2, 3]))
      ..broadcast('message', {'user': 1});
    socket.send('message', {'user': 'a', 'text': 'b'}); // echoed back

    await waitUntil(() => chats.values.isNotEmpty, reason: 'the echo');
    expect(frameErrors.values, hasLength(2));
    expect(frameErrors.values[0].raw, 'not json');
    expect(frameErrors.values[1].raw, isA<Uint8List>());
    expect(chats.errors.single, isA<TypedSocketDecodeError>());
    expect(chats.values, [const Chat('a', 'b')]);
    expect(states, isNot(contains(TypedSocketState.reconnecting)));
    expect(server.upgrades, 1);
  });

  test('a connection cut mid-frame is recovered', () async {
    final frameErrors = Recorder(socket.frameErrors);
    server.cutMidMessage = true;
    unawaited(socket.connect());

    await waitUntil(
      () =>
          server.midMessageCuts >= 1 &&
          states.contains(TypedSocketState.reconnecting),
      reason: 'the mid-frame cut',
    );

    server.cutMidMessage = false;
    await waitUntil(
      () => socket.state == TypedSocketState.connected && server.upgrades == 1,
      reason: 'recovery after the cut',
    );
    expect(
      frameErrors.values,
      isEmpty,
      reason: 'a partial frame must never be delivered',
    );
  });

  test('uriProvider, headers, and protocols reach the server', () async {
    var token = 0;
    track(
      TypedSocket(
        uriProvider: () =>
            server.uri.replace(queryParameters: {'token': '${++token}'}),
        transport: const WebSocketTransport(
          headers: {'x-auth': 'secret'},
          protocols: ['chat.v1'],
        ),
        backoff: Backoff.fixed(_fastRetry),
      ),
    );
    await connectAndSettle();
    expect(await server.killConnections(), 1);
    await waitUntil(() => server.upgrades == 2, reason: 'the reconnect');

    expect(
      [for (final uri in server.requestUris) uri.queryParameters['token']],
      ['1', '2'],
    );
    expect(server.requestHeaders.first['x-auth'], ['secret']);
    expect(server.requestHeaders.first['sec-websocket-protocol'], ['chat.v1']);
  });

  test('close() sends a normal close frame', () async {
    await connectAndSettle();
    await socket.close();
    await waitUntil(() => server.closeCodes.isNotEmpty, reason: 'close');
    expect(server.closeCodes, [1000]);
    expect(server.openConnections, 0);
  });

  test('a handshake that never completes times out', () async {
    final silent = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final accepted = <Socket>[];
    silent.listen(accepted.add);
    addTearDown(() async {
      for (final s in accepted) {
        s.destroy();
      }
      await silent.close();
    });

    track(
      TypedSocket(
        uri: Uri.parse('ws://127.0.0.1:${silent.port}/ws'),
        transport: const WebSocketTransport(
          connectTimeout: Duration(milliseconds: 300),
        ),
        maxReconnectAttempts: 0,
      ),
    );
    final errors = Recorder(socket.connectionErrors);
    final stopwatch = Stopwatch()..start();
    await expectLater(
      socket.connect(),
      throwsA(isA<TypedSocketClosedException>()),
    );
    expect(stopwatch.elapsed, lessThan(const Duration(seconds: 5)));
    expect(accepted, isNotEmpty, reason: 'TCP connected, handshake stalled');
    expect(
      errors.values.single.cause,
      anyOf(isA<TimeoutException>(), isA<WebSocketChannelException>()),
    );
  });

  test('a refused TCP connection fails the attempt', () async {
    final port = server.uri.port;
    await server.stop();
    track(
      TypedSocket(
        uri: Uri.parse('ws://127.0.0.1:$port/ws'),
        maxReconnectAttempts: 1,
        backoff: Backoff.fixed(_fastRetry),
      ),
    );
    await expectLater(
      socket.connect(),
      throwsA(isA<TypedSocketClosedException>()),
    );
    expect(socket.closeReason, CloseReason.attemptsExhausted);
  });
}
