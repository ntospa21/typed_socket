import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:test/test.dart';
import 'package:typed_socket/testing.dart';
import 'package:typed_socket/typed_socket.dart';

import '../support/harness.dart';

/// Closes [socket] in whatever state [arrange] leaves it, then asserts that
/// nothing is left behind: no timers, no open streams, no subscriptions.
void expectCleanClose(
  void Function(FakeAsync async, TypedSocket socket, FakeTransport transport)
      arrange, {
  OnConnected? onConnected,
}) {
  fakeAsync((async) {
    final transport = FakeTransport();
    final socket = fakeSocket(
      transport,
      heartbeat: const HeartbeatConfig(),
      onConnected: onConnected,
    )..on<Chat>('chat', Chat.fromJson);

    final recorders = <Recorder<Object?>>[
      Recorder(socket.states),
      Recorder(socket.pendingSendsChanges),
      Recorder(socket.unhandledFrames),
      Recorder(socket.frameErrors),
      Recorder(socket.connectionErrors),
      Recorder(socket.stream<Chat>('chat')),
    ];

    arrange(async, socket, transport);
    socket.close();
    async.flushMicrotasks();
    // A delayed fake connect may still be in flight; let it land.
    async.elapse(transport.connectDelay);

    expect(async.pendingTimers, isEmpty, reason: 'leaked timers');
    for (final recorder in recorders) {
      expect(recorder.isDone, isTrue, reason: 'a stream was left open');
    }
    for (final conn in transport.connections) {
      expect(conn.isClosed, isTrue, reason: 'connection left open');
      expect(conn.hasListener, isFalse, reason: 'subscription leaked');
    }
    expect(socket.pendingSends, 0);
  });
}

void main() {
  test('closing while connected with a heartbeat', () {
    expectCleanClose((async, socket, transport) {
      socket.connect();
      async.elapse(const Duration(seconds: 25)); // ping sent, timeout armed
      socket.send('m', 1);
      transport.lastConnection!.receiveEvent('chat', {'user': 'a'});
      async.flushMicrotasks();
    });
  });

  test('closing during backoff', () {
    expectCleanClose((async, socket, transport) {
      transport.failNextConnects = 1;
      socket.connect();
      socket.send('m', 1);
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.reconnecting);
    });
  });

  test('closing while the transport is connecting', () {
    expectCleanClose((async, socket, transport) {
      transport.connectDelay = oneSecond;
      socket.connect();
      async.elapse(const Duration(milliseconds: 10));
    });
  });

  test('closing while onConnected is running', () {
    final gate = Completer<void>();
    expectCleanClose(
      (async, socket, transport) {
        socket.connect();
        async.flushMicrotasks();
        expect(socket.state, TypedSocketState.connecting);
      },
      onConnected: (ctx) => gate.future,
    );
  });

  test('closing after many reconnects', () {
    expectCleanClose((async, socket, transport) {
      socket.connect();
      for (var i = 0; i < 20; i++) {
        async.flushMicrotasks();
        transport.lastConnection!.kill();
        async.elapse(oneSecond);
      }
      expect(transport.connections, hasLength(21));
    });
  });

  test('exhausting attempts cleans up too', () {
    fakeAsync((async) {
      final transport = FakeTransport()..failNextConnects = 10;
      final socket = fakeSocket(transport, maxReconnectAttempts: 3);
      final states = Recorder(socket.states);
      socket.connect();
      async.elapse(const Duration(seconds: 10));
      expect(socket.closeReason, CloseReason.attemptsExhausted);
      expect(async.pendingTimers, isEmpty);
      expect(states.isDone, isTrue);
    });
  });
}
