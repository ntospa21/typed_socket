import 'package:fake_async/fake_async.dart';
import 'package:test/test.dart';
import 'package:typed_socket/testing.dart';
import 'package:typed_socket/typed_socket.dart';

import '../support/harness.dart';

void main() {
  test('goes through the exact state sequence', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport);
      final states = Recorder(socket.states);

      socket.connect();
      async.flushMicrotasks();
      transport.lastConnection!.kill();
      async.flushMicrotasks();
      async.elapse(oneSecond);
      socket.close();
      async.flushMicrotasks();

      expect(states.values, [
        TypedSocketState.idle,
        TypedSocketState.connecting,
        TypedSocketState.connected,
        TypedSocketState.reconnecting,
        TypedSocketState.connecting,
        TypedSocketState.connected,
        TypedSocketState.closed,
      ]);
      expect(states.isDone, isTrue);
    });
  });

  test('states replays the current state to every new listener', () {
    fakeAsync((async) {
      final socket = fakeSocket(FakeTransport());
      socket.connect();
      async.flushMicrotasks();

      final late = Recorder(socket.states);
      async.flushMicrotasks();
      expect(late.values, [TypedSocketState.connected]);
      expect(socket.states, same(socket.states), reason: 'stable identity');

      socket.close();
      async.flushMicrotasks();
      expect(
          late.values, [TypedSocketState.connected, TypedSocketState.closed]);

      final afterClose = Recorder(socket.states);
      async.flushMicrotasks();
      expect(afterClose.values, [TypedSocketState.closed]);
      expect(afterClose.isDone, isTrue);
    });
  });

  test('states never repeats a value consecutively', () {
    fakeAsync((async) {
      final transport = FakeTransport()..failNextConnects = 2;
      final socket = fakeSocket(transport);
      final states = Recorder(socket.states);

      socket
        ..connect()
        ..reconnect()
        ..reconnect();
      async.elapse(const Duration(seconds: 5));
      socket.reconnect();
      async.flushMicrotasks();
      transport.lastConnection!.kill();
      async.elapse(const Duration(seconds: 5));
      socket.close();
      async.flushMicrotasks();

      for (var i = 1; i < states.values.length; i++) {
        expect(
          states.values[i],
          isNot(states.values[i - 1]),
          reason: 'duplicate at $i in ${states.values}',
        );
      }
      expect(states.values.last, TypedSocketState.closed);
    });
  });

  test('connect() is idempotent and completes once connected', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport);
      final first = Outcome(socket.connect());
      final second = Outcome(socket.connect());
      expect(socket.state, TypedSocketState.connecting);
      expect(first.isComplete, isFalse);

      async.flushMicrotasks();
      expect(first.isComplete, isTrue);
      expect(second.isComplete, isTrue);
      expect(first.error, isNull);

      final third = Outcome(socket.connect());
      async.flushMicrotasks();
      expect(third.isComplete, isTrue);
      expect(transport.connectCount, 1);
    });
  });

  test('the first connect does not wait for backoff', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(
        transport,
        backoff: Backoff.fixed(const Duration(hours: 1)),
      );
      socket.connect();
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.connected);
      expect(async.elapsed, Duration.zero);
    });
  });

  test('close() is terminal', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport)
        ..on<Chat>('chat', Chat.fromJson, toJson: (c) => c.toJson());
      socket.connect();
      async.flushMicrotasks();

      final closed = Outcome(socket.close());
      final done = Outcome(socket.done);
      expect(socket.state, TypedSocketState.closed);
      async.flushMicrotasks();

      expect(closed.isComplete, isTrue);
      expect(done.value, CloseReason.userClosed);
      expect(socket.closeReason, CloseReason.userClosed);
      expect(transport.lastConnection!.closedByClient, isTrue);
      expect(transport.lastConnection!.closeCode, 1000);

      expect(socket.connect, throwsStateError);
      expect(socket.reconnect, throwsStateError);
      expect(() => socket.send('x', 1), throwsStateError);
      expect(
        () => socket.sendTyped('chat', const Chat('a', 'b')),
        throwsStateError,
      );
      expect(() => socket.on<int>('n', (j) => 1), throwsStateError);
      expect(() => socket.stream<Chat>('chat'), throwsStateError);

      // close() stays safe to call, for dispose() methods.
      expect(socket.close(), completes);
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.closed);
    });
  });

  test('close() from idle', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport);
      final states = Recorder(socket.states);
      socket.close();
      async.flushMicrotasks();
      expect(states.values, [TypedSocketState.idle, TypedSocketState.closed]);
      expect(transport.connectCount, 0);
      expect(socket.connect, throwsStateError);
    });
  });

  test('connect() fails with TypedSocketClosedException if closed first', () {
    fakeAsync((async) {
      final socket = fakeSocket(
        FakeTransport()..connectDelay = oneSecond,
      );
      final connecting = Outcome(socket.connect());
      socket.close();
      async.flushMicrotasks();
      expect(
        connecting.error,
        isA<TypedSocketClosedException>()
            .having((e) => e.reason, 'reason', CloseReason.userClosed),
      );
      expect(connecting.error.toString(), contains('userClosed'));
      async.elapse(oneSecond);
    });
  });

  test('an unawaited failing connect() does not surface an unhandled error',
      () {
    fakeAsync((async) {
      final socket = fakeSocket(
        FakeTransport()..failNextConnects = 1,
        maxReconnectAttempts: 0,
      );
      socket.connect(); // deliberately not awaited
      async.flushMicrotasks();
      expect(socket.closeReason, CloseReason.attemptsExhausted);
    });
  });

  group('constructor validation', () {
    test('needs exactly one of uri or uriProvider', () {
      expect(
          () => TypedSocket(transport: FakeTransport()), throwsArgumentError);
      expect(
        () => TypedSocket(
          uri: testUri,
          uriProvider: () => testUri,
          transport: FakeTransport(),
        ),
        throwsArgumentError,
      );
    });

    test('rejects a negative maxReconnectAttempts', () {
      expect(
        () => fakeSocket(FakeTransport(), maxReconnectAttempts: -1),
        throwsArgumentError,
      );
    });

    test('rejects a buffer capacity below 1', () {
      expect(
        () => fakeSocket(FakeTransport(), bufferCapacity: 0),
        throwsArgumentError,
      );
    });

    test('rejects unusable heartbeat timings', () {
      expect(
        () => fakeSocket(
          FakeTransport(),
          heartbeat: const HeartbeatConfig(interval: Duration.zero),
        ),
        throwsArgumentError,
      );
      expect(
        () => fakeSocket(
          FakeTransport(),
          heartbeat: const HeartbeatConfig(timeout: Duration.zero),
        ),
        throwsArgumentError,
      );
    });

    test('defaults to the WebSocket transport', () {
      final socket = TypedSocket(uri: Uri.parse('ws://localhost:1'));
      expect(socket.state, TypedSocketState.idle);
      expect(socket.close(), completes);
    });
  });
}
