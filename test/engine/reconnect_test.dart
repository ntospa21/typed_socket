import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:test/test.dart';
import 'package:typed_socket/testing.dart';
import 'package:typed_socket/typed_socket.dart';

import '../support/harness.dart';

void main() {
  /// A fixed one-second policy that records the attempt numbers it is
  /// asked for.
  BackoffPolicy recording(List<int> attempts) => (attempt) {
        attempts.add(attempt);
        return oneSecond;
      };

  test('reconnects automatically after the connection drops', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(
        transport,
        backoff: Backoff.fixed(const Duration(seconds: 2)),
      );
      final states = Recorder(socket.states);
      socket.connect();
      async.flushMicrotasks();

      transport.lastConnection!.kill();
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.reconnecting);

      async.elapse(const Duration(milliseconds: 1999));
      expect(transport.connectCount, 1, reason: 'must wait for backoff');
      async.elapse(const Duration(milliseconds: 1));
      expect(socket.state, TypedSocketState.connected);
      expect(transport.connections, hasLength(2));
      expect(states.values, contains(TypedSocketState.reconnecting));
    });
  });

  test('retries until a connect succeeds', () {
    fakeAsync((async) {
      final attempts = <int>[];
      final transport = FakeTransport()..failNextConnects = 3;
      final socket = fakeSocket(transport, backoff: recording(attempts));
      final errors = Recorder(socket.connectionErrors);
      final connected = Outcome(socket.connect());

      async.elapse(const Duration(seconds: 2));
      expect(connected.isComplete, isFalse);
      async.elapse(oneSecond);

      expect(connected.isComplete, isTrue);
      expect(socket.state, TypedSocketState.connected);
      expect(attempts, [1, 2, 3]);
      expect(transport.connectCount, 4);
      expect([for (final e in errors.values) e.attempt], [0, 1, 2]);
      expect(errors.values.first.cause, isA<FakeConnectException>());
      expect(errors.values.first.toString(), contains('attempt: 0'));
    });
  });

  test('gives up after maxReconnectAttempts', () {
    fakeAsync((async) {
      final transport = FakeTransport()..failNextConnects = 100;
      final socket = fakeSocket(transport, maxReconnectAttempts: 2);
      final connected = Outcome(socket.connect());
      final done = Outcome(socket.done);

      async.elapse(const Duration(seconds: 10));
      expect(socket.state, TypedSocketState.closed);
      expect(socket.closeReason, CloseReason.attemptsExhausted);
      expect(done.value, CloseReason.attemptsExhausted);
      expect(transport.connectCount, 3, reason: 'initial attempt + 2 retries');
      expect(
        connected.error,
        isA<TypedSocketClosedException>()
            .having((e) => e.reason, 'reason', CloseReason.attemptsExhausted),
      );
      expect(async.pendingTimers, isEmpty);
    });
  });

  test('maxReconnectAttempts: 0 closes on the first failure', () {
    fakeAsync((async) {
      final transport = FakeTransport()..failNextConnects = 1;
      final socket = fakeSocket(transport, maxReconnectAttempts: 0);
      final states = Recorder(socket.states);
      socket.connect();
      async.flushMicrotasks();
      expect(states.values, [
        TypedSocketState.idle,
        TypedSocketState.connecting,
        TypedSocketState.closed,
      ]);
    });
  });

  test('gives up after maxReconnectAttempts following a drop', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport, maxReconnectAttempts: 2);
      socket.connect();
      async.flushMicrotasks();

      transport
        ..failNextConnects = 100
        ..lastConnection!.kill();
      async.elapse(const Duration(seconds: 10));
      expect(socket.closeReason, CloseReason.attemptsExhausted);
      expect(transport.connectCount, 3, reason: 'initial connect + 2 retries');
    });
  });

  test('the attempt counter resets once connected', () {
    fakeAsync((async) {
      final attempts = <int>[];
      final transport = FakeTransport();
      final socket = fakeSocket(transport, backoff: recording(attempts));
      socket.connect();
      async.flushMicrotasks();

      transport.lastConnection!.kill();
      async.elapse(oneSecond);
      transport.lastConnection!.kill();
      async.elapse(oneSecond);
      expect(attempts, [1, 1]);
      expect(socket.state, TypedSocketState.connected);
    });
  });

  test('typed streams survive reconnects', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport)..on<Chat>('message', Chat.fromJson);
      final chats = Recorder(socket.stream<Chat>('message'));
      socket.connect();
      async.flushMicrotasks();

      transport.lastConnection!
        ..receiveEvent('message', {'user': 'a', 'text': 'before'})
        ..kill();
      async.elapse(oneSecond);
      transport.lastConnection!
          .receiveEvent('message', {'user': 'a', 'text': 'after'});
      async.flushMicrotasks();

      expect(transport.connections, hasLength(2));
      expect(chats.values, [
        const Chat('a', 'before'),
        const Chat('a', 'after'),
      ]);
      expect(chats.isDone, isFalse);
    });
  });

  test('reconnect() from connected drops and reconnects immediately', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(
        transport,
        backoff: Backoff.fixed(const Duration(hours: 1)),
      );
      socket.connect();
      async.flushMicrotasks();
      final states = Recorder(socket.states);
      async.flushMicrotasks();

      final reconnected = Outcome(socket.reconnect());
      async.flushMicrotasks();

      expect(reconnected.isComplete, isTrue);
      expect(states.values, [
        TypedSocketState.connected,
        TypedSocketState.reconnecting,
        TypedSocketState.connecting,
        TypedSocketState.connected,
      ]);
      expect(transport.connections.first.closedByClient, isTrue);
      expect(transport.connections, hasLength(2));
      expect(async.elapsed, Duration.zero);
    });
  });

  test('reconnect() resets the attempt counter', () {
    fakeAsync((async) {
      final attempts = <int>[];
      final transport = FakeTransport()..failNextConnects = 2;
      final socket = fakeSocket(transport, backoff: recording(attempts));
      socket.connect();
      async.flushMicrotasks();
      expect(attempts, [1]);
      expect(socket.state, TypedSocketState.reconnecting);

      // Mid-backoff: retry now instead of waiting.
      socket.reconnect();
      async.flushMicrotasks();
      expect(transport.connectCount, 2, reason: 'no backoff wait');
      expect(attempts, [1, 1], reason: 'counter restarted from 0');

      async.elapse(oneSecond);
      expect(socket.state, TypedSocketState.connected);
    });
  });

  test('reconnect() before connect() behaves like connect()', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport);
      final reconnected = Outcome(socket.reconnect());
      final connected = Outcome(socket.connect());
      async.flushMicrotasks();
      expect(reconnected.isComplete, isTrue);
      expect(connected.isComplete, isTrue);
      expect(transport.connectCount, 1);
    });
  });

  test('reconnect() fails with TypedSocketClosedException if closed first', () {
    fakeAsync((async) {
      final transport = FakeTransport()..connectDelay = oneSecond;
      final socket = fakeSocket(transport);
      final reconnected = Outcome(socket.reconnect());
      socket.close();
      async.elapse(oneSecond);
      expect(reconnected.error, isA<TypedSocketClosedException>());
    });
  });

  test('uriProvider is called before every attempt', () {
    fakeAsync((async) {
      var calls = 0;
      final transport = FakeTransport()..failNextConnects = 1;
      final socket = fakeSocket(
        transport,
        uriProvider: () => Uri.parse('ws://test.local/?token=${++calls}'),
      );
      socket.connect();
      async.elapse(oneSecond);
      transport.lastConnection!.kill();
      async.elapse(oneSecond);

      expect(calls, 3);
      expect(
        [
          for (final uri in transport.requestedUris)
            uri.queryParameters['token']
        ],
        ['1', '2', '3'],
      );
      expect(socket.state, TypedSocketState.connected);
    });
  });

  test('an async uriProvider works and a throwing one fails the attempt', () {
    fakeAsync((async) {
      var calls = 0;
      final transport = FakeTransport();
      final socket = fakeSocket(
        transport,
        uriProvider: () async {
          calls++;
          if (calls == 1) throw StateError('token refresh failed');
          return testUri;
        },
      );
      final errors = Recorder(socket.connectionErrors);
      socket.connect();
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.reconnecting);
      expect(errors.values.single.cause, isA<StateError>());
      expect(transport.connectCount, 0);

      async.elapse(oneSecond);
      expect(socket.state, TypedSocketState.connected);
    });
  });

  test('a transport stream error is reported and triggers a reconnect', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport);
      final errors = Recorder(socket.connectionErrors);
      socket.connect();
      async.flushMicrotasks();

      transport.lastConnection!.killWithError(StateError('network down'));
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.reconnecting);
      expect(errors.values.single.cause, isA<StateError>());

      async.elapse(oneSecond);
      expect(socket.state, TypedSocketState.connected);
      expect(errors.values, hasLength(1));
    });
  });

  test('a clean remote close is not reported as an error', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport);
      final errors = Recorder(socket.connectionErrors);
      socket.connect();
      async.flushMicrotasks();
      transport.lastConnection!.kill(code: 1000);
      async.elapse(oneSecond);
      expect(errors.values, isEmpty);
      expect(transport.connections, hasLength(2));
    });
  });

  test('uses exponential jitter by default', () {
    fakeAsync((async) {
      final transport = FakeTransport()..failNextConnects = 1;
      final socket = TypedSocket(uri: testUri, transport: transport);
      socket.connect();
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.reconnecting);
      // The first retry waits at most base = 500ms.
      async.elapse(const Duration(milliseconds: 500));
      expect(socket.state, TypedSocketState.connected);
      unawaited(socket.close());
      async.flushMicrotasks();
    });
  });
}
