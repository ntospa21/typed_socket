import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:test/test.dart';
import 'package:typed_socket/testing.dart';
import 'package:typed_socket/typed_socket.dart';

import '../support/harness.dart';

void main() {
  test('runs on every connect, and its frames precede buffered ones', () {
    fakeAsync((async) {
      final attempts = <int>[];
      final transport = FakeTransport();
      final socket = fakeSocket(
        transport,
        onConnected: (ctx) async {
          attempts.add(ctx.attempt);
          ctx.send('hello', {'attempt': ctx.attempt});
        },
      );

      expect(socket.send('queued', 1), SendOutcome.buffered);
      socket.connect();
      async.flushMicrotasks();
      final first = transport.lastConnection!;
      expect(first.sentEvents, ['hello', 'queued']);

      first.kill();
      async.flushMicrotasks();
      socket.send('queued', 2);
      async.elapse(oneSecond);
      final second = transport.lastConnection!;
      expect(second.sentEvents, ['hello', 'queued']);
      expect(sentData(second), [
        {'attempt': 1},
        2,
      ]);
      expect(attempts, [0, 1]);
    });
  });

  test('state stays connecting and app sends are buffered while it runs', () {
    fakeAsync((async) {
      final gate = Completer<void>();
      final transport = FakeTransport();
      final socket = fakeSocket(transport, onConnected: (ctx) => gate.future);
      final connected = Outcome(socket.connect());
      async.flushMicrotasks();

      final conn = transport.lastConnection!;
      expect(socket.state, TypedSocketState.connecting);
      expect(connected.isComplete, isFalse);
      expect(socket.send('app', 1), SendOutcome.buffered);
      expect(conn.sentFrames, isEmpty);

      gate.complete();
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.connected);
      expect(connected.isComplete, isTrue);
      expect(conn.sentEvents, ['app']);
    });
  });

  test('can wait for a server reply before the socket is connected', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      late final TypedSocket socket;
      socket = fakeSocket(
        transport,
        onConnected: (ctx) async {
          ctx.send('auth', {'token': 't'});
          await socket.unhandledFrames.firstWhere((e) => e.event == 'authed');
        },
      );
      socket.connect();
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.connecting);

      transport.lastConnection!.receiveEvent('authed');
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.connected);
    });
  });

  test('if it throws, the attempt fails and backoff applies', () {
    fakeAsync((async) {
      final backoffAttempts = <int>[];
      var calls = 0;
      final transport = FakeTransport();
      final socket = fakeSocket(
        transport,
        backoff: (attempt) {
          backoffAttempts.add(attempt);
          return oneSecond;
        },
        onConnected: (ctx) async {
          if (++calls == 1) throw StateError('hello rejected');
        },
      );
      final errors = Recorder(socket.connectionErrors);
      final connected = Outcome(socket.connect());
      async.flushMicrotasks();

      expect(socket.state, TypedSocketState.reconnecting);
      expect(transport.connections.single.closedByClient, isTrue);
      expect(errors.values.single.cause, isA<StateError>());
      expect(connected.isComplete, isFalse);
      expect(backoffAttempts, [1]);

      async.elapse(oneSecond);
      expect(socket.state, TypedSocketState.connected);
      expect(connected.isComplete, isTrue);
      expect(transport.connections, hasLength(2));
    });
  });

  test('a hook that always throws exhausts maxReconnectAttempts', () {
    fakeAsync((async) {
      final socket = fakeSocket(
        FakeTransport(),
        maxReconnectAttempts: 2,
        onConnected: (ctx) async => throw StateError('nope'),
      );
      socket.connect();
      async.elapse(const Duration(seconds: 5));
      expect(socket.closeReason, CloseReason.attemptsExhausted);
    });
  });

  test('ConnectedContext.send throws once its connection is gone', () {
    fakeAsync((async) {
      final gate = Completer<void>();
      ConnectedContext? captured;
      final transport = FakeTransport();
      final socket = fakeSocket(
        transport,
        onConnected: (ctx) {
          captured ??= ctx;
          return captured == ctx ? gate.future : Future<void>.value();
        },
      );
      socket.connect();
      async.flushMicrotasks();

      transport.lastConnection!.kill();
      async.flushMicrotasks();
      expect(() => captured!.send('late', 1), throwsStateError);
      gate.complete();
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.reconnecting);
    });
  });
}
