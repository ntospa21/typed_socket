import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:test/test.dart';
import 'package:typed_socket/testing.dart';
import 'package:typed_socket/typed_socket.dart';

import '../support/harness.dart';

void main() {
  test('close() while the transport is connecting', () {
    fakeAsync((async) {
      final transport = FakeTransport()..connectDelay = oneSecond;
      final socket = fakeSocket(transport);
      final connected = Outcome(socket.connect());
      async.elapse(const Duration(milliseconds: 500));

      socket.close();
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.closed);
      expect(connected.error, isA<TypedSocketClosedException>());

      // The in-flight connect lands after close: it must be shut, not used.
      async.elapse(oneSecond);
      final late = transport.connections.single;
      expect(late.closedByClient, isTrue);
      expect(late.hasListener, isFalse);
      expect(socket.state, TypedSocketState.closed);
      expect(async.pendingTimers, isEmpty);
    });
  });

  test('close() during backoff cancels the retry', () {
    fakeAsync((async) {
      final transport = FakeTransport()..failNextConnects = 1;
      final socket = fakeSocket(transport);
      socket.connect();
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.reconnecting);
      expect(async.pendingTimers, hasLength(1));

      socket.close();
      async.flushMicrotasks();
      expect(async.pendingTimers, isEmpty);
      async.elapse(const Duration(minutes: 1));
      expect(transport.connectCount, 1);
    });
  });

  test('close() while onConnected is running', () {
    fakeAsync((async) {
      final gate = Completer<void>();
      final transport = FakeTransport();
      final socket = fakeSocket(transport, onConnected: (ctx) => gate.future);
      socket.send('m', 1);
      socket.connect();
      async.flushMicrotasks();

      socket.close();
      gate.complete();
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.closed);
      expect(transport.lastConnection!.isClosed, isTrue);
      expect(transport.lastConnection!.sentFrames, isEmpty);
    });
  });

  test('a connection that drops while onConnected is running', () {
    fakeAsync((async) {
      final gate = Completer<void>();
      final backoffAttempts = <int>[];
      var calls = 0;
      final transport = FakeTransport();
      final socket = fakeSocket(
        transport,
        backoff: (attempt) {
          backoffAttempts.add(attempt);
          return oneSecond;
        },
        onConnected: (ctx) => ++calls == 1 ? gate.future : Future.value(),
      );
      socket.send('m', 'buffered');
      socket.connect();
      async.flushMicrotasks();

      final first = transport.lastConnection!..kill();
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.reconnecting);
      expect(backoffAttempts, [1], reason: 'counts as a failed attempt');

      // The stale hook finishing must not mark the socket connected or
      // flush onto the dead connection.
      gate.complete();
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.reconnecting);
      expect(first.sentFrames, isEmpty);
      expect(socket.pendingSends, 1);

      async.elapse(oneSecond);
      expect(socket.state, TypedSocketState.connected);
      expect(sentData(transport.lastConnection!), ['buffered']);
    });
  });

  test('reconnect() during a connect keeps one attempt in flight', () {
    fakeAsync((async) {
      final transport = FakeTransport()..connectDelay = oneSecond;
      final socket = fakeSocket(transport);
      socket.connect();
      async.elapse(const Duration(milliseconds: 500));

      final reconnected = Outcome(socket.reconnect());
      async.elapse(const Duration(milliseconds: 500));
      // The abandoned connect landed and was closed straight away.
      final abandoned = transport.connections.single;
      expect(abandoned.closedByClient, isTrue);
      expect(abandoned.hasListener, isFalse);
      expect(socket.state, TypedSocketState.connecting);

      async.elapse(oneSecond);
      expect(socket.state, TypedSocketState.connected);
      expect(reconnected.isComplete, isTrue);
      expect(transport.connections, hasLength(2));
      expect(transport.peakConnectsInFlight, 1);
    });
  });

  test('a stale onConnected finishing after reconnect() has no effect', () {
    fakeAsync((async) {
      final gate = Completer<void>();
      var calls = 0;
      final transport = FakeTransport();
      final socket = fakeSocket(
        transport,
        onConnected: (ctx) => ++calls == 1 ? gate.future : Future.value(),
      );
      final states = Recorder(socket.states);
      socket.connect();
      async.flushMicrotasks();
      socket.reconnect();
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.connected);
      final current = transport.lastConnection!;

      gate.complete();
      async.flushMicrotasks();
      expect(socket.state, TypedSocketState.connected);
      expect(current.isClosed, isFalse);
      expect(
        states.values.where((s) => s == TypedSocketState.connected),
        hasLength(1),
      );
    });
  });

  test("an old connection's done after reconnect() is ignored", () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport);
      socket.connect();
      async.flushMicrotasks();
      socket.reconnect();
      async.elapse(const Duration(minutes: 1));

      expect(transport.connections.first.isClosed, isTrue);
      expect(transport.connectCount, 2);
      expect(socket.state, TypedSocketState.connected);
    });
  });

  test('a kill after the socket already moved on is ignored', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport);
      socket.connect();
      async.flushMicrotasks();
      final old = transport.lastConnection!;
      socket.reconnect();
      async.flushMicrotasks();

      old.kill(); // already closed by the client: a no-op
      async.elapse(const Duration(minutes: 1));
      expect(transport.connectCount, 2);
      expect(socket.state, TypedSocketState.connected);
    });
  });
}
