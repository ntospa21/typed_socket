import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:test/test.dart';
import 'package:typed_socket/testing.dart';
import 'package:typed_socket/typed_socket.dart';

import '../support/harness.dart';

const _heartbeat = HeartbeatConfig(
  interval: Duration(seconds: 10),
  timeout: Duration(seconds: 5),
);

Duration _s(num seconds) => Duration(milliseconds: (seconds * 1000).round());

void main() {
  late FakeTransport transport;
  late TypedSocket socket;

  FakeConnection connectNow(FakeAsync async) {
    socket.connect();
    async.flushMicrotasks();
    return transport.lastConnection!;
  }

  setUp(() {
    transport = FakeTransport();
    socket = fakeSocket(transport, heartbeat: _heartbeat);
  });

  test('pings only after the idle interval', () {
    fakeAsync((async) {
      final conn = connectNow(async);
      async.elapse(_s(9.999));
      expect(conn.sentFrames, isEmpty);
      async.elapse(_s(0.001));
      expect(conn.sentEvents, ['__ping']);
    });
  });

  test('inbound traffic resets the idle timer', () {
    fakeAsync((async) {
      final conn = connectNow(async);
      async.elapse(_s(8));
      conn.receiveEvent('chat', {'x': 1});
      async.elapse(_s(9.999));
      expect(conn.sentFrames, isEmpty);
      async.elapse(_s(0.001));
      expect(conn.sentEvents, ['__ping']);
    });
  });

  test('malformed inbound frames still count as liveness', () {
    fakeAsync((async) {
      final conn = connectNow(async);
      async.elapse(_s(8));
      conn.receiveRaw('garbage');
      async.elapse(_s(9.999));
      expect(conn.sentFrames, isEmpty);
    });
  });

  test('outbound traffic does not delay the ping', () {
    fakeAsync((async) {
      final conn = connectNow(async);
      async.elapse(_s(5));
      socket.send('chat', 1);
      async.elapse(_s(5));
      expect(conn.sentEvents, ['chat', '__ping']);
    });
  });

  test('pongs are swallowed and keep the connection alive', () {
    fakeAsync((async) {
      final unhandled = Recorder(socket.unhandledFrames);
      final conn = connectNow(async);
      async.elapse(_s(10)); // ping
      async.elapse(_s(2));
      conn.receiveEvent('__pong');
      async.elapse(_s(8)); // past the original timeout
      expect(socket.state, TypedSocketState.connected);
      expect(unhandled.values, isEmpty);
      async.elapse(_s(2)); // 10s after the pong
      expect(conn.sentEvents, ['__ping', '__ping']);
    });
  });

  test('a stalled connection is torn down and replaced', () {
    fakeAsync((async) {
      final errors = Recorder(socket.connectionErrors);
      final states = Recorder(socket.states);
      final conn = connectNow(async)..stall();
      async.elapse(_s(10)); // ping, no answer
      conn.receiveEvent('__pong'); // swallowed by the stall
      async.elapse(_s(4.999));
      expect(socket.state, TypedSocketState.connected);
      async.elapse(_s(0.001));

      expect(socket.state, TypedSocketState.reconnecting);
      expect(conn.closedByClient, isTrue);
      expect(conn.closeCode, 4000);
      expect(conn.closeReason, 'heartbeat timeout');
      expect(errors.values.single.cause, isA<TimeoutException>());

      async.elapse(oneSecond);
      expect(socket.state, TypedSocketState.connected);
      expect(transport.connections, hasLength(2));
      expect(states.values, contains(TypedSocketState.reconnecting));
    });
  });

  test('timers are cancelled on disconnect', () {
    fakeAsync((async) {
      final conn = connectNow(async);
      expect(async.pendingTimers, hasLength(1)); // idle timer
      conn.kill();
      async.flushMicrotasks();
      // Only the backoff timer remains.
      expect(async.pendingTimers, hasLength(1));
      expect(async.pendingTimers.single.duration, oneSecond);
    });
  });

  test('custom ping and pong event names are used', () {
    fakeAsync((async) {
      final unhandled = Recorder(socket.unhandledFrames);
      socket = fakeSocket(
        transport,
        heartbeat: const HeartbeatConfig(
          interval: Duration(seconds: 1),
          pingEvent: 'hb:ping',
          pongEvent: 'hb:pong',
        ),
      );
      final other = Recorder(socket.unhandledFrames);
      final conn = connectNow(async);
      async.elapse(oneSecond);
      conn
        ..receiveEvent('hb:pong')
        ..receiveEvent('__pong');
      async.flushMicrotasks();
      expect(conn.sentEvents, ['hb:ping']);
      expect([for (final e in other.values) e.event], ['__pong']);
      expect(unhandled.values, isEmpty);
    });
  });

  test('without a heartbeat nothing is pinged and pongs are ordinary', () {
    fakeAsync((async) {
      socket = fakeSocket(transport);
      final unhandled = Recorder(socket.unhandledFrames);
      final conn = connectNow(async);
      expect(async.pendingTimers, isEmpty);
      async.elapse(const Duration(minutes: 10));
      expect(conn.sentFrames, isEmpty);
      conn.receiveEvent('__pong');
      async.flushMicrotasks();
      expect(unhandled.values.single.event, '__pong');
    });
  });
}
