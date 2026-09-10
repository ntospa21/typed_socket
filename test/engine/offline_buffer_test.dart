import 'package:fake_async/fake_async.dart';
import 'package:test/test.dart';
import 'package:typed_socket/testing.dart';
import 'package:typed_socket/typed_socket.dart';

import '../support/harness.dart';

void main() {
  /// Connects [socket], drops the connection, and returns once the socket is
  /// waiting in backoff.
  void connectThenDrop(
    FakeAsync async,
    TypedSocket socket,
    FakeTransport transport,
  ) {
    socket.connect();
    async.flushMicrotasks();
    transport.lastConnection!.kill();
    async.flushMicrotasks();
    expect(socket.state, TypedSocketState.reconnecting);
  }

  test('flushes in FIFO order after reconnecting', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport);
      connectThenDrop(async, socket, transport);

      for (var i = 1; i <= 3; i++) {
        expect(socket.send('m', i), SendOutcome.buffered);
      }
      expect(socket.pendingSends, 3);

      async.elapse(oneSecond);
      expect(sentData(transport.lastConnection!), [1, 2, 3]);
      expect(socket.pendingSends, 0);
    });
  });

  test('sends before connect() are buffered and flushed', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport);
      expect(socket.send('m', 'early'), SendOutcome.buffered);
      socket.connect();
      async.flushMicrotasks();
      expect(sentData(transport.lastConnection!), ['early']);
    });
  });

  test('sends while connected go straight out', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport);
      socket.connect();
      async.flushMicrotasks();
      expect(socket.send('m', 1), SendOutcome.sent);
      expect(socket.pendingSends, 0);
      expect(sentData(transport.lastConnection!), [1]);
    });
  });

  test('bufferDropOldest evicts the oldest frames at capacity', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport, bufferCapacity: 3);
      final outcomes = [for (var i = 1; i <= 5; i++) socket.send('m', i)];
      expect(outcomes, everyElement(SendOutcome.buffered));
      expect(socket.pendingSends, 3);
      socket.connect();
      async.flushMicrotasks();
      expect(sentData(transport.lastConnection!), [3, 4, 5]);
    });
  });

  test('bufferDropNew discards new frames at capacity', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(
        transport,
        offlinePolicy: OfflinePolicy.bufferDropNew,
        bufferCapacity: 3,
      );
      final outcomes = [for (var i = 1; i <= 5; i++) socket.send('m', i)];
      expect(outcomes, [
        SendOutcome.buffered,
        SendOutcome.buffered,
        SendOutcome.buffered,
        SendOutcome.dropped,
        SendOutcome.dropped,
      ]);
      socket.connect();
      async.flushMicrotasks();
      expect(sentData(transport.lastConnection!), [1, 2, 3]);
    });
  });

  test('drop discards everything sent while disconnected', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport, offlinePolicy: OfflinePolicy.drop);
      expect(socket.send('m', 1), SendOutcome.dropped);
      expect(socket.pendingSends, 0);
      socket.connect();
      async.flushMicrotasks();
      expect(transport.lastConnection!.sentFrames, isEmpty);
      expect(socket.send('m', 2), SendOutcome.sent);
    });
  });

  test('reject throws while disconnected and sends while connected', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport, offlinePolicy: OfflinePolicy.reject);
      expect(
        () => socket.send('m', 1),
        throwsA(isA<TypedSocketOfflineException>()),
      );
      socket.connect();
      async.flushMicrotasks();
      expect(socket.send('m', 2), SendOutcome.sent);

      transport.lastConnection!.kill();
      async.flushMicrotasks();
      expect(
        () => socket.send('m', 3),
        throwsA(
          isA<TypedSocketOfflineException>()
              .having((e) => e.event, 'event', 'm'),
        ),
      );
      expect(socket.pendingSends, 0);
    });
  });

  test('an interrupted flush keeps the unwritten frames in order', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport, bufferCapacity: 10);
      for (var i = 1; i <= 5; i++) {
        socket.send('m', i);
      }
      // Kill the first connection right after it accepts its second frame.
      transport.onConnect = (conn) {
        if (transport.connections.length > 1) return;
        conn.onSend = (_) {
          if (conn.sentFrames.length == 2) conn.kill();
        };
      };
      socket.connect();
      async.flushMicrotasks();

      expect(sentData(transport.connections.first), [1, 2]);
      expect(socket.pendingSends, 3);
      expect(socket.state, TypedSocketState.reconnecting);

      async.elapse(oneSecond);
      expect(sentData(transport.connections[1]), [3, 4, 5]);
      expect(socket.pendingSends, 0);
    });
  });

  test('a send on a connection that died unnoticed is buffered', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport);
      socket.connect();
      async.flushMicrotasks();

      // Killed, but the socket has not processed the drop yet.
      transport.lastConnection!.kill();
      expect(socket.state, TypedSocketState.connected);
      expect(socket.send('m', 'x'), SendOutcome.buffered);
      expect(socket.state, TypedSocketState.reconnecting);

      async.elapse(oneSecond);
      expect(sentData(transport.lastConnection!), ['x']);
    });
  });

  test('pendingSendsChanges emits after every change', () {
    fakeAsync((async) {
      final transport = FakeTransport();
      final socket = fakeSocket(transport, bufferCapacity: 3);
      final changes = Recorder(socket.pendingSendsChanges);
      for (var i = 1; i <= 4; i++) {
        socket.send('m', i); // the 4th evicts, so the count stays at 3
      }
      socket.connect();
      async.flushMicrotasks();
      expect(changes.values, [1, 2, 3, 2, 1, 0]);
    });
  });

  test('close() clears the buffer', () {
    fakeAsync((async) {
      final socket = fakeSocket(FakeTransport());
      final changes = Recorder(socket.pendingSendsChanges);
      socket
        ..send('m', 1)
        ..send('m', 2)
        ..close();
      async.flushMicrotasks();
      expect(socket.pendingSends, 0);
      expect(changes.values, [1, 2, 0]);
      expect(changes.isDone, isTrue);
    });
  });
}
