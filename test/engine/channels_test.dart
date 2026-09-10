import 'dart:typed_data';

import 'package:fake_async/fake_async.dart';
import 'package:test/test.dart';
import 'package:typed_socket/testing.dart';
import 'package:typed_socket/typed_socket.dart';

import '../support/harness.dart';

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
    socket = fakeSocket(transport)
      ..on<Chat>('message', Chat.fromJson, toJson: (c) => c.toJson())
      ..on<int>('count', (json) => json['n'] as int);
  });

  test('routes each event to its typed stream', () {
    fakeAsync((async) {
      final chats = Recorder(socket.stream<Chat>('message'));
      final counts = Recorder(socket.stream<int>('count'));
      connectNow(async)
        ..receiveEvent('message', {'user': 'sam', 'text': 'hi'})
        ..receiveEvent('count', {'n': 3})
        ..receiveEvent('message', {'user': 'kim', 'text': 'yo'});
      async.flushMicrotasks();
      expect(chats.values, [const Chat('sam', 'hi'), const Chat('kim', 'yo')]);
      expect(counts.values, [3]);
    });
  });

  test('a wrong or missing type argument throws at stream<T>()', () {
    expect(() => socket.stream<String>('message'), throwsArgumentError);
    expect(() => socket.stream<dynamic>('message'), throwsArgumentError);
    expect(() => socket.stream<Chat>('count'), throwsArgumentError);
    expect(() => socket.stream<Chat>('unregistered'), throwsArgumentError);
  });

  test('registration errors are ArgumentErrors', () {
    expect(
      () => socket.on<Chat>('message', Chat.fromJson),
      throwsArgumentError,
    );
    final withHeartbeat = fakeSocket(
      transport,
      heartbeat: const HeartbeatConfig(),
    );
    expect(
      () => withHeartbeat.on<int>('__pong', (j) => 1),
      throwsArgumentError,
    );
    expect(
      () => withHeartbeat.on<int>('__ping', (j) => 1),
      throwsArgumentError,
    );
    // Without a heartbeat the names are not reserved.
    expect(socket.on<int>('__pong', (j) => 1), same(socket));
  });

  test('decode errors are isolated to one message', () {
    fakeAsync((async) {
      final chats = Recorder(socket.stream<Chat>('message'));
      connectNow(async)
        ..receiveEvent('message', {'user': 42})
        ..receiveEvent('message', 'not an object')
        ..receiveEvent('message', [1, 2])
        ..receiveEvent('message')
        ..receiveEvent('message', {'user': 'sam', 'text': 'still here'});
      async.flushMicrotasks();

      expect(chats.values, [const Chat('sam', 'still here')]);
      expect(chats.errors, hasLength(4));
      final first = chats.errors.first as TypedSocketDecodeError;
      expect(first.event, 'message');
      expect(first.raw, {'user': 42});
      expect(first.cause, isA<TypeError>());
      for (final error in chats.errors.skip(1)) {
        expect(
          (error as TypedSocketDecodeError).cause,
          isA<FormatException>(),
        );
      }
      expect(chats.isDone, isFalse);
      expect(socket.state, TypedSocketState.connected);
      expect(transport.connectCount, 1);
    });
  });

  test('valid envelopes for unregistered events go to unhandledFrames', () {
    fakeAsync((async) {
      final unhandled = Recorder(socket.unhandledFrames);
      connectNow(async).receiveEvent('presence', {'online': 3});
      async.flushMicrotasks();
      expect(unhandled.values.single.event, 'presence');
      expect(unhandled.values.single.data, {'online': 3});
    });
  });

  test('malformed frames go to frameErrors and the connection stays up', () {
    fakeAsync((async) {
      final frameErrors = Recorder(socket.frameErrors);
      final chats = Recorder(socket.stream<Chat>('message'));
      final binary = Uint8List.fromList([1, 2, 3]);
      connectNow(async)
        ..receiveRaw('not json')
        ..receiveRaw('[1, 2]')
        ..receiveRaw('{"data": {}}')
        ..receiveRaw('{"event": 7}')
        ..receiveRaw(binary)
        ..receiveEvent('message', {'user': 'a', 'text': 'b'});
      async.flushMicrotasks();

      expect(frameErrors.values, hasLength(5));
      for (final error in frameErrors.values) {
        expect(error.event, TypedSocketDecodeError.frameEvent);
        expect(error.cause, isA<FormatException>());
      }
      expect(frameErrors.values.first.raw, 'not json');
      expect(frameErrors.values.last.raw, same(binary));
      expect(chats.values, [const Chat('a', 'b')]);
      expect(socket.state, TypedSocketState.connected);
    });
  });

  test('sendTyped round-trips through the codec', () {
    fakeAsync((async) {
      final chats = Recorder(socket.stream<Chat>('message'));
      final conn = connectNow(async);
      expect(
        socket.sendTyped('message', const Chat('sam', 'hi')),
        SendOutcome.sent,
      );
      final sent = conn.sentEnvelopes.single;
      expect(sent.event, 'message');
      expect(sent.data, {'user': 'sam', 'text': 'hi'});

      conn.receiveRaw(conn.sentFrames.single); // echo it back
      async.flushMicrotasks();
      expect(chats.values, [const Chat('sam', 'hi')]);
    });
  });

  test('sendTyped rejects programmer mistakes', () {
    fakeAsync((async) {
      final conn = connectNow(async);
      expect(() => socket.sendTyped('count', 1), throwsArgumentError);
      expect(() => socket.sendTyped('nope', 1), throwsArgumentError);
      expect(
          () => socket.sendTyped<Object>('message', 42), throwsArgumentError);
      expect(() => socket.send('raw', Object()), throwsArgumentError);
      expect(conn.sentFrames, isEmpty);
    });
  });

  test('send accepts any JSON-encodable payload', () {
    fakeAsync((async) {
      final conn = connectNow(async);
      socket
        ..send('a', null)
        ..send('b', [1, 'x'])
        ..send('c', {'k': true});
      expect(sentData(conn), [
        null,
        [1, 'x'],
        {'k': true},
      ]);
    });
  });

  test('channels can be registered after connecting', () {
    fakeAsync((async) {
      final conn = connectNow(async);
      socket.on<String>('late', (json) => json['v'] as String);
      final values = Recorder(socket.stream<String>('late'));
      conn.receiveEvent('late', {'v': 'ok'});
      async.flushMicrotasks();
      expect(values.values, ['ok']);
    });
  });

  test('a custom codec changes the envelope shape', () {
    fakeAsync((async) {
      const codec = JsonEnvelopeCodec(eventKey: 'type', dataKey: 'payload');
      final custom = FakeTransport(codec: codec);
      final s = TypedSocket(uri: testUri, transport: custom, codec: codec)
        ..on<int>('n', (json) => json['v'] as int);
      final values = Recorder(s.stream<int>('n'));
      s.connect();
      async.flushMicrotasks();
      custom.lastConnection!.receiveRaw('{"type":"n","payload":{"v":5}}');
      s.send('out', 1);
      async.flushMicrotasks();
      expect(values.values, [5]);
      expect(custom.lastConnection!.sentFrames.single,
          '{"type":"out","payload":1}');
    });
  });
}
