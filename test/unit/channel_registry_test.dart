import 'package:test/test.dart';
import 'package:typed_socket/src/channel_registry.dart';
import 'package:typed_socket/typed_socket.dart';

import '../support/harness.dart';

void main() {
  late ChannelRegistry registry;

  setUp(() {
    registry = ChannelRegistry()
      ..register<Chat>('chat', Chat.fromJson, (c) => c.toJson())
      ..register<int>('count', (json) => json['n'] as int, null);
  });

  test('routes envelopes to the matching typed stream', () async {
    final chats = Recorder(registry.stream<Chat>('chat'));
    final counts = Recorder(registry.stream<int>('count'));
    expect(
      registry.dispatch(const Envelope('chat', {'user': 'a', 'text': 'b'})),
      isTrue,
    );
    expect(registry.dispatch(const Envelope('count', {'n': 3})), isTrue);
    await pumpEventQueue();
    expect(chats.values, [const Chat('a', 'b')]);
    expect(counts.values, [3]);
  });

  test('returns false for unregistered events', () {
    expect(registry.dispatch(const Envelope('other')), isFalse);
    expect(registry.contains('other'), isFalse);
    expect(registry.contains('chat'), isTrue);
  });

  test('rejects duplicate registration', () {
    expect(
      () => registry.register<String>('chat', (j) => '', null),
      throwsA(
        isA<ArgumentError>().having(
          (e) => e.message,
          'message',
          contains('already registered'),
        ),
      ),
    );
  });

  test('rejects mismatched stream types', () {
    expect(() => registry.stream<String>('chat'), throwsArgumentError);
    expect(() => registry.stream<Object>('chat'), throwsArgumentError);
    // What `socket.stream('chat')` infers when the type argument is left off.
    expect(() => registry.stream<dynamic>('chat'), throwsArgumentError);
    expect(() => registry.stream<int?>('count'), throwsArgumentError);
    expect(registry.stream<Chat>('chat'), isA<Stream<Chat>>());
  });

  test('treats dynamic and Object? as the same type', () {
    registry.register<Object?>('any', (j) => j, null);
    expect(registry.stream<dynamic>('any'), isA<Stream<dynamic>>());
    expect(registry.stream<Object?>('any'), isA<Stream<Object?>>());
  });

  test('rejects streams for unregistered events', () {
    expect(() => registry.stream<Chat>('nope'), throwsArgumentError);
  });

  test('isolates decode errors to one message', () async {
    final chats = Recorder(registry.stream<Chat>('chat'));
    registry
      ..dispatch(const Envelope('chat', {'user': 1}))
      ..dispatch(const Envelope('chat', 'not a map'))
      ..dispatch(const Envelope('chat', null))
      ..dispatch(const Envelope('chat', {'user': 'a', 'text': 'ok'}));
    await pumpEventQueue();
    expect(chats.values, [const Chat('a', 'ok')]);
    expect(chats.errors, hasLength(3));
    final errors = chats.errors.cast<TypedSocketDecodeError>();
    expect(errors[0].event, 'chat');
    expect(errors[0].raw, {'user': 1});
    expect(errors[0].cause, isA<TypeError>());
    expect(errors[1].cause, isA<FormatException>());
    expect(errors[1].raw, 'not a map');
    expect(errors[2].cause, isA<FormatException>());
    expect(errors[0].toString(), contains('event: chat'));
    expect(chats.isDone, isFalse);
  });

  group('encode', () {
    test('uses the registered toJson', () {
      expect(registry.encode('chat', const Chat('a', 'b')), {
        'user': 'a',
        'text': 'b',
      });
    });

    test('requires toJson', () {
      expect(() => registry.encode('count', 1), throwsArgumentError);
    });

    test('requires a value of the registered type', () {
      expect(() => registry.encode('chat', 42), throwsArgumentError);
      expect(() => registry.encode('chat', null), throwsArgumentError);
    });

    test('requires a registered event', () {
      expect(() => registry.encode('nope', 1), throwsArgumentError);
    });
  });

  test('close ends every stream', () async {
    final chats = Recorder(registry.stream<Chat>('chat'));
    registry.close();
    await pumpEventQueue();
    expect(chats.isDone, isTrue);
  });
}
