import 'dart:convert';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:typed_socket/typed_socket.dart';

void main() {
  const codec = JsonEnvelopeCodec();

  group('JsonEnvelopeCodec.encode', () {
    test('produces the default envelope shape', () {
      final frame = codec.encode(
        const Envelope('message', {'user': 'sam', 'text': 'hi'}),
      );
      expect(jsonDecode(frame), {
        'event': 'message',
        'data': {'user': 'sam', 'text': 'hi'},
      });
    });

    test('encodes a missing payload as null', () {
      expect(jsonDecode(codec.encode(const Envelope('ping'))), {
        'event': 'ping',
        'data': null,
      });
    });

    test('throws ArgumentError for payloads JSON cannot encode', () {
      expect(
        () => codec.encode(Envelope('bad', Object())),
        throwsA(
          isA<ArgumentError>().having(
            (e) => e.message,
            'message',
            contains('"bad"'),
          ),
        ),
      );
      expect(
        () => codec.encode(Envelope('bad', {'nested': DateTime(2020)})),
        throwsArgumentError,
      );
    });
  });

  group('JsonEnvelopeCodec round trip', () {
    for (final data in <Object?>[
      null,
      1,
      2.5,
      'text',
      true,
      [1, 'two', null],
      {
        'user': 'sam',
        'nested': {'a': 1},
      },
    ]) {
      test('preserves ${jsonEncode(data)}', () {
        final decoded = codec.decode(codec.encode(Envelope('evt', data)));
        expect(decoded.event, 'evt');
        expect(decoded.data, data);
      });
    }

    test('decodes objects as Map<String, dynamic>', () {
      final decoded = codec.decode('{"event":"e","data":{"a":1}}');
      expect(decoded.data, isA<Map<String, dynamic>>());
    });

    test('supports custom keys', () {
      const custom = JsonEnvelopeCodec(eventKey: 'type', dataKey: 'payload');
      final frame = custom.encode(const Envelope('join', {'room': 'a'}));
      expect(jsonDecode(frame), {
        'type': 'join',
        'payload': {'room': 'a'},
      });
      final decoded = custom.decode('{"type":"left","payload":[1]}');
      expect(decoded.event, 'left');
      expect(decoded.data, [1]);
    });

    test('treats a missing data field as null', () {
      final decoded = codec.decode('{"event":"e"}');
      expect(decoded.event, 'e');
      expect(decoded.data, isNull);
    });

    test('ignores extra fields', () {
      final decoded = codec.decode('{"event":"e","data":1,"id":7}');
      expect(decoded.data, 1);
    });
  });

  group('JsonEnvelopeCodec.decode rejects', () {
    final cases = <String, Object>{
      'binary frames': Uint8List.fromList([123, 125]),
      'invalid JSON': 'not json',
      'truncated JSON': '{"event":"e"',
      'an array root': '[1, 2]',
      'a string root': '"event"',
      'a number root': '42',
      'a null root': 'null',
      'a missing event field': '{"data":{}}',
      'a numeric event': '{"event":1}',
      'a null event': '{"event":null}',
      'an object event': '{"event":{"name":"x"}}',
      'an empty frame': '',
    };
    cases.forEach((name, frame) {
      test(name, () {
        expect(() => codec.decode(frame), throwsFormatException);
      });
    });

    test('with a message naming the problem', () {
      expect(
        () => codec.decode('{"data":1}'),
        throwsA(
          isA<FormatException>().having(
            (e) => e.message,
            'message',
            contains('Missing "event"'),
          ),
        ),
      );
      expect(
        () => codec.decode('{"event":true}'),
        throwsA(
          isA<FormatException>().having(
            (e) => e.message,
            'message',
            contains('a boolean'),
          ),
        ),
      );
      expect(
        () => codec.decode('{"event":{}}'),
        throwsA(
          isA<FormatException>().having(
            (e) => e.message,
            'message',
            contains('must be a string'),
          ),
        ),
      );
    });
  });

  test('Envelope.toString includes event and data', () {
    expect(const Envelope('e', 1).toString(), 'Envelope(e, 1)');
  });
}
