import 'dart:convert';

import 'package:meta/meta.dart';

/// One routed message: an [event] name plus its [data] payload.
///
/// On the wire the default codec represents it as
/// `{"event": "message", "data": {...}}`.
@immutable
class Envelope {
  /// Creates an envelope for [event] carrying [data].
  const Envelope(this.event, [this.data]);

  /// The routing key, used to pick the typed channel.
  final String event;

  /// The payload. With the JSON codec this is whatever `jsonDecode`
  /// produced: a map, list, string, number, bool, or null.
  final Object? data;

  @override
  String toString() => 'Envelope($event, $data)';
}

/// Converts between [Envelope]s and transport frames.
///
/// Implement this to adapt to a backend whose envelope differs from the
/// default, or to add a binary format later.
abstract class EnvelopeCodec {
  /// Allows subclasses to have const constructors.
  const EnvelopeCodec();

  /// Encodes [envelope] into a frame: a `String` for text frames or a
  /// `Uint8List` for binary frames.
  ///
  /// Throws [ArgumentError] if the payload cannot be encoded.
  Object encode(Envelope envelope);

  /// Decodes a received [frame].
  ///
  /// Throws [FormatException] if the frame is not a valid envelope.
  Envelope decode(Object frame);
}

/// The default codec: one JSON object per text frame.
///
/// ```json
/// { "event": "message", "data": { "user": "sam", "text": "hi" } }
/// ```
///
/// Use [eventKey] and [dataKey] for backends with a different shape, for
/// example `JsonEnvelopeCodec(eventKey: 'type', dataKey: 'payload')`.
class JsonEnvelopeCodec extends EnvelopeCodec {
  /// Creates a JSON codec that reads the event name from [eventKey] and the
  /// payload from [dataKey].
  const JsonEnvelopeCodec({this.eventKey = 'event', this.dataKey = 'data'})
      : assert(eventKey != dataKey, 'eventKey and dataKey must differ');

  /// The JSON field holding the event name.
  final String eventKey;

  /// The JSON field holding the payload.
  final String dataKey;

  @override
  String encode(Envelope envelope) {
    try {
      return jsonEncode(<String, Object?>{
        eventKey: envelope.event,
        dataKey: envelope.data,
      });
    } on JsonUnsupportedObjectError catch (e) {
      throw ArgumentError.value(
        envelope.data,
        'data',
        'is not JSON-encodable for event "${envelope.event}" '
            '(${e.cause ?? 'unsupported ${e.unsupportedObject.runtimeType}'})',
      );
    }
  }

  @override
  Envelope decode(Object frame) {
    if (frame is! String) {
      throw FormatException(
        'JsonEnvelopeCodec accepts only text frames, got ${frame.runtimeType}',
      );
    }
    final Object? root = jsonDecode(frame);
    if (root is! Map<String, dynamic>) {
      throw FormatException(
        'Expected a JSON object at the root, got ${_describe(root)}',
        frame,
      );
    }
    final event = root[eventKey];
    if (event is! String) {
      throw FormatException(
        event == null && !root.containsKey(eventKey)
            ? 'Missing "$eventKey" field'
            : '"$eventKey" must be a string, got ${_describe(event)}',
        frame,
      );
    }
    return Envelope(event, root[dataKey]);
  }

  static String _describe(Object? value) => switch (value) {
        null => 'null',
        List<dynamic>() => 'an array',
        String() => 'a string',
        num() => 'a number',
        bool() => 'a boolean',
        _ => value.runtimeType.toString(),
      };
}
