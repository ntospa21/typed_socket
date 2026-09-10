import 'dart:async';

import 'envelope.dart';
import 'errors.dart';

/// Turns the JSON object in an envelope's `data` field into a [T].
typedef Decoder<T> = T Function(Map<String, dynamic> json);

/// Turns a [T] into a JSON object for an envelope's `data` field.
typedef Encoder<T> = Map<String, dynamic> Function(T value);

/// Typed channel registration and routing.
///
/// Each registered event owns one broadcast stream controller for the life of
/// the socket, which is why typed streams survive reconnects.
class ChannelRegistry {
  final Map<String, _Channel<Object?>> _channels = {};

  /// Whether [event] has a registered channel.
  bool contains(String event) => _channels.containsKey(event);

  /// Registers a channel of [T] for [event].
  ///
  /// Throws [ArgumentError] if [event] is already registered.
  void register<T>(String event, Decoder<T> decode, Encoder<T>? toJson) {
    final existing = _channels[event];
    if (existing != null) {
      throw ArgumentError.value(
        event,
        'event',
        'is already registered (as ${existing.type})',
      );
    }
    _channels[event] = _Channel<T>(event, decode, toJson);
  }

  /// The broadcast stream for [event].
  ///
  /// Throws [ArgumentError] if [event] is not registered or was registered
  /// with a type other than [T].
  Stream<T> stream<T>(String event) {
    final channel = _require(event);
    if (!channel.hasType<T>()) {
      throw ArgumentError(
        'Event "$event" is registered as ${channel.type}, '
        'but stream<$T>() was requested. '
        'Pass the registered type explicitly: stream<${channel.type}>().',
      );
    }
    return (channel as _Channel<T>).stream;
  }

  /// Encodes [value] with the `toJson` registered for [event].
  ///
  /// Throws [ArgumentError] if [event] is not registered, has no `toJson`,
  /// or [value] is not of the registered type.
  Map<String, dynamic> encode(String event, Object? value) =>
      _require(event).encodeValue(value);

  /// Routes [envelope] to its channel. Returns false if no channel is
  /// registered for its event.
  bool dispatch(Envelope envelope) {
    final channel = _channels[envelope.event];
    if (channel == null) return false;
    channel.deliver(envelope.data);
    return true;
  }

  /// Closes every channel stream.
  void close() {
    for (final channel in _channels.values) {
      channel.close();
    }
  }

  _Channel<Object?> _require(String event) {
    final channel = _channels[event];
    if (channel == null) {
      throw ArgumentError.value(
        event,
        'event',
        'is not registered; call on<T>("$event", ...) first',
      );
    }
    return channel;
  }
}

class _Channel<T> {
  _Channel(this.event, this._decode, this._toJson);

  final String event;
  final Decoder<T> _decode;
  final Encoder<T>? _toJson;
  final StreamController<T> _controller = StreamController<T>.broadcast();

  Type get type => T;

  Stream<T> get stream => _controller.stream;

  /// Mutual subtyping, so `dynamic` and `Object?` count as the same type but
  /// an inferred `dynamic` never matches a concrete registered type.
  bool hasType<R>() => <R>[] is List<T> && <T>[] is List<R>;

  void deliver(Object? data) {
    if (data is! Map<String, dynamic>) {
      _addDecodeError(
        data,
        FormatException(
          'Expected a JSON object in the data of "$event", '
          'got ${data == null ? 'null' : data.runtimeType}',
        ),
        StackTrace.current,
      );
      return;
    }
    final T value;
    try {
      value = _decode(data);
    } catch (error, stackTrace) {
      _addDecodeError(data, error, stackTrace);
      return;
    }
    _controller.add(value);
  }

  Map<String, dynamic> encodeValue(Object? value) {
    final toJson = _toJson;
    if (toJson == null) {
      throw ArgumentError(
        'Event "$event" has no toJson encoder. '
        'Pass toJson to on<$T>("$event", ...) to use sendTyped.',
      );
    }
    if (value is! T) {
      throw ArgumentError.value(
        value,
        'value',
        'must be a $T for event "$event"',
      );
    }
    return toJson(value);
  }

  void _addDecodeError(Object? raw, Object cause, StackTrace stackTrace) {
    _controller.addError(
      TypedSocketDecodeError(
        event: event,
        raw: raw,
        cause: cause,
        stackTrace: stackTrace,
      ),
      stackTrace,
    );
  }

  void close() => unawaited(_controller.close());
}
