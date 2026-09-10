import 'state.dart';

/// A received frame or payload that could not be decoded.
///
/// Delivered as a stream *error* on a typed channel when its decoder fails,
/// and as a data event on `TypedSocket.frameErrors` when a frame is not a
/// valid envelope at all. Either way only that one message is affected: the
/// stream and the connection stay open.
class TypedSocketDecodeError implements Exception {
  /// Creates a decode error for [event].
  TypedSocketDecodeError({
    required this.event,
    required this.raw,
    required this.cause,
    required this.stackTrace,
  });

  /// The [event] value used for frames that are not valid envelopes.
  static const String frameEvent = '<frame>';

  /// The event whose payload failed to decode, or [frameEvent] when the
  /// frame itself was malformed.
  final String event;

  /// The raw payload (for channel errors) or raw frame (for frame errors).
  final Object? raw;

  /// The underlying error, usually a [FormatException] or a `TypeError`
  /// thrown by a decoder.
  final Object cause;

  /// Where [cause] was thrown.
  final StackTrace stackTrace;

  @override
  String toString() => 'TypedSocketDecodeError(event: $event, cause: $cause)';
}

/// Thrown by `send` and `sendTyped` under `OfflinePolicy.reject` when the
/// socket is not connected.
class TypedSocketOfflineException implements Exception {
  /// Creates an offline exception for a send of [event].
  const TypedSocketOfflineException([this.event]);

  /// The event that could not be sent, if known.
  final String? event;

  @override
  String toString() => event == null
      ? 'TypedSocketOfflineException: socket is not connected'
      : 'TypedSocketOfflineException: cannot send "$event", '
          'socket is not connected';
}

/// Completes the futures of `connect()` and `reconnect()` when the socket
/// closes before reaching the connected state.
class TypedSocketClosedException implements Exception {
  /// Creates a closed exception carrying [reason].
  const TypedSocketClosedException(this.reason);

  /// Why the socket closed.
  final CloseReason reason;

  @override
  String toString() => 'TypedSocketClosedException: socket closed ($reason)';
}

/// A connection attempt that failed, or a live connection that was torn down
/// because of an error.
///
/// Emitted on `TypedSocket.connectionErrors` purely for visibility (logging,
/// diagnostics). The socket has already reacted by scheduling a reconnect or
/// closing; there is nothing to handle.
class TypedSocketConnectionError implements Exception {
  /// Creates a connection error.
  const TypedSocketConnectionError({
    required this.cause,
    required this.stackTrace,
    required this.attempt,
  });

  /// What went wrong: a transport error, an exception thrown by
  /// `onConnected` or `uriProvider`, or a `TimeoutException` from the
  /// heartbeat.
  final Object cause;

  /// Where [cause] was thrown.
  final StackTrace stackTrace;

  /// The attempt number the error belongs to: 0 for the first attempt after
  /// `connect()` or `reconnect()`, then 1, 2, ... for retries.
  final int attempt;

  @override
  String toString() =>
      'TypedSocketConnectionError(attempt: $attempt, cause: $cause)';
}
