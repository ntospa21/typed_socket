/// Opens one connection per attempt.
///
/// This is the testability seam of `TypedSocket`. A transport never
/// reconnects on its own; the socket owns the reconnect loop and calls
/// [connect] once per attempt.
///
/// Implement it to plug in another wire protocol, or use `FakeTransport`
/// from `package:typed_socket/testing.dart` in tests.
abstract class Transport {
  /// Opens a connection to [uri].
  ///
  /// The returned future must complete only once the connection is ready to
  /// send and receive, and must complete with an error if the handshake
  /// fails.
  Future<TransportConnection> connect(Uri uri);
}

/// One live connection produced by a [Transport].
abstract class TransportConnection {
  /// Received frames: `String` for text frames, `Uint8List` for binary.
  ///
  /// Single-subscription. The stream ends when the connection ends.
  Stream<Object> get incoming;

  /// Writes a frame: a `String` or a `Uint8List`.
  ///
  /// Should throw [StateError] if the connection is known to be closed.
  void send(Object frame);

  /// Closes the connection with an optional WebSocket close [code] and
  /// [reason]. Safe to call more than once.
  Future<void> close([int? code, String? reason]);

  /// Completes when the connection ends for any reason: a remote close, a
  /// network error, or [close].
  Future<void> get done;
}
