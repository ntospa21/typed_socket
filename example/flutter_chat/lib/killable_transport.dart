import 'package:typed_socket/typed_socket.dart';

/// Wraps a real transport so the demo can cut the live connection on demand.
///
/// Closing the connection underneath the socket looks exactly like a network
/// drop to `TypedSocket`, so it backs off and reconnects on its own.
class KillableTransport implements Transport {
  KillableTransport([this._inner = const WebSocketTransport()]);

  final Transport _inner;
  TransportConnection? _current;

  @override
  Future<TransportConnection> connect(Uri uri) async =>
      _current = await _inner.connect(uri);

  /// Drops the current connection, if any.
  Future<void> kill() async => _current?.close(4001, 'killed from the demo');
}
