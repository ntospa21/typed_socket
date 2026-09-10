import 'dart:async';
import 'dart:typed_data';

import 'package:web_socket_channel/web_socket_channel.dart';

import 'transport.dart';
import 'web_socket_connect_stub.dart'
    if (dart.library.io) 'web_socket_connect_io.dart' as platform;

/// The production [Transport], built on `package:web_socket_channel`.
///
/// Works on the Dart VM, Flutter (mobile and desktop), and the web.
class WebSocketTransport implements Transport {
  /// Creates a WebSocket transport.
  ///
  /// [protocols] are offered as `Sec-WebSocket-Protocol` values on every
  /// platform. [headers] are sent with the handshake on native platforms
  /// only: browsers cannot set handshake headers, so they are ignored on
  /// the web. Put credentials in query parameters or subprotocols there.
  ///
  /// A handshake that has not completed within [connectTimeout] fails the
  /// attempt with a [TimeoutException].
  const WebSocketTransport({
    this.protocols,
    this.headers,
    this.connectTimeout = const Duration(seconds: 10),
  });

  /// Subprotocols to offer during the handshake.
  final Iterable<String>? protocols;

  /// Handshake headers. Native platforms only; ignored on the web.
  final Map<String, dynamic>? headers;

  /// The maximum time a handshake may take.
  final Duration connectTimeout;

  @override
  Future<TransportConnection> connect(Uri uri) async {
    final channel = platform.connectChannel(
      uri,
      protocols: protocols,
      headers: headers,
      connectTimeout: connectTimeout,
    );
    try {
      // Awaiting `ready` makes a refused handshake surface as a failed
      // connect rather than as a silent stream error.
      await channel.ready.timeout(connectTimeout);
    } catch (_) {
      // Tear down the half-open channel without leaking its errors into the
      // zone as unhandled async errors.
      unawaited(channel.sink.close().then<void>((_) {}, onError: (_) {}));
      rethrow;
    }
    return _WebSocketConnection(channel);
  }
}

class _WebSocketConnection implements TransportConnection {
  _WebSocketConnection(this._channel) {
    _subscription = _channel.stream.listen(
      _onData,
      onError: _incoming.addError,
      onDone: _finish,
    );
    unawaited(
      _channel.sink.done.then<void>((_) => _finish(), onError: (_) {
        _finish();
      }),
    );
  }

  final WebSocketChannel _channel;
  final StreamController<Object> _incoming = StreamController<Object>();
  final Completer<void> _done = Completer<void>();
  late final StreamSubscription<dynamic> _subscription;
  bool _closed = false;

  @override
  Stream<Object> get incoming => _incoming.stream;

  @override
  Future<void> get done => _done.future;

  void _onData(dynamic data) {
    if (data is String) {
      _incoming.add(data);
    } else if (data is Uint8List) {
      _incoming.add(data);
    } else if (data is List<int>) {
      _incoming.add(Uint8List.fromList(data));
    }
  }

  @override
  void send(Object frame) {
    if (_closed) throw StateError('WebSocket connection is closed');
    _channel.sink.add(frame);
  }

  @override
  Future<void> close([int? code, String? reason]) async {
    if (!_closed) {
      _closed = true;
      try {
        // A dead peer never answers the close handshake; do not wait on it
        // forever.
        await _channel.sink
            .close(code, reason)
            .timeout(const Duration(seconds: 5));
      } catch (_) {
        // Closing an already broken socket can fail; the connection is
        // gone either way.
      }
    }
    await _subscription.cancel();
    _finish();
  }

  void _finish() {
    if (_done.isCompleted) return;
    _closed = true;
    unawaited(_incoming.close());
    _done.complete();
  }
}
