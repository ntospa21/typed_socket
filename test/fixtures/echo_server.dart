import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:web_socket_channel/web_socket_channel.dart';

/// A real WebSocket echo server on `dart:io`, with failure injection.
///
/// It speaks the default typed_socket envelope: every text frame is echoed
/// back unchanged, except `__ping`, which is answered with `__pong`.
///
/// Lesson from the first prototype: `HttpServer.close(force: true)` does
/// **not** close WebSockets that were already upgraded, so a "kill" that
/// only closes the HTTP server never cuts anything. This server tracks every
/// upgraded socket and closes each one explicitly in [killConnections] and
/// [stop], and counts what it did so tests can assert that the failure
/// really happened.
class EchoServer {
  EchoServer._(this._port);

  /// Starts a server on [port] (0 picks a free port).
  static Future<EchoServer> start({int port = 0}) async {
    final server = EchoServer._(port);
    await server._bind();
    return server;
  }

  int _port;
  HttpServer? _http;
  final Set<WebSocket> _sockets = {};
  final StreamController<String> _received =
      StreamController<String>.broadcast();

  /// Every non-ping text frame received, in arrival order.
  final List<String> received = [];

  /// The request URI of every HTTP request, including refused ones.
  final List<Uri> requestUris = [];

  /// The headers of every HTTP request, lower-cased names.
  final List<Map<String, List<String>>> requestHeaders = [];

  /// The close code of every upgraded connection that ended.
  final List<int?> closeCodes = [];

  /// Completed WebSocket upgrades.
  int upgrades = 0;

  /// Upgrade requests rejected because of [refuseUpgrades].
  int refusedUpgrades = 0;

  /// Live connections closed by [killConnections] or [stop].
  int killedConnections = 0;

  /// Connections cut in the middle of a frame because of [cutMidMessage].
  int midMessageCuts = 0;

  /// Pings answered with a pong.
  int pingsAnswered = 0;

  /// Pings swallowed because of [stalled].
  int pingsIgnored = 0;

  /// Answer upgrade requests with HTTP 503 instead of upgrading.
  bool refuseUpgrades = false;

  /// Keep connections open but never send anything: no echoes, no pongs.
  bool stalled = false;

  /// Complete the handshake, then send half a frame and drop the TCP
  /// connection.
  bool cutMidMessage = false;

  /// The WebSocket URI of this server.
  Uri get uri => Uri.parse('ws://127.0.0.1:$_port/ws');

  /// Whether the server is accepting connections.
  bool get isRunning => _http != null;

  /// Upgraded connections that are still open.
  int get openConnections => _sockets.length;

  /// Emits every frame appended to [received].
  Stream<String> get onReceived => _received.stream;

  /// Waits until at least [count] frames have been received.
  Future<void> waitForReceived(
    int count, {
    Duration timeout = const Duration(seconds: 10),
  }) async {
    if (received.length >= count) return;
    await onReceived
        .firstWhere((_) => received.length >= count)
        .timeout(timeout);
  }

  /// Sends a raw frame (`String` or bytes) to every open connection.
  void broadcastRaw(Object frame) {
    for (final socket in _sockets) {
      socket.add(frame);
    }
  }

  /// Sends an envelope to every open connection.
  void broadcast(String event, Object? data) =>
      broadcastRaw(jsonEncode({'event': event, 'data': data}));

  /// Closes every upgraded connection. Returns how many were open.
  Future<int> killConnections() async {
    final sockets = _sockets.toList();
    _sockets.clear();
    killedConnections += sockets.length;
    await Future.wait(
      sockets.map((s) => s.close(WebSocketStatus.goingAway, 'killed')),
    );
    return sockets.length;
  }

  /// Stops accepting connections and kills the open ones.
  Future<void> stop() async {
    final http = _http;
    _http = null;
    await http?.close(force: true);
    await killConnections();
  }

  /// Starts accepting connections again on the same port.
  Future<void> restart() async {
    if (isRunning) await stop();
    await _bind();
  }

  /// Stops the server for good.
  Future<void> close() async {
    await stop();
    await _received.close();
  }

  Future<void> _bind() async {
    final http = await HttpServer.bind(InternetAddress.loopbackIPv4, _port);
    _port = http.port;
    _http = http;
    http.listen(_handle);
  }

  Future<void> _handle(HttpRequest request) async {
    requestUris.add(request.uri);
    final headers = <String, List<String>>{};
    request.headers.forEach((name, values) => headers[name] = values);
    requestHeaders.add(headers);

    if (!WebSocketTransformer.isUpgradeRequest(request)) {
      request.response.statusCode = HttpStatus.notFound;
      await request.response.close();
      return;
    }
    if (refuseUpgrades) {
      refusedUpgrades++;
      request.response.statusCode = HttpStatus.serviceUnavailable;
      await request.response.close();
      return;
    }
    if (cutMidMessage) {
      await _cutMidMessage(request);
      return;
    }

    final socket = await WebSocketTransformer.upgrade(
      request,
      protocolSelector: (protocols) => protocols.first,
    );
    upgrades++;
    _sockets.add(socket);
    socket.listen(
      (Object? data) => _onFrame(socket, data),
      onError: (Object _) => _sockets.remove(socket),
      onDone: () {
        _sockets.remove(socket);
        closeCodes.add(socket.closeCode);
      },
    );
  }

  void _onFrame(WebSocket socket, Object? data) {
    // Frames can still arrive on a socket this server has just killed;
    // replying to them would write to a closed sink.
    final canReply = !stalled && _sockets.contains(socket);
    if (data is! String) {
      if (canReply) socket.add(data);
      return;
    }
    Object? decoded;
    try {
      decoded = jsonDecode(data);
    } on FormatException {
      decoded = null;
    }
    if (decoded is Map && decoded['event'] == '__ping') {
      if (stalled) {
        pingsIgnored++;
      } else if (canReply) {
        pingsAnswered++;
        socket.add(jsonEncode({'event': '__pong', 'data': null}));
      }
      return;
    }
    received.add(data);
    _received.add(data);
    if (canReply) socket.add(data);
  }

  /// Completes the handshake by hand, then writes a text frame header that
  /// announces 100 payload bytes, sends only 10, and destroys the socket.
  Future<void> _cutMidMessage(HttpRequest request) async {
    final key = request.headers.value('sec-websocket-key')!;
    // Detached below rather than closed: the socket takes over.
    // ignore: close_sinks
    final response = request.response
      ..statusCode = HttpStatus.switchingProtocols
      ..headers.set(HttpHeaders.connectionHeader, 'Upgrade')
      ..headers.set(HttpHeaders.upgradeHeader, 'websocket')
      ..headers.set('Sec-WebSocket-Accept', WebSocketChannel.signKey(key))
      ..headers.contentLength = 0;
    // Destroyed below on purpose: an abrupt cut is the point.
    // ignore: close_sinks
    final socket = await response.detachSocket();
    midMessageCuts++;
    socket.add([0x81, 100, ...List.filled(10, 0x61)]);
    await socket.flush();
    await Future<void>.delayed(const Duration(milliseconds: 50));
    socket.destroy();
  }
}
