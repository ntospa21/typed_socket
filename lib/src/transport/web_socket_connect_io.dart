import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// Opens a channel with `dart:io`, which supports handshake [headers].
WebSocketChannel connectChannel(
  Uri uri, {
  Iterable<String>? protocols,
  Map<String, dynamic>? headers,
  Duration? connectTimeout,
}) =>
    IOWebSocketChannel.connect(
      uri,
      protocols: protocols,
      headers: headers,
      connectTimeout: connectTimeout,
    );
