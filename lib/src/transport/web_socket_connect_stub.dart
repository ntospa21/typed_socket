import 'package:web_socket_channel/web_socket_channel.dart';

/// Opens a channel on platforms without `dart:io`, such as the web.
///
/// [headers] and [connectTimeout] are not supported by browsers and are
/// ignored here; the caller applies its own timeout to `channel.ready`.
WebSocketChannel connectChannel(
  Uri uri, {
  Iterable<String>? protocols,
  Map<String, dynamic>? headers,
  Duration? connectTimeout,
}) =>
    WebSocketChannel.connect(uri, protocols: protocols);
