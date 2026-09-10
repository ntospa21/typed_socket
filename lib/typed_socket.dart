/// A typed, resilient WebSocket client for Dart and Flutter.
///
/// Start with [TypedSocket]. For tests, see
/// `package:typed_socket/testing.dart`.
library;

import 'src/typed_socket_client.dart';

export 'src/backoff.dart' show Backoff, BackoffPolicy;
export 'src/channel_registry.dart' show Decoder, Encoder;
export 'src/envelope.dart';
export 'src/errors.dart';
export 'src/heartbeat.dart' show HeartbeatConfig;
export 'src/send_buffer.dart' show OfflinePolicy, SendOutcome;
export 'src/state.dart';
export 'src/transport/transport.dart';
export 'src/transport/web_socket_transport.dart';
export 'src/typed_socket_client.dart';
