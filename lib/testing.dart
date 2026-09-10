/// Test support for apps built on `typed_socket`.
///
/// [FakeTransport] replaces the network so widget and unit tests can script
/// connects, messages, failures, and dropped connections deterministically.
library;

import 'src/transport/fake_transport.dart';

export 'src/transport/fake_transport.dart';
