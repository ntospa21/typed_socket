@TestOn('browser')
library;

import 'package:test/test.dart';
import 'package:typed_socket/testing.dart';
import 'package:typed_socket/typed_socket.dart';

/// Proves the package compiles and runs in a browser:
///
///     dart test -p chrome test/web
void main() {
  test('typed channels work in the browser', () async {
    final transport = FakeTransport();
    final socket = TypedSocket(
      uri: Uri.parse('ws://example.test/ws'),
      transport: transport,
    )..on<int>('n', (json) => json['v'] as int);
    final value = socket.stream<int>('n').first;
    await socket.connect();
    transport.lastConnection!.receiveEvent('n', {'v': 42});
    expect(await value, 42);
    await socket.close();
  });

  test('the default WebSocket transport reports a failed connect', () async {
    final socket = TypedSocket(
      uri: Uri.parse('ws://127.0.0.1:9/ws'),
      maxReconnectAttempts: 0,
    );
    await expectLater(
      socket.connect(),
      throwsA(isA<TypedSocketClosedException>()),
    );
    expect(socket.closeReason, CloseReason.attemptsExhausted);
  });
}
