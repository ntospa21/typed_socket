import 'package:flutter/material.dart';
import 'package:flutter_chat/chat_screen.dart';
import 'package:flutter_chat/connection_badge.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:typed_socket/testing.dart';
import 'package:typed_socket/typed_socket.dart';

void main() {
  late FakeTransport transport;
  late TypedSocket socket;

  setUp(() {
    transport = FakeTransport();
    socket = TypedSocket(
      uri: Uri.parse('ws://test.local/ws'),
      transport: transport,
      backoff: Backoff.fixed(const Duration(seconds: 1)),
    );
  });

  Widget host(Widget child) => MaterialApp(home: Scaffold(body: child));

  testWidgets(
      'ConnectionBadge changes to Reconnecting when the connection '
      'drops, then recovers', (tester) async {
    await tester.pumpWidget(host(ConnectionBadge(socket: socket)));
    expect(find.text('Idle'), findsOneWidget);

    socket.connect();
    await tester.pump();
    expect(find.text('Connected'), findsOneWidget);

    transport.lastConnection!.kill();
    await tester.pump();
    expect(find.text('Reconnecting'), findsOneWidget);

    await tester.pump(const Duration(seconds: 1));
    expect(find.text('Connected'), findsOneWidget);
    expect(transport.connections, hasLength(2));

    socket.close();
    await tester.pump();
    expect(find.text('Closed'), findsOneWidget);
  });

  testWidgets('PendingSendsBanner counts messages waiting to send',
      (tester) async {
    await tester.pumpWidget(host(PendingSendsBanner(socket: socket)));
    expect(find.textContaining('waiting to send'), findsNothing);

    socket
      ..send('message', {'text': 'a'})
      ..send('message', {'text': 'b'});
    await tester.pump();
    expect(find.text('2 waiting to send'), findsOneWidget);

    socket.connect();
    await tester.pump();
    await tester.pump();
    expect(find.textContaining('waiting to send'), findsNothing);
    expect(transport.lastConnection!.sentFrames, hasLength(2));

    socket.close();
    await tester.pump();
  });
}
