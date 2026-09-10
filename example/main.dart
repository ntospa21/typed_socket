// A CLI demo: start an echo server, connect, kill the server, send while it
// is down, restart it, and watch the buffered messages flush in order.
//
//     dart run example/main.dart
import 'dart:async';

import 'package:typed_socket/typed_socket.dart';

import '../test/fixtures/echo_server.dart';

class ChatMessage {
  const ChatMessage(this.user, this.text);

  factory ChatMessage.fromJson(Map<String, dynamic> json) =>
      ChatMessage(json['user'] as String, json['text'] as String);

  final String user;
  final String text;

  Map<String, dynamic> toJson() => {'user': user, 'text': text};
}

Future<void> main() async {
  final server = await EchoServer.start();
  print('Echo server listening on ${server.uri}\n');

  final socket = TypedSocket(
    uri: server.uri,
    backoff: Backoff.exponentialJitter(
      base: const Duration(milliseconds: 200),
      cap: const Duration(seconds: 2),
    ),
  )..on<ChatMessage>(
      'message',
      ChatMessage.fromJson,
      toJson: (m) => m.toJson(),
    );

  socket.states.listen((s) => print('[state]  ${s.name}'));
  socket.pendingSendsChanges.listen((n) => print('[buffer] $n waiting'));
  socket
      .stream<ChatMessage>('message')
      .listen((m) => print('[echo]   ${m.user}: ${m.text}'));

  await socket.connect();
  socket.sendTyped('message', const ChatMessage('demo', 'hello, live'));
  await Future<void>.delayed(const Duration(milliseconds: 300));

  print('\n--- killing the server (upgraded sockets closed explicitly) ---');
  await server.stop();
  await socket.states.firstWhere((s) => s != TypedSocketState.connected);

  for (var i = 1; i <= 3; i++) {
    final outcome =
        socket.sendTyped('message', ChatMessage('demo', 'queued #$i'));
    print('[send]   queued #$i -> ${outcome.name}');
  }

  await Future<void>.delayed(const Duration(seconds: 1));
  print('\n--- restarting the server ---');
  await server.restart();
  await socket.states.firstWhere((s) => s == TypedSocketState.connected);
  await server.waitForReceived(4);
  await Future<void>.delayed(const Duration(milliseconds: 300));

  print('\nThe server received, in order:');
  for (final frame in server.received) {
    print('  $frame');
  }

  await socket.close();
  await server.close();
}
