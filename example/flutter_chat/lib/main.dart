import 'dart:async';

import 'package:flutter/material.dart';
import 'package:typed_socket/typed_socket.dart';

import 'chat_message.dart';
import 'chat_screen.dart';
import 'killable_transport.dart';

/// Start the server with `dart run example/serve.dart` from the package root.
/// On the Android emulator, pass
/// `--dart-define=SERVER_URL=ws://10.0.2.2:8080/ws`.
const serverUrl = String.fromEnvironment(
  'SERVER_URL',
  defaultValue: 'ws://localhost:8080/ws',
);

void main() => runApp(const ChatApp());

class ChatApp extends StatefulWidget {
  const ChatApp({super.key});

  @override
  State<ChatApp> createState() => _ChatAppState();
}

class _ChatAppState extends State<ChatApp> {
  final _transport = KillableTransport();
  late final TypedSocket _socket = TypedSocket(
    uri: Uri.parse(serverUrl),
    transport: _transport,
    heartbeat: const HeartbeatConfig(
      interval: Duration(seconds: 10),
      timeout: Duration(seconds: 5),
    ),
  )..on<ChatMessage>(
      'message',
      ChatMessage.fromJson,
      toJson: (m) => m.toJson(),
    );

  @override
  void initState() {
    super.initState();
    unawaited(_socket.connect());
  }

  @override
  void dispose() {
    unawaited(_socket.close());
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'typed_socket chat',
      theme: ThemeData(colorSchemeSeed: Colors.indigo, useMaterial3: true),
      home: ChatScreen(socket: _socket, onKill: _transport.kill),
    );
  }
}
