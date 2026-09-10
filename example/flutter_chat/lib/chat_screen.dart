import 'dart:async';

import 'package:flutter/material.dart';
import 'package:typed_socket/typed_socket.dart';

import 'chat_message.dart';
import 'connection_badge.dart';

class ChatScreen extends StatefulWidget {
  const ChatScreen({
    super.key,
    required this.socket,
    required this.onKill,
    this.user = 'me',
  });

  final TypedSocket socket;
  final Future<void> Function() onKill;
  final String user;

  @override
  State<ChatScreen> createState() => _ChatScreenState();
}

class _ChatScreenState extends State<ChatScreen> {
  final _messages = <ChatMessage>[];
  final _input = TextEditingController();
  late final StreamSubscription<ChatMessage> _subscription;

  @override
  void initState() {
    super.initState();
    _subscription = widget.socket.stream<ChatMessage>('message').listen(
          (message) => setState(() => _messages.add(message)),
          // A malformed message affects only itself; the stream stays open.
          onError: (Object error) => _notify('Skipped a malformed message'),
        );
  }

  @override
  void dispose() {
    unawaited(_subscription.cancel());
    _input.dispose();
    super.dispose();
  }

  void _notify(String text) {
    ScaffoldMessenger.of(context)
      ..hideCurrentSnackBar()
      ..showSnackBar(SnackBar(content: Text(text)));
  }

  void _send() {
    final text = _input.text.trim();
    if (text.isEmpty) return;
    final outcome = widget.socket.sendTyped(
      'message',
      ChatMessage(user: widget.user, text: text),
    );
    _input.clear();
    if (outcome == SendOutcome.dropped) {
      _notify('Message dropped: the offline buffer is full');
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('typed_socket chat'),
        actions: [
          ConnectionBadge(socket: widget.socket),
          IconButton(
            tooltip: 'Kill the connection',
            icon: const Icon(Icons.power_off),
            onPressed: widget.onKill,
          ),
          const SizedBox(width: 8),
        ],
      ),
      body: Column(
        children: [
          PendingSendsBanner(socket: widget.socket),
          Expanded(
            child: _messages.isEmpty
                ? const Center(child: Text('No messages yet. Say hi!'))
                : ListView.builder(
                    padding: const EdgeInsets.all(12),
                    itemCount: _messages.length,
                    itemBuilder: (context, i) {
                      final m = _messages[i];
                      return ListTile(
                        leading: CircleAvatar(child: Text(m.user[0])),
                        title: Text(m.text),
                        subtitle: Text(m.user),
                      );
                    },
                  ),
          ),
          SafeArea(
            child: Padding(
              padding: const EdgeInsets.all(8),
              child: Row(
                children: [
                  Expanded(
                    child: TextField(
                      controller: _input,
                      decoration: const InputDecoration(
                        hintText: 'Message',
                        border: OutlineInputBorder(),
                      ),
                      onSubmitted: (_) => _send(),
                    ),
                  ),
                  const SizedBox(width: 8),
                  IconButton.filled(
                    tooltip: 'Send',
                    icon: const Icon(Icons.send),
                    onPressed: _send,
                  ),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }
}

/// Shows how many messages are waiting in the offline buffer.
class PendingSendsBanner extends StatelessWidget {
  const PendingSendsBanner({super.key, required this.socket});

  final TypedSocket socket;

  @override
  Widget build(BuildContext context) {
    return StreamBuilder<int>(
      stream: socket.pendingSendsChanges,
      initialData: socket.pendingSends,
      builder: (context, snapshot) {
        final count = snapshot.data ?? 0;
        if (count == 0) return const SizedBox.shrink();
        return Container(
          width: double.infinity,
          color: Theme.of(context).colorScheme.secondaryContainer,
          padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
          child: Text(
            '$count waiting to send',
            style: TextStyle(
              color: Theme.of(context).colorScheme.onSecondaryContainer,
            ),
          ),
        );
      },
    );
  }
}
