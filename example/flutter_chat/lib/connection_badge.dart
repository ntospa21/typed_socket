import 'package:flutter/material.dart';
import 'package:typed_socket/typed_socket.dart';

/// A chip showing the live connection state of a [TypedSocket].
///
/// `TypedSocket.states` replays the current state to every new listener, so
/// the badge is correct on its first frame and survives rebuilds.
class ConnectionBadge extends StatelessWidget {
  const ConnectionBadge({super.key, required this.socket});

  final TypedSocket socket;

  @override
  Widget build(BuildContext context) {
    return StreamBuilder<TypedSocketState>(
      stream: socket.states,
      initialData: socket.state,
      builder: (context, snapshot) {
        final (label, color, icon) = describe(snapshot.data ?? socket.state);
        return Semantics(
          liveRegion: true,
          label: 'Connection: $label',
          child: Chip(
            avatar: Icon(icon, size: 18, color: color),
            label: Text(label),
            side: BorderSide(color: color),
          ),
        );
      },
    );
  }

  static (String, Color, IconData) describe(TypedSocketState state) =>
      switch (state) {
        TypedSocketState.idle => ('Idle', Colors.grey, Icons.circle_outlined),
        TypedSocketState.connecting => ('Connecting', Colors.amber, Icons.sync),
        TypedSocketState.connected => (
            'Connected',
            Colors.green,
            Icons.check_circle
          ),
        TypedSocketState.reconnecting => (
            'Reconnecting',
            Colors.orange,
            Icons.sync_problem
          ),
        TypedSocketState.closed => ('Closed', Colors.red, Icons.cancel),
      };
}
