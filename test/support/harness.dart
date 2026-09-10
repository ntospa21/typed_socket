import 'dart:async';

import 'package:typed_socket/testing.dart';
import 'package:typed_socket/typed_socket.dart';

final Uri testUri = Uri.parse('ws://test.local/socket');
const Duration oneSecond = Duration(seconds: 1);

/// A socket on [transport] with deterministic defaults: a fixed one-second
/// backoff and no heartbeat.
TypedSocket fakeSocket(
  FakeTransport transport, {
  BackoffPolicy? backoff,
  int? maxReconnectAttempts,
  HeartbeatConfig? heartbeat,
  OfflinePolicy offlinePolicy = OfflinePolicy.bufferDropOldest,
  int bufferCapacity = 100,
  OnConnected? onConnected,
  FutureOr<Uri> Function()? uriProvider,
}) =>
    TypedSocket(
      uri: uriProvider == null ? testUri : null,
      uriProvider: uriProvider,
      transport: transport,
      backoff: backoff ?? Backoff.fixed(oneSecond),
      maxReconnectAttempts: maxReconnectAttempts,
      heartbeat: heartbeat,
      offlinePolicy: offlinePolicy,
      bufferCapacity: bufferCapacity,
      onConnected: onConnected,
    );

class Chat {
  const Chat(this.user, this.text);

  factory Chat.fromJson(Map<String, dynamic> json) =>
      Chat(json['user'] as String, json['text'] as String);

  final String user;
  final String text;

  Map<String, dynamic> toJson() => {'user': user, 'text': text};

  @override
  bool operator ==(Object other) =>
      other is Chat && other.user == user && other.text == text;

  @override
  int get hashCode => Object.hash(user, text);

  @override
  String toString() => 'Chat($user: $text)';
}

/// Records every event of a stream, for inspection inside fake_async.
class Recorder<T> {
  Recorder(Stream<T> stream) {
    _subscription = stream.listen(
      values.add,
      onError: errors.add,
      onDone: () => isDone = true,
    );
  }

  final List<T> values = [];
  final List<Object> errors = [];
  bool isDone = false;
  late final StreamSubscription<T> _subscription;

  Future<void> cancel() => _subscription.cancel();
}

/// Tracks how a future completes without awaiting it.
class Outcome<T> {
  Outcome(Future<T> future) {
    future.then(
      (v) {
        value = v;
        isComplete = true;
      },
      onError: (Object e) {
        error = e;
        isComplete = true;
      },
    );
  }

  bool isComplete = false;
  T? value;
  Object? error;
}

/// The `data` payloads of every envelope sent on [connection].
List<Object?> sentData(FakeConnection connection) =>
    [for (final e in connection.sentEnvelopes) e.data];
