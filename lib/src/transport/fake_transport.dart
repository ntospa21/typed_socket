import 'dart:async';
import 'dart:math';

import '../envelope.dart';
import 'transport.dart';

/// A scriptable in-memory [Transport] for tests.
///
/// ```dart
/// final fake = FakeTransport();
/// final socket = TypedSocket(uri: Uri.parse('ws://test'), transport: fake);
/// await socket.connect();
///
/// fake.lastConnection!.receiveEvent('message', {'text': 'hi'});
/// fake.lastConnection!.kill(); // simulate a dropped connection
/// ```
///
/// Works with `fake_async`: the only timer it creates is for
/// [connectDelay].
class FakeTransport implements Transport {
  /// Creates a fake transport. [codec] is used by
  /// [FakeConnection.receiveEvent] and [FakeConnection.sentEnvelopes] and
  /// should match the codec given to the socket.
  FakeTransport({this.codec = const JsonEnvelopeCodec()});

  /// The codec used to build and inspect frames.
  final EnvelopeCodec codec;

  /// The next this many calls to [connect] fail with a
  /// [FakeConnectException]. Decrements on each failure.
  int failNextConnects = 0;

  /// How long each [connect] takes before it succeeds or fails.
  Duration connectDelay = Duration.zero;

  /// Called with every new connection before the client receives it. Use it
  /// to install [FakeConnection.onSend] before the buffer flushes.
  void Function(FakeConnection connection)? onConnect;

  final List<FakeConnection> _connections = [];
  final List<Uri> _requestedUris = [];
  int _inFlight = 0;
  int _peakInFlight = 0;

  /// Every connection ever created, oldest first.
  List<FakeConnection> get connections => List.unmodifiable(_connections);

  /// The most recently created connection, or null if none succeeded yet.
  FakeConnection? get lastConnection =>
      _connections.isEmpty ? null : _connections.last;

  /// The URI passed to every [connect] call, including failed ones.
  List<Uri> get requestedUris => List.unmodifiable(_requestedUris);

  /// The number of [connect] calls so far, including failed ones.
  int get connectCount => _requestedUris.length;

  /// The number of [connect] calls that have not completed yet.
  int get connectsInFlight => _inFlight;

  /// The highest value [connectsInFlight] has ever reached.
  int get peakConnectsInFlight => _peakInFlight;

  @override
  Future<TransportConnection> connect(Uri uri) async {
    _requestedUris.add(uri);
    // Decide now, so changing failNextConnects during the delay does not
    // affect an attempt that already started.
    final shouldFail = failNextConnects > 0;
    if (shouldFail) failNextConnects--;
    _inFlight++;
    _peakInFlight = max(_peakInFlight, _inFlight);
    try {
      if (connectDelay > Duration.zero) {
        await Future<void>.delayed(connectDelay);
      }
      if (shouldFail) throw FakeConnectException(uri);
      final connection = FakeConnection._(uri, codec);
      _connections.add(connection);
      onConnect?.call(connection);
      return connection;
    } finally {
      _inFlight--;
    }
  }
}

/// The error thrown by [FakeTransport.connect] when
/// [FakeTransport.failNextConnects] is positive.
class FakeConnectException implements Exception {
  /// Creates the exception for a connect to [uri].
  const FakeConnectException(this.uri);

  /// The URI of the failed attempt.
  final Uri uri;

  @override
  String toString() => 'FakeConnectException: connect to $uri failed';
}

/// A connection produced by [FakeTransport].
class FakeConnection implements TransportConnection {
  FakeConnection._(this.uri, this._codec);

  /// The URI this connection was opened with.
  final Uri uri;

  final EnvelopeCodec _codec;
  final StreamController<Object> _incoming = StreamController<Object>();
  final Completer<void> _done = Completer<void>();
  final List<Object> _sent = [];
  bool _stalled = false;
  bool _closedByClient = false;
  int? _closeCode;
  String? _closeReason;

  /// Called synchronously with every frame the client sends, after it is
  /// recorded. Use it to inject failures at an exact point, for example to
  /// kill the connection halfway through a buffer flush.
  void Function(Object frame)? onSend;

  /// Every frame the client sent on this connection, oldest first.
  List<Object> get sentFrames => List.unmodifiable(_sent);

  /// [sentFrames] decoded with the transport's codec.
  List<Envelope> get sentEnvelopes => [for (final f in _sent) _codec.decode(f)];

  /// The event names of [sentEnvelopes].
  List<String> get sentEvents => [for (final e in sentEnvelopes) e.event];

  /// Whether the connection has ended, by [kill] or by the client closing it.
  bool get isClosed => _done.isCompleted;

  /// Whether the client, rather than [kill], closed the connection.
  bool get closedByClient => _closedByClient;

  /// The close code passed to [close] or [kill], if any.
  int? get closeCode => _closeCode;

  /// The close reason passed to [close] or [kill], if any.
  String? get closeReason => _closeReason;

  /// Whether [stall] was called.
  bool get isStalled => _stalled;

  /// Whether the client is currently listening to [incoming]. After the
  /// client lets go of a connection this must be false, otherwise a
  /// subscription leaked.
  bool get hasListener => _incoming.hasListener;

  @override
  Stream<Object> get incoming => _incoming.stream;

  @override
  Future<void> get done => _done.future;

  @override
  void send(Object frame) {
    if (isClosed) {
      throw StateError('FakeConnection to $uri is closed; frame not sent');
    }
    _sent.add(frame);
    onSend?.call(frame);
  }

  @override
  Future<void> close([int? code, String? reason]) async {
    if (isClosed) return;
    _closedByClient = true;
    _finish(code, reason);
  }

  /// Delivers an envelope for [event] with [data], encoded by the codec.
  void receiveEvent(String event, [Object? data]) =>
      receiveRaw(_codec.encode(Envelope(event, data)));

  /// Delivers a raw [frame] (`String` or `Uint8List`) exactly as given.
  ///
  /// Ignored after [stall]. Throws [StateError] if the connection is closed,
  /// because that is almost always a bug in the test.
  void receiveRaw(Object frame) {
    if (isClosed) {
      throw StateError('FakeConnection to $uri is closed; cannot receive');
    }
    if (_stalled) return;
    _incoming.add(frame);
  }

  /// Simulates the connection dropping: the incoming stream ends and [done]
  /// completes. Does nothing if already closed.
  void kill({int? code, String? reason}) {
    if (isClosed) return;
    _finish(code, reason);
  }

  /// Simulates a network failure the way a real socket reports one: an
  /// [error] on the incoming stream, then the connection ends.
  void killWithError(Object error, [StackTrace? stackTrace]) {
    if (isClosed) return;
    _incoming.addError(error, stackTrace);
    _finish(null, null);
  }

  /// Stops delivering anything, including pongs, while leaving the
  /// connection open. Simulates a half-open connection.
  void stall() => _stalled = true;

  void _finish(int? code, String? reason) {
    _closeCode = code;
    _closeReason = reason;
    unawaited(_incoming.close());
    _done.complete();
  }
}
