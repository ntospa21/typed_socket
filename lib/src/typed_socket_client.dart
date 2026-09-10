import 'dart:async';

import 'backoff.dart';
import 'channel_registry.dart';
import 'envelope.dart';
import 'errors.dart';
import 'heartbeat.dart';
import 'send_buffer.dart';
import 'state.dart';
import 'transport/transport.dart';
import 'transport/web_socket_transport.dart';

/// Runs after every successful transport connect, before the offline buffer
/// flushes. See [TypedSocket.new].
typedef OnConnected = Future<void> Function(ConnectedContext ctx);

const int _normalClosure = 1000;
const int _heartbeatTimeoutClosure = 4000;

/// A typed, resilient WebSocket client.
///
/// ```dart
/// final socket = TypedSocket(uri: Uri.parse('wss://example.com/ws'))
///   ..on<ChatMessage>('message', ChatMessage.fromJson,
///       toJson: (m) => m.toJson());
///
/// socket.stream<ChatMessage>('message').listen(print);
/// await socket.connect();
/// socket.sendTyped('message', ChatMessage('sam', 'hi'));
/// ```
///
/// The socket reconnects automatically with backoff, detects dead
/// connections with an optional heartbeat, isolates malformed messages, and
/// applies an explicit [OfflinePolicy] to sends made while disconnected.
/// Network problems never throw from its methods; they surface through
/// [states], [frameErrors], [connectionErrors], and channel stream errors.
///
/// Delivery is **at-most-once**: [SendOutcome.sent] means the frame was
/// handed to a live connection, not that the server received it. A frame
/// written in the same instant the connection dies can be lost.
class TypedSocket {
  /// Creates a socket. Nothing happens until [connect] is called.
  ///
  /// Provide exactly one of [uri] or [uriProvider]. [uriProvider] is called
  /// before every attempt, so short-lived tokens in query parameters can be
  /// refreshed.
  ///
  /// [transport] defaults to [WebSocketTransport]. [backoff] defaults to
  /// [Backoff.exponentialJitter]. [maxReconnectAttempts] limits consecutive
  /// retries per outage; null retries forever. [heartbeat] enables liveness
  /// detection; null disables it.
  ///
  /// [onConnected] runs after every successful transport connect and before
  /// the buffer flushes: the place for hello frames, auth messages, and
  /// resubscription. While it runs the state stays
  /// [TypedSocketState.connecting] and app sends are buffered. If it throws,
  /// the attempt counts as failed and backoff applies.
  ///
  /// Throws [ArgumentError] for invalid configuration.
  TypedSocket({
    Uri? uri,
    FutureOr<Uri> Function()? uriProvider,
    Transport? transport,
    EnvelopeCodec codec = const JsonEnvelopeCodec(),
    BackoffPolicy? backoff,
    int? maxReconnectAttempts,
    HeartbeatConfig? heartbeat,
    OfflinePolicy offlinePolicy = OfflinePolicy.bufferDropOldest,
    int bufferCapacity = 100,
    OnConnected? onConnected,
  })  : _uri = uri,
        _uriProvider = uriProvider,
        _transport = transport ?? const WebSocketTransport(),
        _codec = codec,
        _backoff = backoff ?? Backoff.exponentialJitter(),
        _maxReconnectAttempts = maxReconnectAttempts,
        _heartbeatConfig = heartbeat,
        _buffer = SendBuffer(policy: offlinePolicy, capacity: bufferCapacity),
        _onConnected = onConnected {
    if ((uri == null) == (uriProvider == null)) {
      throw ArgumentError('Provide exactly one of uri or uriProvider');
    }
    if (maxReconnectAttempts != null && maxReconnectAttempts < 0) {
      throw ArgumentError.value(
        maxReconnectAttempts,
        'maxReconnectAttempts',
        'must be >= 0, or null to retry forever',
      );
    }
    heartbeat?.validate();
    // Fire-and-forget `connect()` must not crash the app through an
    // unhandled error; awaiting callers still receive it.
    _firstConnect.future.ignore();
  }

  final Uri? _uri;
  final FutureOr<Uri> Function()? _uriProvider;
  final Transport _transport;
  final EnvelopeCodec _codec;
  final BackoffPolicy _backoff;
  final int? _maxReconnectAttempts;
  final HeartbeatConfig? _heartbeatConfig;
  final SendBuffer _buffer;
  final OnConnected? _onConnected;
  final ChannelRegistry _channels = ChannelRegistry();

  final StreamController<TypedSocketState> _stateController =
      StreamController<TypedSocketState>.broadcast();
  final StreamController<int> _pendingController =
      StreamController<int>.broadcast();
  final StreamController<Envelope> _unhandledController =
      StreamController<Envelope>.broadcast();
  final StreamController<TypedSocketDecodeError> _frameErrorController =
      StreamController<TypedSocketDecodeError>.broadcast();
  final StreamController<TypedSocketConnectionError>
      _connectionErrorController =
      StreamController<TypedSocketConnectionError>.broadcast();

  final Completer<void> _firstConnect = Completer<void>();
  Completer<void>? _nextConnect;
  final Completer<CloseReason> _done = Completer<CloseReason>();
  Future<void>? _closeFuture;

  TypedSocketState _state = TypedSocketState.idle;
  CloseReason? _closeReason;

  /// Incremented whenever the current attempt or connection is abandoned.
  /// Every asynchronous callback captures the generation it belongs to and
  /// does nothing if it has changed, so stale events from an old connection
  /// can never affect a newer one.
  int _generation = 0;

  /// 0 for the first attempt after [connect] or [reconnect], then 1, 2, ...
  /// for retries. Reset when the socket reaches connected.
  int _attempt = 0;

  TransportConnection? _connection;
  StreamSubscription<Object>? _incomingSub;
  HeartbeatMonitor? _heartbeat;
  Timer? _retryTimer;

  /// Completes when the transport connect in flight settles, abandoned or
  /// not. A new attempt waits for it, so at most one is ever in flight.
  Future<void>? _connectInFlight;
  int _lastReportedPending = 0;

  // ---------------------------------------------------------------------
  // Channels
  // ---------------------------------------------------------------------

  /// Registers a typed channel for [event]. Returns this for cascades.
  ///
  /// Incoming envelopes for [event] are decoded with [decode] and delivered
  /// on [stream]. Pass [toJson] to enable [sendTyped] for this event.
  ///
  /// Throws [ArgumentError] if [event] is already registered or is one of
  /// the heartbeat's reserved event names.
  TypedSocket on<T>(String event, Decoder<T> decode, {Encoder<T>? toJson}) {
    _checkOpen('on');
    final heartbeat = _heartbeatConfig;
    if (heartbeat != null &&
        (event == heartbeat.pingEvent || event == heartbeat.pongEvent)) {
      throw ArgumentError.value(
        event,
        'event',
        'is reserved for heartbeat frames',
      );
    }
    _channels.register<T>(event, decode, toJson);
    return this;
  }

  /// A broadcast stream of decoded values for [event].
  ///
  /// The stream lives as long as the socket, across reconnects; listeners
  /// never need to resubscribe. Decode failures arrive as stream errors of
  /// type [TypedSocketDecodeError]; the stream and the connection stay open.
  ///
  /// Throws [ArgumentError] if [event] is not registered or [T] does not
  /// match the registered type.
  Stream<T> stream<T>(String event) {
    _checkOpen('stream');
    return _channels.stream<T>(event);
  }

  // ---------------------------------------------------------------------
  // Sending
  // ---------------------------------------------------------------------

  /// Sends [value] on [event], encoded with the `toJson` registered by [on].
  ///
  /// Returns synchronously. See [send] for the meaning of the outcome.
  ///
  /// Throws [ArgumentError] if [event] is not registered, has no `toJson`,
  /// or [value] is not of the registered type.
  SendOutcome sendTyped<T>(String event, T value) {
    _checkOpen('sendTyped');
    final json = _channels.encode(event, value);
    return _sendFrame(event, _codec.encode(Envelope(event, json)));
  }

  /// Sends a raw, already JSON-encodable [data] payload on [event].
  ///
  /// Returns synchronously:
  ///
  /// * [SendOutcome.sent] if the frame was handed to a live connection. This
  ///   does not mean the server received it.
  /// * [SendOutcome.buffered] if the socket is not connected and the frame
  ///   was queued. Buffered frames flush in order after the next connect.
  /// * [SendOutcome.dropped] if the offline policy discarded it.
  ///
  /// Throws [TypedSocketOfflineException] under [OfflinePolicy.reject] when
  /// not connected, and [ArgumentError] if [data] cannot be encoded.
  SendOutcome send(String event, Object? data) {
    _checkOpen('send');
    return _sendFrame(event, _codec.encode(Envelope(event, data)));
  }

  // ---------------------------------------------------------------------
  // Observation
  // ---------------------------------------------------------------------

  /// The current connection state.
  TypedSocketState get state => _state;

  /// Every state transition, exactly once each.
  ///
  /// Replays the current state to each new listener, so a Flutter
  /// `StreamBuilder` shows the right state immediately. Each listener gets
  /// its own subscription; the stream can be listened to any number of
  /// times.
  Stream<TypedSocketState> get states => _states;

  late final Stream<TypedSocketState> _states =
      Stream<TypedSocketState>.multi((listener) {
    listener.add(_state);
    if (_state == TypedSocketState.closed) {
      listener.close();
      return;
    }
    // The source controller is already asynchronous, so forward
    // synchronously: states then arrive in the same microtask order as every
    // other stream of this socket. addSync never overtakes the replayed
    // value above, because it waits for pending events.
    final sub = _stateController.stream.listen(
      listener.addSync,
      onError: listener.addErrorSync,
      onDone: listener.closeSync,
    );
    listener
      ..onPause = sub.pause
      ..onResume = sub.resume
      // Deliberately not returning the cancel future: a finished broadcast
      // subscription returns a root-zone future, which would delay this
      // stream's done event past fake_async's microtask queue.
      ..onCancel = () {
        unawaited(sub.cancel());
      };
  }, isBroadcast: true);

  /// The number of frames waiting in the offline buffer.
  int get pendingSends => _buffer.length;

  /// Emits the new [pendingSends] value after every change to it.
  Stream<int> get pendingSendsChanges => _pendingController.stream;

  /// Valid envelopes whose event has no registered channel.
  Stream<Envelope> get unhandledFrames => _unhandledController.stream;

  /// Frames that are not valid envelopes, such as invalid JSON or a missing
  /// event field. The connection stays up.
  Stream<TypedSocketDecodeError> get frameErrors =>
      _frameErrorController.stream;

  /// Failed connection attempts and connections torn down by errors
  /// (including heartbeat timeouts), for logging and diagnostics.
  ///
  /// A clean remote close is not an error and is only visible on [states].
  Stream<TypedSocketConnectionError> get connectionErrors =>
      _connectionErrorController.stream;

  /// Why the socket closed, or null if it has not.
  CloseReason? get closeReason => _closeReason;

  /// Completes with the [CloseReason] when the socket reaches
  /// [TypedSocketState.closed].
  Future<CloseReason> get done => _done.future;

  // ---------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------

  /// Starts connecting. Idempotent.
  ///
  /// Completes when the socket first reaches [TypedSocketState.connected].
  /// Completes with [TypedSocketClosedException] if the socket closes
  /// first. It is safe not to await it: the socket keeps retrying in the
  /// background either way.
  ///
  /// Throws [StateError] after [close].
  Future<void> connect() {
    _checkOpen('connect');
    if (_state == TypedSocketState.idle) {
      _attempt = 0;
      _startAttempt();
    }
    return _firstConnect.future;
  }

  /// Drops the current connection (or pending attempt) and reconnects
  /// immediately, resetting the attempt counter. Use after refreshing an
  /// auth token.
  ///
  /// Completes when the socket next reaches [TypedSocketState.connected],
  /// or with [TypedSocketClosedException] if it closes first.
  ///
  /// Throws [StateError] after [close].
  Future<void> reconnect() {
    _checkOpen('reconnect');
    final completer = _nextConnect ??= Completer<void>()..future.ignore();
    _attempt = 0;
    _cancelRetry();
    _dropConnection(code: _normalClosure, reason: 'reconnect requested');
    if (_state == TypedSocketState.connected) {
      _setState(TypedSocketState.reconnecting);
    }
    _startAttempt();
    return completer.future;
  }

  /// Closes the socket for good: clears the buffer, cancels timers, closes
  /// the connection, and closes all streams.
  ///
  /// Idempotent, so it is safe to call from a `dispose` method even if the
  /// socket already closed itself. Every other method throws [StateError]
  /// afterwards.
  Future<void> close() => _shutdown(CloseReason.userClosed);

  // ---------------------------------------------------------------------
  // Engine
  // ---------------------------------------------------------------------

  void _checkOpen(String method) {
    if (_state == TypedSocketState.closed) {
      throw StateError(
        'TypedSocket.$method() called after the socket closed '
        '($_closeReason)',
      );
    }
  }

  void _setState(TypedSocketState next) {
    if (next == _state) return;
    _state = next;
    _stateController.add(next);
  }

  void _startAttempt() {
    final gen = ++_generation;
    _setState(TypedSocketState.connecting);
    unawaited(_runAttempt(gen, _attempt));
  }

  Future<void> _runAttempt(int gen, int attempt) async {
    while (_connectInFlight != null) {
      await _connectInFlight;
      if (gen != _generation) return;
    }

    final TransportConnection connection;
    final inFlight = Completer<void>();
    _connectInFlight = inFlight.future;
    try {
      final uri = await Future<Uri>.sync(
        () => _uriProvider != null ? _uriProvider() : _uri!,
      );
      if (gen != _generation) return;
      connection = await _transport.connect(uri);
    } catch (error, stackTrace) {
      if (gen == _generation) _attemptFailed(error, stackTrace);
      return;
    } finally {
      _connectInFlight = null;
      inFlight.complete();
    }

    if (gen != _generation) {
      // Abandoned by close() or reconnect() while the connect was in flight.
      unawaited(_closeQuietly(connection, _normalClosure, 'abandoned'));
      return;
    }

    _connection = connection;
    _incomingSub = connection.incoming.listen(
      (frame) => _onFrame(gen, frame),
      onError: (Object error, StackTrace stackTrace) =>
          _onConnectionLost(gen, error, stackTrace),
      onDone: () => _onConnectionLost(gen),
    );
    unawaited(
      connection.done.then<void>(
        (_) => _onConnectionLost(gen),
        onError: (Object error, StackTrace stackTrace) =>
            _onConnectionLost(gen, error, stackTrace),
      ),
    );
    _startHeartbeat(gen);

    final onConnected = _onConnected;
    if (onConnected != null) {
      try {
        await onConnected(ConnectedContext._(this, connection, gen, attempt));
      } catch (error, stackTrace) {
        if (gen != _generation) return;
        _dropConnection(code: _normalClosure, reason: 'onConnected failed');
        _attemptFailed(error, stackTrace);
        return;
      }
      // The connection dropped, or close()/reconnect() ran, meanwhile.
      if (gen != _generation) return;
    }

    _attempt = 0;
    _setState(TypedSocketState.connected);
    if (!_firstConnect.isCompleted) _firstConnect.complete();
    final next = _nextConnect;
    _nextConnect = null;
    next?.complete();
    _flush(gen);
  }

  void _attemptFailed(Object error, StackTrace stackTrace) {
    _reportConnectionError(error, stackTrace);
    _scheduleRetry();
  }

  void _onConnectionLost(int gen, [Object? error, StackTrace? stackTrace]) {
    if (gen != _generation) return;
    if (error != null) {
      _reportConnectionError(error, stackTrace ?? StackTrace.empty);
    }
    _dropConnection();
    _scheduleRetry();
  }

  void _scheduleRetry() {
    final next = _attempt + 1;
    final max = _maxReconnectAttempts;
    if (max != null && next > max) {
      unawaited(_shutdown(CloseReason.attemptsExhausted));
      return;
    }
    _attempt = next;
    final gen = ++_generation;
    _setState(TypedSocketState.reconnecting);
    _retryTimer = Timer(_backoff(next), () {
      _retryTimer = null;
      if (gen == _generation) _startAttempt();
    });
  }

  void _cancelRetry() {
    _retryTimer?.cancel();
    _retryTimer = null;
  }

  /// Abandons the current connection, if any, and invalidates every callback
  /// belonging to it.
  TransportConnection? _detachConnection() {
    _generation++;
    _heartbeat?.stop();
    _heartbeat = null;
    unawaited(_incomingSub?.cancel());
    _incomingSub = null;
    final connection = _connection;
    _connection = null;
    return connection;
  }

  void _dropConnection({int? code, String? reason}) {
    final connection = _detachConnection();
    if (connection != null) {
      unawaited(_closeQuietly(connection, code, reason));
    }
  }

  Future<void> _closeQuietly(
    TransportConnection connection,
    int? code,
    String? reason,
  ) async {
    try {
      await connection.close(code, reason);
    } catch (_) {
      // Closing a connection that is already broken can fail. It is gone
      // either way, and the socket has already moved on.
    }
  }

  void _onFrame(int gen, Object frame) {
    if (gen != _generation) return;
    _heartbeat?.onInbound();

    final Envelope envelope;
    try {
      envelope = _codec.decode(frame);
    } catch (error, stackTrace) {
      _frameErrorController.add(
        TypedSocketDecodeError(
          event: TypedSocketDecodeError.frameEvent,
          raw: frame,
          cause: error,
          stackTrace: stackTrace,
        ),
      );
      return;
    }

    final heartbeat = _heartbeatConfig;
    if (heartbeat != null && envelope.event == heartbeat.pongEvent) return;
    if (!_channels.dispatch(envelope)) _unhandledController.add(envelope);
  }

  void _startHeartbeat(int gen) {
    final config = _heartbeatConfig;
    if (config == null) return;
    _heartbeat = HeartbeatMonitor(
      config,
      onPing: () {
        if (gen != _generation) return;
        try {
          _connection!.send(_codec.encode(Envelope(config.pingEvent)));
        } catch (error, stackTrace) {
          _onConnectionLost(gen, error, stackTrace);
        }
      },
      onTimeout: () {
        if (gen != _generation) return;
        _reportConnectionError(
          TimeoutException(
            'No inbound traffic within ${config.timeout} of a heartbeat ping',
            config.timeout,
          ),
          StackTrace.current,
        );
        _dropConnection(
          code: _heartbeatTimeoutClosure,
          reason: 'heartbeat timeout',
        );
        _scheduleRetry();
      },
    )..start();
  }

  SendOutcome _sendFrame(String event, Object frame) {
    if (_state == TypedSocketState.connected) {
      final gen = _generation;
      try {
        _connection!.send(frame);
        return SendOutcome.sent;
      } catch (error, stackTrace) {
        // The connection died before the socket noticed. Treat it as lost
        // and let the offline policy decide what happens to this frame.
        _onConnectionLost(gen, error, stackTrace);
      }
    }
    final outcome = _buffer.add(frame, event: event);
    _reportPending();
    return outcome;
  }

  void _flush(int gen) {
    final connection = _connection!;
    while (_buffer.isNotEmpty) {
      final frame = _buffer.first;
      try {
        connection.send(frame);
      } catch (error, stackTrace) {
        // Frames not yet written stay buffered, in order.
        _onConnectionLost(gen, error, stackTrace);
        return;
      }
      if (_buffer.isNotEmpty && identical(_buffer.first, frame)) {
        _buffer.removeFirst();
        _reportPending();
      }
      if (gen != _generation) return;
    }
  }

  void _reportPending() {
    final pending = _buffer.length;
    if (pending == _lastReportedPending) return;
    _lastReportedPending = pending;
    _pendingController.add(pending);
  }

  void _reportConnectionError(Object error, StackTrace stackTrace) {
    _connectionErrorController.add(
      TypedSocketConnectionError(
        cause: error,
        stackTrace: stackTrace,
        attempt: _attempt,
      ),
    );
  }

  Future<void> _shutdown(CloseReason reason) =>
      _closeFuture ??= _doShutdown(reason);

  Future<void> _doShutdown(CloseReason reason) async {
    _cancelRetry();
    final connection = _detachConnection();
    _buffer.clear();
    _reportPending();
    _closeReason = reason;
    _setState(TypedSocketState.closed);
    _done.complete(reason);

    final error = TypedSocketClosedException(reason);
    if (!_firstConnect.isCompleted) _firstConnect.completeError(error);
    _nextConnect?.completeError(error);
    _nextConnect = null;

    for (final controller in <StreamController<Object?>>[
      _stateController,
      _pendingController,
      _unhandledController,
      _frameErrorController,
      _connectionErrorController,
    ]) {
      unawaited(controller.close());
    }
    _channels.close();

    if (connection != null) {
      await _closeQuietly(connection, _normalClosure, 'client closed');
    }
  }
}

/// Passed to `onConnected`: access to the freshly opened connection before
/// the socket is marked connected.
final class ConnectedContext {
  ConnectedContext._(
    this._socket,
    this._connection,
    this._generation,
    this.attempt,
  );

  final TypedSocket _socket;
  final TransportConnection _connection;
  final int _generation;

  /// Which attempt produced this connection: 0 for the first attempt after
  /// `connect()` or `reconnect()`, then 1, 2, ... for retries.
  final int attempt;

  /// Sends directly on the new connection, bypassing the offline buffer.
  /// These frames go out before any buffered frames.
  ///
  /// Throws [StateError] if this connection has already been abandoned, for
  /// example because it dropped while `onConnected` was running.
  void send(String event, Object? data) {
    if (_socket._generation != _generation) {
      throw StateError(
        'ConnectedContext.send() called after its connection ended',
      );
    }
    _connection.send(_socket._codec.encode(Envelope(event, data)));
  }
}
