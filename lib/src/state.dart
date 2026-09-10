/// The connection lifecycle of a `TypedSocket`.
///
/// ```text
/// idle -> connecting -> connected -> reconnecting -> connecting -> ...
///                                                  \-> closed
/// ```
enum TypedSocketState {
  /// Created, but `connect()` has not been called yet.
  idle,

  /// A connection attempt is in progress. This includes the time the
  /// `onConnected` hook runs on a freshly opened transport connection.
  connecting,

  /// The connection is live, `onConnected` has completed, and sends go
  /// straight to the wire.
  connected,

  /// The previous attempt failed or the connection was lost. The socket is
  /// waiting for the backoff delay before the next attempt.
  reconnecting,

  /// Terminal. See `TypedSocket.closeReason` for why.
  closed,
}

/// Why a `TypedSocket` reached [TypedSocketState.closed].
enum CloseReason {
  /// `close()` was called.
  userClosed,

  /// `maxReconnectAttempts` consecutive attempts failed.
  attemptsExhausted,
}
