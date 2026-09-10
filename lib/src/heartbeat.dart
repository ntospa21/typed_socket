import 'dart:async';

import 'package:meta/meta.dart';

/// Application-level ping/pong liveness detection.
///
/// A half-open TCP connection (for example after a phone switches networks)
/// can look alive forever. With a heartbeat configured, the socket sends a
/// [pingEvent] envelope after [interval] passes with no inbound traffic, and
/// tears the connection down if nothing at all arrives within [timeout]
/// after that.
///
/// Your server must answer a ping with any frame, ideally a [pongEvent]
/// envelope. Any inbound frame counts as proof of liveness, so busy
/// connections are never pinged.
@immutable
class HeartbeatConfig {
  /// Creates a heartbeat configuration.
  const HeartbeatConfig({
    this.interval = const Duration(seconds: 25),
    this.timeout = const Duration(seconds: 10),
    this.pingEvent = '__ping',
    this.pongEvent = '__pong',
  });

  /// How long the connection may be silent before a ping is sent.
  final Duration interval;

  /// How long to wait for any inbound frame after a ping before the
  /// connection is considered dead.
  final Duration timeout;

  /// The event name of ping envelopes sent by the client.
  final String pingEvent;

  /// The event name of pong envelopes. Pongs are swallowed and never reach
  /// `unhandledFrames`.
  final String pongEvent;

  /// Throws [ArgumentError] if this configuration is unusable.
  void validate() {
    if (interval <= Duration.zero) {
      throw ArgumentError.value(interval, 'interval', 'must be positive');
    }
    if (timeout <= Duration.zero) {
      throw ArgumentError.value(timeout, 'timeout', 'must be positive');
    }
  }
}

/// Runs the idle and timeout timers for one connection.
class HeartbeatMonitor {
  /// Creates a monitor that calls [onPing] after an idle interval and
  /// [onTimeout] if no traffic follows the ping.
  HeartbeatMonitor(
    this.config, {
    required this.onPing,
    required this.onTimeout,
  });

  /// The timing configuration.
  final HeartbeatConfig config;

  /// Called when a ping should be sent.
  final void Function() onPing;

  /// Called when the connection should be considered dead.
  final void Function() onTimeout;

  Timer? _idleTimer;
  Timer? _timeoutTimer;

  /// Whether the monitor has live timers.
  bool get isRunning => _idleTimer != null || _timeoutTimer != null;

  /// Starts (or restarts) the idle timer.
  void start() {
    stop();
    _idleTimer = Timer(config.interval, _onIdle);
  }

  /// Records inbound traffic: cancels any pending timeout and restarts the
  /// idle timer. Does nothing if the monitor is stopped.
  void onInbound() {
    if (isRunning) start();
  }

  /// Cancels all timers.
  void stop() {
    _idleTimer?.cancel();
    _timeoutTimer?.cancel();
    _idleTimer = null;
    _timeoutTimer = null;
  }

  void _onIdle() {
    _idleTimer = null;
    // Arm the timeout before pinging: if the ping itself tears the
    // connection down, stop() cancels it.
    _timeoutTimer = Timer(config.timeout, _onTimeout);
    onPing();
  }

  void _onTimeout() {
    _timeoutTimer = null;
    onTimeout();
  }
}
