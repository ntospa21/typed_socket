import 'dart:math';

/// Computes how long to wait before reconnect attempt number [attempt].
///
/// [attempt] starts at 1 for the first retry after a failure or a dropped
/// connection. The very first connect after `connect()` never waits.
///
/// Any `Duration Function(int)` is a valid policy, so custom strategies need
/// no subclassing.
typedef BackoffPolicy = Duration Function(int attempt);

/// Factories for common [BackoffPolicy] shapes.
abstract final class Backoff {
  /// Retries immediately with no delay.
  ///
  /// Against a server that is down this retries in a tight loop, so it is
  /// mostly useful in tests.
  static BackoffPolicy none() => (_) => Duration.zero;

  /// Waits the same [delay] before every attempt.
  static BackoffPolicy fixed(Duration delay) {
    if (delay.isNegative) {
      throw ArgumentError.value(delay, 'delay', 'must not be negative');
    }
    return (_) => delay;
  }

  /// Exponential backoff with full jitter, as described by AWS:
  /// `delay = random(0, min(cap, base * 2^(attempt - 1)))`.
  ///
  /// Full jitter spreads reconnecting clients evenly over the window, which
  /// prevents reconnect storms when a server restarts and every client
  /// notices at once.
  ///
  /// Pass [random] to make delays deterministic in tests.
  static BackoffPolicy exponentialJitter({
    Duration base = const Duration(milliseconds: 500),
    Duration cap = const Duration(seconds: 30),
    Random? random,
  }) {
    if (base <= Duration.zero) {
      throw ArgumentError.value(base, 'base', 'must be positive');
    }
    if (cap < base) {
      throw ArgumentError.value(cap, 'cap', 'must be at least base ($base)');
    }
    final rng = random ?? Random();
    return (attempt) {
      final ceiling = exponentialCeiling(attempt, base: base, cap: cap);
      return Duration(
        microseconds: (rng.nextDouble() * ceiling.inMicroseconds).floor(),
      );
    };
  }

  /// The upper bound of the jitter window for [attempt]:
  /// `min(cap, base * 2^(attempt - 1))`.
  ///
  /// Computed by repeated doubling with an early exit, so it never overflows
  /// for large attempt numbers and never relies on bit shifts, which are
  /// limited to 32 bits when compiled to JavaScript.
  static Duration exponentialCeiling(
    int attempt, {
    required Duration base,
    required Duration cap,
  }) {
    if (attempt < 1) {
      throw RangeError.range(attempt, 1, null, 'attempt');
    }
    final capUs = cap.inMicroseconds;
    var ceilingUs = base.inMicroseconds;
    if (ceilingUs >= capUs) return cap;
    for (var i = 1; i < attempt; i++) {
      if (ceilingUs > capUs ~/ 2) return cap;
      ceilingUs *= 2;
    }
    return Duration(microseconds: ceilingUs);
  }
}
