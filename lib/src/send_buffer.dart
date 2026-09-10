import 'dart:collection';

import 'errors.dart';

/// The result of a `send` or `sendTyped` call.
enum SendOutcome {
  /// The frame was handed to a live connection. This does not mean the
  /// server received it: delivery is at-most-once.
  sent,

  /// The socket is not connected; the frame is queued and will be flushed
  /// in order after the next successful connect.
  buffered,

  /// The frame was discarded because of the offline policy.
  dropped,
}

/// What happens to sends while the socket is not connected.
///
/// Loss must be chosen, never accidental: every policy either keeps the
/// frame, reports that it was dropped, or throws.
enum OfflinePolicy {
  /// Buffer the frame; when the buffer is full, evict the oldest frame.
  bufferDropOldest,

  /// Buffer the frame; when the buffer is full, discard the new frame and
  /// return [SendOutcome.dropped].
  bufferDropNew,

  /// Discard the frame and return [SendOutcome.dropped]. Suits presence and
  /// telemetry, where stale data is worthless.
  drop,

  /// Throw `TypedSocketOfflineException`; the caller manages its own queue.
  reject,
}

/// A bounded FIFO queue of encoded frames awaiting a connection.
class SendBuffer {
  /// Creates a buffer that applies [policy] and holds at most [capacity]
  /// frames.
  SendBuffer({required this.policy, required this.capacity}) {
    if (capacity < 1) {
      throw ArgumentError.value(capacity, 'bufferCapacity', 'must be >= 1');
    }
  }

  /// The policy applied by [add].
  final OfflinePolicy policy;

  /// The maximum number of buffered frames.
  final int capacity;

  final Queue<Object> _frames = ListQueue<Object>();

  /// The number of buffered frames.
  int get length => _frames.length;

  /// Whether no frames are buffered.
  bool get isEmpty => _frames.isEmpty;

  /// Whether at least one frame is buffered.
  bool get isNotEmpty => _frames.isNotEmpty;

  /// The oldest buffered frame.
  Object get first => _frames.first;

  /// Removes the oldest buffered frame.
  void removeFirst() => _frames.removeFirst();

  /// Applies [policy] to [frame], which is a send of [event].
  ///
  /// Throws [TypedSocketOfflineException] under [OfflinePolicy.reject].
  SendOutcome add(Object frame, {required String event}) {
    switch (policy) {
      case OfflinePolicy.reject:
        throw TypedSocketOfflineException(event);
      case OfflinePolicy.drop:
        return SendOutcome.dropped;
      case OfflinePolicy.bufferDropNew:
        if (_frames.length >= capacity) return SendOutcome.dropped;
        _frames.addLast(frame);
        return SendOutcome.buffered;
      case OfflinePolicy.bufferDropOldest:
        if (_frames.length >= capacity) _frames.removeFirst();
        _frames.addLast(frame);
        return SendOutcome.buffered;
    }
  }

  /// Discards every buffered frame.
  void clear() => _frames.clear();

  /// A snapshot of the buffered frames, oldest first.
  List<Object> toList() => List.unmodifiable(_frames);
}
