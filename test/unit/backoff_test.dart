import 'dart:math';

import 'package:test/test.dart';
import 'package:typed_socket/typed_socket.dart';

class _FixedRandom implements Random {
  _FixedRandom(this.value);

  final double value;

  @override
  double nextDouble() => value;

  @override
  int nextInt(int max) => 0;

  @override
  bool nextBool() => false;
}

const _ms = Duration(milliseconds: 1);

void main() {
  group('Backoff.none', () {
    test('never waits', () {
      final policy = Backoff.none();
      for (var attempt = 1; attempt < 10; attempt++) {
        expect(policy(attempt), Duration.zero);
      }
    });
  });

  group('Backoff.fixed', () {
    test('always returns the same delay', () {
      final policy = Backoff.fixed(const Duration(seconds: 3));
      expect(policy(1), const Duration(seconds: 3));
      expect(policy(100), const Duration(seconds: 3));
    });

    test('rejects negative delays', () {
      expect(() => Backoff.fixed(-_ms), throwsArgumentError);
    });
  });

  group('Backoff.exponentialCeiling', () {
    const base = Duration(milliseconds: 500);
    const cap = Duration(seconds: 30);

    Duration ceiling(int attempt) =>
        Backoff.exponentialCeiling(attempt, base: base, cap: cap);

    test('doubles from base', () {
      expect(ceiling(1), base);
      expect(ceiling(2), base * 2);
      expect(ceiling(3), base * 4);
      expect(ceiling(6), base * 32);
    });

    test('is capped', () {
      expect(ceiling(7), cap); // 32s would exceed 30s
      expect(ceiling(50), cap);
    });

    test('does not overflow for huge attempt numbers', () {
      expect(ceiling(63), cap);
      expect(ceiling(64), cap);
      expect(ceiling(1 << 40), cap);
      expect(ceiling(9007199254740991), cap);
      expect(
        Backoff.exponentialCeiling(
          200,
          base: const Duration(microseconds: 1),
          cap: const Duration(days: 100000),
        ),
        const Duration(days: 100000),
      );
    });

    test('returns cap when base equals cap', () {
      expect(Backoff.exponentialCeiling(1, base: cap, cap: cap), cap);
      expect(Backoff.exponentialCeiling(5, base: cap, cap: cap), cap);
    });

    test('rejects attempts below 1', () {
      expect(() => ceiling(0), throwsRangeError);
      expect(() => ceiling(-1), throwsRangeError);
    });
  });

  group('Backoff.exponentialJitter', () {
    test('scales the ceiling by the random value', () {
      final policy = Backoff.exponentialJitter(random: _FixedRandom(0.5));
      expect(policy(1), const Duration(milliseconds: 250));
      expect(policy(2), const Duration(milliseconds: 500));
      expect(policy(20), const Duration(seconds: 15));
    });

    test('can return zero', () {
      final policy = Backoff.exponentialJitter(random: _FixedRandom(0));
      expect(policy(1), Duration.zero);
      expect(policy(10), Duration.zero);
    });

    test('stays below the ceiling for the largest random value', () {
      final policy = Backoff.exponentialJitter(
        random: _FixedRandom(0.9999999999),
      );
      expect(policy(1), lessThan(const Duration(milliseconds: 500)));
      expect(policy(1), greaterThan(const Duration(milliseconds: 499)));
      expect(policy(100), lessThan(const Duration(seconds: 30)));
    });

    test('stays within [0, ceiling] across many samples', () {
      const base = Duration(milliseconds: 100);
      const cap = Duration(seconds: 5);
      final policy =
          Backoff.exponentialJitter(base: base, cap: cap, random: Random(42));
      for (var attempt = 1; attempt <= 40; attempt++) {
        final ceiling =
            Backoff.exponentialCeiling(attempt, base: base, cap: cap);
        for (var i = 0; i < 200; i++) {
          final delay = policy(attempt);
          expect(delay, greaterThanOrEqualTo(Duration.zero));
          expect(delay, lessThanOrEqualTo(ceiling));
        }
      }
    });

    test('actually jitters', () {
      final policy = Backoff.exponentialJitter(random: Random(7));
      final delays = {for (var i = 0; i < 50; i++) policy(10)};
      expect(delays.length, greaterThan(40));
    });

    test('validates its arguments', () {
      expect(
        () => Backoff.exponentialJitter(base: Duration.zero),
        throwsArgumentError,
      );
      expect(
        () => Backoff.exponentialJitter(
          base: const Duration(seconds: 2),
          cap: const Duration(seconds: 1),
        ),
        throwsArgumentError,
      );
    });

    test('works with the default random source', () {
      final delay = Backoff.exponentialJitter()(3);
      expect(delay, lessThanOrEqualTo(const Duration(seconds: 2)));
    });
  });

  test('any Duration Function(int) is a policy', () {
    Duration linear(int attempt) => Duration(seconds: attempt);
    final BackoffPolicy policy = linear;
    expect(policy(3), const Duration(seconds: 3));
  });
}
