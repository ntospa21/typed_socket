@Tags(['soak'])
@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:test/test.dart';
import 'package:typed_socket/typed_socket.dart';

import '../fixtures/echo_server.dart';

/// Runs for SOAK_SECONDS (default 1800) against the echo server while the
/// server is randomly killed and restarted every few seconds.
///
///     dart test --run-skipped -t soak
///     SOAK_SECONDS=60 dart test --run-skipped -t soak
void main() {
  test('survives random kills without reordering or leaks', () async {
    final seconds =
        int.tryParse(Platform.environment['SOAK_SECONDS'] ?? '') ?? 1800;

    // Track every timer typed_socket creates, to prove none outlive close().
    final engineTimers = <Timer>[];
    final spec = ZoneSpecification(
      createTimer: (self, parent, zone, duration, callback) {
        final timer = parent.createTimer(zone, duration, callback);
        if (StackTrace.current.toString().contains('package:typed_socket/')) {
          engineTimers.add(timer);
        }
        return timer;
      },
    );

    await runZoned(
      () => _soak(Duration(seconds: seconds)),
      zoneSpecification: spec,
    );

    // Allow dart:io's own close handshakes to finish.
    await Future<void>.delayed(const Duration(seconds: 6));
    final leaked = engineTimers.where((t) => t.isActive).length;
    expect(leaked, 0, reason: '$leaked typed_socket timers leaked');
    expect(engineTimers, isNotEmpty, reason: 'the timer tracking must work');
  }, timeout: Timeout.none);
}

Future<void> _soak(Duration duration) async {
  final server = await EchoServer.start();
  final random = Random(1);
  final socket = TypedSocket(
    uri: server.uri,
    backoff: Backoff.exponentialJitter(
      base: const Duration(milliseconds: 50),
      cap: const Duration(seconds: 1),
    ),
    heartbeat: const HeartbeatConfig(
      interval: Duration(seconds: 2),
      timeout: Duration(seconds: 2),
    ),
    bufferCapacity: 1 << 20,
  );
  var reconnects = 0;
  var statesDone = false;
  socket.states.listen(
    (s) {
      if (s == TypedSocketState.reconnecting) reconnects++;
    },
    onDone: () => statesDone = true,
  );
  await socket.connect();

  var running = true;
  var seq = 0;
  var kills = 0;
  var restarts = 0;

  final chaos = () async {
    while (running) {
      await Future<void>.delayed(
        Duration(milliseconds: 1000 + random.nextInt(3000)),
      );
      if (!running) break;
      if (random.nextBool()) {
        kills += await server.killConnections();
      } else {
        await server.stop();
        restarts++;
        await Future<void>.delayed(
          Duration(milliseconds: random.nextInt(1500)),
        );
        await server.restart();
      }
    }
  }();

  final sender = () async {
    while (running) {
      socket.send('seq', ++seq);
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
  }();

  await Future<void>.delayed(duration);
  running = false;
  await Future.wait([chaos, sender]);
  if (!server.isRunning) await server.restart();

  // A final marker proves the last buffered frames were flushed.
  final marker = ++seq;
  socket.send('seq', marker);
  List<int> received() => [
        for (final frame in server.received)
          (jsonDecode(frame) as Map<String, dynamic>)['data'] as int,
      ];
  final deadline = DateTime.now().add(const Duration(seconds: 30));
  while (!received().contains(marker)) {
    if (DateTime.now().isAfter(deadline)) fail('final flush never arrived');
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }

  final seqs = received();
  for (var i = 1; i < seqs.length; i++) {
    if (seqs[i] <= seqs[i - 1]) {
      fail('Reordered or duplicated at $i: ${seqs[i - 1]} then ${seqs[i]}');
    }
  }
  final lost = seq - seqs.length;
  // ignore: avoid_print
  print('soak: ${duration.inSeconds}s, sent $seq, received ${seqs.length}, '
      'lost $lost (at-most-once), kills $kills, restarts $restarts, '
      'reconnects $reconnects');
  expect(kills + restarts, greaterThan(0), reason: 'chaos must happen');
  expect(reconnects, greaterThan(0), reason: 'the client must reconnect');

  await socket.close();
  final closeDeadline = DateTime.now().add(const Duration(seconds: 10));
  while (server.openConnections > 0) {
    if (DateTime.now().isAfter(closeDeadline)) fail('connection leaked');
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
  await server.close();
  expect(statesDone, isTrue, reason: 'states stream left open');
}
