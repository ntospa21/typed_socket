import 'dart:typed_data';

import 'package:fake_async/fake_async.dart';
import 'package:test/test.dart';
import 'package:typed_socket/testing.dart';

import '../support/harness.dart';

void main() {
  test('creates one connection per successful connect', () async {
    final fake = FakeTransport();
    expect(fake.lastConnection, isNull);
    final a = await fake.connect(testUri);
    final b = await fake.connect(testUri);
    expect(fake.connections, [a, b]);
    expect(fake.lastConnection, b);
    expect(fake.connectCount, 2);
    expect(fake.requestedUris, [testUri, testUri]);
  });

  test('failNextConnects fails that many connects', () async {
    final fake = FakeTransport()..failNextConnects = 2;
    await expectLater(
      fake.connect(testUri),
      throwsA(isA<FakeConnectException>()),
    );
    await expectLater(
        fake.connect(testUri), throwsA(isA<FakeConnectException>()));
    expect(await fake.connect(testUri), isA<FakeConnection>());
    expect(fake.failNextConnects, 0);
    expect(fake.connections, hasLength(1));
    expect(fake.connectCount, 3);
    expect(
      FakeConnectException(testUri).toString(),
      contains(testUri.toString()),
    );
  });

  test('connectDelay delays connects and tracks concurrency', () {
    fakeAsync((async) {
      final fake = FakeTransport()..connectDelay = oneSecond;
      final first = Outcome(fake.connect(testUri));
      final second = Outcome(fake.connect(testUri));
      async.flushMicrotasks();
      expect(fake.connectsInFlight, 2);
      expect(first.isComplete, isFalse);
      async.elapse(oneSecond);
      expect(first.isComplete, isTrue);
      expect(second.isComplete, isTrue);
      expect(fake.connectsInFlight, 0);
      expect(fake.peakConnectsInFlight, 2);
    });
  });

  group('FakeConnection', () {
    late FakeConnection conn;

    setUp(() async {
      conn = await FakeTransport().connect(testUri) as FakeConnection;
    });

    test('delivers events and raw frames', () async {
      final frames = Recorder(conn.incoming);
      conn
        ..receiveEvent('e', {'a': 1})
        ..receiveRaw('raw')
        ..receiveRaw(Uint8List.fromList([1]));
      await pumpEventQueue();
      expect(frames.values, [
        '{"event":"e","data":{"a":1}}',
        'raw',
        Uint8List.fromList([1]),
      ]);
    });

    test('records sent frames and decodes them', () {
      final seen = <Object>[];
      conn
        ..onSend = seen.add
        ..send('{"event":"a","data":1}')
        ..send('{"event":"b","data":2}');
      expect(conn.sentFrames, hasLength(2));
      expect(conn.sentEvents, ['a', 'b']);
      expect(sentData(conn), [1, 2]);
      expect(seen, conn.sentFrames);
    });

    test('kill ends the connection and is idempotent', () async {
      final frames = Recorder(conn.incoming);
      conn.kill(code: 1006, reason: 'gone');
      conn.kill();
      await conn.done;
      await pumpEventQueue();
      expect(frames.isDone, isTrue);
      expect(conn.isClosed, isTrue);
      expect(conn.closedByClient, isFalse);
      expect(conn.closeCode, 1006);
      expect(conn.closeReason, 'gone');
      expect(() => conn.send('x'), throwsStateError);
      expect(() => conn.receiveRaw('x'), throwsStateError);
    });

    test('close records the client close', () async {
      await conn.close(1000, 'bye');
      await conn.close();
      expect(conn.closedByClient, isTrue);
      expect(conn.closeCode, 1000);
      expect(conn.closeReason, 'bye');
    });

    test('killWithError delivers the error then ends', () async {
      final frames = Recorder(conn.incoming);
      conn.killWithError(StateError('net down'));
      conn.killWithError(StateError('ignored'));
      await pumpEventQueue();
      expect(frames.errors, [isA<StateError>()]);
      expect(frames.isDone, isTrue);
    });

    test('stall swallows everything', () async {
      final frames = Recorder(conn.incoming);
      conn
        ..stall()
        ..receiveEvent('__pong');
      await pumpEventQueue();
      expect(conn.isStalled, isTrue);
      expect(frames.values, isEmpty);
      expect(conn.isClosed, isFalse);
    });

    test('hasListener reflects the subscription', () async {
      expect(conn.hasListener, isFalse);
      final frames = Recorder(conn.incoming);
      expect(conn.hasListener, isTrue);
      await frames.cancel();
      expect(conn.hasListener, isFalse);
    });
  });
}
