import 'package:test/test.dart';
import 'package:typed_socket/src/send_buffer.dart';
import 'package:typed_socket/typed_socket.dart';

void main() {
  SendBuffer buffer(OfflinePolicy policy, [int capacity = 2]) =>
      SendBuffer(policy: policy, capacity: capacity);

  test('rejects a capacity below 1', () {
    expect(
        () => buffer(OfflinePolicy.bufferDropOldest, 0), throwsArgumentError);
  });

  test('bufferDropOldest evicts the oldest frame when full', () {
    final b = buffer(OfflinePolicy.bufferDropOldest);
    expect(b.add('a', event: 'e'), SendOutcome.buffered);
    expect(b.add('b', event: 'e'), SendOutcome.buffered);
    expect(b.add('c', event: 'e'), SendOutcome.buffered);
    expect(b.toList(), ['b', 'c']);
    expect(b.length, 2);
  });

  test('bufferDropNew discards the new frame when full', () {
    final b = buffer(OfflinePolicy.bufferDropNew);
    expect(b.add('a', event: 'e'), SendOutcome.buffered);
    expect(b.add('b', event: 'e'), SendOutcome.buffered);
    expect(b.add('c', event: 'e'), SendOutcome.dropped);
    expect(b.toList(), ['a', 'b']);
  });

  test('drop keeps nothing', () {
    final b = buffer(OfflinePolicy.drop);
    expect(b.add('a', event: 'e'), SendOutcome.dropped);
    expect(b.isEmpty, isTrue);
  });

  test('reject throws TypedSocketOfflineException naming the event', () {
    final b = buffer(OfflinePolicy.reject);
    expect(
      () => b.add('a', event: 'chat'),
      throwsA(
        isA<TypedSocketOfflineException>()
            .having((e) => e.event, 'event', 'chat')
            .having((e) => e.toString(), 'toString', contains('"chat"')),
      ),
    );
    expect(b.isEmpty, isTrue);
    expect(
      const TypedSocketOfflineException().toString(),
      contains('not connected'),
    );
  });

  test('is FIFO', () {
    final b = buffer(OfflinePolicy.bufferDropOldest, 10);
    for (final f in ['1', '2', '3']) {
      b.add(f, event: 'e');
    }
    expect(b.isNotEmpty, isTrue);
    expect(b.first, '1');
    b.removeFirst();
    expect(b.first, '2');
    b.clear();
    expect(b.isEmpty, isTrue);
    expect(b.length, 0);
  });
}
