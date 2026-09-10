// Runs the echo server on a fixed port for the Flutter chat example.
//
//     dart run example/serve.dart [port]
import '../test/fixtures/echo_server.dart';

Future<void> main(List<String> args) async {
  final port = args.isEmpty ? 8080 : int.parse(args.first);
  final server = await EchoServer.start(port: port);
  print('Echo server listening on ${server.uri} (Ctrl+C to stop)');
}
