# flutter_chat

A chat screen built on `typed_socket`. It shows:

- `ConnectionBadge`: a reusable chip driven by `socket.states`. This is the
  widget every adopter would otherwise write.
- A "waiting to send" banner driven by `socket.pendingSendsChanges`.
- A **Kill the connection** button (power icon) that cuts the live socket
  underneath `TypedSocket`, so you can watch it back off, reconnect, and
  flush anything you typed while it was down.

## Run it

From the package root, start the echo server:

```sh
dart run example/serve.dart
```

Then, in this directory:

```sh
flutter run -d chrome
```

This app ships only the `web/` platform folder. Run `flutter create .` here
to add the others. On the Android emulator the host is `10.0.2.2`:

```sh
flutter run --dart-define=SERVER_URL=ws://10.0.2.2:8080/ws
```

## Test it

```sh
flutter test
```

The widget tests use `FakeTransport` from `package:typed_socket/testing.dart`,
so they need no server. They prove the badge changes to "Reconnecting" when
the connection drops.
