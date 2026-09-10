class ChatMessage {
  const ChatMessage({required this.user, required this.text});

  factory ChatMessage.fromJson(Map<String, dynamic> json) => ChatMessage(
        user: json['user'] as String,
        text: json['text'] as String,
      );

  final String user;
  final String text;

  Map<String, dynamic> toJson() => {'user': user, 'text': text};
}
