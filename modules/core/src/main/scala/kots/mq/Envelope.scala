package kots.mq

/** Message as received: its identity at the broker and its attempt number. */
final case class Envelope[A](id: MessageId, message: Message[A], attempt: Int)
