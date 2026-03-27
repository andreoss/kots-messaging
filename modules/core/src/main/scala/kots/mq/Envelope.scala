package kots.mq

/** Message as received: its identity, its attempt, what the broker says. */
final case class Envelope[A](
  id: MessageId,
  message: Message[A],
  attempt: Int,
  redelivered: Boolean = false,
)
