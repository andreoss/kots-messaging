package kots.mq

/** Identity a broker gives a published message. */
final case class MessageId(value: String)

/** Value that fixes order between related messages. */
final case class MessageKey(value: String)

/** Message as published: payload, headers and an optional ordering key. */
final case class Message[A](
  payload: A,
  headers: Map[String, String],
  key: Option[MessageKey],
) {

  /** Same headers and key, another payload. */
  def as[B](payload: B): Message[B] = Message(payload, headers, key)
}

object Message {

  /** Message with no headers and no ordering key. */
  def of[A](payload: A): Message[A] = Message(payload, Map.empty, None)
}
