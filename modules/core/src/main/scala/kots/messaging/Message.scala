package kots.messaging

import scala.concurrent.duration.FiniteDuration

/** Identity a broker gives a published message. */
final case class MessageId(value: String)

/** Value that fixes order between related messages. */
final case class MessageKey(value: String)

/** The fields brokers carry themselves, rather than as headers. */
final case class MessageProperties(
  contentType: Option[String],
  correlationId: Option[String],
  replyTo: Option[Destination],
  priority: Option[Int],
  persistent: Boolean,
  expiry: Option[FiniteDuration],
) {
  def withContentType(value: String): MessageProperties = copy(contentType = Some(value))
  def withCorrelationId(value: String): MessageProperties = copy(correlationId = Some(value))
  def withReplyTo(destination: Destination): MessageProperties = copy(replyTo = Some(destination))
  def withPriority(value: Int): MessageProperties = copy(priority = Some(value))
  def withPersistent(value: Boolean): MessageProperties = copy(persistent = value)
  def withExpiry(after: FiniteDuration): MessageProperties = copy(expiry = Some(after))
}

object MessageProperties {

  val default: MessageProperties =
    MessageProperties(None, None, None, None, persistent = true, None)
}

/** Message as published: payload, headers, ordering key and properties. */
final case class Message[A](
  payload: A,
  headers: Map[String, String],
  key: Option[MessageKey],
  properties: MessageProperties = MessageProperties.default,
) {

  /** Same headers, key and properties, another payload. */
  def as[B](payload: B): Message[B] = Message(payload, headers, key, properties)

  def withProperties(values: MessageProperties): Message[A] = copy(properties = values)
}

object Message {

  /** Message with no headers, no ordering key and the default properties. */
  def of[A](payload: A): Message[A] = Message(payload, Map.empty, None)
}
