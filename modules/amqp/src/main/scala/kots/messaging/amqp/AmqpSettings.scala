package kots.messaging.amqp

import scala.concurrent.duration._

/** Where the broker is, how a publish is confirmed, how a receive waits. */
final case class AmqpSettings(
  uris: List[String],
  mandatory: Boolean,
  confirmTimeout: FiniteDuration,
  receiveTimeout: FiniteDuration,
  connectionName: Option[String],
) {
  def withMandatory(required: Boolean): AmqpSettings = copy(mandatory = required)
  def withConfirmTimeout(timeout: FiniteDuration): AmqpSettings = copy(confirmTimeout = timeout)
  def withReceiveTimeout(timeout: FiniteDuration): AmqpSettings = copy(receiveTimeout = timeout)
  def withConnectionName(name: String): AmqpSettings = copy(connectionName = Some(name))

  /** Endpoints to try in order; the first that answers is used. */
  def withUris(endpoints: List[String]): AmqpSettings = copy(uris = endpoints)
}

object AmqpSettings {

  def local(uri: String): AmqpSettings =
    AmqpSettings(List(uri), mandatory = false, 5.seconds, 200.millis, Some("kots-messaging"))
}

/** Why the broker would not take a publish. */
sealed abstract class AmqpPublishFailed(message: String) extends RuntimeException(message)

object AmqpPublishFailed {

  /** No queue is bound to the routing key the publish named. */
  final case class Unroutable(routingKey: String, replyText: String)
    extends AmqpPublishFailed(s"unroutable to $routingKey: $replyText")

  /** The destination exists but nothing is consuming from it. */
  final case class NoConsumers(routingKey: String, replyText: String)
    extends AmqpPublishFailed(s"no consumers on $routingKey: $replyText")

  /** The exchange or queue the publish named does not exist. */
  final case class NotFound(routingKey: String, replyText: String)
    extends AmqpPublishFailed(s"not found: $routingKey: $replyText")

  /** The broker refused the publish outright. */
  final case class Refused(reason: String) extends AmqpPublishFailed(reason)

  /** The broker neither confirmed nor refused within the timeout. */
  final case class NotConfirmed(reason: String) extends AmqpPublishFailed(reason)

  private[amqp] def returned(replyCode: Int, replyText: String, routingKey: String): AmqpPublishFailed =
    replyCode match {
      case 312 => Unroutable(routingKey, replyText)
      case 313 => NoConsumers(routingKey, replyText)
      case 404 => NotFound(routingKey, replyText)
      case _ => Refused(s"$replyCode $replyText on $routingKey")
    }
}

/** Raised when a consumer is used after its channel has closed. */
final case class AmqpChannelClosed(destination: String)
  extends RuntimeException(s"the channel serving $destination is closed")
