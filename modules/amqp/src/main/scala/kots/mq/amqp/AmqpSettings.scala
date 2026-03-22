package kots.mq.amqp

import scala.concurrent.duration._

/** Where the broker is and how a publish is confirmed. */
final case class AmqpSettings(
  uri: String,
  mandatory: Boolean,
  confirmTimeout: FiniteDuration,
  receiveTimeout: FiniteDuration,
) {
  def withMandatory(required: Boolean): AmqpSettings = copy(mandatory = required)
  def withConfirmTimeout(timeout: FiniteDuration): AmqpSettings = copy(confirmTimeout = timeout)
  def withReceiveTimeout(timeout: FiniteDuration): AmqpSettings = copy(receiveTimeout = timeout)
}

object AmqpSettings {
  def local(uri: String): AmqpSettings =
    AmqpSettings(uri, mandatory = false, 5.seconds, 200.millis)
}

/** Raised when the broker refuses or fails to route a publish. */
final case class AmqpPublishFailed(reason: String) extends RuntimeException(reason)

/** Raised when a consumer is used after its channel has closed. */
final case class AmqpChannelClosed(destination: String)
  extends RuntimeException(s"the channel serving $destination is closed")
