package kots.mq.amqp

import scala.concurrent.duration._

/** Where the broker is and how a publish is confirmed. */
final case class AmqpSettings(
  uri: String,
  mandatory: Boolean,
  confirmTimeout: FiniteDuration,
) {
  def withMandatory(required: Boolean): AmqpSettings = copy(mandatory = required)
  def withConfirmTimeout(timeout: FiniteDuration): AmqpSettings = copy(confirmTimeout = timeout)
}

object AmqpSettings {
  def local(uri: String): AmqpSettings = AmqpSettings(uri, mandatory = false, 5.seconds)
}

/** Raised when the broker refuses or fails to route a publish. */
final case class AmqpPublishFailed(reason: String) extends RuntimeException(reason)
