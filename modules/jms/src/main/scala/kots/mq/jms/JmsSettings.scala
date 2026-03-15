package kots.mq.jms

import jakarta.jms.Session

import scala.concurrent.duration._

/** How a session acknowledges and what the provider honours. */
final case class JmsSettings(
  acknowledgeMode: Int,
  receiveTimeout: FiniteDuration,
  prioritySupported: Boolean,
  delaySupported: Boolean,
) {
  def withDelaySupported(supported: Boolean): JmsSettings = copy(delaySupported = supported)
  def withAcknowledgeMode(mode: Int): JmsSettings = copy(acknowledgeMode = mode)
  def withReceiveTimeout(timeout: FiniteDuration): JmsSettings = copy(receiveTimeout = timeout)
  def withPrioritySupported(supported: Boolean): JmsSettings = copy(prioritySupported = supported)
}

object JmsSettings {

  val default: JmsSettings =
    JmsSettings(
      Session.CLIENT_ACKNOWLEDGE,
      200.millis,
      prioritySupported = false,
      delaySupported = false,
    )
}
