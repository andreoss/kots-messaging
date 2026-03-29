package kots.messaging

import scala.concurrent.duration._

/** How long a consumer holds a message and what it does when it fails. */
final case class ConsumerSettings(
  lease: FiniteDuration,
  maxAttempts: Int,
  prefetch: Int,
  backoff: Backoff,
  deadLetter: Option[Destination],
) {
  def withPrefetch(messages: Int): ConsumerSettings = copy(prefetch = messages max 1)
  def withLease(duration: FiniteDuration): ConsumerSettings = copy(lease = duration)
  def withMaxAttempts(attempts: Int): ConsumerSettings = copy(maxAttempts = attempts max 1)
  def withBackoff(policy: Backoff): ConsumerSettings = copy(backoff = policy)
  def withDeadLetter(destination: Destination): ConsumerSettings =
    copy(deadLetter = Some(destination))
}

object ConsumerSettings {

  val default: ConsumerSettings = ConsumerSettings(30.seconds, 5, 16, Backoff.default, None)
}
