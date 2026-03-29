package kots.messaging

import scala.concurrent.duration._

/** How a batching producer gathers messages and retries what it may. */
final case class ProducerSettings(
  batchSize: Int,
  linger: FiniteDuration,
  parallelism: Int,
  maxAttempts: Int,
  backoff: Backoff,
) {
  def withBatchSize(size: Int): ProducerSettings = copy(batchSize = size max 1)
  def withLinger(window: FiniteDuration): ProducerSettings = copy(linger = window)
  def withParallelism(requests: Int): ProducerSettings = copy(parallelism = requests max 1)
  def withMaxAttempts(attempts: Int): ProducerSettings = copy(maxAttempts = attempts max 1)
  def withBackoff(policy: Backoff): ProducerSettings = copy(backoff = policy)
}

object ProducerSettings {
  val default: ProducerSettings = ProducerSettings(10, 100.millis, 4, 3, Backoff.default)
}
