package kots.mq

import cats.effect.IO
import cats.effect.kernel.Ref

import scala.concurrent.duration.FiniteDuration

final case class MetricCounts(
  published: Int,
  received: Int,
  redelivered: Int,
  acknowledged: Int,
  rejected: Int,
  publishLatencies: List[FiniteDuration],
  receiveLatencies: List[FiniteDuration],
  timesToAck: List[FiniteDuration],
)

object MetricCounts {
  val empty: MetricCounts = MetricCounts(0, 0, 0, 0, 0, Nil, Nil, Nil)
}

/** Metrics sink that records what the port was told, for assertions. */
final class MetricsProbe(state: Ref[IO, MetricCounts]) extends MqMetrics[IO] {
  val published: IO[Unit] = state.update(c => c.copy(published = c.published + 1))
  val received: IO[Unit] = state.update(c => c.copy(received = c.received + 1))
  val redelivered: IO[Unit] = state.update(c => c.copy(redelivered = c.redelivered + 1))
  val acknowledged: IO[Unit] = state.update(c => c.copy(acknowledged = c.acknowledged + 1))
  val rejected: IO[Unit] = state.update(c => c.copy(rejected = c.rejected + 1))

  def publishLatency(duration: FiniteDuration): IO[Unit] =
    state.update(c => c.copy(publishLatencies = c.publishLatencies :+ duration))

  def receiveLatency(duration: FiniteDuration): IO[Unit] =
    state.update(c => c.copy(receiveLatencies = c.receiveLatencies :+ duration))

  def timeToAck(duration: FiniteDuration): IO[Unit] =
    state.update(c => c.copy(timesToAck = c.timesToAck :+ duration))

  val counts: IO[MetricCounts] = state.get
}

object MetricsProbe {
  def create: IO[MetricsProbe] = Ref.of[IO, MetricCounts](MetricCounts.empty).map(new MetricsProbe(_))
}
