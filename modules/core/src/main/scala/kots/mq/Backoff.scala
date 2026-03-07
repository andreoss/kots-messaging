package kots.mq

import cats.Functor
import cats.syntax.all._

import scala.concurrent.duration._

/** Delay a redelivery waits, growing per attempt and bounded by a cap. */
final case class Backoff(
  initial: FiniteDuration,
  factor: Double,
  max: FiniteDuration,
  jitter: Double,
) {

  def delay(attempt: Int, sample: Double): FiniteDuration = {
    val steps = ((attempt max 1) - 1).toDouble
    val grown = initial.toNanos.toDouble * math.pow(factor max 1.0, steps)
    val capped = math.min(grown, max.toNanos.toDouble)
    val spread = clamp(jitter)
    val scaled = capped * (1.0 - spread + spread * clamp(sample))
    FiniteDuration(scaled.toLong max 0L, NANOSECONDS)
  }

  def next[F[_]](attempt: Int)(implicit F: Functor[F], entropy: Entropy[F]): F[FiniteDuration] =
    entropy.nextDouble.map(delay(attempt, _))

  private def clamp(value: Double): Double = math.min(1.0, math.max(0.0, value))
}

object Backoff {

  /** No wait: a rejected message is visible again at once. */
  val none: Backoff = Backoff(Duration.Zero, 1.0, Duration.Zero, 0.0)

  val default: Backoff = Backoff(1.second, 2.0, 1.minute, 0.2)
}
