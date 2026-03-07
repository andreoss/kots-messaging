package kots.mq

import cats.Applicative
import cats.effect.kernel.Sync

/** Source of the jitter a backoff mixes into a redelivery delay. */
trait Entropy[F[_]] {
  def nextDouble: F[Double]
}

object Entropy {

  /** Fixed sample; the deterministic source tests use. */
  def const[F[_]](value: Double)(implicit F: Applicative[F]): Entropy[F] =
    new Entropy[F] {
      val nextDouble: F[Double] = F.pure(value)
    }

  /** Samples the platform's generator on each read. */
  def system[F[_]](implicit F: Sync[F]): Entropy[F] =
    new Entropy[F] {
      val nextDouble: F[Double] = F.delay(scala.util.Random.nextDouble())
    }
}
