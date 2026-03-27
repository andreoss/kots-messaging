package kots.mq

import cats.effect.kernel.{Ref, Temporal}
import cats.syntax.all._

import scala.concurrent.duration._

/** Paces what a consumer hands out, by a token bucket over the clock. */
object RateLimit {

  def tokenBucket[F[_], A](
    consumer: Consumer[F, A],
    perSecond: Double,
    burst: Int,
  )(implicit F: Temporal[F]): F[Consumer[F, A]] =
    F.monotonic.flatMap(now => F.ref((burst.toDouble max 1.0, now))).map { state =>
      val rate = perSecond max 0.000001d
      val capacity = burst.toDouble max 1.0

      def acquire(permits: Int): F[Unit] =
        if (permits <= 0) F.unit
        else
          F.monotonic
            .flatMap(now => state.modify(refill(_, now, permits.toDouble, rate, capacity)))
            .flatMap(wait => if (wait > Duration.Zero) F.sleep(wait) *> acquire(permits) else F.unit)

      new Consumer[F, A] {

        def receive: F[Option[Delivery[F, A]]] = acquire(1) *> consumer.receive

        def receiveBatch(max: Int): F[List[Delivery[F, A]]] =
          acquire(max max 1) *> consumer.receiveBatch(max)

        def ackAll(deliveries: List[Delivery[F, A]]): F[Unit] = consumer.ackAll(deliveries)

        def extendAll(deliveries: List[Delivery[F, A]], by: FiniteDuration): F[Unit] =
          consumer.extendAll(deliveries, by)
      }
    }

  private def refill(
    state: (Double, FiniteDuration),
    now: FiniteDuration,
    permits: Double,
    rate: Double,
    capacity: Double,
  ): ((Double, FiniteDuration), FiniteDuration) = {
    val (tokens, stamp) = state
    val elapsed = (now - stamp).toNanos.toDouble / 1e9d
    val filled = math.min(capacity, tokens + elapsed * rate)
    if (filled >= permits) (((filled - permits), now), Duration.Zero)
    else {
      val missing = permits - filled
      val wait = FiniteDuration((missing / rate * 1e9d).toLong max 1L, NANOSECONDS)
      ((filled, now), wait)
    }
  }
}
