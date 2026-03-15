package kots.mq

import cats.Monad
import cats.effect.kernel.Clock
import cats.syntax.all._

import scala.concurrent.duration.FiniteDuration

/** Wraps a producer or a consumer with the metrics port. */
object Metered {

  def producer[F[_], A](underlying: Producer[F, A], metrics: MqMetrics[F])(implicit
    F: Monad[F],
    clock: Clock[F],
  ): Producer[F, A] =
    new Producer[F, A] {
      def send(message: Message[A]): F[MessageId] = measured(underlying.send(message))

      def sendAfter(message: Message[A], delay: FiniteDuration): F[MessageId] =
        measured(underlying.sendAfter(message, delay))

      def sendBatch(messages: List[Message[A]]): F[List[Either[SendFailure, MessageId]]] =
        clock.timed(underlying.sendBatch(messages)).flatMap { case (elapsed, outcomes) =>
          metrics.publishLatency(elapsed) *>
            outcomes.traverse_(outcome => metrics.published.whenA(outcome.isRight)).as(outcomes)
        }

      private def measured(publish: F[MessageId]): F[MessageId] =
        clock.timed(publish).flatMap { case (elapsed, id) =>
          metrics.publishLatency(elapsed) *> metrics.published.as(id)
        }
    }

  def consumer[F[_], A](underlying: Consumer[F, A], metrics: MqMetrics[F])(implicit
    F: Monad[F],
    clock: Clock[F],
  ): Consumer[F, A] =
    new Consumer[F, A] {
      def receive: F[Option[Delivery[F, A]]] =
        measured(underlying.receive.map(_.toList)).map(_.headOption)

      def receiveBatch(max: Int): F[List[Delivery[F, A]]] =
        measured(underlying.receiveBatch(max))

      private def measured(read: F[List[Delivery[F, A]]]): F[List[Delivery[F, A]]] =
        clock.timed(read).flatMap { case (elapsed, deliveries) =>
          metrics.receiveLatency(elapsed) *>
            deliveries.traverse_(delivery =>
              metrics.received *> metrics.redelivered.whenA(delivery.envelope.attempt > 1)
            ) *>
            clock.monotonic.map(at => deliveries.map(settled(_, metrics, at)))
        }
    }

  private def settled[F[_], A](
    delivery: Delivery[F, A],
    metrics: MqMetrics[F],
    receivedAt: FiniteDuration,
  )(implicit F: Monad[F], clock: Clock[F]): Delivery[F, A] =
    new Delivery[F, A] {
      val envelope: Envelope[A] = delivery.envelope

      val ack: F[Unit] =
        delivery.ack *>
          clock.monotonic.flatMap(now => metrics.timeToAck(now - receivedAt)) *>
          metrics.acknowledged

      val reject: F[Unit] = delivery.reject *> metrics.rejected

      def extend(by: FiniteDuration): F[Unit] = delivery.extend(by)
    }
}
