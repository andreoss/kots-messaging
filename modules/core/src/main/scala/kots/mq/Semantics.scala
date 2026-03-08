package kots.mq

import cats.{Applicative, Monad}
import cats.effect.kernel.{MonadCancelThrow, Outcome}
import cats.syntax.all._

import scala.concurrent.duration.FiniteDuration

/** The two delivery modes, written at the call site rather than assumed. */
object Semantics {

  /** Acknowledges before the caller sees the delivery: a failure loses it. */
  def atMostOnce[F[_], A](consumer: Consumer[F, A])(implicit F: Monad[F]): Consumer[F, A] =
    new Consumer[F, A] {
      def receive: F[Option[Delivery[F, A]]] = consumer.receive.flatMap(_.traverse(settle))

      def receiveBatch(max: Int): F[List[Delivery[F, A]]] =
        consumer.receiveBatch(max).flatMap(_.traverse(settle))

      private def settle(delivery: Delivery[F, A]): F[Delivery[F, A]] =
        delivery.ack.as(alreadySettled(delivery))
    }

  /** Acknowledges after the caller's effect succeeds; a failure redelivers. */
  def atLeastOnce[F[_], A](consumer: Consumer[F, A])(handle: Envelope[A] => F[Unit])(implicit
    F: MonadCancelThrow[F]
  ): F[Option[Envelope[A]]] =
    consumer.receive.flatMap(_.traverse { delivery =>
      F.guaranteeCase(handle(delivery.envelope)) {
        case Outcome.Succeeded(_) => delivery.ack
        case _ => delivery.reject
      }.as(delivery.envelope)
    })

  private def alreadySettled[F[_], A](
    delivery: Delivery[F, A]
  )(implicit F: Applicative[F]): Delivery[F, A] =
    new Delivery[F, A] {
      val envelope: Envelope[A] = delivery.envelope
      val ack: F[Unit] = F.unit
      val reject: F[Unit] = F.unit
      def extend(by: FiniteDuration): F[Unit] = F.unit
    }
}
