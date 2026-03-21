package kots.mq

import cats.{Applicative, Monad}
import cats.effect.kernel.{MonadCancelThrow, Outcome, Temporal}
import cats.syntax.all._

import scala.concurrent.duration.FiniteDuration

/** The delivery modes, written at the call site rather than assumed. */
object Semantics {

  /** Acknowledges before the caller sees the delivery: a failure loses it. */
  def atMostOnce[F[_], A](consumer: Consumer[F, A])(implicit F: Monad[F]): Consumer[F, A] =
    new Consumer[F, A] {
      def receive: F[Option[Delivery[F, A]]] = consumer.receive.flatMap(_.traverse(settle))

      def receiveBatch(max: Int): F[List[Delivery[F, A]]] =
        consumer.receiveBatch(max).flatMap(_.traverse(settle))

      def ackAll(deliveries: List[Delivery[F, A]]): F[Unit] = deliveries.traverse_(_.ack)

      def extendAll(deliveries: List[Delivery[F, A]], by: FiniteDuration): F[Unit] =
        deliveries.traverse_(_.extend(by))

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
        case Outcome.Canceled() => delivery.release
        case _ => delivery.reject
      }.as(delivery.envelope)
    })

  /** Runs the handler and settles the delivery the way the handler asked. */
  def process[F[_], A](consumer: Consumer[F, A], handling: Handling)(
    handle: Envelope[A] => F[Settlement]
  )(implicit F: Temporal[F]): F[Option[Envelope[A]]] =
    consumer.receive.flatMap(_.traverse(settleWith(_, handling)(handle)))

  /** One delivery through a handler, settled by its outcome. */
  def settleWith[F[_], A](delivery: Delivery[F, A], handling: Handling)(
    handle: Envelope[A] => F[Settlement]
  )(implicit F: Temporal[F]): F[Envelope[A]] = {
    val work = handle(delivery.envelope).handleError(handling.onError)
    val bounded =
      handling.timeout.fold(work)(limit => F.timeoutTo(work, limit, F.pure(handling.onTimeout)))
    val renewed = handling.renewEvery.fold(bounded) { interval =>
      val renewal = (F.sleep(interval) *> delivery.extend(interval * 2)).foreverM[Unit]
      F.background(renewal).use(_ => bounded)
    }
    F.guaranteeCase(renewed) {
      case Outcome.Succeeded(_) => F.unit
      case Outcome.Canceled() => delivery.release
      case Outcome.Errored(_) => delivery.reject
    }.flatMap(settle(delivery, _)).as(delivery.envelope)
  }

  private def settle[F[_], A](delivery: Delivery[F, A], settlement: Settlement): F[Unit] =
    settlement match {
      case Settlement.Done => delivery.ack
      case Settlement.Drop => delivery.ack
      case Settlement.Retry => delivery.reject
      case Settlement.Release => delivery.release
      case Settlement.DeadLetter => delivery.deadLetter
    }

  private def alreadySettled[F[_], A](
    delivery: Delivery[F, A]
  )(implicit F: Applicative[F]): Delivery[F, A] =
    new Delivery[F, A] {
      val envelope: Envelope[A] = delivery.envelope
      val ack: F[Unit] = F.unit
      val reject: F[Unit] = F.unit
      val release: F[Unit] = F.unit
      val deadLetter: F[Unit] = F.unit
      def extend(by: FiniteDuration): F[Unit] = F.unit
    }
}
