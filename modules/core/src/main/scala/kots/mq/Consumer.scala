package kots.mq

import scala.concurrent.duration.FiniteDuration

/** Hands out deliveries from one destination; settling is the caller's. */
trait Consumer[F[_], A] {
  def receive: F[Option[Delivery[F, A]]]
  def receiveBatch(max: Int): F[List[Delivery[F, A]]]
  def ackAll(deliveries: List[Delivery[F, A]]): F[Unit]
  def extendAll(deliveries: List[Delivery[F, A]], by: FiniteDuration): F[Unit]
}
