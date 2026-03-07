package kots.mq

import scala.concurrent.duration.FiniteDuration

/** Received message together with the effects that settle it. */
trait Delivery[F[_], A] {
  def envelope: Envelope[A]
  def ack: F[Unit]
  def reject: F[Unit]
  def extend(by: FiniteDuration): F[Unit]
}
