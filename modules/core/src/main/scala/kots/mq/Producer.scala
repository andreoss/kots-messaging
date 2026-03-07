package kots.mq

import scala.concurrent.duration.FiniteDuration

/** Publishes messages to one destination. */
trait Producer[F[_], A] {
  def send(message: Message[A]): F[MessageId]
  def sendAfter(message: Message[A], delay: FiniteDuration): F[MessageId]
}
