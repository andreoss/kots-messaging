package kots.messaging

import cats.Applicative

import scala.concurrent.duration.FiniteDuration

/** Counters and durations a queue reports; carries no payload contents. */
trait MqMetrics[F[_]] {
  def published: F[Unit]
  def received: F[Unit]
  def redelivered: F[Unit]
  def acknowledged: F[Unit]
  def rejected: F[Unit]
  def publishLatency(duration: FiniteDuration): F[Unit]
  def receiveLatency(duration: FiniteDuration): F[Unit]
  def timeToAck(duration: FiniteDuration): F[Unit]
}

object MqMetrics {

  /** Metrics sink that discards every signal. */
  def noop[F[_]](implicit F: Applicative[F]): MqMetrics[F] =
    new MqMetrics[F] {
      val published: F[Unit] = F.unit
      val received: F[Unit] = F.unit
      val redelivered: F[Unit] = F.unit
      val acknowledged: F[Unit] = F.unit
      val rejected: F[Unit] = F.unit
      def publishLatency(duration: FiniteDuration): F[Unit] = F.unit
      def receiveLatency(duration: FiniteDuration): F[Unit] = F.unit
      def timeToAck(duration: FiniteDuration): F[Unit] = F.unit
    }
}
