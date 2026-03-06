package kots.mq

/** Publishes messages to one destination. */
trait Producer[F[_], A] {
  def send(message: Message[A]): F[MessageId]
}
