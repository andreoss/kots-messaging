package kots.messaging

import scala.concurrent.duration.FiniteDuration

/** Publishes messages to one destination. */
trait Producer[F[_], A] {
  def send(message: Message[A]): F[MessageId]
  def sendAfter(message: Message[A], delay: FiniteDuration): F[MessageId]
  def sendBatch(messages: List[Message[A]]): F[List[Either[SendFailure, MessageId]]]
}

/** Why one entry was not published, and whether trying again could help. */
final case class SendFailure(code: String, description: String, recoverable: Boolean)

object SendFailure {

  /** A failure this library cannot tell is transient. */
  def of(error: Throwable): SendFailure =
    SendFailure(
      error.getClass.getSimpleName,
      Option(error.getMessage).getOrElse(error.getClass.getName),
      recoverable = false,
    )
}
