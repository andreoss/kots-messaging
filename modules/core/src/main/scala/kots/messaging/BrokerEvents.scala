package kots.messaging

import cats.Applicative

/** What the broker says about itself while a client is attached. */
trait BrokerEvents[F[_]] {

  /** The reason while the broker is refusing to take publishes. */
  def blocked: F[Option[String]]
}

object BrokerEvents {

  /** For a broker that tells a client nothing about its own state. */
  def quiet[F[_]](implicit F: Applicative[F]): BrokerEvents[F] =
    new BrokerEvents[F] {
      val blocked: F[Option[String]] = F.pure(None)
    }
}
