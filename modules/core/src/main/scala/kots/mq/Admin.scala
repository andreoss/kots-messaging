package kots.mq

import cats.Applicative

/** What a broker can tell about a destination without consuming from it. */
trait Admin[F[_]] {
  def depth(destination: Destination): F[Option[Long]]
}

object Admin {

  /** For a broker that does not publish the figure. */
  def unknown[F[_]](implicit F: Applicative[F]): Admin[F] =
    new Admin[F] {
      def depth(destination: Destination): F[Option[Long]] = F.pure(None)
    }
}
