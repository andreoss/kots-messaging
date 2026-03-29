package kots.messaging

import cats.ApplicativeThrow

/** What a broker can be asked about or asked to do to a destination. */
trait Admin[F[_]] {
  def depth(destination: Destination): F[Option[Long]]
  def declare(destination: Destination): F[Unit]
  def purge(destination: Destination): F[Option[Long]]
  def delete(destination: Destination): F[Unit]
}

object Admin {

  /** For a broker whose topology this library cannot state or change. */
  def unsupported[F[_]](implicit F: ApplicativeThrow[F]): Admin[F] =
    new Admin[F] {
      def depth(destination: Destination): F[Option[Long]] = F.pure(None)

      def declare(destination: Destination): F[Unit] =
        F.raiseError(CapabilityUnsupported(Capability.Topology))

      def purge(destination: Destination): F[Option[Long]] =
        F.raiseError(CapabilityUnsupported(Capability.Topology))

      def delete(destination: Destination): F[Unit] =
        F.raiseError(CapabilityUnsupported(Capability.Topology))
    }
}
