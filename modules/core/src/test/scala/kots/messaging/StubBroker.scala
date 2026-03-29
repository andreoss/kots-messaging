package kots.messaging

import cats.effect.kernel.{Concurrent, Ref, Resource}
import cats.syntax.all._

import scala.concurrent.duration.FiniteDuration

/** Smallest broker the core can test its own wrappers against. */
object StubBroker {

  def create[F[_], A](declared: Capabilities)(implicit F: Concurrent[F]): F[Broker[F, A]] =
    F.ref((Vector.empty[Envelope[A]], 0L)).map(state =>
      new Broker[F, A] {

        val capabilities: Capabilities = declared

        val events: BrokerEvents[F] = BrokerEvents.quiet[F]

        val admin: Admin[F] = new Admin[F] {
          def depth(destination: Destination): F[Option[Long]] =
            state.get.map { case (queue, _) => Some(queue.size.toLong) }

          def declare(destination: Destination): F[Unit] = F.unit

          def purge(destination: Destination): F[Option[Long]] =
            state.modify { case (queue, published) =>
              ((Vector.empty, published), Some(queue.size.toLong))
            }

          def delete(destination: Destination): F[Unit] =
            state.update { case (_, published) => (Vector.empty, published) }
        }

        def producer(destination: Destination): Resource[F, Producer[F, A]] =
          Resource.pure(new Producer[F, A] {
            def send(message: Message[A]): F[MessageId] =
              state.modify { case (queue, published) =>
                val id = MessageId((published + 1).toString)
                ((queue :+ Envelope(id, message, 1), published + 1), id)
              }

            def sendAfter(message: Message[A], delay: FiniteDuration): F[MessageId] =
              if (declared.has(Capability.Delay)) send(message)
              else F.raiseError(CapabilityUnsupported(Capability.Delay))

            def sendBatch(
              messages: List[Message[A]]
            ): F[List[Either[SendFailure, MessageId]]] =
              messages.traverse(message => send(message).attempt.map(_.leftMap(SendFailure.of)))
          })

        def consumer(
          destination: Destination,
          settings: ConsumerSettings,
        ): Resource[F, Consumer[F, A]] =
          Resource.pure(new Consumer[F, A] {

            def receive: F[Option[Delivery[F, A]]] =
              receiveBatch(1).map(_.headOption)

            def receiveBatch(max: Int): F[List[Delivery[F, A]]] =
              state
                .modify { case (queue, published) =>
                  val taken = queue.take(max max 0)
                  ((queue.drop(taken.size), published), taken.toList)
                }
                .map(_.map(delivery))

            def ackAll(deliveries: List[Delivery[F, A]]): F[Unit] =
              deliveries.traverse_(_.ack)

            def extendAll(deliveries: List[Delivery[F, A]], by: FiniteDuration): F[Unit] =
              deliveries.traverse_(_.extend(by))
          })

        private def delivery(taken: Envelope[A]): Delivery[F, A] =
          new Delivery[F, A] {
            val envelope: Envelope[A] = taken
            val ack: F[Unit] = F.unit
            val reject: F[Unit] =
              state.update { case (queue, published) =>
                (queue :+ taken.copy(attempt = taken.attempt + 1), published)
              }
            val release: F[Unit] =
              state.update { case (queue, published) => (queue :+ taken, published) }
            val deadLetter: F[Unit] = F.unit
            def extend(by: FiniteDuration): F[Unit] = F.unit
          }
      }
    )
}
