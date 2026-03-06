package kots.mq.mem

import cats.effect.kernel.{Concurrent, Ref, Resource}
import cats.syntax.all._
import kots.mq._

/** In-memory broker over a single reference; the contract's baseline. */
object MemBroker {

  def create[F[_], A](implicit F: Concurrent[F]): F[Broker[F, A]] =
    F.ref(State.empty[A]).map(new MemBroker[F, A](_))

  private[mem] final case class Pending[A](envelope: Envelope[A])

  private[mem] final case class State[A](
    ready: Map[Destination, Vector[Pending[A]]],
    inflight: Map[MessageId, (Destination, Pending[A])],
    published: Long,
  )

  private[mem] object State {
    def empty[A]: State[A] = State(Map.empty, Map.empty, 0L)
  }
}

private final class MemBroker[F[_], A](state: Ref[F, MemBroker.State[A]])(implicit
  F: Concurrent[F]
) extends Broker[F, A] {

  import MemBroker._

  def producer(destination: Destination): Resource[F, Producer[F, A]] =
    Resource.pure(new Producer[F, A] {
      def send(message: Message[A]): F[MessageId] =
        state.modify { current =>
          val id = MessageId((current.published + 1).toString)
          val pending = Pending(Envelope(id, message, 1))
          val queue = current.ready.getOrElse(destination, Vector.empty) :+ pending
          val next = current.copy(
            ready = current.ready.updated(destination, queue),
            published = current.published + 1,
          )
          (next, id)
        }
    })

  def consumer(destination: Destination): Resource[F, Consumer[F, A]] =
    Resource.pure(new Consumer[F, A] {
      def receive: F[Option[Delivery[F, A]]] =
        state
          .modify { current =>
            current.ready.getOrElse(destination, Vector.empty) match {
              case head +: rest =>
                val next = current.copy(
                  ready = current.ready.updated(destination, rest),
                  inflight = current.inflight.updated(head.envelope.id, (destination, head)),
                )
                (next, Some(head))
              case _ => (current, None)
            }
          }
          .map(_.map(pending => delivery(destination, pending)))
    })

  private def delivery(destination: Destination, pending: Pending[A]): Delivery[F, A] =
    new Delivery[F, A] {
      val envelope: Envelope[A] = pending.envelope

      val ack: F[Unit] =
        state.update(current => current.copy(inflight = current.inflight - envelope.id))

      val reject: F[Unit] =
        state.update { current =>
          current.inflight.get(envelope.id) match {
            case None => current
            case Some(_) =>
              val redelivered =
                Pending(envelope.copy(attempt = envelope.attempt + 1))
              val queue = redelivered +: current.ready.getOrElse(destination, Vector.empty)
              current.copy(
                ready = current.ready.updated(destination, queue),
                inflight = current.inflight - envelope.id,
              )
          }
        }
    }
}
