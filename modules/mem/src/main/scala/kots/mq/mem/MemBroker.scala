package kots.mq.mem

import cats.effect.kernel.{Ref, Resource, Temporal}
import cats.syntax.all._
import kots.mq._

import scala.concurrent.duration.{Duration, FiniteDuration}

/** In-memory broker over a single reference; the contract's baseline. */
object MemBroker {

  def create[F[_], A](entropy: Entropy[F])(implicit F: Temporal[F]): F[Broker[F, A]] =
    F.ref(State.empty[A]).map(new MemBroker[F, A](_, entropy))

  private[mem] final case class ConsumerId(value: Long)

  private[mem] final case class Pending[A](envelope: Envelope[A], visibleAt: FiniteDuration)

  private[mem] final case class Leased[A](
    destination: Destination,
    envelope: Envelope[A],
    expiresAt: FiniteDuration,
    settings: ConsumerSettings,
    holder: ConsumerId,
    token: Long,
  )

  private[mem] final case class State[A](
    ready: Map[Destination, Vector[Pending[A]]],
    leased: Map[MessageId, Leased[A]],
    published: Long,
    consumers: Long,
    leases: Long,
  )

  private[mem] object State {
    def empty[A]: State[A] = State(Map.empty, Map.empty, 0L, 0L, 0L)
  }
}

private final class MemBroker[F[_], A](
  state: Ref[F, MemBroker.State[A]],
  entropy: Entropy[F],
)(implicit F: Temporal[F])
  extends Broker[F, A] {

  import MemBroker._

  val capabilities: Capabilities =
    Capabilities.of(
      Capability.Delay,
      Capability.DeadLetter,
      Capability.Batch,
      Capability.LeaseExtension,
    )

  val admin: Admin[F] = new Admin[F] {
    def depth(destination: Destination): F[Option[Long]] =
      state.get.map(current =>
        Some(current.ready.getOrElse(destination, Vector.empty).size.toLong)
      )
  }

  def producer(destination: Destination): Resource[F, Producer[F, A]] =
    Resource.pure(new Producer[F, A] {

      def send(message: Message[A]): F[MessageId] = sendAfter(message, Duration.Zero)

      def sendBatch(messages: List[Message[A]]): F[List[Either[SendFailure, MessageId]]] =
        messages.traverse(message => send(message).attempt.map(_.leftMap(SendFailure.of)))

      def sendAfter(message: Message[A], delay: FiniteDuration): F[MessageId] =
        F.monotonic.flatMap { now =>
          state.modify { current =>
            val id = MessageId((current.published + 1).toString)
            val visibleAt = if (delay > Duration.Zero) now + delay else now
            val pending = Pending(Envelope(id, message, 1), visibleAt)
            (push(current, destination, pending).copy(published = current.published + 1), id)
          }
        }
    })

  def consumer(
    destination: Destination,
    settings: ConsumerSettings,
  ): Resource[F, Consumer[F, A]] =
    Resource
      .eval(
        state.modify(current =>
          (current.copy(consumers = current.consumers + 1), ConsumerId(current.consumers + 1))
        )
      )
      .map(holder =>
        new Consumer[F, A] {

          def receive: F[Option[Delivery[F, A]]] = receiveBatch(1).map(_.headOption)

          def receiveBatch(max: Int): F[List[Delivery[F, A]]] =
            for {
              now <- F.monotonic
              sample <- entropy.nextDouble
              taken <- state.modify(take(_, destination, settings, holder, now, sample, max))
            } yield taken.map { case (envelope, token) => delivery(envelope, token) }

          def ackAll(deliveries: List[Delivery[F, A]]): F[Unit] =
            deliveries.traverse_(_.ack)

          def extendAll(deliveries: List[Delivery[F, A]], by: FiniteDuration): F[Unit] =
            deliveries.traverse_(_.extend(by))
        }
      )

  private def delivery(taken: Envelope[A], token: Long): Delivery[F, A] =
    new Delivery[F, A] {

      val envelope: Envelope[A] = taken

      val ack: F[Unit] =
        state.update(current =>
          held(current).fold(current)(_ => current.copy(leased = current.leased - taken.id))
        )

      val reject: F[Unit] =
        for {
          now <- F.monotonic
          sample <- entropy.nextDouble
          _ <- state.update { current =>
            held(current) match {
              case None => current
              case Some(lease) =>
                requeue(
                  current.copy(leased = current.leased - taken.id),
                  lease.destination,
                  lease.envelope,
                  lease.settings,
                  now,
                  sample,
                )
            }
          }
        } yield ()

      val release: F[Unit] =
        F.monotonic.flatMap { now =>
          state.update { current =>
            held(current).fold(current)(lease =>
              push(
                current.copy(leased = current.leased - taken.id),
                lease.destination,
                Pending(lease.envelope, now),
              )
            )
          }
        }

      val deadLetter: F[Unit] =
        F.monotonic.flatMap { now =>
          state.update { current =>
            held(current).fold(current) { lease =>
              val settled = current.copy(leased = current.leased - taken.id)
              lease.settings.deadLetter
                .fold(settled)(parked => push(settled, parked, Pending(lease.envelope, now)))
            }
          }
        }

      def extend(by: FiniteDuration): F[Unit] =
        F.monotonic.flatMap { now =>
          state.update { current =>
            held(current).fold(current)(lease =>
              current.copy(
                leased = current.leased.updated(taken.id, lease.copy(expiresAt = now + by))
              )
            )
          }
        }

      private def held(current: State[A]): Option[Leased[A]] =
        current.leased.get(taken.id).filter(_.token == token)
    }

  private def take(
    current: State[A],
    destination: Destination,
    settings: ConsumerSettings,
    holder: ConsumerId,
    now: FiniteDuration,
    sample: Double,
    max: Int,
  ): (State[A], List[(Envelope[A], Long)]) = {
    val swept = sweep(current, now, sample)
    val inflight = swept.leased.values.count(_.holder == holder)
    val capacity = math.max(0, math.min(max, settings.prefetch - inflight))

    def loop(
      accumulated: State[A],
      left: Int,
      taken: List[(Envelope[A], Long)],
    ): (State[A], List[(Envelope[A], Long)]) =
      if (left <= 0) (accumulated, taken.reverse)
      else {
        val queue = accumulated.ready.getOrElse(destination, Vector.empty)
        queue.indexWhere(_.visibleAt <= now) match {
          case -1 => (accumulated, taken.reverse)
          case index =>
            val pending = queue(index)
            val token = accumulated.leases + 1L
            val lease =
              Leased(destination, pending.envelope, now + settings.lease, settings, holder, token)
            loop(
              accumulated.copy(
                ready = accumulated.ready.updated(destination, queue.patch(index, Vector.empty, 1)),
                leased = accumulated.leased.updated(pending.envelope.id, lease),
                leases = token,
              ),
              left - 1,
              (pending.envelope, token) :: taken,
            )
        }
      }

    loop(swept, capacity, Nil)
  }

  private def sweep(current: State[A], now: FiniteDuration, sample: Double): State[A] = {
    val expired = current.leased.values.filter(_.expiresAt <= now).toList
    expired.foldLeft(current.copy(leased = current.leased -- expired.map(_.envelope.id))) {
      (acc, lease) =>
        requeue(acc, lease.destination, lease.envelope, lease.settings, now, sample)
    }
  }

  private def requeue(
    current: State[A],
    destination: Destination,
    envelope: Envelope[A],
    settings: ConsumerSettings,
    now: FiniteDuration,
    sample: Double,
  ): State[A] =
    if (envelope.attempt >= settings.maxAttempts)
      settings.deadLetter.fold(current)(parked => push(current, parked, Pending(envelope, now)))
    else {
      val next = envelope.copy(attempt = envelope.attempt + 1)
      push(
        current,
        destination,
        Pending(next, now + settings.backoff.delay(envelope.attempt, sample)),
      )
    }

  private def push(current: State[A], destination: Destination, pending: Pending[A]): State[A] =
    current.copy(
      ready = current.ready
        .updated(destination, current.ready.getOrElse(destination, Vector.empty) :+ pending)
    )
}
