package kots.mq

import cats.effect.kernel.{Deferred, Resource, Temporal}
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all._

import scala.concurrent.duration.{Duration, FiniteDuration}

/** Gathers what callers publish one at a time into batches for the broker. */
object BatchingProducer {

  def resource[F[_], A](
    producer: Producer[F, A],
    settings: ProducerSettings,
    entropy: Entropy[F],
  )(implicit F: Temporal[F]): Resource[F, Producer[F, A]] =
    for {
      queue <- Resource.eval(
        Queue.bounded[F, Entry[F, A]](settings.batchSize * settings.parallelism)
      )
      supervisor <- Supervisor[F]
      gathering = new Gathering[F, A](producer, settings, entropy, queue, supervisor)
      _ <- Resource.eval(
        List.range(0, settings.parallelism).traverse_(_ => supervisor.supervise(gathering.loop))
      )
    } yield new Batching[F, A](producer, queue)

  private[mq] final case class Entry[F[_], A](
    message: Message[A],
    attempt: Int,
    outcome: Deferred[F, Either[SendFailure, MessageId]],
  )

  private final class Batching[F[_], A](producer: Producer[F, A], queue: Queue[F, Entry[F, A]])(
    implicit F: Temporal[F]
  ) extends Producer[F, A] {

    def send(message: Message[A]): F[MessageId] =
      offer(message).flatMap(_.outcome.get).flatMap {
        case Right(id) => F.pure(id)
        case Left(failure) => F.raiseError[MessageId](SendRefused(failure))
      }

    def sendAfter(message: Message[A], delay: FiniteDuration): F[MessageId] =
      producer.sendAfter(message, delay)

    def sendBatch(messages: List[Message[A]]): F[List[Either[SendFailure, MessageId]]] =
      messages.traverse(offer).flatMap(_.traverse(_.outcome.get))

    private def offer(message: Message[A]): F[Entry[F, A]] =
      F.deferred[Either[SendFailure, MessageId]]
        .map(Entry(message, 1, _))
        .flatTap(queue.offer)
  }

  private final class Gathering[F[_], A](
    producer: Producer[F, A],
    settings: ProducerSettings,
    entropy: Entropy[F],
    queue: Queue[F, Entry[F, A]],
    supervisor: Supervisor[F],
  )(implicit F: Temporal[F]) {

    val loop: F[Unit] = step.foreverM[Unit]

    private def step: F[Unit] =
      for {
        first <- queue.take
        now <- F.monotonic
        batch <- gather(List(first), now + settings.linger)
        _ <- dispatch(batch)
      } yield ()

    private def gather(taken: List[Entry[F, A]], deadline: FiniteDuration): F[List[Entry[F, A]]] =
      if (taken.size >= settings.batchSize) F.pure(taken.reverse)
      else
        F.monotonic.flatMap { now =>
          val left = deadline - now
          if (left <= Duration.Zero) F.pure(taken.reverse)
          else
            F.timeoutTo(queue.take.map(Option(_)), left, F.pure(Option.empty[Entry[F, A]]))
              .flatMap {
                case Some(entry) => gather(entry :: taken, deadline)
                case None => F.pure(taken.reverse)
              }
        }

    private def dispatch(batch: List[Entry[F, A]]): F[Unit] =
      producer.sendBatch(batch.map(_.message)).attempt.flatMap {
        case Right(outcomes) =>
          batch.zip(outcomes).traverse_ { case (entry, outcome) => settle(entry, outcome) }
        case Left(error) =>
          batch.traverse_(_.outcome.complete(Left(SendFailure.of(error))).void)
      }

    private def settle(entry: Entry[F, A], outcome: Either[SendFailure, MessageId]): F[Unit] =
      outcome match {
        case Right(id) => entry.outcome.complete(Right(id)).void
        case Left(failure) if failure.recoverable && entry.attempt < settings.maxAttempts =>
          entropy.nextDouble.flatMap { sample =>
            supervisor
              .supervise(
                F.sleep(settings.backoff.delay(entry.attempt, sample)) *>
                  queue.offer(entry.copy(attempt = entry.attempt + 1))
              )
              .void
          }
        case Left(failure) => entry.outcome.complete(Left(failure)).void
      }
  }
}

/** Raised when a single publish through a batching producer was refused. */
final case class SendRefused(failure: SendFailure)
  extends RuntimeException(s"${failure.code}: ${failure.description}")
