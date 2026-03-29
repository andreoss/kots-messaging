package kots.messaging.stream

import cats.effect.kernel.{Outcome, Temporal}
import cats.syntax.all._
import fs2.{Pipe, Stream}
import kots.messaging._

import scala.concurrent.duration.FiniteDuration

/** Streaming view of the pull-based algebra. */
object MqStream {

  def deliveries[F[_], A](
    consumer: Consumer[F, A],
    chunkSize: Int,
    idle: FiniteDuration,
  )(implicit F: Temporal[F]): Stream[F, Delivery[F, A]] =
    Stream
      .repeatEval(consumer.receiveBatch(chunkSize))
      .evalTap(batch => F.sleep(idle).whenA(batch.isEmpty))
      .flatMap(Stream.emits)

  /** Every delivery until the destination is drained, then done. */
  def drain[F[_], A](
    consumer: Consumer[F, A],
    chunkSize: Int,
  )(implicit F: Temporal[F]): Stream[F, Delivery[F, A]] =
    Stream
      .repeatEval(consumer.receiveBatch(chunkSize))
      .takeWhile(_.nonEmpty)
      .flatMap(Stream.emits)

  def process[F[_], A](
    consumer: Consumer[F, A],
    concurrency: Int,
    chunkSize: Int,
    idle: FiniteDuration,
  )(handle: Envelope[A] => F[Unit])(implicit F: Temporal[F]): Stream[F, Envelope[A]] =
    deliveries(consumer, chunkSize, idle).parEvalMap(concurrency) { delivery =>
      F.guaranteeCase(handle(delivery.envelope)) {
        case Outcome.Succeeded(_) => delivery.ack
        case _ => delivery.reject
      }.as(delivery.envelope)
    }

  def settle[F[_], A](
    consumer: Consumer[F, A],
    concurrency: Int,
    chunkSize: Int,
    idle: FiniteDuration,
    handling: Handling,
  )(handle: Envelope[A] => F[Settlement])(implicit F: Temporal[F]): Stream[F, Envelope[A]] =
    deliveries(consumer, chunkSize, idle)
      .parEvalMap(concurrency)(Semantics.settleWith(_, handling)(handle))

  def sink[F[_], A](
    producer: Producer[F, A],
    batchSize: Int,
    within: FiniteDuration,
  )(implicit F: Temporal[F]): Pipe[F, Message[A], Either[SendFailure, MessageId]] =
    _.groupWithin(batchSize, within)
      .evalMap(chunk => producer.sendBatch(chunk.toList))
      .flatMap(Stream.emits)
}
