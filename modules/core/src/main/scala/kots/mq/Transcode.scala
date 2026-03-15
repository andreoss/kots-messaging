package kots.mq

import cats.MonadThrow
import cats.syntax.all._

import scala.concurrent.duration.FiniteDuration

/** Carries the algebra from the bytes on the wire to a typed payload. */
object Transcode {

  def producer[F[_], A](underlying: Producer[F, Array[Byte]], codec: Codec[A]): Producer[F, A] =
    new Producer[F, A] {
      def send(message: Message[A]): F[MessageId] = underlying.send(encode(message, codec))

      def sendAfter(message: Message[A], delay: FiniteDuration): F[MessageId] =
        underlying.sendAfter(encode(message, codec), delay)

      def sendBatch(messages: List[Message[A]]): F[List[Either[SendFailure, MessageId]]] =
        underlying.sendBatch(messages.map(encode(_, codec)))
    }

  def consumer[F[_], A](underlying: Consumer[F, Array[Byte]], codec: Codec[A])(implicit
    F: MonadThrow[F]
  ): Consumer[F, A] =
    new Consumer[F, A] {
      def receive: F[Option[Delivery[F, A]]] = underlying.receive.flatMap(_.traverse(decode))

      def receiveBatch(max: Int): F[List[Delivery[F, A]]] =
        underlying.receiveBatch(max).flatMap(_.traverse(decode))

      private def decode(delivery: Delivery[F, Array[Byte]]): F[Delivery[F, A]] =
        F.fromEither(codec.decode(delivery.envelope.message.payload))
          .map(payload => typed(delivery, delivery.envelope.message.as(payload)))
    }

  def broker[F[_], A](underlying: Broker[F, Array[Byte]], codec: Codec[A])(implicit
    F: MonadThrow[F]
  ): Broker[F, A] =
    new Broker[F, A] {
      val capabilities: Capabilities = underlying.capabilities

      val admin: Admin[F] = underlying.admin

      def producer(destination: Destination): cats.effect.kernel.Resource[F, Producer[F, A]] =
        underlying.producer(destination).map(Transcode.producer(_, codec))

      def consumer(
        destination: Destination,
        settings: ConsumerSettings,
      ): cats.effect.kernel.Resource[F, Consumer[F, A]] =
        underlying.consumer(destination, settings).map(Transcode.consumer(_, codec))
    }

  private def encode[A](message: Message[A], codec: Codec[A]): Message[Array[Byte]] =
    message.as(codec.encode(message.payload))

  private def typed[F[_], A](
    underlying: Delivery[F, Array[Byte]],
    message: Message[A],
  ): Delivery[F, A] =
    new Delivery[F, A] {
      val envelope: Envelope[A] =
        Envelope(underlying.envelope.id, message, underlying.envelope.attempt)
      val ack: F[Unit] = underlying.ack
      val reject: F[Unit] = underlying.reject
      def extend(by: FiniteDuration): F[Unit] = underlying.extend(by)
    }
}
