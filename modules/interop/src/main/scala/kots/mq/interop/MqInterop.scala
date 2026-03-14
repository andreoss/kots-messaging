package kots.mq.interop

import cats.effect.kernel.{MonadCancel, Resource}
import cats.syntax.all._
import cats.~>
import kots.mq._

import scala.concurrent.duration.FiniteDuration

/** Views a broker through another effect via a natural transformation. */
object MqInterop {

  def mapK[F[_], G[_], A](broker: Broker[F, A])(fk: F ~> G)(implicit
    F: MonadCancel[F, Throwable],
    G: MonadCancel[G, Throwable],
  ): Broker[G, A] =
    new Broker[G, A] {

      val capabilities: Capabilities = broker.capabilities

      def producer(destination: Destination): Resource[G, Producer[G, A]] =
        broker.producer(destination).mapK(fk).map(producerK(_)(fk))

      def consumer(
        destination: Destination,
        settings: ConsumerSettings,
      ): Resource[G, Consumer[G, A]] =
        broker.consumer(destination, settings).mapK(fk).map(consumerK(_)(fk))
    }

  def producerK[F[_], G[_], A](producer: Producer[F, A])(fk: F ~> G): Producer[G, A] =
    new Producer[G, A] {
      def send(message: Message[A]): G[MessageId] = fk(producer.send(message))

      def sendAfter(message: Message[A], delay: FiniteDuration): G[MessageId] =
        fk(producer.sendAfter(message, delay))

      def sendBatch(messages: List[Message[A]]): G[List[Either[SendFailure, MessageId]]] =
        fk(producer.sendBatch(messages))
    }

  def consumerK[F[_], G[_], A](consumer: Consumer[F, A])(fk: F ~> G)(implicit
    G: cats.Functor[G]
  ): Consumer[G, A] =
    new Consumer[G, A] {
      def receive: G[Option[Delivery[G, A]]] =
        fk(consumer.receive).map(_.map(deliveryK(_)(fk)))

      def receiveBatch(max: Int): G[List[Delivery[G, A]]] =
        fk(consumer.receiveBatch(max)).map(_.map(deliveryK(_)(fk)))
    }

  def deliveryK[F[_], G[_], A](delivery: Delivery[F, A])(fk: F ~> G): Delivery[G, A] =
    new Delivery[G, A] {
      val envelope: Envelope[A] = delivery.envelope
      val ack: G[Unit] = fk(delivery.ack)
      val reject: G[Unit] = fk(delivery.reject)
      def extend(by: FiniteDuration): G[Unit] = fk(delivery.extend(by))
    }
}
