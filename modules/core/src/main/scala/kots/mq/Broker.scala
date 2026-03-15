package kots.mq

import cats.effect.kernel.Resource

/** Opens producers and consumers over one broker. */
trait Broker[F[_], A] {
  def capabilities: Capabilities
  def admin: Admin[F]
  def producer(destination: Destination): Resource[F, Producer[F, A]]
  def consumer(destination: Destination, settings: ConsumerSettings): Resource[F, Consumer[F, A]]
}
