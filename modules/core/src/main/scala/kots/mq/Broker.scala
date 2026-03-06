package kots.mq

import cats.effect.kernel.Resource

/** Opens producers and consumers over one broker. */
trait Broker[F[_], A] {
  def producer(destination: Destination): Resource[F, Producer[F, A]]
  def consumer(destination: Destination): Resource[F, Consumer[F, A]]
}
