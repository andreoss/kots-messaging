package kots.mq.mem

import cats.effect.IO
import cats.effect.kernel.Resource
import kots.mq.{Broker, QueueContract}

final class MemBrokerSuite extends QueueContract {

  def broker: Resource[IO, Broker[IO, String]] =
    Resource.eval(MemBroker.create[IO, String])
}
