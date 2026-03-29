package kots.messaging.mem

import cats.effect.IO
import cats.effect.kernel.Resource
import kots.messaging.{Broker, Entropy, QueueContract}

final class MemBrokerSuite extends QueueContract {

  def broker: Resource[IO, Broker[IO, String]] =
    Resource.eval(MemBroker.create[IO, String](Entropy.const[IO](1.0)))
}
