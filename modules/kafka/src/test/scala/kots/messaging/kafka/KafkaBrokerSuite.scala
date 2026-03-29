package kots.messaging.kafka

import cats.effect.IO
import cats.effect.kernel.Resource
import kots.messaging.{Broker, QueueContract}

import scala.concurrent.duration._

final class KafkaBrokerSuite extends QueueContract {

  override def munitIOTimeout: Duration = 120.seconds

  def broker: Resource[IO, Broker[IO, String]] = KafkaTestSupport.broker("contract")
}
