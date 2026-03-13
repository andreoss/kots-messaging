package kots.mq.amqp

import cats.effect.IO
import cats.effect.kernel.Resource
import kots.mq.{Broker, QueueContract}

import scala.concurrent.duration._

final class AmqpBrokerSuite extends QueueContract {

  override def munitIOTimeout: Duration = 120.seconds

  def broker: Resource[IO, Broker[IO, String]] = AmqpTestSupport.broker()
}
