package kots.mq.jms

import cats.effect.IO
import cats.effect.kernel.Resource
import kots.mq.{Broker, QueueContract}

import scala.concurrent.duration._

final class ActiveMqAdapterSuite extends QueueContract {

  override def munitIOTimeout: Duration = 180.seconds

  def broker: Resource[IO, Broker[IO, String]] =
    JmsTestSupport.broker(JmsTestSupport.activeMqFactory, JmsTestSupport.activeMqSettings)
}
