package kots.messaging.sqs

import cats.effect.IO
import cats.effect.kernel.Resource
import kots.messaging.{Broker, QueueContract}

import scala.concurrent.duration._

final class SqsBrokerSuite extends QueueContract {

  override def munitIOTimeout: Duration = 180.seconds

  def broker: Resource[IO, Broker[IO, String]] = SqsTestSupport.broker
}
