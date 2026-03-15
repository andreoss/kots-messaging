package kots.mq.kafka

import cats.effect.IO
import kots.mq._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class KafkaClosedSuite extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 120.seconds

  test("receiving after the consumer is closed fails rather than reporting an empty topic") {
    val destination = KafkaTestSupport.topic("closed")
    KafkaTestSupport.broker("closed").use { broker =>
      broker
        .consumer(destination, KafkaTestSupport.settingsWithoutBackoff)
        .use(IO.pure)
        .flatMap(_.receive.attempt)
        .map(result => assert(result.isLeft, "a closed consumer reported an empty topic"))
    }
  }
}
