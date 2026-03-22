package kots.mq.jms

import cats.effect.IO
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class JmsClosedSuite extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 120.seconds

  test("receiving after the consumer is closed fails rather than reporting an empty queue") {
    val destination = JmsTestSupport.queue("closed")
    JmsTestSupport
      .broker(JmsTestSupport.artemisFactory, JmsTestSupport.artemisSettings)
      .use(broker =>
        broker
          .consumer(destination, JmsTestSupport.settingsWithoutBackoff)
          .use(IO.pure)
      )
      .flatMap(_.receive.attempt)
      .map(result => assert(result.isLeft, "a closed consumer reported an empty queue"))
  }
}
