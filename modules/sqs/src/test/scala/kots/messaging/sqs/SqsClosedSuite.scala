package kots.messaging.sqs

import cats.effect.IO
import kots.messaging._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class SqsClosedSuite extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 120.seconds

  test("receiving after the client is closed fails rather than reporting an empty queue") {
    val destination = SqsTestSupport.queue("closed")
    SqsTestSupport.broker
      .use(broker =>
        broker
          .consumer(destination, SqsTestSupport.settingsWithoutBackoff)
          .use(IO.pure)
      )
      .flatMap(_.receive.attempt)
      .map(result => assert(result.isLeft, "a closed client reported an empty queue"))
  }
}
