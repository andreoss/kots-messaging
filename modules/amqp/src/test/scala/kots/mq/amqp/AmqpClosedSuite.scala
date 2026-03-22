package kots.mq.amqp

import cats.effect.IO
import kots.mq._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class AmqpClosedSuite extends AmqpSuite {

  test("receiving after the channel is closed fails rather than reporting an empty queue") {
    val destination = queue("closed")
    AmqpTestSupport.broker().use { broker =>
      broker
        .consumer(destination, AmqpTestSupport.settingsWithoutBackoff)
        .use(IO.pure)
        .flatMap(_.receive.attempt)
        .map(result => assert(result.isLeft, "a closed consumer reported an empty queue"))
    }
  }

  test("publishing after the channel is closed fails rather than reporting success") {
    val destination = queue("closed-publish")
    AmqpTestSupport.broker().use { broker =>
      broker
        .producer(destination)
        .use(IO.pure)
        .flatMap(_.send(Message.of("body")).attempt)
        .map(result => assert(result.isLeft, "a closed producer reported a publish"))
    }
  }
}
