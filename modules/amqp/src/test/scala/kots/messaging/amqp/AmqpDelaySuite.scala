package kots.messaging.amqp

import cats.effect.IO
import cats.syntax.all._
import kots.messaging._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class AmqpDelaySuite extends AmqpSuite {

  test("a delayed message waits in the broker's holding queue") {
    val destination = queue("delayed")
    AmqpTestSupport.broker().use { broker =>
      (
        broker.consumer(destination, AmqpTestSupport.settingsWithoutBackoff),
        broker.producer(destination),
      ).tupled.use { case (consumer, producer) =>
        for {
          _ <- producer.sendAfter(Message.of("later"), 2.seconds)
          early <- consumer.receive
          arrived <- CapabilityChecks.receiveWithin(consumer, 30.seconds)
          _ <- arrived.traverse_(_.ack)
        } yield {
          assertEquals(early.map(_.envelope.message.payload), None)
          assertEquals(arrived.map(_.envelope.message.payload), Some("later"))
        }
      }
    }
  }
}
