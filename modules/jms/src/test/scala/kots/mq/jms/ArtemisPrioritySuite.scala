package kots.mq.jms

import cats.effect.IO
import cats.syntax.all._
import kots.mq._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class ArtemisPrioritySuite extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 180.seconds

  test("a delivery delay keeps a message invisible until it elapses") {
    val destination = JmsTestSupport.queue("delayed")
    JmsTestSupport.broker(JmsTestSupport.artemisFactory, JmsTestSupport.artemisSettings).use {
      broker =>
        (
          broker.consumer(destination, JmsTestSupport.settingsWithoutBackoff),
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
