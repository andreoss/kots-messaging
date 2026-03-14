package kots.mq.jms

import cats.effect.IO
import cats.syntax.all._
import kots.mq._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class ArtemisPrioritySuite extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 180.seconds

  private def prioritised(body: String, priority: Int): Message[String] =
    Message(body, Map(JmsBroker.priorityHeader -> priority.toString), None)

  test("a higher priority message published later is received first") {
    val destination = JmsTestSupport.queue("priority")
    JmsTestSupport.broker(JmsTestSupport.artemisFactory, JmsTestSupport.artemisSettings).use {
      broker =>
        (
          broker.consumer(destination, JmsTestSupport.settingsWithoutBackoff),
          broker.producer(destination),
        ).tupled.use { case (consumer, producer) =>
          for {
            _ <- producer.send(prioritised("low", 1))
            _ <- producer.send(prioritised("high", 9))
            received <- CapabilityChecks.batchWithin(consumer, 2, 60.seconds)
            _ <- received.traverse_(_.ack)
          } yield assertEquals(
            received.map(_.envelope.message.payload),
            List("high", "low"),
          )
        }
    }
  }

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
