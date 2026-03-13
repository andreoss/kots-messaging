package kots.mq.kafka

import cats.effect.IO
import cats.syntax.all._
import kots.mq._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class KafkaOrderSuite extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 120.seconds

  test("messages sharing a key keep their publication order") {
    val destination = KafkaTestSupport.topic("ordering")
    val key = MessageKey("customer-1")
    val bodies = List("one", "two", "three", "four", "five")
    KafkaTestSupport.broker("ordering").use { broker =>
      (
        broker.producer(destination),
        broker.consumer(destination, KafkaTestSupport.settingsWithoutBackoff),
      ).tupled.use { case (producer, consumer) =>
        for {
          _ <- bodies.traverse_(body => producer.send(Message(body, Map.empty, Some(key))))
          received <- CapabilityChecks.batchWithin(consumer, bodies.size, 60.seconds)
          _ <- received.traverse_(_.ack)
        } yield {
          assertEquals(received.map(_.envelope.message.payload), bodies)
          assertEquals(received.flatMap(_.envelope.message.key.toList).distinct, List(key))
        }
      }
    }
  }
}
