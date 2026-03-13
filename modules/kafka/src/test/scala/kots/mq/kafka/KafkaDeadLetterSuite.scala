package kots.mq.kafka

import cats.effect.IO
import cats.syntax.all._
import kots.mq._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class KafkaDeadLetterSuite extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 120.seconds

  test("a poisoned record reaches the dead-letter topic once and leaves its own") {
    val destination = KafkaTestSupport.topic("poison")
    val parked = KafkaTestSupport.topic("poison-dead")
    val spent = KafkaTestSupport.settingsWithoutBackoff.withMaxAttempts(1).withDeadLetter(parked)
    KafkaTestSupport.broker("poison").use { broker =>
      (
        broker.producer(destination),
        broker.consumer(destination, spent),
        broker.consumer(parked, KafkaTestSupport.settingsWithoutBackoff),
      ).tupled.use { case (producer, consumer, dead) =>
        for {
          _ <- producer.send(Message.of("poison"))
          first <- CapabilityChecks.receiveWithin(consumer, 30.seconds)
          _ <- first.traverse_(_.reject)
          parkedMessage <- CapabilityChecks.receiveWithin(dead, 30.seconds)
          _ <- parkedMessage.traverse_(_.ack)
          again <- CapabilityChecks.receiveWithin(consumer, 5.seconds)
          twice <- CapabilityChecks.receiveWithin(dead, 5.seconds)
        } yield {
          assertEquals(parkedMessage.map(_.envelope.message.payload), Some("poison"))
          assertEquals(again.map(_.envelope.message.payload), None)
          assertEquals(twice.map(_.envelope.message.payload), None)
        }
      }
    }
  }

  test("a rejected record within its budget is republished with a higher attempt") {
    val destination = KafkaTestSupport.topic("retry")
    KafkaTestSupport.broker("retry").use { broker =>
      (
        broker.producer(destination),
        broker.consumer(destination, KafkaTestSupport.settingsWithoutBackoff),
      ).tupled.use { case (producer, consumer) =>
        for {
          _ <- producer.send(Message.of("body"))
          first <- CapabilityChecks.receiveWithin(consumer, 30.seconds)
          _ <- first.traverse_(_.reject)
          second <- CapabilityChecks.receiveWithin(consumer, 30.seconds)
          _ <- second.traverse_(_.ack)
        } yield {
          assertEquals(first.map(_.envelope.attempt), Some(1))
          assertEquals(second.map(_.envelope.attempt), Some(2))
          assertEquals(second.map(_.envelope.message.payload), Some("body"))
        }
      }
    }
  }
}
