package kots.messaging.amqp

import cats.effect.IO
import cats.syntax.all._
import kots.messaging._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class AmqpDeadLetterSuite extends AmqpSuite {

  test("a spent message is routed by the broker's dead-letter exchange") {
    val source = queue("poison")
    val parked = queue("poison-dead")
    val spent = AmqpTestSupport.settingsWithoutBackoff.withMaxAttempts(1).withDeadLetter(parked)
    AmqpTestSupport.broker().use { broker =>
      (
        broker.consumer(source, spent),
        broker.consumer(parked, AmqpTestSupport.settingsWithoutBackoff),
        broker.producer(source),
      ).tupled.use { case (consumer, dead, producer) =>
        for {
          _ <- producer.send(Message.of("poison"))
          first <- CapabilityChecks.receiveWithin(consumer, 30.seconds)
          _ <- first.traverse_(_.reject)
          parkedMessage <- CapabilityChecks.receiveWithin(dead, 30.seconds)
          _ <- parkedMessage.traverse_(_.ack)
          again <- CapabilityChecks.receiveWithin(consumer, 3.seconds)
        } yield {
          assertEquals(parkedMessage.map(_.envelope.message.payload), Some("poison"))
          assertEquals(parkedMessage.map(_.envelope.attempt), Some(1))
          assertEquals(again.map(_.envelope.message.payload), None)
        }
      }
    }
  }

  test("a message within its budget is republished with a higher attempt") {
    val source = queue("retry")
    AmqpTestSupport.broker().use { broker =>
      (
        broker.consumer(source, AmqpTestSupport.settingsWithoutBackoff),
        broker.producer(source),
      ).tupled.use { case (consumer, producer) =>
        for {
          _ <- producer.send(Message.of("body"))
          first <- CapabilityChecks.receiveWithin(consumer, 30.seconds)
          _ <- first.traverse_(_.reject)
          second <- CapabilityChecks.receiveWithin(consumer, 30.seconds)
          _ <- second.traverse_(_.ack)
        } yield {
          assertEquals(first.map(_.envelope.attempt), Some(1))
          assertEquals(second.map(_.envelope.attempt), Some(2))
        }
      }
    }
  }
}
