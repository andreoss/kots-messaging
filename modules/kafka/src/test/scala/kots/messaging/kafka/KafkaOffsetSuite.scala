package kots.messaging.kafka

import cats.effect.IO
import cats.syntax.all._
import kots.messaging._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class KafkaOffsetSuite extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 120.seconds

  private val settings = KafkaTestSupport.settingsWithoutBackoff

  test("a restarted consumer replays exactly the uncommitted deliveries") {
    val destination = KafkaTestSupport.topic("offsets")
    KafkaTestSupport.broker("offsets").use { broker =>
      for {
        _ <- broker.producer(destination).use { producer =>
          List("one", "two", "three").traverse_(body => producer.send(Message.of(body)))
        }
        _ <- broker.consumer(destination, settings).use { consumer =>
          for {
            first <- CapabilityChecks.receiveWithin(consumer, 30.seconds)
            _ <- first.traverse_(_.ack)
            second <- CapabilityChecks.receiveWithin(consumer, 30.seconds)
            _ <- IO(assertEquals(first.map(_.envelope.message.payload), Some("one")))
            _ <- IO(assertEquals(second.map(_.envelope.message.payload), Some("two")))
          } yield ()
        }
        replayed <- broker.consumer(destination, settings).use { consumer =>
          CapabilityChecks.receiveWithin(consumer, 30.seconds)
        }
      } yield assertEquals(replayed.map(_.envelope.message.payload), Some("two"))
    }
  }

  test("an acknowledged delivery commits through its partition's position") {
    val destination = KafkaTestSupport.topic("positions")
    KafkaTestSupport.broker("positions").use { broker =>
      for {
        _ <- broker.producer(destination).use { producer =>
          List("one", "two").traverse_(body => producer.send(Message.of(body)))
        }
        _ <- broker.consumer(destination, settings).use { consumer =>
          for {
            held <- CapabilityChecks.batchWithin(consumer, 2, 30.seconds)
            _ <- held.lastOption.traverse_(_.ack)
            _ <- held.headOption.traverse_(_.ack)
          } yield ()
        }
        after <- broker.consumer(destination, settings).use { consumer =>
          CapabilityChecks.receiveWithin(consumer, 10.seconds)
        }
      } yield assertEquals(after.map(_.envelope.message.payload), None)
    }
  }
}
