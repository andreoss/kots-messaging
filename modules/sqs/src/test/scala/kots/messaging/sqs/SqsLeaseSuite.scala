package kots.messaging.sqs

import cats.effect.IO
import cats.syntax.all._
import kots.messaging._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class SqsLeaseSuite extends CatsEffectSuite {

  private def redriven(
    source: Consumer[IO, String],
    parked: Consumer[IO, String],
    timeout: FiniteDuration,
  ): IO[Option[Delivery[IO, String]]] = {
    lazy val poll: IO[Option[Delivery[IO, String]]] =
      parked.receive.flatMap {
        case Some(delivery) => IO.pure(Some(delivery))
        case None =>
          source.receive.flatMap(_.traverse_(_.reject)) *> IO.sleep(200.millis) *> poll
      }
    poll.timeoutTo(timeout, IO.pure(None))
  }

  override def munitIOTimeout: Duration = 180.seconds

  test("a visibility timeout that expires redelivers with a higher attempt") {
    val destination = SqsTestSupport.queue("lease")
    val settings = SqsTestSupport.settingsWithoutBackoff.withLease(2.seconds)
    SqsTestSupport.broker.use { broker =>
      (broker.producer(destination), broker.consumer(destination, settings)).tupled.use {
        case (producer, consumer) =>
          for {
            _ <- producer.send(Message.of("body"))
            first <- CapabilityChecks.receiveWithin(consumer, 30.seconds)
            again <- CapabilityChecks.receiveWithin(consumer, 30.seconds)
            _ <- again.traverse_(_.ack)
          } yield {
            assertEquals(first.map(_.envelope.attempt), Some(1))
            assertEquals(again.map(_.envelope.attempt), Some(2))
          }
      }
    }
  }

  test("a message past the queue's receive count is redriven to the dead-letter queue") {
    val destination = SqsTestSupport.queue("redrive")
    val parked = SqsTestSupport.queue("redrive-dead")
    val settings =
      SqsTestSupport.settingsWithoutBackoff.withMaxAttempts(1).withDeadLetter(parked)
    SqsTestSupport.broker.use { broker =>
      (
        broker.consumer(destination, settings),
        broker.consumer(parked, SqsTestSupport.settingsWithoutBackoff),
        broker.producer(destination),
      ).tupled.use { case (consumer, dead, producer) =>
        for {
          _ <- producer.send(Message.of("poison"))
          first <- CapabilityChecks.receiveWithin(consumer, 30.seconds)
          _ <- first.traverse_(_.reject)
          parkedMessage <- redriven(consumer, dead, 60.seconds)
          _ <- parkedMessage.traverse_(_.ack)
        } yield assertEquals(parkedMessage.map(_.envelope.message.payload), Some("poison"))
      }
    }
  }
}
