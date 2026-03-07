package kots.mq.mem

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all._
import kots.mq._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class MemDeadLetterSuite extends CatsEffectSuite {

  private val source = Destination("source")
  private val parked = Destination("parked")

  private def drain(
    producer: Producer[IO, String],
    consumer: Consumer[IO, String],
    rejections: Int,
  ): IO[Unit] =
    producer.send(Message.of("body")) *>
      List.fill(rejections)(()).traverse_(_ => consumer.receive.flatMap(_.traverse_(_.reject)))

  private def run[A](settings: ConsumerSettings)(
    f: (Producer[IO, String], Consumer[IO, String], Broker[IO, String]) => IO[A]
  ): IO[A] =
    TestControl.executeEmbed(
      MemBroker.create[IO, String](Entropy.const[IO](1.0)).flatMap { broker =>
        (broker.producer(source), broker.consumer(source, settings)).tupled.use {
          case (producer, consumer) => f(producer, consumer, broker)
        }
      }
    )

  private val settings =
    ConsumerSettings.default
      .withLease(10.seconds)
      .withMaxAttempts(3)
      .withBackoff(Backoff.none)

  test("a message rejected past its budget reaches the dead-letter destination once") {
    run(settings.withDeadLetter(parked)) { (producer, consumer, broker) =>
      broker.consumer(parked, settings).use { dead =>
        for {
          _ <- drain(producer, consumer, 3)
          source <- consumer.receive
          first <- dead.receive
          _ <- first.traverse_(_.ack)
          second <- dead.receive
        } yield {
          assertEquals(source.map(_.envelope.attempt), None)
          assertEquals(first.map(_.envelope.message.payload), Some("body"))
          assertEquals(first.map(_.envelope.attempt), Some(3))
          assertEquals(second.map(_.envelope.attempt), None)
        }
      }
    }
  }

  test("without a dead-letter destination the message is discarded once its budget is spent") {
    run(settings) { (producer, consumer, _) =>
      for {
        _ <- drain(producer, consumer, 3)
        _ <- IO.sleep(1.minute)
        again <- consumer.receive
      } yield assertEquals(again.map(_.envelope.attempt), None)
    }
  }

  test("a delivery within its budget keeps returning") {
    run(settings) { (producer, consumer, _) =>
      for {
        _ <- drain(producer, consumer, 2)
        again <- consumer.receive
      } yield assertEquals(again.map(_.envelope.attempt), Some(3))
    }
  }
}
