package kots.messaging.mem

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all._
import kots.messaging._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class MemLeaseSuite extends CatsEffectSuite {

  private val settings =
    ConsumerSettings.default
      .withLease(10.seconds)
      .withMaxAttempts(5)
      .withBackoff(Backoff.none)

  private val destination = Destination("leases")

  private def run[A](settings: ConsumerSettings)(
    f: (Producer[IO, String], Consumer[IO, String]) => IO[A]
  ): IO[A] =
    TestControl.executeEmbed(
      MemBroker.create[IO, String](Entropy.const[IO](1.0)).flatMap { broker =>
        (broker.producer(destination), broker.consumer(destination, settings)).tupled.use {
          case (producer, consumer) => f(producer, consumer)
        }
      }
    )

  test("a delivery held past its lease is received again with a higher attempt") {
    run(settings) { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        _ <- consumer.receive
        _ <- IO.sleep(11.seconds)
        again <- consumer.receive
      } yield assertEquals(again.map(_.envelope.attempt), Some(2))
    }
  }

  test("a delivery settled within its lease is not received again") {
    run(settings) { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        first <- consumer.receive
        _ <- first.traverse_(_.ack)
        _ <- IO.sleep(11.seconds)
        again <- consumer.receive
      } yield assertEquals(again.map(_.envelope.attempt), None)
    }
  }

  test("an extended lease postpones redelivery") {
    run(settings) { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        first <- consumer.receive
        _ <- IO.sleep(8.seconds)
        _ <- first.traverse_(_.extend(30.seconds))
        _ <- IO.sleep(11.seconds)
        during <- consumer.receive
        _ <- IO.sleep(30.seconds)
        after <- consumer.receive
      } yield {
        assertEquals(during.map(_.envelope.attempt), None)
        assertEquals(after.map(_.envelope.attempt), Some(2))
      }
    }
  }

  test("a rejected delivery waits for its backoff before it is visible") {
    val backing = settings.withBackoff(Backoff(4.seconds, 2.0, 1.minute, 0.0))
    run(backing) { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        first <- consumer.receive
        _ <- first.traverse_(_.reject)
        immediate <- consumer.receive
        _ <- IO.sleep(9.seconds)
        later <- consumer.receive
      } yield {
        assertEquals(immediate.map(_.envelope.attempt), None)
        assertEquals(later.map(_.envelope.attempt), Some(2))
      }
    }
  }
}
