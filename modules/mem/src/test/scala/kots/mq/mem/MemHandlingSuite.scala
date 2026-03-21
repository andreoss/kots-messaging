package kots.mq.mem

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all._
import kots.mq._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class MemHandlingSuite extends CatsEffectSuite {

  private val destination = Destination("handled")

  private val settings =
    ConsumerSettings.default.withLease(4.seconds).withBackoff(Backoff.none)

  private def run[A](
    f: (Producer[IO, String], Consumer[IO, String], Consumer[IO, String]) => IO[A]
  ): IO[A] =
    TestControl.executeEmbed(
      MemBroker.create[IO, String](Entropy.const[IO](1.0)).flatMap { broker =>
        (
          broker.producer(destination),
          broker.consumer(destination, settings),
          broker.consumer(destination, settings),
        ).tupled.use { case (producer, first, second) => f(producer, first, second) }
      }
    )

  test("a handler that outlives its lease keeps its message") {
    val handling = Handling.default.renewingEvery(1.second)
    run { (producer, first, second) =>
      for {
        _ <- producer.send(Message.of("body"))
        handled <- Semantics.process(first, handling)(_ => IO.sleep(10.seconds).as(Settlement.Done))
        stolen <- second.receive
      } yield {
        assertEquals(handled.map(_.message.payload), Some("body"))
        assertEquals(stolen.map(_.envelope.attempt), None)
      }
    }
  }

  test("a handler that is not renewed loses its message to another consumer") {
    run { (producer, first, second) =>
      for {
        _ <- producer.send(Message.of("body"))
        handled <- Semantics
          .process(first, Handling.default)(_ => IO.sleep(10.seconds).as(Settlement.Done))
          .start
        _ <- IO.sleep(5.seconds)
        stolen <- second.receive
        _ <- handled.cancel
      } yield assertEquals(stolen.map(_.envelope.attempt), Some(2))
    }
  }

  test("a handler past its time limit is canceled and settled by the stated outcome") {
    val handling = Handling.default.withTimeout(2.seconds, Settlement.Retry)
    run { (producer, first, _) =>
      for {
        _ <- producer.send(Message.of("body"))
        _ <- Semantics.process(first, handling)(_ => IO.never[Settlement])
        again <- first.receive
      } yield assertEquals(again.map(_.envelope.attempt), Some(2))
    }
  }

  test("a time limit can park the message instead of retrying it") {
    val parked = Destination("handled-dead")
    val handling = Handling.default.withTimeout(2.seconds, Settlement.DeadLetter)
    TestControl.executeEmbed(
      MemBroker.create[IO, String](Entropy.const[IO](1.0)).flatMap { broker =>
        (
          broker.producer(destination),
          broker.consumer(destination, settings.withDeadLetter(parked)),
          broker.consumer(parked, settings),
        ).tupled.use { case (producer, consumer, dead) =>
          for {
            _ <- producer.send(Message.of("body"))
            _ <- Semantics.process(consumer, handling)(_ => IO.never[Settlement])
            parkedMessage <- dead.receive
            source <- consumer.receive
          } yield {
            assertEquals(parkedMessage.map(_.envelope.message.payload), Some("body"))
            assertEquals(source.map(_.envelope.attempt), None)
          }
        }
      }
    )
  }

  test("a handler within its time limit is left alone") {
    val handling = Handling.default.withTimeout(10.seconds, Settlement.Retry)
    run { (producer, first, _) =>
      for {
        _ <- producer.send(Message.of("body"))
        _ <- Semantics.process(first, handling)(_ => IO.sleep(1.second).as(Settlement.Done))
        again <- first.receive
      } yield assertEquals(again.map(_.envelope.attempt), None)
    }
  }
}
