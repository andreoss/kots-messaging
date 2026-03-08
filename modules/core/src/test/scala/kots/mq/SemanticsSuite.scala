package kots.mq

import cats.effect.IO
import cats.syntax.all._
import munit.CatsEffectSuite

final class SemanticsSuite extends CatsEffectSuite {

  private val destination = Destination("semantics")

  private def endpoints(
    f: (Producer[IO, String], Consumer[IO, String]) => IO[Unit]
  ): IO[Unit] =
    StubBroker.create[IO, String](Capabilities.none).flatMap { broker =>
      (broker.producer(destination), broker.consumer(destination, ConsumerSettings.default)).tupled
        .use { case (producer, consumer) => f(producer, consumer) }
    }

  private val failure = new RuntimeException("handler failed")

  test("at-most-once loses the message when the caller's effect fails") {
    endpoints { (producer, consumer) =>
      val once = Semantics.atMostOnce(consumer)
      for {
        _ <- producer.send(Message.of("body"))
        delivery <- once.receive
        _ <- IO.raiseError[Unit](failure).attempt
        _ <- delivery.traverse_(_.reject)
        again <- consumer.receive
      } yield {
        assertEquals(delivery.map(_.envelope.message.payload), Some("body"))
        assertEquals(again.map(_.envelope.message.payload), None)
      }
    }
  }

  test("at-least-once redelivers when the caller's effect fails") {
    endpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        outcome <- Semantics.atLeastOnce(consumer)(_ => IO.raiseError[Unit](failure)).attempt
        again <- consumer.receive
      } yield {
        assertEquals(outcome.left.map(_.getMessage), Left("handler failed"))
        assertEquals(again.map(_.envelope.attempt), Some(2))
      }
    }
  }

  test("at-least-once acknowledges when the caller's effect succeeds") {
    endpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        handled <- Semantics.atLeastOnce(consumer)(_ => IO.unit)
        again <- consumer.receive
      } yield {
        assertEquals(handled.map(_.message.payload), Some("body"))
        assertEquals(again.map(_.envelope.attempt), None)
      }
    }
  }

  test("at-least-once on a drained destination handles nothing") {
    endpoints { (_, consumer) =>
      Semantics
        .atLeastOnce(consumer)(_ => IO.unit)
        .map(handled => assertEquals(handled.map(_.message.payload), None))
    }
  }
}
