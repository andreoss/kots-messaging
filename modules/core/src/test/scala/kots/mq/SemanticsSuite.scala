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

final class SettlementSuite extends CatsEffectSuite {

  private val destination = Destination("settled")

  private def endpoints(
    f: (Producer[IO, String], Consumer[IO, String]) => IO[Unit]
  ): IO[Unit] =
    StubBroker.create[IO, String](Capabilities.none).flatMap { broker =>
      (broker.producer(destination), broker.consumer(destination, ConsumerSettings.default)).tupled
        .use { case (producer, consumer) => f(producer, consumer) }
    }

  private def handled(outcome: Settlement)(
    f: (Consumer[IO, String], Option[Delivery[IO, String]]) => IO[Unit]
  ): IO[Unit] =
    endpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        _ <- Semantics.process(consumer, _ => Settlement.Retry)(_ => IO.pure(outcome))
        next <- consumer.receive
        _ <- f(consumer, next)
      } yield ()
    }

  test("a handler that reports done acknowledges") {
    handled(Settlement.Done)((_, next) => IO(assertEquals(next.map(_.envelope.attempt), None)))
  }

  test("a handler that reports drop acknowledges although the work failed") {
    handled(Settlement.Drop)((_, next) => IO(assertEquals(next.map(_.envelope.attempt), None)))
  }

  test("a handler that reports retry redelivers with a higher attempt") {
    handled(Settlement.Retry)((_, next) => IO(assertEquals(next.map(_.envelope.attempt), Some(2))))
  }

  test("a handler that reports release redelivers with the attempt it had") {
    handled(Settlement.Release)((_, next) => IO(assertEquals(next.map(_.envelope.attempt), Some(1))))
  }

  test("a handler that reports dead-letter does not redeliver to its own destination") {
    handled(Settlement.DeadLetter)((_, next) =>
      IO(assertEquals(next.map(_.envelope.attempt), None))
    )
  }

  test("a raised error settles by the stated default") {
    endpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        outcome <- Semantics
          .process(consumer, _ => Settlement.Drop)(_ => IO.raiseError[Settlement](new RuntimeException("no")))
          .attempt
        next <- consumer.receive
      } yield {
        assert(outcome.isRight, outcome.toString)
        assertEquals(next.map(_.envelope.attempt), None)
      }
    }
  }

  test("a raised error can be told to retry instead") {
    endpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        _ <- Semantics
          .process(consumer, _ => Settlement.Retry)(_ => IO.raiseError[Settlement](new RuntimeException("no")))
        next <- consumer.receive
      } yield assertEquals(next.map(_.envelope.attempt), Some(2))
    }
  }

  test("a canceled handler releases rather than rejects") {
    endpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        fiber <- Semantics
          .process(consumer, _ => Settlement.Retry)(_ => IO.canceled *> IO.pure(Settlement.Done))
          .start
        outcome <- fiber.join
        next <- consumer.receive
      } yield {
        assert(outcome.isCanceled, outcome.toString)
        assertEquals(next.map(_.envelope.attempt), Some(1))
      }
    }
  }

  test("processing a drained destination handles nothing") {
    endpoints { (_, consumer) =>
      Semantics
        .process(consumer, _ => Settlement.Retry)(_ => IO.pure(Settlement.Done))
        .map(handledEnvelope => assertEquals(handledEnvelope.map(_.message.payload), None))
    }
  }
}
