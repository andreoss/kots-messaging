package kots.mq.stream

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all._
import fs2.Stream
import kots.mq._
import kots.mq.mem.MemBroker
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class MqStreamSuite extends CatsEffectSuite {

  private val destination = Destination("streamed")

  private val settings = ConsumerSettings.default.withBackoff(Backoff.none)

  private def endpoints[A](
    f: (Producer[IO, String], Consumer[IO, String]) => IO[A]
  ): IO[A] =
    MemBroker.create[IO, String](Entropy.const[IO](1.0)).flatMap { broker =>
      (broker.producer(destination), broker.consumer(destination, settings)).tupled.use {
        case (producer, consumer) => f(producer, consumer)
      }
    }

  test("a stream carries the deliveries the producer published") {
    val bodies = List("one", "two", "three")
    endpoints { (producer, consumer) =>
      for {
        _ <- bodies.traverse_(body => producer.send(Message.of(body)))
        received <- MqStream
          .deliveries(consumer, 2, 10.millis)
          .take(bodies.size.toLong)
          .evalTap(_.ack)
          .map(_.envelope.message.payload)
          .compile
          .toList
      } yield assertEquals(received, bodies)
    }
  }

  test("processing acknowledges what the handler completes") {
    endpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        handled <- MqStream
          .process(consumer, 1, 1, 10.millis)(_ => IO.unit)
          .take(1)
          .compile
          .toList
        again <- consumer.receive
      } yield {
        assertEquals(handled.map(_.message.payload), List("body"))
        assertEquals(again.map(_.envelope.attempt), None)
      }
    }
  }

  test("processing rejects what the handler fails") {
    endpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        outcome <- MqStream
          .process(consumer, 1, 1, 10.millis)(_ => IO.raiseError[Unit](new RuntimeException("no")))
          .take(1)
          .compile
          .toList
          .attempt
        again <- consumer.receive
      } yield {
        assert(outcome.isLeft)
        assertEquals(again.map(_.envelope.attempt), Some(2))
      }
    }
  }

  test("the sink publishes every message and reports an outcome for each") {
    val bodies = List.range(0, 25).map(index => Message.of(s"body-$index"))
    endpoints { (producer, consumer) =>
      for {
        outcomes <- Stream
          .emits(bodies)
          .covary[IO]
          .through(MqStream.sink(producer, 10, 50.millis))
          .compile
          .toList
        received <- consumer.receiveBatch(bodies.size)
        _ <- received.traverse_(_.ack)
      } yield {
        assertEquals(outcomes.size, bodies.size)
        assert(outcomes.forall(_.isRight))
        assertEquals(received.size, math.min(bodies.size, settings.prefetch))
      }
    }
  }

  test("a handler that runs concurrently never exceeds its bound") {
    val bodies = List.range(0, 8).map(index => Message.of(s"body-$index"))
    endpoints { (producer, consumer) =>
      for {
        _ <- bodies.traverse_(producer.send)
        peak <- Ref.of[IO, (Int, Int)]((0, 0))
        _ <- MqStream
          .process(consumer, 2, 4, 10.millis) { _ =>
            peak.update { case (running, highest) =>
              (running + 1, math.max(highest, running + 1))
            } *> IO.sleep(20.millis) *> peak.update { case (running, highest) =>
              (running - 1, highest)
            }
          }
          .take(bodies.size.toLong)
          .compile
          .drain
        highest <- peak.get.map(_._2)
      } yield assert(highest <= 2, s"ran $highest handlers at once")
    }
  }
}

final class MqStreamSettleSuite extends CatsEffectSuite {

  private val destination = Destination("settled-stream")

  private val settings = ConsumerSettings.default.withBackoff(Backoff.none)

  private def endpoints[A](
    f: (Producer[IO, String], Consumer[IO, String]) => IO[A]
  ): IO[A] =
    MemBroker.create[IO, String](Entropy.const[IO](1.0)).flatMap { broker =>
      (broker.producer(destination), broker.consumer(destination, settings)).tupled.use {
        case (producer, consumer) => f(producer, consumer)
      }
    }

  test("the stream settles each element the way its handler asked") {
    endpoints { (producer, consumer) =>
      for {
        _ <- List("done", "release").traverse_(body => producer.send(Message.of(body)))
        handled <- MqStream
          .settle(consumer, 1, 1, 10.millis, _ => Settlement.Retry) { envelope =>
            IO.pure(
              if (envelope.message.payload == "done") Settlement.Done else Settlement.Release
            )
          }
          .take(2)
          .compile
          .toList
        remaining <- consumer.receiveBatch(10)
        _ <- remaining.traverse_(_.ack)
      } yield {
        assertEquals(handled.map(_.message.payload), List("done", "release"))
        assertEquals(remaining.map(_.envelope.message.payload), List("release"))
        assertEquals(remaining.map(_.envelope.attempt), List(1))
      }
    }
  }

  test("a handler that fails settles by the stated default") {
    endpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        handled <- MqStream
          .settle(consumer, 1, 1, 10.millis, _ => Settlement.Drop)(_ =>
            IO.raiseError[Settlement](new RuntimeException("no"))
          )
          .take(1)
          .compile
          .toList
        remaining <- consumer.receive
      } yield {
        assertEquals(handled.map(_.message.payload), List("body"))
        assertEquals(remaining.map(_.envelope.attempt), None)
      }
    }
  }
}
