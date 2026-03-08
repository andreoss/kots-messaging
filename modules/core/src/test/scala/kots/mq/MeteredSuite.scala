package kots.mq

import cats.effect.IO
import cats.syntax.all._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class MeteredSuite extends CatsEffectSuite {

  private val destination = Destination("metered")

  private def probed(
    f: (Producer[IO, String], Consumer[IO, String], MetricsProbe) => IO[Unit]
  ): IO[Unit] =
    for {
      probe <- MetricsProbe.create
      broker <- StubBroker.create[IO, String](Capabilities.none)
      _ <- (
        broker.producer(destination),
        broker.consumer(destination, ConsumerSettings.default.withBackoff(Backoff.none)),
      ).tupled.use { case (producer, consumer) =>
        f(Metered.producer(producer, probe), Metered.consumer(consumer, probe), probe)
      }
    } yield ()

  test("a publish is counted and timed") {
    probed { (producer, _, probe) =>
      for {
        _ <- producer.send(Message.of("body"))
        counts <- probe.counts
      } yield {
        assertEquals(counts.published, 1)
        assertEquals(counts.publishLatencies.size, 1)
      }
    }
  }

  test("a receive is counted and timed, and acknowledging is measured") {
    probed { (producer, consumer, probe) =>
      for {
        _ <- producer.send(Message.of("body"))
        delivery <- consumer.receive
        _ <- delivery.traverse_(_.ack)
        counts <- probe.counts
      } yield {
        assertEquals(counts.received, 1)
        assertEquals(counts.redelivered, 0)
        assertEquals(counts.acknowledged, 1)
        assertEquals(counts.receiveLatencies.size, 1)
        assertEquals(counts.timesToAck.size, 1)
      }
    }
  }

  test("a redelivery is counted apart from a first delivery") {
    probed { (producer, consumer, probe) =>
      for {
        _ <- producer.send(Message.of("body"))
        first <- consumer.receive
        _ <- first.traverse_(_.reject)
        second <- consumer.receive
        _ <- second.traverse_(_.ack)
        counts <- probe.counts
      } yield {
        assertEquals(counts.received, 2)
        assertEquals(counts.redelivered, 1)
        assertEquals(counts.rejected, 1)
      }
    }
  }

  test("an empty receive is timed but counts no message") {
    probed { (_, consumer, probe) =>
      for {
        _ <- consumer.receive
        counts <- probe.counts
      } yield {
        assertEquals(counts.received, 0)
        assertEquals(counts.receiveLatencies.size, 1)
      }
    }
  }

  test("the no-op sink discards every signal") {
    val metrics = MqMetrics.noop[IO]
    val signals = List(
      metrics.published,
      metrics.received,
      metrics.redelivered,
      metrics.acknowledged,
      metrics.rejected,
      metrics.publishLatency(1.second),
      metrics.receiveLatency(1.second),
      metrics.timeToAck(1.second),
    )
    signals.sequence_.map(unit => assertEquals(unit, ()))
  }
}
