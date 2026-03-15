package kots.mq.prometheus

import cats.effect.IO
import cats.syntax.all._
import io.prometheus.metrics.model.registry.PrometheusRegistry
import io.prometheus.metrics.model.snapshots.{CounterSnapshot, HistogramSnapshot}
import kots.mq._
import kots.mq.mem.MemBroker
import munit.CatsEffectSuite

import scala.jdk.CollectionConverters._

final class PrometheusMqMetricsSuite extends CatsEffectSuite {

  private val destination = Destination("metered")

  private def counterValue(registry: PrometheusRegistry, name: String): Double =
    registry
      .scrape()
      .asScala
      .collectFirst {
        case snapshot: CounterSnapshot if snapshot.getMetadata.getName == name =>
          snapshot.getDataPoints.asScala.map(_.getValue).sum
      }
      .getOrElse(0.0d)

  private def histogramCount(registry: PrometheusRegistry, name: String): Long =
    registry
      .scrape()
      .asScala
      .collectFirst {
        case snapshot: HistogramSnapshot if snapshot.getMetadata.getName == name =>
          snapshot.getDataPoints.asScala.map(_.getCount).sum
      }
      .getOrElse(0L)

  test("every signal of the port reaches the registry") {
    val registry = new PrometheusRegistry()
    for {
      metricsFor <- PrometheusMqMetrics.register[IO](registry)
      broker <- MemBroker.create[IO, String](Entropy.const[IO](1.0))
      metrics = metricsFor(destination)
      _ <- (
        broker.producer(destination),
        broker.consumer(destination, ConsumerSettings.default.withBackoff(Backoff.none)),
      ).tupled.use { case (producer, consumer) =>
        val metered = Metered.producer(producer, metrics)
        val reading = Metered.consumer(consumer, metrics)
        for {
          _ <- metered.send(Message.of("body"))
          first <- reading.receive
          _ <- first.traverse_(_.reject)
          second <- reading.receive
          _ <- second.traverse_(_.ack)
        } yield ()
      }
    } yield {
      assertEquals(counterValue(registry, "kots_mq_published"), 1.0d)
      assertEquals(counterValue(registry, "kots_mq_received"), 2.0d)
      assertEquals(counterValue(registry, "kots_mq_redelivered"), 1.0d)
      assertEquals(counterValue(registry, "kots_mq_rejected"), 1.0d)
      assertEquals(counterValue(registry, "kots_mq_acknowledged"), 1.0d)
      assertEquals(histogramCount(registry, "kots_mq_publish_latency_seconds"), 1L)
      assert(histogramCount(registry, "kots_mq_receive_latency_seconds") >= 2L)
      assertEquals(histogramCount(registry, "kots_mq_time_to_ack_seconds"), 1L)
    }
  }

  test("a destination labels its own series") {
    val registry = new PrometheusRegistry()
    for {
      metricsFor <- PrometheusMqMetrics.register[IO](registry)
      _ <- metricsFor(Destination("left")).published
      _ <- metricsFor(Destination("right")).published
      _ <- metricsFor(Destination("right")).published
    } yield {
      val labels = registry
        .scrape()
        .asScala
        .collectFirst {
          case snapshot: CounterSnapshot if snapshot.getMetadata.getName == "kots_mq_published" =>
            snapshot.getDataPoints.asScala
              .map(point => point.getLabels.get("destination") -> point.getValue)
              .toMap
        }
        .getOrElse(Map.empty[String, Double])
      assertEquals(labels.get("left"), Some(1.0d))
      assertEquals(labels.get("right"), Some(2.0d))
    }
  }
}
