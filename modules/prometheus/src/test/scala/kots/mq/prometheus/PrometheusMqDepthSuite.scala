package kots.mq.prometheus

import cats.effect.IO
import cats.syntax.all._
import io.prometheus.metrics.model.registry.PrometheusRegistry
import io.prometheus.metrics.model.snapshots.GaugeSnapshot
import kots.mq._
import kots.mq.mem.MemBroker
import munit.CatsEffectSuite

import scala.jdk.CollectionConverters._

final class PrometheusMqDepthSuite extends CatsEffectSuite {

  private val destination = Destination("depth")

  private def gaugeValue(registry: PrometheusRegistry, name: String): Option[Double] =
    registry
      .scrape()
      .asScala
      .collectFirst {
        case snapshot: GaugeSnapshot if snapshot.getMetadata.getName == name =>
          snapshot.getDataPoints.asScala.map(_.getValue).sum
      }

  test("the gauge reports the depth a broker publishes") {
    val registry = new PrometheusRegistry()
    for {
      sample <- PrometheusMqDepth.register[IO](registry)
      broker <- MemBroker.create[IO, String](Entropy.const[IO](1.0))
      _ <- broker.producer(destination).use { producer =>
        List("one", "two").traverse_(body => producer.send(Message.of(body)))
      }
      _ <- sample(broker.admin, destination)
    } yield assertEquals(gaugeValue(registry, "kots_mq_depth"), Some(2.0d))
  }

  test("a broker without the figure leaves the gauge alone") {
    val registry = new PrometheusRegistry()
    for {
      sample <- PrometheusMqDepth.register[IO](registry)
      _ <- sample(Admin.unknown[IO], destination)
    } yield assert(gaugeValue(registry, "kots_mq_depth").forall(_ == 0.0d))
  }
}
