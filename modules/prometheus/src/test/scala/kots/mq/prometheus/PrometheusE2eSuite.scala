package kots.mq.prometheus

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._
import io.prometheus.metrics.exporter.httpserver.HTTPServer
import io.prometheus.metrics.model.registry.PrometheusRegistry
import kots.mq._
import kots.mq.mem.MemBroker
import munit.CatsEffectSuite

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import scala.concurrent.duration._

final class PrometheusE2eSuite extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 180.seconds

  private val exporterPort = 19096

  private val destination = Destination("scraped")

  private val prometheusUrl =
    sys.env.getOrElse("PROMETHEUS_URL", "http://127.0.0.1:9091")

  private def exporter(registry: PrometheusRegistry): Resource[IO, HTTPServer] =
    Resource.make(
      IO.blocking(HTTPServer.builder().port(exporterPort).registry(registry).buildAndStart())
    )(server => IO.blocking(server.close()))

  private def query(expression: String): IO[String] =
    IO.blocking {
      val client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
      val request = HttpRequest
        .newBuilder(URI.create(s"$prometheusUrl/api/v1/query?query=$expression"))
        .timeout(java.time.Duration.ofSeconds(5))
        .GET()
        .build()
      client.send(request, HttpResponse.BodyHandlers.ofString()).body()
    }

  private def queryUntilSeries(expression: String, timeout: FiniteDuration): IO[String] = {
    lazy val poll: IO[String] =
      query(expression).flatMap { body =>
        if (body.contains("\"value\"")) IO.pure(body) else IO.sleep(1.second) *> poll
      }
    poll.timeoutTo(timeout, IO.pure(""))
  }

  test("a live workload's series are visible through the query api") {
    val registry = new PrometheusRegistry()
    val workload = for {
      metricsFor <- PrometheusMqMetrics.register[IO](registry)
      broker <- MemBroker.create[IO, String](Entropy.const[IO](1.0))
      metrics = metricsFor(destination)
      _ <- (
        broker.producer(destination),
        broker.consumer(destination, ConsumerSettings.default.withBackoff(Backoff.none)),
      ).tupled.use { case (producer, consumer) =>
        val metered = Metered.producer(producer, metrics)
        val reading = Metered.consumer(consumer, metrics)
        List.range(0, 5).traverse_ { index =>
          metered.send(Message.of(s"body-$index")) *>
            reading.receive.flatMap(_.traverse_(_.ack))
        }
      }
    } yield ()

    exporter(registry).use { _ =>
      for {
        _ <- workload
        body <- queryUntilSeries("kots_mq_published_total", 60.seconds)
      } yield {
        assert(body.contains("kots_mq_published_total"), body.take(200))
        assert(body.contains(destination.name), body.take(200))
      }
    }
  }
}
