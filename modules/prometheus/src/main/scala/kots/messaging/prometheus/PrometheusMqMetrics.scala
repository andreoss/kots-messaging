package kots.messaging.prometheus

import cats.effect.kernel.Sync
import io.prometheus.metrics.core.metrics.{Counter, Histogram}
import io.prometheus.metrics.model.registry.PrometheusRegistry
import kots.messaging.{Destination, MqMetrics}

import scala.concurrent.duration.FiniteDuration

/** Binds the metrics port to the Prometheus client, labelled by destination. */
object PrometheusMqMetrics {

  def register[F[_]](
    registry: PrometheusRegistry
  )(implicit F: Sync[F]): F[Destination => MqMetrics[F]] =
    F.delay {
      val publishedTotal = counter(registry, "kots_messaging_published", "messages published")
      val receivedTotal = counter(registry, "kots_messaging_received", "deliveries received")
      val redeliveredTotal =
        counter(registry, "kots_messaging_redelivered", "deliveries past their first attempt")
      val acknowledgedTotal = counter(registry, "kots_messaging_acknowledged", "deliveries acknowledged")
      val rejectedTotal = counter(registry, "kots_messaging_rejected", "deliveries rejected")
      val publishSeconds = histogram(registry, "kots_messaging_publish_latency_seconds", "publish latency")
      val receiveSeconds = histogram(registry, "kots_messaging_receive_latency_seconds", "receive latency")
      val ackSeconds = histogram(registry, "kots_messaging_time_to_ack_seconds", "time to acknowledge")

      destination =>
        new MqMetrics[F] {
          private val name = destination.name

          val published: F[Unit] = F.delay(publishedTotal.labelValues(name).inc())
          val received: F[Unit] = F.delay(receivedTotal.labelValues(name).inc())
          val redelivered: F[Unit] = F.delay(redeliveredTotal.labelValues(name).inc())
          val acknowledged: F[Unit] = F.delay(acknowledgedTotal.labelValues(name).inc())
          val rejected: F[Unit] = F.delay(rejectedTotal.labelValues(name).inc())

          def publishLatency(duration: FiniteDuration): F[Unit] =
            F.delay(publishSeconds.labelValues(name).observe(seconds(duration)))

          def receiveLatency(duration: FiniteDuration): F[Unit] =
            F.delay(receiveSeconds.labelValues(name).observe(seconds(duration)))

          def timeToAck(duration: FiniteDuration): F[Unit] =
            F.delay(ackSeconds.labelValues(name).observe(seconds(duration)))
        }
    }

  private def counter(registry: PrometheusRegistry, name: String, help: String): Counter =
    Counter.builder().name(name).help(help).labelNames("destination").register(registry)

  private def histogram(registry: PrometheusRegistry, name: String, help: String): Histogram =
    Histogram.builder().name(name).help(help).labelNames("destination").register(registry)

  private def seconds(duration: FiniteDuration): Double = duration.toNanos.toDouble / 1e9d
}
