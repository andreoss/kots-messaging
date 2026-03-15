package kots.mq.prometheus

import cats.effect.kernel.Sync
import cats.syntax.all._
import io.prometheus.metrics.core.metrics.Gauge
import io.prometheus.metrics.model.registry.PrometheusRegistry
import kots.mq.{Admin, Destination}

/** Gauge fed from the admin port; a broker without the figure sets nothing. */
object PrometheusMqDepth {

  def register[F[_]](
    registry: PrometheusRegistry
  )(implicit F: Sync[F]): F[(Admin[F], Destination) => F[Unit]] =
    F.delay {
      val depth = Gauge
        .builder()
        .name("kots_mq_depth")
        .help("messages waiting at a destination")
        .labelNames("destination")
        .register(registry)

      (admin, destination) =>
        admin
          .depth(destination)
          .flatMap(_.traverse_(value => F.delay(depth.labelValues(destination.name).set(value.toDouble))))
    }
}
