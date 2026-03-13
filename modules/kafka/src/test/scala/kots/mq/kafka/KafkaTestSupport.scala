package kots.mq.kafka

import cats.effect.IO
import cats.effect.kernel.Resource
import kots.mq._

import scala.concurrent.duration._

object KafkaTestSupport {

  val bootstrapServers: String =
    sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

  val nonce: String = java.lang.Long.toHexString(System.nanoTime())

  def settings(prefix: String): KafkaSettings =
    KafkaSettings.local(bootstrapServers, s"$prefix-$nonce").withPollTimeout(500.millis)

  def broker(prefix: String): Resource[IO, Broker[IO, String]] =
    KafkaBroker
      .bytes[IO](settings(prefix), Entropy.system[IO])
      .map(Transcode.broker(_, Codec.utf8))

  def topic(name: String): Destination = Destination(s"$name-$nonce")

  val settingsWithoutBackoff: ConsumerSettings =
    ConsumerSettings.default.withBackoff(Backoff.none)
}
