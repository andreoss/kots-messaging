package kots.mq.kafka

import scala.concurrent.duration._

/** Where the broker is, which group a consumer joins, how long a poll waits. */
final case class KafkaSettings(
  bootstrapServers: String,
  groupPrefix: String,
  pollTimeout: FiniteDuration,
) {
  def withGroupPrefix(prefix: String): KafkaSettings = copy(groupPrefix = prefix)
  def withPollTimeout(timeout: FiniteDuration): KafkaSettings = copy(pollTimeout = timeout)
}

object KafkaSettings {

  def local(bootstrapServers: String, groupPrefix: String): KafkaSettings =
    KafkaSettings(bootstrapServers, groupPrefix, 1.second)
}
