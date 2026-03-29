package kots.messaging.sqs

import scala.concurrent.duration._

/** Where the service is, how long a receive polls, whether queues are made. */
final case class SqsSettings(
  endpoint: Option[String],
  region: String,
  accessKey: String,
  secretKey: String,
  waitTime: FiniteDuration,
  createIfMissing: Boolean,
) {
  def withWaitTime(timeout: FiniteDuration): SqsSettings = copy(waitTime = timeout)
  def withCreateIfMissing(create: Boolean): SqsSettings = copy(createIfMissing = create)
}

object SqsSettings {

  def local(endpoint: String): SqsSettings =
    SqsSettings(Some(endpoint), "elasticmq", "x", "x", 1.second, createIfMissing = true)
}
