package kots.messaging.sqs

import cats.effect.IO
import cats.effect.kernel.Resource
import kots.messaging._

import scala.concurrent.duration._

object SqsTestSupport {

  val endpoint: String = sys.env.getOrElse("SQS_ENDPOINT", "http://127.0.0.1:9324")

  val nonce: String = java.lang.Long.toHexString(System.nanoTime())

  def settings: SqsSettings = SqsSettings.local(endpoint).withWaitTime(Duration.Zero)

  def broker: Resource[IO, Broker[IO, String]] =
    SqsBroker.bytes[IO](settings, Entropy.system[IO]).map(Transcode.broker(_, Codec.utf8))

  def queue(name: String): Destination = Destination(s"$name-$nonce")

  val settingsWithoutBackoff: ConsumerSettings =
    ConsumerSettings.default.withBackoff(Backoff.none).withLease(5.seconds)
}
