package kots.mq.amqp

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._
import kots.mq._

object AmqpTestSupport {

  val uri: String = sys.env.getOrElse("AMQP_URI", "amqp://guest:guest@127.0.0.1:5672")

  val nonce: String = java.lang.Long.toHexString(System.nanoTime())

  def broker(settings: AmqpSettings = AmqpSettings.local(uri)): Resource[IO, Broker[IO, String]] =
    AmqpBroker.bytes[IO](settings, Entropy.system[IO]).map(Transcode.broker(_, Codec.utf8))

  def queue(name: String): Destination = Destination(s"$name-$nonce")

  val settingsWithoutBackoff: ConsumerSettings =
    ConsumerSettings.default.withBackoff(Backoff.none)
}
