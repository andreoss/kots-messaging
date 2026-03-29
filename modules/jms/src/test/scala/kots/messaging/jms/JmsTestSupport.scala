package kots.messaging.jms

import cats.effect.IO
import cats.effect.kernel.Resource
import jakarta.jms.ConnectionFactory
import kots.messaging._

import scala.concurrent.duration._

object JmsTestSupport {

  val nonce: String = java.lang.Long.toHexString(System.nanoTime())

  val activeMqUrl: String = sys.env.getOrElse("ACTIVEMQ_URL", "tcp://127.0.0.1:61616")
  val artemisUrl: String = sys.env.getOrElse("ARTEMIS_URL", "tcp://127.0.0.1:61617")

  def activeMqFactory: ConnectionFactory = {
    val factory = new org.apache.activemq.ActiveMQConnectionFactory(activeMqUrl)
    factory.setUserName("admin")
    factory.setPassword("admin")
    factory
  }

  val activeMqSettings: JmsSettings =
    JmsSettings.default
      .withAcknowledgeMode(org.apache.activemq.ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE)
      .withReceiveTimeout(300.millis)

  def artemisFactory: ConnectionFactory = artemisFactoryWindowed(1024 * 1024)

  /** The consumer window is what the provider hands out before choosing again. */
  def artemisFactoryWindowed(windowSize: Int): ConnectionFactory = {
    val factory = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(artemisUrl)
    factory.setUser("artemis")
    factory.setPassword("artemis")
    factory.setConsumerWindowSize(windowSize)
    factory
  }

  val artemisSettings: JmsSettings =
    JmsSettings.default
      .withAcknowledgeMode(
        org.apache.activemq.artemis.api.jms.ActiveMQJMSConstants.INDIVIDUAL_ACKNOWLEDGE
      )
      .withReceiveTimeout(300.millis)
      .withDelaySupported(true)
      .withPrioritySupported(true)

  def broker(factory: => ConnectionFactory, settings: JmsSettings): Resource[IO, Broker[IO, String]] =
    JmsBroker
      .bytes[IO](factory, settings, Entropy.system[IO])
      .map(Transcode.broker(_, Codec.utf8))

  def queue(name: String): Destination = Destination(s"$name-$nonce")

  val settingsWithoutBackoff: ConsumerSettings =
    ConsumerSettings.default.withBackoff(Backoff.none)
}
