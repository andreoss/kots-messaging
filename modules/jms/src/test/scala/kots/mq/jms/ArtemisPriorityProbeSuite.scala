package kots.mq.jms

import cats.effect.IO
import kots.mq._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class ArtemisPriorityProbeSuite extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 120.seconds

  test("the priority header reaches the provider's own priority field") {
    val destination = JmsTestSupport.queue("priority-probe")
    for {
      _ <- JmsTestSupport
        .broker(JmsTestSupport.artemisFactory, JmsTestSupport.artemisSettings)
        .use(
          _.producer(destination).use(
            _.send(
              Message.of("body").withProperties(MessageProperties.default.withPriority(9))
            )
          )
        )
      priority <- IO.blocking {
        val factory = JmsTestSupport.artemisFactory
        val connection = factory.createConnection()
        connection.start()
        val session = connection.createSession(false, jakarta.jms.Session.AUTO_ACKNOWLEDGE)
        val consumer = session.createConsumer(session.createQueue(destination.name))
        val message = consumer.receive(10000)
        val read = Option(message).map(_.getJMSPriority)
        connection.close()
        read
      }
    } yield assertEquals(priority, Some(9))
  }
}
