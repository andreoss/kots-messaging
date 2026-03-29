package kots.messaging.amqp

import cats.effect.IO
import cats.syntax.all._
import com.rabbitmq.client.ConnectionFactory
import kots.messaging._

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

final class AmqpEventsSuite extends AmqpSuite {

  test("a healthy broker reports that it is not blocking publishes") {
    AmqpTestSupport.broker().use(_.events.blocked).map(assertEquals(_, None))
  }

  test("a publish the broker refuses fails rather than waiting") {
    val destination = queue("refusing")
    val declareFull = IO.blocking {
      val factory = new ConnectionFactory()
      factory.setUri(AmqpTestSupport.uri)
      val connection = factory.newConnection()
      val channel = connection.createChannel()
      channel.queueDeclare(
        destination.name,
        true,
        false,
        false,
        Map[String, AnyRef]("x-max-length" -> (1: Integer), "x-overflow" -> "reject-publish").asJava,
      )
      connection.close()
    }
    for {
      _ <- declareFull
      outcome <- AmqpTestSupport.broker().use { broker =>
        broker.producer(destination).use { producer =>
          producer.send(Message.of("first")) *>
            producer.send(Message.of("second")).attempt
        }
      }
    } yield assert(
      outcome.left.exists(_.isInstanceOf[AmqpPublishFailed]),
      s"the broker's refusal was not reported: $outcome",
    )
  }
}
