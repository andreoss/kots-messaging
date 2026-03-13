package kots.mq.amqp

import cats.effect.IO
import cats.syntax.all._
import kots.mq._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class AmqpConfirmSuite extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 120.seconds

  test("a confirmed publish returns the identity the consumer reads back") {
    val destination = AmqpTestSupport.queue("confirms")
    AmqpTestSupport.broker().use { broker =>
      (broker.consumer(destination, AmqpTestSupport.settingsWithoutBackoff), broker.producer(destination))
        .tupled
        .use { case (consumer, producer) =>
          for {
            id <- producer.send(Message.of("body"))
            received <- CapabilityChecks.receiveWithin(consumer, 30.seconds)
            _ <- received.traverse(_.ack).void
          } yield assertEquals(received.map(_.envelope.id), Some(id))
        }
    }
  }

  test("an unroutable mandatory publish fails the effect") {
    val missing = AmqpTestSupport.queue("never-declared")
    AmqpTestSupport.broker(AmqpSettings.local(AmqpTestSupport.uri).withMandatory(true)).use {
      broker =>
        broker
          .producer(missing)
          .use(_.send(Message.of("body")))
          .attempt
          .map(result => assert(result.left.exists(_.isInstanceOf[AmqpPublishFailed]), result.toString))
    }
  }
}
