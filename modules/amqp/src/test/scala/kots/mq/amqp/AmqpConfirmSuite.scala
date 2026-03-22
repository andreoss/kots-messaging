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

final class AmqpPipelineSuite extends CatsEffectSuite {

  override def munitIOTimeout: Duration = 180.seconds

  private val bodies = List.range(0, 20).map(index => Message.of(s"body-$index"))

  test("confirms overlap: twenty publishes cost far less than twenty round trips") {
    val destination = AmqpTestSupport.queue("pipelined")
    AmqpTestSupport.broker().use { broker =>
      (
        broker.consumer(destination, AmqpTestSupport.settingsWithoutBackoff.withPrefetch(64)),
        broker.producer(destination),
      ).tupled.use { case (consumer, producer) =>
        for {
          _ <- producer.send(Message.of("warm"))
          sequential <- IO.monotonic.flatMap(start =>
            bodies.traverse(producer.send) *> IO.monotonic.map(_ - start)
          )
          pipelined <- IO.monotonic.flatMap(start =>
            bodies.parTraverse(producer.send) *> IO.monotonic.map(_ - start)
          )
          received <- CapabilityChecks.batchWithin(consumer, bodies.size * 2 + 1, 60.seconds)
          _ <- consumer.ackAll(received)
        } yield {
          assertEquals(received.size, bodies.size * 2 + 1)
          assert(
            pipelined * 3 < sequential * 2,
            s"pipelined $pipelined against sequential $sequential",
          )
        }
      }
    }
  }

  test("a publish is confirmed before its identity is returned") {
    val destination = AmqpTestSupport.queue("confirmed-order")
    AmqpTestSupport.broker().use { broker =>
      (
        broker.consumer(destination, AmqpTestSupport.settingsWithoutBackoff.withPrefetch(32)),
        broker.producer(destination),
      ).tupled.use { case (consumer, producer) =>
        for {
          ids <- bodies.traverse(producer.send)
          received <- CapabilityChecks.batchWithin(consumer, bodies.size, 60.seconds)
          _ <- consumer.ackAll(received)
        } yield {
          assertEquals(ids.distinct.size, bodies.size)
          assertEquals(received.size, bodies.size)
        }
      }
    }
  }
}
