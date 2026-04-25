package kots.messaging.amqp

import cats.effect.IO
import cats.syntax.all._
import kots.messaging._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class AmqpConfirmSuite extends AmqpSuite {

  test("a confirmed publish returns the identity the consumer reads back") {
    val destination = queue("confirms")
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
    val missing = queue("never-declared")
    AmqpTestSupport.broker(AmqpSettings.local(AmqpTestSupport.uri).withMandatory(true)).use {
      broker =>
        broker
          .producer(missing)
          .use(_.send(Message.of("body")))
          .attempt
          .map(result =>
          assert(
            result.left.exists(_.isInstanceOf[AmqpPublishFailed.Unroutable]),
            result.toString,
          )
        )
    }
  }
}

final class AmqpPipelineSuite extends AmqpSuite {

  private val bodies = List.range(0, 20).map(index => Message.of(s"body-$index"))

  // The timings are reported, never asserted: against a broker on the same
  // host a round trip is too small to gate a build on (R-38).
  test("twenty publishes in flight together each get their own confirmation") {
    val destination = queue("pipelined")
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
          start <- IO.monotonic
          ids <- bodies.parTraverse(producer.send)
          overlapped <- IO.monotonic.map(_ - start)
          received <- CapabilityChecks.batchWithin(consumer, bodies.size * 2 + 1, 60.seconds)
          _ <- consumer.ackAll(received)
          _ <- IO.println(
            s"${bodies.size} publishes: $overlapped overlapped, $sequential one at a time"
          )
        } yield {
          assertEquals(ids.distinct.size, bodies.size)
          assertEquals(received.size, bodies.size * 2 + 1)
          assert(overlapped.toNanos > 0L)
        }
      }
    }
  }

  test("a publish is confirmed before its identity is returned") {
    val destination = queue("confirmed-order")
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

final class AmqpFailoverSuite extends AmqpSuite {

  test("a client survives the first endpoint being down") {
    val destination = queue("failover")
    val settings = AmqpSettings
      .local(AmqpTestSupport.uri)
      .withUris(List("amqp://guest:guest@127.0.0.1:1", AmqpTestSupport.uri))
      .withConnectionName("kots-messaging-failover")
    AmqpTestSupport.broker(settings).use { broker =>
      (
        broker.consumer(destination, AmqpTestSupport.settingsWithoutBackoff),
        broker.producer(destination),
      ).tupled.use { case (consumer, producer) =>
        for {
          _ <- producer.send(Message.of("body"))
          received <- CapabilityChecks.receiveWithin(consumer, 30.seconds)
          _ <- received.traverse_(_.ack)
        } yield assertEquals(received.map(_.envelope.message.payload), Some("body"))
      }
    }
  }

  test("no endpoint that answers is reported as the last failure") {
    val settings = AmqpSettings
      .local(AmqpTestSupport.uri)
      .withUris(List("amqp://guest:guest@127.0.0.1:1", "amqp://guest:guest@127.0.0.1:2"))
    AmqpTestSupport
      .broker(settings)
      .use(_ => IO.unit)
      .attempt
      .map(result => assert(result.isLeft, "a broker with no endpoint connected anyway"))
  }
}
