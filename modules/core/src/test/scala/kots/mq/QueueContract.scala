package kots.mq

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._
import munit.CatsEffectSuite

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

/** Contract every adapter honours; run it from each adapter's suite. */
abstract class QueueContract extends CatsEffectSuite {

  def broker: Resource[IO, Broker[IO, String]]

  private val settings: ConsumerSettings =
    ConsumerSettings.default.withBackoff(Backoff.none)

  private val counter = new AtomicInteger(0)

  private val nonce: String = java.lang.Long.toHexString(System.nanoTime())

  private def fresh: Destination =
    Destination(s"${getClass.getSimpleName}-$nonce-${counter.incrementAndGet()}")

  private def withEndpoints[A](
    f: (Producer[IO, String], Consumer[IO, String]) => IO[A]
  ): IO[A] = withSettings(settings)(f)

  private def withSettings[A](chosen: ConsumerSettings)(
    f: (Producer[IO, String], Consumer[IO, String]) => IO[A]
  ): IO[A] =
    broker.use { b =>
      val destination = fresh
      (b.producer(destination), b.consumer(destination, chosen)).tupled.use {
        case (producer, consumer) => f(producer, consumer)
      }
    }

  private def whenDeclared(capability: Capability)(f: Broker[IO, String] => IO[Unit]): IO[Unit] =
    broker.use(b => f(b).whenA(b.capabilities.has(capability)))

  test("a published message is received with payload, headers and key") {
    val message = Message("body", Map("trace" -> "1"), Some(MessageKey("k")))
    withEndpoints { (producer, consumer) =>
      for {
        _ <- producer.send(message)
        received <- CapabilityChecks.receiveWithin(consumer, 10.seconds)
      } yield assertEquals(received.map(_.envelope.message), Some(message))
    }
  }

  test("a received delivery starts at its first attempt") {
    withEndpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        received <- CapabilityChecks.receiveWithin(consumer, 10.seconds)
      } yield assertEquals(received.map(_.envelope.attempt), Some(1))
    }
  }

  test("an acknowledged delivery is not received again") {
    withEndpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        first <- CapabilityChecks.receiveWithin(consumer, 10.seconds)
        _ <- first.traverse_(_.ack)
        second <- consumer.receive
      } yield assertEquals(second.map(_.envelope.message.payload), None)
    }
  }

  test("a rejected delivery is received again with a higher attempt") {
    withEndpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        first <- CapabilityChecks.receiveWithin(consumer, 10.seconds)
        _ <- first.traverse_(_.reject)
        second <- CapabilityChecks.receiveWithin(consumer, 10.seconds)
      } yield {
        assertEquals(second.map(_.envelope.message.payload), Some("body"))
        assertEquals(second.map(_.envelope.attempt), Some(2))
      }
    }
  }

  test("receiving from a drained destination yields nothing") {
    withEndpoints((_, consumer) =>
      consumer.receive.map(received => assertEquals(received.map(_.envelope.attempt), None))
    )
  }

  test("messages are received in publication order") {
    val bodies = List("one", "two", "three")
    withEndpoints { (producer, consumer) =>
      for {
        _ <- bodies.traverse_(body => producer.send(Message.of(body)))
        received <- bodies.traverse(_ =>
          CapabilityChecks.receiveWithin(consumer, 10.seconds).flatTap(_.traverse_(_.ack))
        )
      } yield assertEquals(received.map(_.map(_.envelope.message.payload)), bodies.map(_.some))
    }
  }

  test("each published message is given its own identity") {
    withEndpoints { (producer, consumer) =>
      for {
        firstId <- producer.send(Message.of("one"))
        secondId <- producer.send(Message.of("two"))
        first <- CapabilityChecks.receiveWithin(consumer, 10.seconds)
        second <- CapabilityChecks.receiveWithin(consumer, 10.seconds)
      } yield {
        assertNotEquals(firstId, secondId)
        assertEquals(first.map(_.envelope.id), Some(firstId))
        assertEquals(second.map(_.envelope.id), Some(secondId))
      }
    }
  }

  test("a batch receive returns no more than the maximum asked for") {
    broker.use { b =>
      val destination = fresh
      (b.producer(destination), b.consumer(destination, settings)).tupled.use {
        case (producer, consumer) =>
          val bodies = List("one", "two", "three")
          for {
            _ <- bodies.traverse_(body => producer.send(Message.of(body)))
            batch <- CapabilityChecks.batchWithin(consumer, 2, 10.seconds)
          } yield assert(batch.size == 2)
      }
    }
  }

  test("a batch send reports an outcome for every message") {
    val bodies = List("one", "two", "three")
    withEndpoints { (producer, consumer) =>
      for {
        outcomes <- producer.sendBatch(bodies.map(Message.of))
        received <- CapabilityChecks.batchWithin(consumer, bodies.size, 30.seconds)
        _ <- received.traverse_(_.ack)
      } yield {
        assertEquals(outcomes.size, bodies.size)
        assert(outcomes.forall(_.isRight), outcomes.toString)
        assertEquals(received.map(_.envelope.message.payload).sorted, bodies.sorted)
      }
    }
  }

  test("a consumer holding its prefetch receives nothing more") {
    withSettings(settings.withPrefetch(2)) { (producer, consumer) =>
      for {
        _ <- List("one", "two", "three").traverse_(body => producer.send(Message.of(body)))
        held <- CapabilityChecks.batchWithin(consumer, 2, 10.seconds)
        blocked <- consumer.receive
        _ <- held.headOption.traverse_(_.ack)
        freed <- CapabilityChecks.receiveWithin(consumer, 10.seconds)
      } yield {
        assertEquals(held.size, 2)
        assertEquals(blocked.map(_.envelope.message.payload), None)
        assert(freed.nonEmpty)
      }
    }
  }

  test("a delay is honoured where it is declared and refused where it is not") {
    broker.use { b =>
      val destination = fresh
      (b.producer(destination), b.consumer(destination, settings)).tupled.use {
        case (producer, consumer) =>
          if (b.capabilities.has(Capability.Delay)) CapabilityChecks.delay(producer, consumer)
          else
            producer
              .sendAfter(Message.of("body"), 1.second)
              .attempt
              .map(result => assert(result.left.exists(_.isInstanceOf[CapabilityUnsupported])))
      }
    }
  }

  test("an extended lease postpones redelivery where it is declared") {
    whenDeclared(Capability.LeaseExtension) { b =>
      val destination = fresh
      (b.producer(destination), b.consumer(destination, settings.withLease(2.seconds))).tupled
        .use { case (producer, consumer) =>
          CapabilityChecks.leaseExtension(producer, consumer)
        }
    }
  }

  test("a message past its attempt budget reaches the dead-letter destination") {
    whenDeclared(Capability.DeadLetter) { b =>
      val source = fresh
      val parked = fresh
      val spent = settings.withMaxAttempts(1).withDeadLetter(parked)
      (b.producer(source), b.consumer(source, spent), b.consumer(parked, settings)).tupled.use {
        case (producer, consumer, dead) => CapabilityChecks.deadLetter(producer, consumer, dead)
      }
    }
  }

  test("the admin port reports a depth or nothing at all") {
    broker.use { b =>
      val destination = fresh
      (b.producer(destination), b.consumer(destination, settings)).tupled.use {
        case (producer, consumer) =>
          for {
            _ <- producer.send(Message.of("body"))
            depth <- b.admin.depth(destination)
            received <- CapabilityChecks.receiveWithin(consumer, 10.seconds)
            _ <- received.traverse_(_.ack)
          } yield depth.foreach(value => assert(value >= 0L, s"a depth of $value"))
      }
    }
  }

  test("a destination is isolated from its neighbour") {
    broker.use { b =>
      val left = fresh
      val right = fresh
      (b.producer(left), b.consumer(right, settings)).tupled.use { case (producer, consumer) =>
        for {
          _ <- producer.send(Message.of("body"))
          received <- consumer.receive
        } yield assertEquals(received.map(_.envelope.message.payload), None)
      }
    }
  }
}
