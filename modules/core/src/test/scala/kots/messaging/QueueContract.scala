package kots.messaging

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

  private val created = new java.util.concurrent.ConcurrentLinkedQueue[Destination]()

  private def fresh: Destination = {
    val destination =
      Destination(s"${getClass.getSimpleName}-$nonce-${counter.incrementAndGet()}")
    created.add(destination)
    destination
  }

  override def afterAll(): Unit = {
    val removal = broker.use { b =>
      if (!b.capabilities.has(Capability.Topology)) IO.unit
      else {
        val destinations = Iterator
          .continually(Option(created.poll()))
          .takeWhile(_.isDefined)
          .flatten
          .toList
        destinations.traverse_(destination => b.admin.delete(destination).attempt.void)
      }
    }
    removal.attempt.unsafeRunTimed(30.seconds)
    super.afterAll()
  }

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
        _ <- received.traverse_(_.ack)
      } yield {
        assertEquals(received.map(_.envelope.message.payload), Some("body"))
        assertEquals(received.map(_.envelope.message.headers), Some(Map("trace" -> "1")))
        assertEquals(received.map(_.envelope.message.key), Some(Some(MessageKey("k"))))
      }
    }
  }

  test("a message keeps the properties brokers carry themselves") {
    val replyTo = Destination("replies")
    val properties = MessageProperties.default
      .withContentType("application/json")
      .withCorrelationId("correlation-1")
      .withReplyTo(replyTo)
      .withPriority(7)
    withEndpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body").withProperties(properties))
        received <- CapabilityChecks.receiveWithin(consumer, 10.seconds)
        _ <- received.traverse_(_.ack)
      } yield {
        val carried = received.map(_.envelope.message.properties)
        assertEquals(carried.flatMap(_.contentType), Some("application/json"))
        assertEquals(carried.flatMap(_.correlationId), Some("correlation-1"))
        assertEquals(carried.flatMap(_.replyTo), Some(replyTo))
        assertEquals(carried.flatMap(_.priority), Some(7))
      }
    }
  }

  test("a message past its expiry is never delivered where expiry is declared") {
    whenDeclared(Capability.Expiry) { b =>
      val destination = fresh
      for {
        _ <- b.producer(destination).use(
          _.send(
            Message.of("fleeting").withProperties(MessageProperties.default.withExpiry(1.second))
          )
        )
        _ <- IO.sleep(3.seconds)
        _ <- b.consumer(destination, settings).use { consumer =>
          for {
            expired <- CapabilityChecks.receiveWithin(consumer, 2.seconds)
            _ <- expired.traverse_(_.ack)
            _ <- IO(assertEquals(expired.map(_.envelope.message.payload), None))
          } yield ()
        }
        _ <- b.producer(destination).use(_.send(Message.of("lasting")))
        kept <- b.consumer(destination, settings).use(consumer =>
          CapabilityChecks.receiveWithin(consumer, 10.seconds).flatTap(_.traverse_(_.ack))
        )
      } yield assertEquals(kept.map(_.envelope.message.payload), Some("lasting"))
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

  test("a batch acknowledgement settles every delivery in it") {
    val bodies = List("one", "two", "three")
    withEndpoints { (producer, consumer) =>
      for {
        _ <- bodies.traverse_(body => producer.send(Message.of(body)))
        held <- CapabilityChecks.batchWithin(consumer, bodies.size, 30.seconds)
        _ <- consumer.ackAll(held)
        again <- consumer.receive
      } yield {
        assertEquals(held.size, bodies.size)
        assertEquals(again.map(_.envelope.message.payload), None)
      }
    }
  }

  test("a batch lease extension postpones every delivery in it") {
    whenDeclared(Capability.LeaseExtension) { b =>
      val destination = fresh
      (b.producer(destination), b.consumer(destination, settings.withLease(2.seconds))).tupled
        .use { case (producer, consumer) =>
          for {
            _ <- List("one", "two").traverse_(body => producer.send(Message.of(body)))
            held <- CapabilityChecks.batchWithin(consumer, 2, 30.seconds)
            _ <- consumer.extendAll(held, 30.seconds)
            _ <- IO.sleep(3.seconds)
            stolen <- consumer.receive
            _ <- consumer.ackAll(held)
          } yield assertEquals(stolen.map(_.envelope.message.payload), None)
        }
    }
  }

  test("a delivery that came back says so") {
    withEndpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        first <- CapabilityChecks.receiveWithin(consumer, 10.seconds)
        _ <- first.traverse_(_.reject)
        again <- CapabilityChecks.receiveWithin(consumer, 30.seconds)
        _ <- again.traverse_(_.ack)
      } yield {
        assertEquals(first.map(_.envelope.redelivered), Some(false))
        assert(
          again.exists(delivery => delivery.envelope.redelivered || delivery.envelope.attempt > 1),
          s"a redelivery reported neither the broker's flag nor an attempt: $again",
        )
      }
    }
  }

  test("a released delivery is received again") {
    withEndpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        first <- CapabilityChecks.receiveWithin(consumer, 10.seconds)
        _ <- first.traverse_(_.release)
        again <- CapabilityChecks.receiveWithin(consumer, 10.seconds)
        _ <- again.traverse_(_.ack)
      } yield {
        assertEquals(first.map(_.envelope.message.payload), Some("body"))
        assertEquals(again.map(_.envelope.message.payload), Some("body"))
      }
    }
  }

  test("a dead-lettered delivery leaves its destination for the parked one") {
    whenDeclared(Capability.DeadLetter) { b =>
      val source = fresh
      val parked = fresh
      val parking = settings.withDeadLetter(parked)
      (b.producer(source), b.consumer(source, parking), b.consumer(parked, settings)).tupled.use {
        case (producer, consumer, dead) =>
          for {
            _ <- producer.send(Message.of("poison"))
            first <- CapabilityChecks.receiveWithin(consumer, 10.seconds)
            _ <- first.traverse_(_.deadLetter)
            parkedMessage <- CapabilityChecks.receiveWithin(dead, 30.seconds)
            _ <- parkedMessage.traverse_(_.ack)
            stale <- consumer.receive
          } yield {
            assertEquals(parkedMessage.map(_.envelope.message.payload), Some("poison"))
            assertEquals(stale.map(_.envelope.message.payload), None)
          }
      }
    }
  }

  test("a destination can be declared, purged and deleted where topology is declared") {
    whenDeclared(Capability.Topology) { b =>
      val destination = fresh
      for {
        _ <- b.admin.declare(destination)
        _ <- b.producer(destination).use(_.send(Message.of("body")))
        _ <- b.admin.purge(destination)
        _ <- b.admin.delete(destination)
        depth <- b.admin.depth(destination)
      } yield assert(depth.forall(_ == 0L), s"a deleted destination reported $depth")
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
