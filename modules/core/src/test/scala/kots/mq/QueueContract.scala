package kots.mq

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._
import munit.CatsEffectSuite

import java.util.concurrent.atomic.AtomicInteger

/** Contract every adapter honours; run it from each adapter's suite. */
abstract class QueueContract extends CatsEffectSuite {

  def broker: Resource[IO, Broker[IO, String]]

  private val counter = new AtomicInteger(0)

  private def fresh: Destination =
    Destination(s"${getClass.getSimpleName}-${counter.incrementAndGet()}")

  private def endpoints(
    destination: Destination
  )(implicit b: Broker[IO, String]): Resource[IO, (Producer[IO, String], Consumer[IO, String])] =
    (b.producer(destination), b.consumer(destination)).tupled

  private def withEndpoints[A](
    f: (Producer[IO, String], Consumer[IO, String]) => IO[A]
  ): IO[A] =
    broker.use { implicit b =>
      endpoints(fresh).use { case (producer, consumer) => f(producer, consumer) }
    }

  test("a published message is received with payload, headers and key") {
    val message = Message("body", Map("trace" -> "1"), Some(MessageKey("k")))
    withEndpoints { (producer, consumer) =>
      for {
        _ <- producer.send(message)
        received <- consumer.receive
      } yield assertEquals(received.map(_.envelope.message), Some(message))
    }
  }

  test("a received delivery starts at its first attempt") {
    withEndpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        received <- consumer.receive
      } yield assertEquals(received.map(_.envelope.attempt), Some(1))
    }
  }

  test("an acknowledged delivery is not received again") {
    withEndpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        first <- consumer.receive
        _ <- first.traverse_(_.ack)
        second <- consumer.receive
      } yield assertEquals(second.map(_.envelope.message.payload), None)
    }
  }

  test("a rejected delivery is received again with a higher attempt") {
    withEndpoints { (producer, consumer) =>
      for {
        _ <- producer.send(Message.of("body"))
        first <- consumer.receive
        _ <- first.traverse_(_.reject)
        second <- consumer.receive
      } yield {
        assertEquals(second.map(_.envelope.message.payload), Some("body"))
        assertEquals(second.map(_.envelope.attempt), Some(2))
      }
    }
  }

  test("receiving from a drained destination yields nothing") {
    withEndpoints((_, consumer) => consumer.receive.map(r => assertEquals(r.map(_.envelope.attempt), None)))
  }

  test("messages are received in publication order") {
    val bodies = List("one", "two", "three")
    withEndpoints { (producer, consumer) =>
      for {
        _ <- bodies.traverse_(body => producer.send(Message.of(body)))
        received <- bodies.traverse(_ => consumer.receive.flatTap(_.traverse_(_.ack)))
      } yield assertEquals(received.map(_.map(_.envelope.message.payload)), bodies.map(_.some))
    }
  }

  test("each published message is given its own identity") {
    withEndpoints { (producer, consumer) =>
      for {
        firstId <- producer.send(Message.of("one"))
        secondId <- producer.send(Message.of("two"))
        first <- consumer.receive
        second <- consumer.receive
      } yield {
        assertNotEquals(firstId, secondId)
        assertEquals(first.map(_.envelope.id), Some(firstId))
        assertEquals(second.map(_.envelope.id), Some(secondId))
      }
    }
  }

  test("a destination is isolated from its neighbour") {
    broker.use { implicit b =>
      val left = fresh
      val right = fresh
      (endpoints(left), endpoints(right)).tupled.use { case ((producer, _), (_, consumer)) =>
        for {
          _ <- producer.send(Message.of("body"))
          received <- consumer.receive
        } yield assertEquals(received.map(_.envelope.message.payload), None)
      }
    }
  }
}
