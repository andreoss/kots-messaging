package kots.messaging.mem

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all._
import kots.messaging._
import munit.CatsEffectSuite

import scala.concurrent.duration._

final class MemDelaySuite extends CatsEffectSuite {

  private val destination = Destination("delayed")

  private def run[A](f: (Producer[IO, String], Consumer[IO, String]) => IO[A]): IO[A] =
    TestControl.executeEmbed(
      MemBroker.create[IO, String](Entropy.const[IO](1.0)).flatMap { broker =>
        (
          broker.producer(destination),
          broker.consumer(destination, ConsumerSettings.default.withBackoff(Backoff.none)),
        ).tupled.use { case (producer, consumer) => f(producer, consumer) }
      }
    )

  test("a delayed message is invisible until its delay elapses") {
    run { (producer, consumer) =>
      for {
        _ <- producer.sendAfter(Message.of("body"), 20.seconds)
        early <- consumer.receive
        _ <- IO.sleep(21.seconds)
        late <- consumer.receive
      } yield {
        assertEquals(early.map(_.envelope.message.payload), None)
        assertEquals(late.map(_.envelope.message.payload), Some("body"))
      }
    }
  }

  test("a message published without a delay overtakes a delayed one") {
    run { (producer, consumer) =>
      for {
        _ <- producer.sendAfter(Message.of("later"), 20.seconds)
        _ <- producer.send(Message.of("now"))
        first <- consumer.receive
        _ <- first.traverse_(_.ack)
        _ <- IO.sleep(21.seconds)
        second <- consumer.receive
      } yield {
        assertEquals(first.map(_.envelope.message.payload), Some("now"))
        assertEquals(second.map(_.envelope.message.payload), Some("later"))
      }
    }
  }
}

final class MemAdminSuite extends CatsEffectSuite {

  private val destination = Destination("depth")

  test("the depth counts what is waiting and not what is held") {
    MemBroker.create[IO, String](Entropy.const[IO](1.0)).flatMap { broker =>
      (
        broker.producer(destination),
        broker.consumer(destination, ConsumerSettings.default.withBackoff(Backoff.none)),
      ).tupled.use { case (producer, consumer) =>
        for {
          _ <- List("one", "two", "three").traverse_(body => producer.send(Message.of(body)))
          waiting <- broker.admin.depth(destination)
          held <- consumer.receive
          afterReceive <- broker.admin.depth(destination)
          _ <- held.traverse_(_.ack)
          afterAck <- broker.admin.depth(destination)
        } yield {
          assertEquals(waiting, Some(3L))
          assertEquals(afterReceive, Some(2L))
          assertEquals(afterAck, Some(2L))
        }
      }
    }
  }

  test("a destination nothing was published to is empty, not unknown") {
    MemBroker
      .create[IO, String](Entropy.const[IO](1.0))
      .flatMap(_.admin.depth(Destination("untouched")))
      .map(depth => assertEquals(depth, Some(0L)))
  }
}
