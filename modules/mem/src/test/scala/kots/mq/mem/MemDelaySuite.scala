package kots.mq.mem

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all._
import kots.mq._
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
